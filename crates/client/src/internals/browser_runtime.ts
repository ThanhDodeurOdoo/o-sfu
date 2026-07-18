import {
    CLIENT_LOG_LEVEL,
    CLIENT_UPDATE,
    SFU_CLIENT_STATE,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectionState,
    type DownloadStates,
    type RecordingOptions,
    type SessionId,
    type SessionInfo,
    type SourceDescriptor,
    type SfuStats,
    type StreamType
} from "../public_api.js";
import {
    createProtocolCore,
    wrapProtocolCoreBindings,
    type HostCommand,
    type ProtocolCoreBindings
} from "../runtime_contract.js";
import { REMOTE_MEDIA_UPDATE } from "../protocol_host_commands.js";
import { COMMAND_KIND } from "../protocol_contract.js";
import type {
    ClientPeerConnection,
    ClientWebSocket,
    ConsumersCompat,
    SfuClientDependencies,
    TimerHandle
} from "./browser_types.js";
import { PendingRequests } from "./pending_requests.js";
import { PeerSession } from "./peer_session.js";
import { RemoteMedia } from "./remote_media.js";
import { SocketSession } from "./socket_session.js";
import { TurnQueue, type TurnGuard } from "./turn_queue.js";

type BrowserRuntimeContext = {
    onLog: (detail: ClientLogDetail) => void;
    onPublicState: (state: PublicState) => void;
    onRuntimeError: (error: Error) => void;
    onStateChange: (state: ConnectionState, cause?: string) => void;
    onUpdate: (update: ClientUpdateDetail) => void;
};

type PublicState = Pick<ProtocolCoreBindings, "state" | "features" | "recordingState"> & {
    sourceDescriptors: readonly SourceDescriptor[];
};

const TURN_POLICY = {
    DROP_ON_RECOVERY: "dropOnRecovery",
    RETAIN_ON_RECOVERY: "retainOnRecovery"
} as const;

type TurnPolicy = (typeof TURN_POLICY)[keyof typeof TURN_POLICY];

const CLIENT_RECOVERABLE_CLOSE_CODE = 4000;
const BROWSER_RUNTIME_LOG_SOURCE = "browser_runtime";

export class BrowserRuntime {
    private readonly _clearTimer: (handle: TimerHandle) => void;
    private readonly _core: ProtocolCoreBindings;
    private readonly _turnQueue = new TurnQueue<TurnPolicy>((error) =>
        this.handleRuntimeError(error)
    );
    private readonly _pendingRequests = new PendingRequests(
        (getCommands) =>
            this._turnQueue.enqueueAndWait(
                (isCurrent) => this.processCommands(getCommands(), isCurrent),
                TURN_POLICY.DROP_ON_RECOVERY
            ),
        (id, ms) => this.scheduleTimer(id, ms)
    );
    private readonly _media = new RemoteMedia();
    public readonly consumers: ReadonlyMap<SessionId, ConsumersCompat> = this._media.consumers;

    private readonly _peerSession: PeerSession;
    private readonly _setTimer: (callback: () => void, ms: number) => TimerHandle;
    private readonly _socketSession: SocketSession;
    private readonly _context: BrowserRuntimeContext;

    private _timerHandles = new Map<number, TimerHandle>();

    constructor(context: BrowserRuntimeContext, dependencies: SfuClientDependencies = {}) {
        this._context = context;
        this._core = dependencies.createProtocolCore
            ? wrapProtocolCoreBindings(dependencies.createProtocolCore())
            : createProtocolCore();
        this._setTimer = dependencies.setTimer ?? ((callback, ms) => setTimeout(callback, ms));
        this._clearTimer = dependencies.clearTimer ?? ((handle) => clearTimeout(handle));
        const log = (level: ClientLogDetail["level"], message: string) => this.log(level, message);
        this._socketSession = new SocketSession(
            dependencies.createWebSocket ?? ((url) => new WebSocket(url) as ClientWebSocket),
            log,
            () => this.enqueueProtocolCommands(() => this._core.onWsOpen()),
            (frame) => this.enqueueProtocolCommands(() => this._core.onWsMessage(frame)),
            (code) => this.handleSocketClose(code)
        );
        this._peerSession = new PeerSession(
            dependencies.createPeerConnection ??
                ((config) => new RTCPeerConnection(config) as ClientPeerConnection),
            this._media,
            this._context.onUpdate,
            () => this.enqueueProtocolCommands(() => this.onTransportReady()),
            () => {
                log(
                    CLIENT_LOG_LEVEL.WARN,
                    "closing websocket because the peer connection transport failed"
                );
                this._socketSession.close(CLIENT_RECOVERABLE_CLOSE_CODE);
            },
            log
        );
        this.syncPublicState();
    }

    connect(url: string, jwt: string, room: string | null, iceServers?: RTCIceServer[]): void {
        this.enqueueProtocolCommands(() => {
            const commands = this._core.connect(url, jwt, room);
            if (commands.some((command) => command.kind === COMMAND_KIND.CONNECT)) {
                this._peerSession.clearPublications();
                this._media.resetAll();
                this._peerSession.setIceServers(iceServers);
            }
            return commands;
        });
    }

    disconnect(): void {
        // Let the active cleanup finish before disconnecting.
        if (this._turnQueue.hasControlTurn) {
            this._turnQueue.cancelPending();
            this.enqueueProtocolCommands(() => this._core.disconnect());
            return;
        }
        const commands = this.tryControlTransition(() => this._core.disconnect());
        if (!commands) {
            return;
        }
        this.interrupt(commands, false);
    }

    subscribe(sessionId: SessionId, states: DownloadStates): void {
        const snapshot = { ...states };
        this._turnQueue.enqueue((isCurrent) => {
            const commands = this._core.subscribe(sessionId, snapshot);
            if (!isCurrent()) {
                return;
            }
            this._media.updateSubscriptionStates(sessionId, snapshot, this._context.onUpdate);
            return this.processCommands(commands, isCurrent);
        }, TURN_POLICY.RETAIN_ON_RECOVERY);
    }

    updateInfo(info: SessionInfo): void {
        const snapshot = { ...info };
        this.enqueueProtocolCommands(
            () => this._core.updateInfo(snapshot),
            TURN_POLICY.RETAIN_ON_RECOVERY
        );
    }

    broadcast(message: unknown): void {
        try {
            const snapshot = structuredClone(message);
            this.enqueueProtocolCommands(() => this._core.broadcast(snapshot));
        } catch (error) {
            this.handleRuntimeError(error);
        }
    }

    startRecording(options: RecordingOptions = {}): Promise<boolean> {
        const input = { ...options };
        return this._pendingRequests.drainRequestCommands(() => this._core.startRecording(input));
    }

    stopRecording(): Promise<boolean> {
        return this._pendingRequests.drainRequestCommands(() => this._core.stopRecording());
    }

    async getStats(): Promise<SfuStats> {
        return this._peerSession.getStats();
    }

    publish(type: StreamType, track: MediaStreamTrack | null): void {
        this._turnQueue.enqueue(async (isCurrent) => {
            const { active, peerTask } = this._peerSession.setPublication(
                type,
                track,
                this._core.state
            );
            if (!isCurrent()) {
                return;
            }
            const commands = active === undefined ? undefined : this._core.publish(type, active);
            if (peerTask) {
                await peerTask();
            }
            if (commands) {
                return this.processCommands(commands, isCurrent);
            }
        }, TURN_POLICY.RETAIN_ON_RECOVERY);
    }

    private abortResources(): void {
        this._timerHandles.forEach((handle) => this._clearTimer(handle));
        this._timerHandles.clear();
        this._peerSession.close();
        this._peerSession.clearPublications();
        this._media.resetAll();
        this._socketSession.abort(CLIENT_RECOVERABLE_CLOSE_CODE);
    }

    private enqueueProtocolCommands(
        getCommands: () => HostCommand[],
        policy: TurnPolicy = TURN_POLICY.DROP_ON_RECOVERY
    ): void {
        this._turnQueue.enqueue(
            (isCurrent) => this.processCommands(getCommands(), isCurrent),
            policy
        );
    }

    private handleSocketClose(code: number): void {
        const commands = this.tryControlTransition(() => this._core.onWsClose(code));
        if (!commands?.length) {
            return;
        }
        this.interrupt(commands, this._core.state === SFU_CLIENT_STATE.RECOVERING);
    }

    private tryControlTransition(getCommands: () => HostCommand[]): HostCommand[] | undefined {
        try {
            return getCommands();
        } catch (error) {
            this.handleRuntimeError(error);
            return undefined;
        }
    }

    private interrupt(commands: HostCommand[], recovering: boolean, error?: Error): void {
        if (commands.length === 0 && this._turnQueue.hasControlTurn) {
            this._turnQueue.cancelPending(error);
            return;
        }
        this._turnQueue.interrupt(
            (isCurrent) => this.processCommands(commands, isCurrent),
            (policy) => recovering && policy === TURN_POLICY.RETAIN_ON_RECOVERY,
            error
        );
    }

    private async processCommands(commands: HostCommand[], isCurrent: TurnGuard): Promise<void> {
        if (!isCurrent()) {
            return;
        }
        const pending = [...commands];
        if (pending.length === 0) {
            this.syncPublicState();
            return;
        }
        for (let index = 0; index < pending.length; index += 1) {
            if (!isCurrent()) {
                return;
            }
            const command = pending[index];
            const followUp = await this.executeCommand(command, isCurrent);
            if (!isCurrent()) {
                return;
            }
            this.syncPublicState();
            pending.push(...followUp);
        }
    }

    private async executeCommand(
        command: HostCommand,
        isCurrent: TurnGuard
    ): Promise<HostCommand[]> {
        switch (command.kind) {
            case COMMAND_KIND.SEND_WEB_SOCKET:
                this._socketSession.send(command.frame);
                return [];
            case COMMAND_KIND.APPLY_NEGOTIATION: {
                const result = await this._peerSession.negotiate(
                    command.requestId,
                    command.negotiationKind,
                    command.sdp,
                    command.uploadSlots
                );
                if (!result || !isCurrent()) {
                    return [];
                }
                const commands = this._core.submitNegotiationAnswer(
                    command.requestId,
                    command.negotiationKind,
                    result.answerSdp
                );
                if (result.shouldSignalTransportReady) {
                    commands.push(...this.onTransportReady());
                }
                return commands;
            }
            case COMMAND_KIND.CREATE_PEER_CONNECTION:
                this._peerSession.create();
                return [];
            case COMMAND_KIND.CLOSE_PEER_CONNECTION:
                this._peerSession.close();
                return [];
            case COMMAND_KIND.CLOSE_WEB_SOCKET:
                this._socketSession.close(command.code);
                return [];
            case COMMAND_KIND.EMIT_STATE_CHANGE:
                if (
                    command.state === SFU_CLIENT_STATE.CLOSED ||
                    command.state === SFU_CLIENT_STATE.DISCONNECTED
                ) {
                    this._peerSession.clearPublications();
                    this._media.resetAll();
                }
                this._context.onStateChange(command.state, command.cause);
                return [];
            case COMMAND_KIND.EMIT_UPDATE:
                if (command.update.name === REMOTE_MEDIA_UPDATE) {
                    this._media.replaceTrackBindings(
                        command.update.payload.bindings,
                        this._context.onUpdate
                    );
                    return [];
                }
                if (command.update.name === CLIENT_UPDATE.SOURCE) {
                    this._media.replaceSources(command.update.payload.sources);
                } else if (command.update.name === CLIENT_UPDATE.DISCONNECT) {
                    this._media.removeSession(command.update.payload.sessionId);
                }
                this.syncPublicState();
                this._context.onUpdate(command.update);
                return [];
            case COMMAND_KIND.BEGIN_PENDING_REQUEST:
                throw new Error("beginPendingRequest is only valid for request command drains");
            case COMMAND_KIND.RESOLVE_PENDING_REQUEST:
                this._pendingRequests.resolve(command.requestId, command.ok);
                return [];
            case COMMAND_KIND.SCHEDULE_TIMER:
                this.scheduleTimer(command.id, command.ms);
                return [];
            case COMMAND_KIND.CANCEL_TIMER:
                this.cancelTimer(command.id);
                return [];
            case COMMAND_KIND.CONNECT:
                this._socketSession.open(command.url);
                return [];
        }
    }

    private onTransportReady(): HostCommand[] {
        this._peerSession.resumePublications();
        return this._core.onTransportReady();
    }

    private cancelTimer(id: number): void {
        const handle = this._timerHandles.get(id);
        if (!handle) {
            return;
        }
        this._clearTimer(handle);
        this._timerHandles.delete(id);
    }

    private scheduleTimer(id: number, ms: number): void {
        this.cancelTimer(id);
        this._timerHandles.set(
            id,
            this._setTimer(() => {
                this.enqueueProtocolCommands(() => this._core.onTimer(id));
            }, ms)
        );
    }

    private handleRuntimeError(error: unknown): void {
        const resolvedError = error instanceof Error ? error : new Error(String(error));
        this._pendingRequests.rejectAll(resolvedError);
        let disconnectCommands: HostCommand[] | undefined;
        try {
            disconnectCommands = this._core.disconnect();
        } catch (disconnectError) {
            this.log(
                CLIENT_LOG_LEVEL.ERROR,
                `protocol disconnect failed: ${String(disconnectError)}`
            );
        }
        this.interrupt(disconnectCommands ?? [], false, resolvedError);
        this.abortResources();
        this.syncPublicState();
        this._context.onRuntimeError(resolvedError);
    }

    private syncPublicState(): void {
        this._context.onPublicState({
            features: this._core.features,
            recordingState: this._core.recordingState,
            sourceDescriptors: this._media.sources,
            state: this._core.state
        });
    }

    private log(level: ClientLogDetail["level"], message: string): void {
        this._context.onLog({
            id: BROWSER_RUNTIME_LOG_SOURCE,
            level,
            message
        });
    }
}
