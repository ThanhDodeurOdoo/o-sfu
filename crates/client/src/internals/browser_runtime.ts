import {
    CLIENT_LOG_LEVEL,
    CLIENT_UPDATE,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectionState,
    type DownloadStates,
    type RecordingOptions,
    type SessionId,
    type SessionInfo,
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
import { RemoteTracks } from "./remote_tracks.js";
import { SocketSession } from "./socket_session.js";

type BrowserRuntimeContext = {
    onLog: (detail: ClientLogDetail) => void;
    onPublicState: (state: PublicState) => void;
    onRuntimeError: (error: Error) => void;
    onStateChange: (state: ConnectionState, cause?: string) => void;
    onUpdate: (update: ClientUpdateDetail) => void;
};

type PublicState = Pick<ProtocolCoreBindings, "state" | "features" | "recordingState">;

const CLIENT_RECOVERABLE_CLOSE_CODE = 4000;
const BROWSER_RUNTIME_LOG_SOURCE = "browser_runtime";

export class BrowserRuntime {
    private readonly _clearTimer: (handle: TimerHandle) => void;
    private readonly _core: ProtocolCoreBindings;
    private readonly _pendingRequests = new PendingRequests(
        (commands) => this.enqueue(commands),
        (commands, begin) => this.enqueueRequest(commands, begin),
        (id, ms) => this.scheduleTimer(id, ms)
    );
    private readonly _remoteTracks = new RemoteTracks();
    private readonly _peerSession: PeerSession;
    private readonly _setTimer: (callback: () => void, ms: number) => TimerHandle;
    private readonly _socketSession: SocketSession;
    private readonly _context: BrowserRuntimeContext;

    private _lastAbortError: Error | undefined;
    private _commandQueue: Promise<void> = Promise.resolve();
    private _epoch = 0;
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
            (code) => this.enqueueProtocolCommands(() => this._core.onWsClose(code))
        );
        this._peerSession = new PeerSession(
            dependencies.createPeerConnection ??
                ((config) => new RTCPeerConnection(config) as ClientPeerConnection),
            this._remoteTracks,
            this._context.onUpdate,
            () => this.enqueueProtocolCommands(() => this._core.onTransportReady()),
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

    get consumers(): ReadonlyMap<SessionId, ConsumersCompat> {
        return this._remoteTracks.consumers;
    }

    connect(url: string, jwt: string, room: string | null, iceServers?: RTCIceServer[]): void {
        this.enqueueProtocolCommands(() => {
            const commands = this._core.connect(url, jwt, room);
            if (commands.some((command) => command.kind === COMMAND_KIND.CONNECT)) {
                this._peerSession.setIceServers(iceServers);
            }
            return commands;
        });
    }

    disconnect(): void {
        this.enqueueProtocolCommands(() => this._core.disconnect());
    }

    subscribe(sessionId: SessionId, states: DownloadStates): void {
        this.enqueueTask(() => {
            this._remoteTracks.updateSubscriptionStates(sessionId, states, this._context.onUpdate);
        });
        this.enqueueProtocolCommands(() => this._core.subscribe(sessionId, states));
    }

    updateInfo(info: SessionInfo): void {
        this.enqueueProtocolCommands(() => this._core.updateInfo(info));
    }

    broadcast(message: unknown): void {
        this.enqueueProtocolCommands(() => this._core.broadcast(message));
    }

    startRecording(options: RecordingOptions = {}): Promise<boolean> {
        return this.processRequestCommands(() => this._core.startRecording(options));
    }

    stopRecording(): Promise<boolean> {
        return this.processRequestCommands(() => this._core.stopRecording());
    }

    async getStats(): Promise<SfuStats> {
        return this._peerSession.getStats();
    }

    publish(type: StreamType, track: MediaStreamTrack | null): void {
        const { publishActive, peerTask } = this._peerSession.updateLocalTrack(type, track);
        if (peerTask) {
            this.enqueueTask(peerTask);
        }
        if (publishActive !== undefined) {
            this.enqueueProtocolCommands(() => this._core.publish(type, publishActive));
        }
    }

    private abort(): void {
        this._epoch += 1;
        this._commandQueue = Promise.resolve();
        this._timerHandles.forEach((handle) => this._clearTimer(handle));
        this._timerHandles.clear();
        this._peerSession.close();
        this._remoteTracks.resetAll();
        this._socketSession.abort(CLIENT_RECOVERABLE_CLOSE_CODE);
    }

    private processRequestCommands(getCommands: () => HostCommand[]): Promise<boolean> {
        let commands: HostCommand[];
        try {
            commands = getCommands();
        } catch (error) {
            this.handleRuntimeError(error);
            return Promise.reject(error);
        }
        return this._pendingRequests.drainRequestCommands(commands);
    }

    private enqueueProtocolCommands(getCommands: () => HostCommand[]): void {
        try {
            this.enqueue(getCommands());
        } catch (error) {
            this.handleRuntimeError(error);
        }
    }

    private enqueue(commands: HostCommand[]): void {
        this.enqueueTask((epoch) => this.processCommands(commands, epoch));
    }

    private enqueueRequest(commands: HostCommand[], begin: () => void): Promise<void> {
        return this.enqueueTask((epoch) => {
            begin();
            return commands.length === 0 ? undefined : this.processCommands(commands, epoch);
        });
    }

    private enqueueTask(operation: (epoch: number) => void | Promise<void>): Promise<void> {
        const epoch = this._epoch;
        const task = this._commandQueue.then(() => {
            if (!this.isCurrent(epoch)) {
                throw this._lastAbortError ?? new Error("runtime command skipped after abort");
            }
            return operation(epoch);
        });
        this._commandQueue = task.catch((error: unknown) => {
            if (this.isCurrent(epoch)) {
                this.handleRuntimeError(error);
            }
        });
        return task;
    }

    private async processCommands(commands: HostCommand[], epoch: number): Promise<void> {
        const pending = [...commands];
        if (pending.length === 0) {
            this.syncPublicState();
            return;
        }
        for (let index = 0; index < pending.length; index += 1) {
            if (!this.isCurrent(epoch)) {
                return;
            }
            const command = pending[index];
            const followUp = await this.executeCommand(command);
            if (!this.isCurrent(epoch)) {
                return;
            }
            this.syncPublicState();
            pending.push(...followUp);
        }
    }

    private async executeCommand(command: HostCommand): Promise<HostCommand[]> {
        switch (command.kind) {
            case COMMAND_KIND.SEND_WEB_SOCKET:
                this._socketSession.send(command.frame);
                return [];
            case COMMAND_KIND.SET_LOCAL_UPLOAD_INTENT:
                this._peerSession.setLocalUploadIntent(command.streamType, command.active);
                return [];
            case COMMAND_KIND.APPLY_NEGOTIATION: {
                const result = await this._peerSession.negotiate(
                    command.requestId,
                    command.negotiationKind,
                    command.sdp,
                    command.uploadSlots
                );
                if (!result) {
                    return [];
                }
                const commands = this._core.submitNegotiationAnswer(
                    command.requestId,
                    command.negotiationKind,
                    result.answerSdp
                );
                if (result.shouldSignalTransportReady) {
                    commands.push(...this._core.onTransportReady());
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
                this._context.onStateChange(command.state, command.cause);
                return [];
            case COMMAND_KIND.EMIT_UPDATE:
                if (command.update.name === REMOTE_MEDIA_UPDATE) {
                    this._remoteTracks.replaceTrackBindings(
                        command.update.payload.bindings,
                        this._context.onUpdate
                    );
                } else {
                    if (command.update.name === CLIENT_UPDATE.DISCONNECT) {
                        this._remoteTracks.removeSessionTracks(command.update.payload.sessionId);
                    }
                    this._context.onUpdate(command.update);
                }
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
        this._lastAbortError = resolvedError;
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
        this.abort();
        if (disconnectCommands) {
            this.enqueue(disconnectCommands);
        }
        this._context.onRuntimeError(resolvedError);
    }

    private syncPublicState(): void {
        this._context.onPublicState({
            features: this._core.features,
            recordingState: this._core.recordingState,
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

    private isCurrent(epoch: number): boolean {
        return epoch === this._epoch;
    }
}
