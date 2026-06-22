import {
    CLIENT_LOG_LEVEL,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectionState,
    type SfuStats,
    type StreamType
} from "../public_api.js";
import { COMMAND_KIND, type HostCommand, type ProtocolCoreBindings } from "../runtime_contract.js";
import type {
    ClientPeerConnection,
    ClientWebSocket,
    SfuClientDependencies,
    TimerHandle
} from "./browser_types.js";
import type { LocalUploads } from "./local_uploads.js";
import type { PendingRequests } from "./pending_requests.js";
import { PeerSession } from "./peer_session.js";
import type { RemoteTracks } from "./remote_tracks.js";
import { SocketSession } from "./socket_session.js";

type BrowserRuntimeContext = {
    localUploads: LocalUploads;
    onLog: (detail: ClientLogDetail) => void;
    onRuntimeError: (error: unknown) => void;
    onStateChange: (state: ConnectionState, cause?: string) => void;
    onUpdate: (update: ClientUpdateDetail) => void;
    pendingRequests: PendingRequests;
    protocolCore: ProtocolCoreBindings;
    remoteTracks: RemoteTracks;
    syncPublicState: () => void;
};

const CLIENT_RECOVERABLE_CLOSE_CODE = 4000;
const BROWSER_RUNTIME_LOG_SOURCE = "browser_runtime";

export class BrowserRuntime {
    private readonly _clearTimer: (handle: TimerHandle) => void;
    private readonly _peerSession: PeerSession;
    private readonly _setTimer: (callback: () => void, ms: number) => TimerHandle;
    private readonly _socketSession: SocketSession;
    private readonly _context: BrowserRuntimeContext;

    private _commandQueue: Promise<void> = Promise.resolve();
    private _epoch = 0;
    private _timerHandles = new Map<number, TimerHandle>();

    constructor(context: BrowserRuntimeContext, dependencies: SfuClientDependencies = {}) {
        this._context = context;
        this._setTimer = dependencies.setTimer ?? ((callback, ms) => setTimeout(callback, ms));
        this._clearTimer = dependencies.clearTimer ?? ((handle) => clearTimeout(handle));
        const log = (level: ClientLogDetail["level"], message: string) => {
            emitRuntimeLog(this._context, level, message);
        };
        this._socketSession = new SocketSession(
            dependencies.createWebSocket ?? ((url) => new WebSocket(url) as ClientWebSocket),
            log,
            () => this.enqueueProtocolCommands(() => this._context.protocolCore.onWsOpen()),
            (frame) =>
                this.enqueueProtocolCommands(() => this._context.protocolCore.onWsMessage(frame)),
            (code) => this.enqueueProtocolCommands(() => this._context.protocolCore.onWsClose(code))
        );
        this._peerSession = new PeerSession(
            dependencies.createPeerConnection ??
                ((config) => new RTCPeerConnection(config) as ClientPeerConnection),
            this._context.protocolCore,
            this._context.localUploads,
            this._context.remoteTracks,
            this._context.onUpdate,
            () => this.enqueueProtocolCommands(() => this._context.protocolCore.onTransportReady()),
            () => {
                log(
                    CLIENT_LOG_LEVEL.WARN,
                    "closing websocket because the peer connection transport failed"
                );
                this._socketSession.close(CLIENT_RECOVERABLE_CLOSE_CODE);
            },
            log
        );
    }

    setIceServers(iceServers?: RTCIceServer[]): void {
        this._peerSession.setIceServers(iceServers);
    }

    enqueueProtocolCommands(getCommands: () => HostCommand[]): void {
        try {
            this.enqueue(getCommands());
        } catch (error) {
            this._context.onRuntimeError(error);
        }
    }

    enqueue(commands: HostCommand[]): void {
        const epoch = this._epoch;
        this._commandQueue = this._commandQueue
            .then(() => (this.isCurrent(epoch) ? this.processCommands(commands, epoch) : undefined))
            .catch((error: unknown) => {
                this._context.onRuntimeError(error);
            });
    }

    enqueueLocalOperation(operation: () => void | Promise<void>): void {
        const epoch = this._epoch;
        this._commandQueue = this._commandQueue
            .then(() => (this.isCurrent(epoch) ? operation() : undefined))
            .catch((error: unknown) => {
                this._context.onRuntimeError(error);
            });
    }

    async getStats(): Promise<SfuStats> {
        return this._peerSession.getStats();
    }

    abort(): void {
        this._epoch += 1;
        this._timerHandles.forEach((handle) => this._clearTimer(handle));
        this._timerHandles.clear();
        this._peerSession.close();
        this._context.remoteTracks.resetAll();
        this._socketSession.abort(CLIENT_RECOVERABLE_CLOSE_CODE);
    }

    replaceLocalTrack(mid: string, streamType: StreamType): void {
        this.enqueue([{ kind: COMMAND_KIND.ATTACH_TRACK, mid, streamType }]);
    }

    detachLocalTrack(streamType: StreamType): void {
        this.enqueue([{ kind: COMMAND_KIND.DETACH_TRACK, streamType }]);
    }

    private async processCommands(commands: HostCommand[], epoch: number): Promise<void> {
        const pending = [...commands];
        if (pending.length === 0) {
            this._context.syncPublicState();
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
            this._context.syncPublicState();
            pending.push(...followUp);
        }
    }

    private async executeCommand(command: HostCommand): Promise<HostCommand[]> {
        switch (command.kind) {
            case COMMAND_KIND.SEND_WEB_SOCKET:
                this._socketSession.send(command.frame);
                return [];
            case COMMAND_KIND.SET_LOCAL_UPLOAD_INTENT:
                this._context.localUploads.setUploadIntent(command.streamType, command.active);
                return [];
            case COMMAND_KIND.APPLY_NEGOTIATION:
                return this._peerSession.negotiate(
                    command.requestId,
                    command.negotiationKind,
                    command.sdp,
                    command.uploadSlots
                );
            case COMMAND_KIND.ATTACH_TRACK:
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.INFO,
                    `attaching ${command.streamType} track to mid ${command.mid}`
                );
                await this._peerSession.attachTrack(command.mid, command.streamType);
                return [];
            case COMMAND_KIND.DETACH_TRACK:
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.INFO,
                    `detaching ${command.streamType} track from the peer connection`
                );
                await this._peerSession.detachTrack(command.streamType);
                return [];
            case COMMAND_KIND.CREATE_PEER_CONNECTION:
                this._peerSession.create();
                return [];
            case COMMAND_KIND.CLOSE_PEER_CONNECTION:
                this._peerSession.close();
                return [];
            case COMMAND_KIND.CLOSE_WEB_SOCKET:
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.INFO,
                    `closing websocket with code ${command.code}`
                );
                this._socketSession.close(command.code);
                return [];
            case COMMAND_KIND.EMIT_STATE_CHANGE:
                this._context.onStateChange(command.state, command.cause);
                return [];
            case COMMAND_KIND.REPLACE_TRACK_BINDINGS:
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received ${command.bindings.length} remote track bindings`
                );
                this._context.remoteTracks.replaceTrackBindings(
                    command.bindings,
                    this._context.onUpdate
                );
                return [];
            case COMMAND_KIND.REMOVE_SESSION_TRACKS:
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.INFO,
                    `removing remote tracks for session ${command.sessionId}`
                );
                this._context.remoteTracks.removeSessionTracks(command.sessionId);
                return [];
            case COMMAND_KIND.EMIT_UPDATE:
                this._context.onUpdate(command.update);
                return [];
            case COMMAND_KIND.BEGIN_PENDING_REQUEST:
                if (this._context.pendingRequests.has(command.requestId)) {
                    this.scheduleTimer(command.timeoutTimerId, command.timeoutMs);
                }
                return [];
            case COMMAND_KIND.RESOLVE_PENDING_REQUEST:
                this._context.pendingRequests.resolve(command.requestId, command.ok);
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
                this.enqueueProtocolCommands(() => this._context.protocolCore.onTimer(id));
            }, ms)
        );
    }

    private isCurrent(epoch: number): boolean {
        return epoch === this._epoch;
    }
}

function emitRuntimeLog(
    context: Pick<BrowserRuntimeContext, "onLog">,
    level: ClientLogDetail["level"],
    message: string
): void {
    context.onLog({
        id: BROWSER_RUNTIME_LOG_SOURCE,
        level,
        message
    });
}
