import {
    CLIENT_UPDATE,
    CLIENT_LOG_LEVEL,
    STREAM_TYPES,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectionState,
    type SfuStats,
    type StreamType
} from "../public_api.js";
import { WS_CLOSE_CODE } from "../protocol_contract.js";
import { COMMAND_KIND, type HostCommand, type ProtocolCoreBindings } from "../runtime_contract.js";
import type { NegotiationUploadSlot } from "../protocol.js";
import type {
    ClientPeerConnection,
    ClientWebSocket,
    SfuClientDependencies,
    TimerHandle
} from "./browser_types.js";
import type { LocalUploads } from "./local_uploads.js";
import type { PendingRequests } from "./pending_requests.js";
import type { RemoteTracks } from "./remote_tracks.js";
import { localDescriptionHasOnlyInactiveMedia } from "./sdp_media_direction.js";

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
const MAX_SERVER_FRAME_BYTES = 256 * 1024;
const TEXT_ENCODER = new TextEncoder();

export { CLIENT_RECOVERABLE_CLOSE_CODE };

export class BrowserRuntime {
    private readonly _clearTimer: (handle: TimerHandle) => void;
    private readonly _createPeerConnection: (config: RTCConfiguration) => ClientPeerConnection;
    private readonly _createWebSocket: (url: string) => ClientWebSocket;
    private readonly _setTimer: (callback: () => void, ms: number) => TimerHandle;
    private readonly _context: BrowserRuntimeContext;

    private _commandQueue: Promise<void> = Promise.resolve();
    private _iceServers?: RTCIceServer[];
    private _peerConnection: ClientPeerConnection | null = null;
    private _timerHandles = new Map<number, TimerHandle>();
    private _webSocket: ClientWebSocket | null = null;

    constructor(context: BrowserRuntimeContext, dependencies: SfuClientDependencies = {}) {
        this._context = context;
        this._createWebSocket =
            dependencies.createWebSocket ?? ((url) => new WebSocket(url) as ClientWebSocket);
        this._createPeerConnection =
            dependencies.createPeerConnection ??
            ((config) => new RTCPeerConnection(config) as ClientPeerConnection);
        this._setTimer = dependencies.setTimer ?? ((callback, ms) => setTimeout(callback, ms));
        this._clearTimer = dependencies.clearTimer ?? ((handle) => clearTimeout(handle));
    }

    setIceServers(iceServers?: RTCIceServer[]): void {
        this._iceServers = iceServers;
    }

    enqueueProtocolCommands(getCommands: () => HostCommand[]): void {
        try {
            this.enqueue(getCommands());
        } catch (error) {
            this._context.onRuntimeError(error);
        }
    }

    enqueue(commands: HostCommand[]): void {
        this._commandQueue = this._commandQueue
            .then(() => this.processCommands(commands))
            .catch((error: unknown) => {
                this._context.onRuntimeError(error);
            });
    }

    enqueueLocalOperation(operation: () => void | Promise<void>): void {
        this._commandQueue = this._commandQueue.then(operation).catch((error: unknown) => {
            this._context.onRuntimeError(error);
        });
    }

    async attachTrack(mid: string, streamType: StreamType): Promise<void> {
        await this._context.localUploads.attachTrack(this._peerConnection, mid, streamType);
    }

    async detachTrack(streamType: StreamType): Promise<void> {
        await this._context.localUploads.detachTrack(this._peerConnection, streamType);
    }

    async getStats(): Promise<SfuStats> {
        const peerConnection = this._peerConnection;
        if (!peerConnection) {
            return {};
        }

        const stats: SfuStats = {};
        if (typeof peerConnection.getStats === "function") {
            const peerConnectionStats = await peerConnection.getStats();
            stats.uploadStats = peerConnectionStats;
            stats.downloadStats = peerConnectionStats;
        }

        for (const streamType of STREAM_TYPES) {
            const senderStats = await this.getSenderStats(streamType);
            if (senderStats) {
                stats[streamType] = senderStats;
            }
        }

        return stats;
    }

    teardown(webSocketCloseCode: number): void {
        for (const timerId of [...this._timerHandles.keys()]) {
            this.cancelTimer(timerId);
        }
        this.closePeerConnection();
        this._context.remoteTracks.resetAll();
        if (this._webSocket && this._webSocket.readyState < 2) {
            this._webSocket.close(webSocketCloseCode);
        }
        this._webSocket = null;
    }

    private async processCommands(commands: HostCommand[]): Promise<void> {
        const pending = [...commands];
        if (pending.length === 0) {
            this._context.syncPublicState();
            return;
        }
        for (let index = 0; index < pending.length; index += 1) {
            const command = pending[index];
            const followUp = await this.executeCommand(command);
            this._context.syncPublicState();
            pending.push(...followUp);
        }
    }

    private async executeCommand(command: HostCommand): Promise<HostCommand[]> {
        switch (command.kind) {
            case COMMAND_KIND.SEND_WEB_SOCKET:
                if (!this._webSocket || this._webSocket.readyState !== 1) {
                    throw new Error("cannot send websocket frame while socket is not open");
                }
                this._webSocket.send(command.frame);
                return [];
            case COMMAND_KIND.SET_LOCAL_UPLOAD_INTENT:
                this._context.localUploads.setUploadIntent(command.streamType, command.active);
                return [];
            case COMMAND_KIND.APPLY_NEGOTIATION:
                return this.applyNegotiation(
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
                await this.attachTrack(command.mid, command.streamType);
                return [];
            case COMMAND_KIND.DETACH_TRACK:
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.INFO,
                    `detaching ${command.streamType} track from the peer connection`
                );
                await this.detachTrack(command.streamType);
                return [];
            case COMMAND_KIND.CREATE_PEER_CONNECTION:
                this.createPeerConnection();
                return [];
            case COMMAND_KIND.CLOSE_PEER_CONNECTION:
                this.closePeerConnection();
                return [];
            case COMMAND_KIND.CLOSE_WEB_SOCKET:
                if (this._webSocket && this._webSocket.readyState < 2) {
                    emitRuntimeLog(
                        this._context,
                        CLIENT_LOG_LEVEL.INFO,
                        `closing websocket with code ${command.code}`
                    );
                    this._webSocket.close(command.code);
                }
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
            case COMMAND_KIND.REPLACE_SOURCE_DESCRIPTORS:
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received ${command.sources.length} remote source descriptors`
                );
                this._context.onUpdate({
                    name: CLIENT_UPDATE.SOURCE,
                    payload: {
                        sources: command.sources
                    }
                });
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
                this.openWebSocket(command.url);
                return [];
        }
    }

    private openWebSocket(url: string): void {
        if (this._webSocket && this._webSocket.readyState < 2) {
            this._webSocket.onclose = null;
            this._webSocket.onerror = null;
            this._webSocket.onmessage = null;
            this._webSocket.onopen = null;
            this._webSocket.close(1000);
        }
        emitRuntimeLog(
            this._context,
            CLIENT_LOG_LEVEL.INFO,
            `opening websocket connection to ${url}`
        );
        const socket = this._createWebSocket(url);
        socket.onopen = () => {
            emitRuntimeLog(this._context, CLIENT_LOG_LEVEL.INFO, "websocket opened");
            this.enqueueProtocolCommands(() => this._context.protocolCore.onWsOpen());
        };
        socket.onmessage = (event) => {
            if (typeof event.data !== "string") {
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.WARN,
                    "received non-text websocket frame; closing with protocol error"
                );
                socket.close(WS_CLOSE_CODE.PROTOCOL_ERROR);
                return;
            }
            const frame = event.data;
            if (serverFrameByteLength(frame) > MAX_SERVER_FRAME_BYTES) {
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.WARN,
                    "received oversized websocket frame; closing with protocol error"
                );
                socket.close(WS_CLOSE_CODE.PROTOCOL_ERROR);
                return;
            }
            this.enqueueProtocolCommands(() => this._context.protocolCore.onWsMessage(frame));
        };
        socket.onclose = (event) => {
            if (this._webSocket !== socket) {
                return;
            }
            this._webSocket = null;
            emitRuntimeLog(
                this._context,
                CLIENT_LOG_LEVEL.INFO,
                `websocket closed with code ${event.code}`
            );
            this.enqueueProtocolCommands(() => this._context.protocolCore.onWsClose(event.code));
        };
        socket.onerror = () => undefined;
        this._webSocket = socket;
    }

    private createPeerConnection(): void {
        this.closePeerConnection();
        const peerConnection = this._createPeerConnection({
            iceServers: this._iceServers
        });
        emitRuntimeLog(this._context, CLIENT_LOG_LEVEL.DEBUG, "created RTCPeerConnection");
        peerConnection.onconnectionstatechange = () => {
            if (this._peerConnection !== peerConnection) {
                return;
            }
            const state = peerConnection.connectionState;
            emitRuntimeLog(
                this._context,
                state === "failed" ? CLIENT_LOG_LEVEL.WARN : CLIENT_LOG_LEVEL.DEBUG,
                `peer connection state changed to ${state}`
            );
            if (state === "connected") {
                this.enqueueProtocolCommands(() => this._context.protocolCore.onTransportReady());
                return;
            }
            if (state === "failed") {
                this.closeWebSocketForTransportFailure();
            }
        };
        peerConnection.oniceconnectionstatechange = () => {
            if (this._peerConnection !== peerConnection) {
                return;
            }
            const state = peerConnection.iceConnectionState;
            if (!state) {
                return;
            }
            emitRuntimeLog(
                this._context,
                state === "failed" || state === "disconnected"
                    ? CLIENT_LOG_LEVEL.WARN
                    : CLIENT_LOG_LEVEL.DEBUG,
                `ICE connection state changed to ${state}`
            );
        };
        peerConnection.onicegatheringstatechange = () => {
            if (this._peerConnection !== peerConnection) {
                return;
            }
            const state = peerConnection.iceGatheringState;
            if (!state) {
                return;
            }
            emitRuntimeLog(
                this._context,
                CLIENT_LOG_LEVEL.DEBUG,
                `ICE gathering state changed to ${state}`
            );
        };
        peerConnection.ontrack = (event) => {
            if (this._peerConnection !== peerConnection) {
                return;
            }
            emitRuntimeLog(
                this._context,
                CLIENT_LOG_LEVEL.DEBUG,
                `received remote track event for mid ${event.transceiver.mid ?? "unknown"} (kind=${event.track.kind})`
            );
            this._context.remoteTracks.handleTrackEvent(event, this._context.onUpdate);
        };
        this._peerConnection = peerConnection;
    }

    private closePeerConnection(): void {
        const hadPeerConnection = this._peerConnection !== null;
        if (this._peerConnection) {
            this._peerConnection.close();
            emitRuntimeLog(this._context, CLIENT_LOG_LEVEL.INFO, "closed RTCPeerConnection");
        }
        this._context.remoteTracks.clearPeerConnectionState();
        if (hadPeerConnection) {
            this._context.localUploads.clearPeerConnectionState();
        }
        this._peerConnection = null;
    }

    private async getSenderStats(streamType: StreamType): Promise<RTCStatsReport | undefined> {
        const peerConnection = this._peerConnection;
        const boundMid = this._context.localUploads.boundMidFor(streamType);
        if (!peerConnection || !boundMid) {
            return undefined;
        }
        const transceiver = peerConnection
            .getTransceivers()
            .find((candidate) => candidate.mid === boundMid);
        if (!transceiver || typeof transceiver.sender.getStats !== "function") {
            return undefined;
        }
        return transceiver.sender.getStats();
    }

    private async applyNegotiation(
        requestId: string,
        negotiationKind: "offer" | "renegotiate",
        sdp: string,
        uploadSlots: NegotiationUploadSlot[]
    ): Promise<HostCommand[]> {
        const peerConnection = this._peerConnection;
        if (!peerConnection) {
            throw new Error("received negotiation command without an active peer connection");
        }
        emitRuntimeLog(
            this._context,
            CLIENT_LOG_LEVEL.DEBUG,
            `applying ${negotiationKind} negotiation request ${requestId}`
        );
        await peerConnection.setRemoteDescription({
            sdp,
            type: "offer"
        });
        const attachmentResult = await this._context.localUploads.attachPendingTracks(
            peerConnection,
            uploadSlots
        );
        if (attachmentResult.attached.length > 0 || attachmentResult.skipped.length > 0) {
            for (const attachment of attachmentResult.attached) {
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.DEBUG,
                    `attached pending ${attachment.streamType} track to ${negotiationKind} mid ${attachment.mid}`
                );
                if (attachment.publicationPolicy.kind === "simulcast") {
                    emitRuntimeLog(
                        this._context,
                        CLIENT_LOG_LEVEL.INFO,
                        `enabled RID simulcast for ${attachment.streamType} on mid ${attachment.mid}`
                    );
                } else if (attachment.publicationPolicy.reason) {
                    emitRuntimeLog(
                        this._context,
                        CLIENT_LOG_LEVEL.DEBUG,
                        `using single-encoding ${attachment.streamType} upload on mid ${attachment.mid}: ${attachment.publicationPolicy.reason}`
                    );
                }
            }
            for (const streamType of attachmentResult.skipped) {
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.WARN,
                    `no eligible ${negotiationKind} mid was available for pending ${streamType} track`
                );
            }
        }
        const answer = await peerConnection.createAnswer();
        await peerConnection.setLocalDescription(answer);
        const answerSdp = await this.awaitStableLocalDescription(peerConnection);
        const commands = this._context.protocolCore.submitNegotiationAnswer(
            requestId,
            negotiationKind,
            answerSdp
        );
        emitRuntimeLog(
            this._context,
            CLIENT_LOG_LEVEL.DEBUG,
            `answered ${negotiationKind} negotiation request ${requestId}`
        );
        if (negotiationKind === "offer") {
            const isPeerConnectionConnected = peerConnection.connectionState === "connected";
            const needsImmediateTransportReadyFallback =
                !isPeerConnectionConnected &&
                (typeof peerConnection.connectionState !== "string" ||
                    localDescriptionHasOnlyInactiveMedia(answerSdp));
            if (needsImmediateTransportReadyFallback) {
                emitRuntimeLog(
                    this._context,
                    CLIENT_LOG_LEVEL.WARN,
                    "falling back to immediate transport-ready because the initial answer stayed inactive"
                );
            }
            if (isPeerConnectionConnected || needsImmediateTransportReadyFallback) {
                commands.push(...this._context.protocolCore.onTransportReady());
            }
        }
        return commands;
    }

    private async awaitStableLocalDescription(
        peerConnection: ClientPeerConnection
    ): Promise<string> {
        const initialSdp = peerConnection.localDescription?.sdp;
        if (!initialSdp) {
            throw new Error("peer connection local description is missing after createAnswer");
        }
        if (peerConnection.iceGatheringState === "complete") {
            return initialSdp;
        }

        return new Promise((resolve) => {
            const previousIceCandidate = peerConnection.onicecandidate;
            const previousIceGatheringStateChange = peerConnection.onicegatheringstatechange;

            const finalizeIfReady = (candidate: { candidate: string } | null | undefined) => {
                const sdp = peerConnection.localDescription?.sdp;
                if (!sdp) {
                    return;
                }
                if (candidate !== null && peerConnection.iceGatheringState !== "complete") {
                    return;
                }
                peerConnection.onicecandidate = previousIceCandidate;
                peerConnection.onicegatheringstatechange = previousIceGatheringStateChange;
                resolve(sdp);
            };

            peerConnection.onicecandidate = (event) => {
                previousIceCandidate?.(event);
                finalizeIfReady(event.candidate);
            };
            peerConnection.onicegatheringstatechange = () => {
                previousIceGatheringStateChange?.();
                if (peerConnection.iceGatheringState === "complete") {
                    finalizeIfReady(undefined);
                }
            };
            if (peerConnection.iceGatheringState === "complete") {
                finalizeIfReady(undefined);
            }
        });
    }

    private closeWebSocketForTransportFailure(): void {
        if (!this._webSocket || this._webSocket.readyState >= 2) {
            return;
        }
        emitRuntimeLog(
            this._context,
            CLIENT_LOG_LEVEL.WARN,
            "closing websocket because the peer connection transport failed"
        );
        this._webSocket.close(CLIENT_RECOVERABLE_CLOSE_CODE);
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

function serverFrameByteLength(frame: string): number {
    return TEXT_ENCODER.encode(frame).byteLength;
}
