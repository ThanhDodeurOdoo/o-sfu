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
import { CommandKind, type HostCommand, type ProtocolCoreBindings } from "../runtime_contract.js";
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

export type BrowserRuntimeHooks = {
    iceServers?: RTCIceServer[];
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

export { CLIENT_RECOVERABLE_CLOSE_CODE };

export class BrowserRuntime {
    private readonly _clearTimer: (handle: TimerHandle) => void;
    private readonly _createPeerConnection: (config: RTCConfiguration) => ClientPeerConnection;
    private readonly _createWebSocket: (url: string) => ClientWebSocket;
    private readonly _setTimer: (callback: () => void, ms: number) => TimerHandle;

    private _commandQueue: Promise<void> = Promise.resolve();
    private _peerConnection: ClientPeerConnection | null = null;
    private _timerHandles = new Map<number, TimerHandle>();
    private _webSocket: ClientWebSocket | null = null;

    constructor(dependencies: SfuClientDependencies = {}) {
        this._createWebSocket =
            dependencies.createWebSocket ?? ((url) => new WebSocket(url) as ClientWebSocket);
        this._createPeerConnection =
            dependencies.createPeerConnection ??
            ((config) => new RTCPeerConnection(config) as ClientPeerConnection);
        this._setTimer = dependencies.setTimer ?? ((callback, ms) => setTimeout(callback, ms));
        this._clearTimer = dependencies.clearTimer ?? ((handle) => clearTimeout(handle));
    }

    enqueueProtocolCommands(getCommands: () => HostCommand[], hooks: BrowserRuntimeHooks): void {
        try {
            this.enqueue(getCommands(), hooks);
        } catch (error) {
            hooks.onRuntimeError(error);
        }
    }

    enqueue(commands: HostCommand[], hooks: BrowserRuntimeHooks): void {
        this._commandQueue = this._commandQueue
            .then(() => this.processCommands(commands, hooks))
            .catch((error: unknown) => {
                hooks.onRuntimeError(error);
            });
    }

    enqueueLocalOperation(operation: () => Promise<void>, hooks: BrowserRuntimeHooks): void {
        this._commandQueue = this._commandQueue.then(operation).catch((error: unknown) => {
            hooks.onRuntimeError(error);
        });
    }

    async attachTrack(
        mid: string,
        streamType: StreamType,
        localUploads: LocalUploads
    ): Promise<void> {
        await localUploads.attachTrack(this._peerConnection, mid, streamType);
    }

    async detachTrack(streamType: StreamType, localUploads: LocalUploads): Promise<void> {
        await localUploads.detachTrack(this._peerConnection, streamType);
    }

    async getStats(localUploads: LocalUploads): Promise<SfuStats> {
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
            const senderStats = await this.getSenderStats(streamType, localUploads);
            if (senderStats) {
                stats[streamType] = senderStats;
            }
        }

        return stats;
    }

    teardown(hooks: BrowserRuntimeHooks, webSocketCloseCode: number): void {
        for (const timerId of [...this._timerHandles.keys()]) {
            this.cancelTimer(timerId);
        }
        this.closePeerConnection(hooks);
        hooks.remoteTracks.resetAll();
        if (this._webSocket && this._webSocket.readyState < 2) {
            this._webSocket.close(webSocketCloseCode);
        }
        this._webSocket = null;
    }

    private async processCommands(
        commands: HostCommand[],
        hooks: BrowserRuntimeHooks
    ): Promise<void> {
        const pending = [...commands];
        let processedAnyCommand = false;
        for (let index = 0; index < pending.length; index += 1) {
            const command = pending[index];
            processedAnyCommand = true;
            const followUp = await this.executeCommand(command, hooks);
            hooks.syncPublicState();
            pending.push(...followUp);
        }
        if (!processedAnyCommand) {
            hooks.syncPublicState();
        }
    }

    private async executeCommand(
        command: HostCommand,
        hooks: BrowserRuntimeHooks
    ): Promise<HostCommand[]> {
        switch (command.kind) {
            case CommandKind.SEND_WEB_SOCKET:
                if (!this._webSocket || this._webSocket.readyState !== 1) {
                    throw new Error("cannot send websocket frame while socket is not open");
                }
                this._webSocket.send(command.frame);
                return [];
            case CommandKind.SET_LOCAL_UPLOAD_INTENT:
                hooks.localUploads.setUploadIntent(command.streamType, command.active);
                return [];
            case CommandKind.APPLY_NEGOTIATION:
                return this.applyNegotiation(
                    command.requestId,
                    command.negotiationKind,
                    command.sdp,
                    command.uploadSlots,
                    hooks.localUploads,
                    hooks.protocolCore,
                    hooks
                );
            case CommandKind.ATTACH_TRACK:
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.INFO,
                    `attaching ${command.streamType} track to mid ${command.mid}`
                );
                await this.attachTrack(command.mid, command.streamType, hooks.localUploads);
                return [];
            case CommandKind.DETACH_TRACK:
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.INFO,
                    `detaching ${command.streamType} track from the peer connection`
                );
                await this.detachTrack(command.streamType, hooks.localUploads);
                return [];
            case CommandKind.CREATE_PEER_CONNECTION:
                this.createPeerConnection(hooks);
                return [];
            case CommandKind.CLOSE_PEER_CONNECTION:
                this.closePeerConnection(hooks);
                return [];
            case CommandKind.CLOSE_WEB_SOCKET:
                if (this._webSocket && this._webSocket.readyState < 2) {
                    emitRuntimeLog(
                        hooks,
                        CLIENT_LOG_LEVEL.INFO,
                        `closing websocket with code ${command.code}`
                    );
                    this._webSocket.close(command.code);
                }
                return [];
            case CommandKind.EMIT_STATE_CHANGE:
                hooks.onStateChange(command.state, command.cause);
                return [];
            case CommandKind.REPLACE_TRACK_BINDINGS:
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received ${command.bindings.length} remote track bindings`
                );
                hooks.remoteTracks.replaceTrackBindings(command.bindings, hooks.onUpdate);
                return [];
            case CommandKind.REPLACE_SOURCE_DESCRIPTORS:
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received ${command.sources.length} remote source descriptors`
                );
                hooks.onUpdate({
                    name: CLIENT_UPDATE.SOURCE,
                    payload: {
                        sources: command.sources
                    }
                });
                return [];
            case CommandKind.REMOVE_SESSION_TRACKS:
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.INFO,
                    `removing remote tracks for session ${command.sessionId}`
                );
                hooks.remoteTracks.removeSessionTracks(command.sessionId);
                return [];
            case CommandKind.EMIT_UPDATE:
                hooks.onUpdate(command.update);
                return [];
            case CommandKind.REGISTER_PENDING_REQUEST:
                hooks.pendingRequests.register(command.requestId, command.requestKind);
                return [];
            case CommandKind.RESOLVE_PENDING_REQUEST:
                hooks.pendingRequests.resolve(command.requestId, command.ok);
                return [];
            case CommandKind.SCHEDULE_TIMER:
                this.cancelTimer(command.id);
                this._timerHandles.set(
                    command.id,
                    this._setTimer(() => {
                        this.enqueueProtocolCommands(
                            () => hooks.protocolCore.onTimer(command.id),
                            hooks
                        );
                    }, command.ms)
                );
                return [];
            case CommandKind.CANCEL_TIMER:
                this.cancelTimer(command.id);
                return [];
            case CommandKind.CONNECT:
                this.openWebSocket(command.url, hooks);
                return [];
        }
    }

    private openWebSocket(url: string, hooks: BrowserRuntimeHooks): void {
        if (this._webSocket && this._webSocket.readyState < 2) {
            this._webSocket.onclose = null;
            this._webSocket.onerror = null;
            this._webSocket.onmessage = null;
            this._webSocket.onopen = null;
            this._webSocket.close(1000);
        }
        emitRuntimeLog(hooks, CLIENT_LOG_LEVEL.INFO, `opening websocket connection to ${url}`);
        const socket = this._createWebSocket(url);
        socket.onopen = () => {
            emitRuntimeLog(hooks, CLIENT_LOG_LEVEL.INFO, "websocket opened");
            this.enqueueProtocolCommands(() => hooks.protocolCore.onWsOpen(), hooks);
        };
        socket.onmessage = (event) => {
            if (typeof event.data !== "string") {
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.WARN,
                    "received non-text websocket frame; closing with protocol error"
                );
                socket.close(1002);
                return;
            }
            const frame = event.data;
            this.enqueueProtocolCommands(() => hooks.protocolCore.onWsMessage(frame), hooks);
        };
        socket.onclose = (event) => {
            if (this._webSocket !== socket) {
                return;
            }
            this._webSocket = null;
            emitRuntimeLog(
                hooks,
                CLIENT_LOG_LEVEL.INFO,
                `websocket closed with code ${event.code}`
            );
            this.enqueueProtocolCommands(() => hooks.protocolCore.onWsClose(event.code), hooks);
        };
        socket.onerror = () => undefined;
        this._webSocket = socket;
    }

    private createPeerConnection(hooks: BrowserRuntimeHooks): void {
        this.closePeerConnection(hooks);
        const peerConnection = this._createPeerConnection({
            iceServers: hooks.iceServers
        });
        emitRuntimeLog(hooks, CLIENT_LOG_LEVEL.DEBUG, "created RTCPeerConnection");
        peerConnection.onconnectionstatechange = () => {
            if (this._peerConnection !== peerConnection) {
                return;
            }
            const state = peerConnection.connectionState;
            emitRuntimeLog(
                hooks,
                state === "failed" ? CLIENT_LOG_LEVEL.WARN : CLIENT_LOG_LEVEL.DEBUG,
                `peer connection state changed to ${state}`
            );
            if (state === "connected") {
                this.enqueueProtocolCommands(() => hooks.protocolCore.onTransportReady(), hooks);
                return;
            }
            if (state === "failed") {
                this.closeWebSocketForTransportFailure(hooks);
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
                hooks,
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
                hooks,
                CLIENT_LOG_LEVEL.DEBUG,
                `ICE gathering state changed to ${state}`
            );
        };
        peerConnection.ontrack = (event) => {
            if (this._peerConnection !== peerConnection) {
                return;
            }
            emitRuntimeLog(
                hooks,
                CLIENT_LOG_LEVEL.DEBUG,
                `received remote track event for mid ${event.transceiver.mid ?? "unknown"} (kind=${event.track.kind})`
            );
            hooks.remoteTracks.handleTrackEvent(event, hooks.onUpdate);
        };
        this._peerConnection = peerConnection;
    }

    private closePeerConnection(hooks: BrowserRuntimeHooks): void {
        const hadPeerConnection = this._peerConnection !== null;
        if (this._peerConnection) {
            this._peerConnection.close();
            emitRuntimeLog(hooks, CLIENT_LOG_LEVEL.INFO, "closed RTCPeerConnection");
        }
        hooks.remoteTracks.clearPeerConnectionState();
        if (hadPeerConnection) {
            hooks.localUploads.clearPeerConnectionState();
        }
        this._peerConnection = null;
    }

    private async getSenderStats(
        streamType: StreamType,
        localUploads: LocalUploads
    ): Promise<RTCStatsReport | undefined> {
        const peerConnection = this._peerConnection;
        const boundMid = localUploads.boundMidFor(streamType);
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
        uploadSlots: NegotiationUploadSlot[],
        localUploads: LocalUploads,
        protocolCore: ProtocolCoreBindings,
        hooks: BrowserRuntimeHooks
    ): Promise<HostCommand[]> {
        const peerConnection = this._peerConnection;
        if (!peerConnection) {
            throw new Error("received negotiation command without an active peer connection");
        }
        emitRuntimeLog(
            hooks,
            CLIENT_LOG_LEVEL.DEBUG,
            `applying ${negotiationKind} negotiation request ${requestId}`
        );
        await peerConnection.setRemoteDescription({
            sdp,
            type: "offer"
        });
        const attachmentResult = await localUploads.attachPendingTracks(
            peerConnection,
            uploadSlots
        );
        if (attachmentResult.attached.length > 0 || attachmentResult.skipped.length > 0) {
            for (const attachment of attachmentResult.attached) {
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.DEBUG,
                    `attached pending ${attachment.streamType} track to ${negotiationKind} mid ${attachment.mid}`
                );
                if (attachment.publicationPolicy.kind === "simulcast") {
                    emitRuntimeLog(
                        hooks,
                        CLIENT_LOG_LEVEL.INFO,
                        `enabled RID simulcast for ${attachment.streamType} on mid ${attachment.mid}`
                    );
                } else if (attachment.publicationPolicy.reason) {
                    emitRuntimeLog(
                        hooks,
                        CLIENT_LOG_LEVEL.DEBUG,
                        `using single-encoding ${attachment.streamType} upload on mid ${attachment.mid}: ${attachment.publicationPolicy.reason}`
                    );
                }
            }
            for (const streamType of attachmentResult.skipped) {
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.WARN,
                    `no eligible ${negotiationKind} mid was available for pending ${streamType} track`
                );
            }
        }
        const answer = await peerConnection.createAnswer();
        await peerConnection.setLocalDescription(answer);
        const answerSdp = await this.awaitStableLocalDescription(peerConnection);
        const commands = protocolCore.submitNegotiationAnswer(
            requestId,
            negotiationKind,
            answerSdp
        );
        emitRuntimeLog(
            hooks,
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
                    hooks,
                    CLIENT_LOG_LEVEL.WARN,
                    "falling back to immediate transport-ready because the initial answer stayed inactive"
                );
            }
            if (isPeerConnectionConnected || needsImmediateTransportReadyFallback) {
                commands.push(...protocolCore.onTransportReady());
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

    private closeWebSocketForTransportFailure(hooks: BrowserRuntimeHooks): void {
        if (!this._webSocket || this._webSocket.readyState >= 2) {
            return;
        }
        emitRuntimeLog(
            hooks,
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
}

function emitRuntimeLog(
    hooks: Pick<BrowserRuntimeHooks, "onLog">,
    level: ClientLogDetail["level"],
    message: string
): void {
    hooks.onLog({
        id: BROWSER_RUNTIME_LOG_SOURCE,
        level,
        message
    });
}
