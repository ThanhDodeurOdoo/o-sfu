import {
    CLIENT_LOG_LEVEL,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectionState,
    type SfuStats,
    type StreamType
} from "../public_api.js";
import type { HostCommand, ProtocolCoreBindings } from "../runtime_contract.js";
import type {
    ClientPeerConnection,
    ClientWebSocket,
    SfuClientDependencies,
    TimerHandle
} from "./browser_types.js";
import type { LocalUploads } from "./local_uploads.js";
import type { PendingRequests } from "./pending_requests.js";
import type { RemoteTracks } from "./remote_tracks.js";

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
            .then(async () => {
                await this.processCommands(commands, hooks);
            })
            .catch((error: unknown) => {
                hooks.onRuntimeError(error);
            });
    }

    enqueueLocalOperation(operation: () => Promise<void>, hooks: BrowserRuntimeHooks): void {
        this._commandQueue = this._commandQueue
            .then(async () => {
                await operation();
            })
            .catch((error: unknown) => {
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

        for (const streamType of orderedStreamTypes()) {
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
        while (pending.length > 0) {
            const command = pending.shift();
            if (!command) {
                continue;
            }
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
            case "sendWebSocket":
                if (!this._webSocket || this._webSocket.readyState !== 1) {
                    throw new Error("cannot send websocket frame while socket is not open");
                }
                this._webSocket.send(command.frame);
                return [];
            case "applyNegotiation":
                return this.applyNegotiation(
                    command.requestId,
                    command.negotiationKind,
                    command.sdp,
                    hooks.localUploads,
                    hooks.protocolCore,
                    hooks
                );
            case "attachTrack":
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.INFO,
                    `attaching ${command.streamType} track to mid ${command.mid}`
                );
                await this.attachTrack(command.mid, command.streamType, hooks.localUploads);
                return [];
            case "detachTrack":
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.INFO,
                    `detaching ${command.streamType} track from the peer connection`
                );
                await this.detachTrack(command.streamType, hooks.localUploads);
                return [];
            case "createPeerConnection":
                this.createPeerConnection(hooks);
                return [];
            case "closePeerConnection":
                this.closePeerConnection(hooks);
                return [];
            case "closeWebSocket":
                if (this._webSocket && this._webSocket.readyState < 2) {
                    emitRuntimeLog(
                        hooks,
                        CLIENT_LOG_LEVEL.INFO,
                        `closing websocket with code ${command.code}`
                    );
                    this._webSocket.close(command.code);
                }
                return [];
            case "emitStateChange":
                hooks.onStateChange(command.state, command.cause);
                return [];
            case "replaceTrackBindings":
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.INFO,
                    `received ${command.bindings.length} remote track bindings`
                );
                hooks.remoteTracks.replaceTrackBindings(command.bindings, hooks.onUpdate);
                return [];
            case "removeSessionTracks":
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.INFO,
                    `removing remote tracks for session ${command.sessionId}`
                );
                hooks.remoteTracks.removeSessionTracks(command.sessionId);
                return [];
            case "emitUpdate":
                hooks.onUpdate(command.update);
                return [];
            case "registerPendingRequest":
                hooks.pendingRequests.register(command.requestId, command.requestKind);
                return [];
            case "resolvePendingRequest":
                hooks.pendingRequests.resolve(command.requestId, command.ok);
                return [];
            case "scheduleTimer":
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
            case "cancelTimer":
                this.cancelTimer(command.id);
                return [];
            case "connect":
                this.openWebSocket(command.url, hooks);
                return [];
        }
    }

    private openWebSocket(url: string, hooks: BrowserRuntimeHooks): void {
        if (this._webSocket && this._webSocket.readyState < 2) {
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
            if (this._webSocket === socket) {
                this._webSocket = null;
            }
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
        emitRuntimeLog(hooks, CLIENT_LOG_LEVEL.INFO, "created RTCPeerConnection");
        peerConnection.onconnectionstatechange = () => {
            if (this._peerConnection !== peerConnection) {
                return;
            }
            const state = peerConnection.connectionState;
            emitRuntimeLog(
                hooks,
                state === "failed" ? CLIENT_LOG_LEVEL.WARN : CLIENT_LOG_LEVEL.INFO,
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
        peerConnection.ontrack = (event) => {
            emitRuntimeLog(
                hooks,
                CLIENT_LOG_LEVEL.INFO,
                `received remote track event for mid ${event.transceiver.mid ?? "unknown"} (kind=${event.track.kind})`
            );
            hooks.remoteTracks.handleTrackEvent(event, hooks.onUpdate);
        };
        this._peerConnection = peerConnection;
    }

    private closePeerConnection(hooks: BrowserRuntimeHooks): void {
        if (this._peerConnection) {
            this._peerConnection.close();
            emitRuntimeLog(hooks, CLIENT_LOG_LEVEL.INFO, "closed RTCPeerConnection");
        }
        hooks.remoteTracks.clearPeerConnectionState();
        hooks.localUploads.clearPeerConnectionState();
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
        localUploads: LocalUploads,
        protocolCore: ProtocolCoreBindings,
        hooks: BrowserRuntimeHooks
    ): Promise<HostCommand[]> {
        if (!this._peerConnection) {
            throw new Error("received negotiation command without an active peer connection");
        }
        emitRuntimeLog(
            hooks,
            CLIENT_LOG_LEVEL.INFO,
            `applying ${negotiationKind} negotiation request ${requestId}`
        );
        await this._peerConnection.setRemoteDescription({
            sdp,
            type: "offer"
        });
        if (negotiationKind === "renegotiate") {
            const pendingAttachment = await localUploads.attachPendingRenegotiationTracks(
                this._peerConnection,
                uploadEligibleMidsFromOfferSdp(sdp)
            );
            for (const attachment of pendingAttachment.attached) {
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.INFO,
                    `attached pending ${attachment.streamType} track to renegotiation mid ${attachment.mid}`
                );
            }
            for (const streamType of pendingAttachment.skipped) {
                emitRuntimeLog(
                    hooks,
                    CLIENT_LOG_LEVEL.WARN,
                    `no eligible renegotiation mid was available for pending ${streamType} track`
                );
            }
        }
        const answer = await this._peerConnection.createAnswer();
        await this._peerConnection.setLocalDescription(answer);
        const answerSdp = await this.awaitStableLocalDescription(this._peerConnection);
        const commands = protocolCore.submitNegotiationAnswer(
            requestId,
            negotiationKind,
            answerSdp
        );
        emitRuntimeLog(
            hooks,
            CLIENT_LOG_LEVEL.INFO,
            `answered ${negotiationKind} negotiation request ${requestId}`
        );
        if (
            negotiationKind === "offer" &&
            (this.shouldFallbackToImmediateTransportReady() ||
                this.localDescriptionHasOnlyInactiveMedia(answerSdp))
        ) {
            emitRuntimeLog(
                hooks,
                CLIENT_LOG_LEVEL.WARN,
                "falling back to immediate transport-ready because the initial answer stayed inactive"
            );
            commands.push(...protocolCore.onTransportReady());
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
        if (
            this.localDescriptionHasCandidate(initialSdp) ||
            peerConnection.iceGatheringState === "complete"
        ) {
            return initialSdp;
        }

        return new Promise((resolve) => {
            const previousIceCandidate = peerConnection.onicecandidate;
            const previousIceGatheringStateChange = peerConnection.onicegatheringstatechange;

            const finalizeIfReady = () => {
                const sdp = peerConnection.localDescription?.sdp;
                if (!sdp) {
                    return;
                }
                if (
                    !this.localDescriptionHasCandidate(sdp) &&
                    peerConnection.iceGatheringState !== "complete"
                ) {
                    return;
                }
                peerConnection.onicecandidate = previousIceCandidate;
                peerConnection.onicegatheringstatechange = previousIceGatheringStateChange;
                resolve(sdp);
            };

            peerConnection.onicecandidate = (event) => {
                previousIceCandidate?.(event);
                finalizeIfReady();
            };
            peerConnection.onicegatheringstatechange = () => {
                previousIceGatheringStateChange?.();
                finalizeIfReady();
            };
            finalizeIfReady();
        });
    }

    private localDescriptionHasCandidate(sdp: string): boolean {
        return /(?:^|\r\n)a=candidate:/.test(sdp);
    }

    private localDescriptionHasOnlyInactiveMedia(sdp: string): boolean {
        const mediaSections = sdp
            .split(/\r?\nm=/)
            .map((section, index) => (index === 0 ? section : `m=${section}`))
            .filter((section) => section.startsWith("m="));
        if (mediaSections.length === 0) {
            return false;
        }
        return mediaSections.every((section) => {
            const direction = section.match(
                /(?:^|\r\n)a=(sendrecv|sendonly|recvonly|inactive)(?:\r?\n|$)/
            )?.[1];
            return direction === "inactive";
        });
    }

    private shouldFallbackToImmediateTransportReady(): boolean {
        return typeof this._peerConnection?.connectionState !== "string";
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

function orderedStreamTypes(): StreamType[] {
    return ["audio", "camera", "screen"];
}

function uploadEligibleMidsFromOfferSdp(sdp: string): Set<string> {
    const eligibleMids = new Set<string>();
    const mediaSections = sdp
        .split(/\r?\nm=/)
        .map((section, index) => (index === 0 ? section : `m=${section}`))
        .filter((section) => section.startsWith("m="));
    for (const section of mediaSections) {
        const direction = section.match(
            /(?:^|\r\n)a=(sendrecv|sendonly|recvonly|inactive)(?:\r?\n|$)/
        )?.[1];
        if (direction !== "recvonly") {
            continue;
        }
        const mid = section.match(/(?:^|\r\n)a=mid:([^\r\n]+)(?:\r?\n|$)/)?.[1];
        if (mid) {
            eligibleMids.add(mid);
        }
    }
    return eligibleMids;
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
