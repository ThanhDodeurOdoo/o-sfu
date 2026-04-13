import type { ClientUpdateDetail, ConnectionState, StreamType } from "../public_api.js";
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
    onRuntimeError: (error: unknown) => void;
    onStateChange: (state: ConnectionState, cause?: string) => void;
    onUpdate: (update: ClientUpdateDetail) => void;
    pendingRequests: PendingRequests;
    protocolCore: ProtocolCoreBindings;
    remoteTracks: RemoteTracks;
    syncPublicState: () => void;
};

const TRANSPORT_FAILURE_CLOSE_CODE = 1011;

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
                    hooks.protocolCore
                );
            case "attachTrack":
                await this.attachTrack(command.mid, command.streamType, hooks.localUploads);
                return [];
            case "detachTrack":
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
                    this._webSocket.close(command.code);
                }
                return [];
            case "emitStateChange":
                hooks.onStateChange(command.state, command.cause);
                return [];
            case "replaceTrackBindings":
                hooks.remoteTracks.replaceTrackBindings(command.bindings, hooks.onUpdate);
                return [];
            case "removeSessionTracks":
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
        const socket = this._createWebSocket(url);
        socket.onopen = () => {
            this.enqueueProtocolCommands(() => hooks.protocolCore.onWsOpen(), hooks);
        };
        socket.onmessage = (event) => {
            if (typeof event.data !== "string") {
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
        peerConnection.onconnectionstatechange = () => {
            if (this._peerConnection !== peerConnection) {
                return;
            }
            const state = peerConnection.connectionState;
            if (state === "connected") {
                this.enqueueProtocolCommands(() => hooks.protocolCore.onTransportReady(), hooks);
                return;
            }
            if (state === "disconnected" || state === "failed") {
                this.closeWebSocketForTransportFailure();
            }
        };
        peerConnection.ontrack = (event) => {
            hooks.remoteTracks.handleTrackEvent(event, hooks.onUpdate);
        };
        this._peerConnection = peerConnection;
    }

    private closePeerConnection(hooks: BrowserRuntimeHooks): void {
        if (this._peerConnection) {
            this._peerConnection.close();
        }
        hooks.remoteTracks.clearPeerConnectionState();
        hooks.localUploads.clearPeerConnectionState();
        this._peerConnection = null;
    }

    private async applyNegotiation(
        requestId: string,
        negotiationKind: "offer" | "renegotiate",
        sdp: string,
        protocolCore: ProtocolCoreBindings
    ): Promise<HostCommand[]> {
        if (!this._peerConnection) {
            throw new Error("received negotiation command without an active peer connection");
        }
        await this._peerConnection.setRemoteDescription({
            sdp,
            type: "offer"
        });
        const answer = await this._peerConnection.createAnswer();
        await this._peerConnection.setLocalDescription(answer);
        const commands = protocolCore.submitNegotiationAnswer(
            requestId,
            negotiationKind,
            answer.sdp
        );
        if (negotiationKind === "offer" && this.shouldFallbackToImmediateTransportReady()) {
            commands.push(...protocolCore.onTransportReady());
        }
        return commands;
    }

    private shouldFallbackToImmediateTransportReady(): boolean {
        return typeof this._peerConnection?.connectionState !== "string";
    }

    private closeWebSocketForTransportFailure(): void {
        if (!this._webSocket || this._webSocket.readyState >= 2) {
            return;
        }
        this._webSocket.close(TRANSPORT_FAILURE_CLOSE_CODE);
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
