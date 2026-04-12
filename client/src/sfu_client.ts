import {
    CLIENT_UPDATE,
    SFU_CLIENT_STATE,
    type AvailableFeatures,
    type ConnectOptions,
    type ConnectionState,
    type DownloadStates,
    type RecordingOptions,
    type RecordingState,
    type SessionId,
    type SessionInfo,
    type SfuClientSurface,
    type StreamType,
    type UpdateInfoOptions
} from "./public_api.js";
import {
    PENDING_REQUEST_KIND,
    createProtocolCore,
    type HostCommand,
    type PendingRequestKind,
    type ProtocolCoreBindings,
    type ProtocolCoreFactory
} from "./runtime_contract.js";
import type { TrackBinding } from "./protocol.js";

type TrackLike = MediaStreamTrack & { muted?: boolean };

type TimerHandle = ReturnType<typeof globalThis.setTimeout>;

type PendingRequestCallbacks = {
    reject: (error: Error) => void;
    resolve: (ok: boolean) => void;
};

type ConsumerCompat = {
    closed: boolean;
    paused: boolean;
    track: TrackLike | null;
};

type ConsumersCompat = {
    audio: ConsumerCompat | null;
    camera: ConsumerCompat | null;
    screen: ConsumerCompat | null;
};

type AppliedTrackBinding = Pick<TrackBinding, "active" | "sessionId" | "type">;

type WebSocketLike = {
    close(code?: number): void;
    onclose: ((event: { code: number }) => void) | null;
    onerror: ((event: Event) => void) | null;
    onmessage: ((event: { data: unknown }) => void) | null;
    onopen: ((event: Event) => void) | null;
    readonly readyState: number;
    send(data: string): void;
};

type RtcSenderLike = {
    replaceTrack(track: TrackLike | null): Promise<void>;
    track?: TrackLike | null;
};

type RtcTransceiverLike = {
    mid: string | null;
    sender: RtcSenderLike;
};

type RtcTrackEventLike = {
    track: TrackLike;
    transceiver: {
        mid: string | null;
    };
};

type PeerConnectionLike = {
    close(): void;
    createAnswer(): Promise<{ sdp: string; type: "answer" }>;
    getTransceivers(): RtcTransceiverLike[];
    ontrack: ((event: RtcTrackEventLike) => void) | null;
    setLocalDescription(description: { sdp: string; type: "answer" }): Promise<void>;
    setRemoteDescription(description: { sdp: string; type: "offer" }): Promise<void>;
};

export interface SfuClientDependencies {
    clearTimer?: (handle: TimerHandle) => void;
    createPeerConnection?: (config: RTCConfiguration) => PeerConnectionLike;
    createProtocolCore?: ProtocolCoreFactory;
    createWebSocket?: (url: string) => WebSocketLike;
    setTimer?: (callback: () => void, ms: number) => TimerHandle;
}

const EMPTY_FEATURES: AvailableFeatures = {
    rtc: false,
    transcription: false,
    audioRecording: false,
    videoRecording: false
};

const EMPTY_CONSUMERS = (): ConsumersCompat => ({
    audio: null,
    camera: null,
    screen: null
});

const STREAM_KIND: Record<StreamType, "audio" | "video"> = {
    audio: "audio",
    camera: "video",
    screen: "video"
};

export class SfuClient extends EventTarget implements SfuClientSurface {
    public availableFeatures: AvailableFeatures = { ...EMPTY_FEATURES };
    public recordingState: RecordingState = {};
    public _consumers = new Map<SessionId, ConsumersCompat>();

    private readonly _clearTimer: (handle: TimerHandle) => void;
    private readonly _createPeerConnection: (config: RTCConfiguration) => PeerConnectionLike;
    private readonly _createWebSocket: (url: string) => WebSocketLike;
    private readonly _protocolCore: ProtocolCoreBindings;
    private readonly _setTimer: (callback: () => void, ms: number) => TimerHandle;

    private _commandQueue: Promise<void> = Promise.resolve();
    private _iceServers?: RTCIceServer[];
    private _localTracks = new Map<StreamType, TrackLike | null>();
    private _peerConnection: PeerConnectionLike | null = null;
    private _pendingRequestResolvers = new Map<string, PendingRequestCallbacks>();
    private _remoteTrackBindings = new Map<string, AppliedTrackBinding>();
    private _remoteTracksByMid = new Map<string, TrackLike>();
    private _requestWaiters: Record<PendingRequestKind, PendingRequestCallbacks[]> = {
        [PENDING_REQUEST_KIND.START_RECORDING]: [],
        [PENDING_REQUEST_KIND.STOP_RECORDING]: []
    };
    private _senderMidByType = new Map<StreamType, string>();
    private _state: ConnectionState = SFU_CLIENT_STATE.DISCONNECTED;
    private _timerHandles = new Map<number, TimerHandle>();
    private _webSocket: WebSocketLike | null = null;

    constructor(dependencies: SfuClientDependencies = {}) {
        super();
        this._protocolCore = dependencies.createProtocolCore
            ? dependencies.createProtocolCore()
            : createProtocolCore();
        this._createWebSocket =
            dependencies.createWebSocket ?? ((url) => new WebSocket(url) as WebSocketLike);
        this._createPeerConnection =
            dependencies.createPeerConnection ??
            ((config) => new RTCPeerConnection(config) as PeerConnectionLike);
        this._setTimer = dependencies.setTimer ?? ((callback, ms) => setTimeout(callback, ms));
        this._clearTimer = dependencies.clearTimer ?? ((handle) => clearTimeout(handle));
        this._syncSnapshot();
    }

    get state(): ConnectionState {
        return this._state;
    }

    connect(url: string, jwt: string, options: ConnectOptions = {}): void {
        validateConnectOptions(options);
        this._iceServers = cloneIceServers(options.iceServers);
        this._enqueue(
            this._protocolCore.connect(normalizeWebSocketUrl(url), jwt, options.channelUUID ?? null)
        );
    }

    disconnect(): void {
        this._enqueue(this._protocolCore.disconnect());
    }

    updateUpload(type: StreamType, track: MediaStreamTrack | null): void {
        validateTrackForStreamType(type, track);
        this._localTracks.set(type, track);
        this._enqueue(this._protocolCore.updateUpload(type, Boolean(track)));
    }

    updateDownload(sessionId: SessionId, states: DownloadStates): void {
        validateDownloadStates(states);
        const consumers = this._consumers.get(sessionId);
        if (consumers) {
            if (states.audio !== undefined && consumers.audio) {
                consumers.audio.paused = !states.audio;
            }
            if (states.camera !== undefined && consumers.camera) {
                consumers.camera.paused = !states.camera;
            }
            if (states.screen !== undefined && consumers.screen) {
                consumers.screen.paused = !states.screen;
            }
        }
        this._enqueue(this._protocolCore.updateDownload(sessionId, states));
    }

    updateInfo(info: SessionInfo, _options: UpdateInfoOptions = {}): void {
        this._enqueue(this._protocolCore.updateInfo(info));
    }

    broadcast(message: unknown): void {
        this._enqueue(this._protocolCore.broadcast(message));
    }

    startRecording(options: RecordingOptions = {}): Promise<boolean> {
        return this._beginPendingRequest(
            this._protocolCore.startRecording(options),
            PENDING_REQUEST_KIND.START_RECORDING
        );
    }

    stopRecording(): Promise<boolean> {
        return this._beginPendingRequest(
            this._protocolCore.stopRecording(),
            PENDING_REQUEST_KIND.STOP_RECORDING
        );
    }

    private _beginPendingRequest(
        commands: HostCommand[],
        requestKind: PendingRequestKind
    ): Promise<boolean> {
        if (!commands.some((command) => command.kind === "registerPendingRequest")) {
            this._enqueue(commands);
            return Promise.resolve(false);
        }
        return new Promise<boolean>((resolve, reject) => {
            this._requestWaiters[requestKind].push({ resolve, reject });
            this._enqueue(commands);
        });
    }

    private _enqueue(commands: HostCommand[]): void {
        this._commandQueue = this._commandQueue
            .then(async () => {
                await this._processCommands(commands);
            })
            .catch((error: unknown) => {
                this._handleRuntimeError(error);
            });
    }

    private async _processCommands(commands: HostCommand[]): Promise<void> {
        const pending = [...commands];
        let processedAnyCommand = false;
        while (pending.length > 0) {
            const command = pending.shift();
            if (!command) {
                continue;
            }
            processedAnyCommand = true;
            const followUp = await this._executeCommand(command);
            this._syncSnapshot();
            this._syncRemoteTracks();
            pending.push(...followUp);
        }
        if (!processedAnyCommand) {
            this._syncSnapshot();
            this._syncRemoteTracks();
        }
    }

    private async _executeCommand(command: HostCommand): Promise<HostCommand[]> {
        switch (command.kind) {
            case "sendWebSocket":
                if (!this._webSocket || this._webSocket.readyState !== 1) {
                    throw new Error("cannot send websocket frame while socket is not open");
                }
                this._webSocket.send(command.frame);
                return [];
            case "applyNegotiation":
                return this._applyNegotiation(
                    command.requestId,
                    command.negotiationKind,
                    command.sdp
                );
            case "attachTrack":
                await this._attachTrack(command.mid, command.streamType);
                return [];
            case "detachTrack":
                await this._detachTrack(command.streamType);
                return [];
            case "createPeerConnection":
                this._createPeerConnectionInstance();
                return [];
            case "closePeerConnection":
                this._closePeerConnection();
                return [];
            case "closeWebSocket":
                if (this._webSocket && this._webSocket.readyState < 2) {
                    this._webSocket.close(command.code);
                }
                return [];
            case "emitStateChange":
                this._state = command.state;
                this.dispatchEvent(
                    new CustomEvent("stateChange", {
                        detail: {
                            cause: command.cause,
                            state: command.state
                        }
                    })
                );
                return [];
            case "emitUpdate":
                this._applyCompatUpdate(command.update);
                this.dispatchEvent(
                    new CustomEvent("update", {
                        detail: command.update
                    })
                );
                return [];
            case "registerPendingRequest": {
                const callbacks = this._requestWaiters[command.requestKind].shift();
                if (!callbacks) {
                    throw new Error(`missing pending request waiter for ${command.requestKind}`);
                }
                this._pendingRequestResolvers.set(command.requestId, callbacks);
                return [];
            }
            case "resolvePendingRequest": {
                const callbacks = this._pendingRequestResolvers.get(command.requestId);
                if (callbacks) {
                    this._pendingRequestResolvers.delete(command.requestId);
                    callbacks.resolve(command.ok);
                }
                return [];
            }
            case "scheduleTimer":
                this._cancelTimer(command.id);
                this._timerHandles.set(
                    command.id,
                    this._setTimer(() => {
                        this._enqueue(this._protocolCore.onTimer(command.id));
                    }, command.ms)
                );
                return [];
            case "cancelTimer":
                this._cancelTimer(command.id);
                return [];
            case "connect":
                this._openWebSocket(command.url);
                return [];
        }
    }

    private _openWebSocket(url: string): void {
        if (this._webSocket && this._webSocket.readyState < 2) {
            this._webSocket.close(1000);
        }
        const socket = this._createWebSocket(url);
        socket.onopen = () => {
            this._enqueue(this._protocolCore.onWsOpen());
        };
        socket.onmessage = (event) => {
            if (typeof event.data !== "string") {
                socket.close(1002);
                return;
            }
            this._enqueue(this._protocolCore.onWsMessage(event.data));
        };
        socket.onclose = (event) => {
            if (this._webSocket === socket) {
                this._webSocket = null;
            }
            this._enqueue(this._protocolCore.onWsClose(event.code));
        };
        socket.onerror = () => undefined;
        this._webSocket = socket;
    }

    private _createPeerConnectionInstance(): void {
        this._closePeerConnection();
        const peerConnection = this._createPeerConnection({
            iceServers: this._iceServers
        });
        peerConnection.ontrack = (event) => {
            this._handleTrackEvent(event);
        };
        this._peerConnection = peerConnection;
    }

    private _closePeerConnection(): void {
        if (this._peerConnection) {
            this._peerConnection.close();
        }
        this._peerConnection = null;
        this._remoteTrackBindings.clear();
        this._remoteTracksByMid.clear();
        this._senderMidByType.clear();
    }

    private async _applyNegotiation(
        requestId: string,
        negotiationKind: "offer" | "renegotiate",
        sdp: string
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
        const commands = this._protocolCore.submitNegotiationAnswer(
            requestId,
            negotiationKind,
            answer.sdp
        );
        commands.push(...this._protocolCore.onTransportReady());
        return commands;
    }

    private async _attachTrack(mid: string, streamType: StreamType): Promise<void> {
        if (!this._peerConnection) {
            throw new Error("cannot attach track without an active peer connection");
        }
        const track = this._localTracks.get(streamType) ?? null;
        const transceiver = this._peerConnection
            .getTransceivers()
            .find((candidate) => candidate.mid === mid);
        if (!transceiver) {
            throw new Error(`missing transceiver for mid ${mid}`);
        }
        await transceiver.sender.replaceTrack(track);
        this._senderMidByType.set(streamType, mid);
    }

    private async _detachTrack(streamType: StreamType): Promise<void> {
        if (!this._peerConnection) {
            return;
        }
        const knownMid = this._senderMidByType.get(streamType);
        const transceiver = this._peerConnection
            .getTransceivers()
            .find((candidate) =>
                knownMid
                    ? candidate.mid === knownMid
                    : candidate.sender.track?.kind === STREAM_KIND[streamType]
            );
        if (transceiver) {
            await transceiver.sender.replaceTrack(null);
        }
        this._senderMidByType.delete(streamType);
    }

    private _handleTrackEvent(event: RtcTrackEventLike): void {
        const mid = event.transceiver.mid;
        if (!mid) {
            return;
        }
        this._remoteTracksByMid.set(mid, event.track);
        this._syncRemoteTrack(mid);
    }

    private _applyCompatUpdate(update: { name: string; payload: unknown }): void {
        switch (update.name) {
            case CLIENT_UPDATE.DISCONNECT: {
                const payload = update.payload as { sessionId: SessionId };
                this._consumers.delete(payload.sessionId);
                for (const [mid, binding] of this._remoteTrackBindings) {
                    if (binding.sessionId === payload.sessionId) {
                        this._remoteTrackBindings.delete(mid);
                        this._remoteTracksByMid.delete(mid);
                    }
                }
                break;
            }
            case CLIENT_UPDATE.CHANNEL_INFO_CHANGE: {
                const payload = update.payload as { state?: RecordingState };
                if (payload.state) {
                    this.recordingState = payload.state;
                }
                break;
            }
            default:
                break;
        }
    }

    private _syncSnapshot(): void {
        this._state = this._protocolCore.state;
        this.availableFeatures = this._protocolCore.features;
        this.recordingState = this._protocolCore.recordingState;
    }

    private _syncRemoteTracks(): void {
        for (const mid of this._remoteTracksByMid.keys()) {
            this._syncRemoteTrack(mid);
        }
    }

    private _syncRemoteTrack(mid: string): void {
        const track = this._remoteTracksByMid.get(mid);
        if (!track) {
            return;
        }
        const binding = this._protocolCore.trackBinding(mid);
        const previousBinding = this._remoteTrackBindings.get(mid);
        if (!binding) {
            if (previousBinding) {
                this._clearConsumer(previousBinding.sessionId, previousBinding.type);
                this._remoteTrackBindings.delete(mid);
            }
            return;
        }
        if (
            previousBinding &&
            previousBinding.active === binding.active &&
            previousBinding.sessionId === binding.sessionId &&
            previousBinding.type === binding.type &&
            this._consumers.get(binding.sessionId)?.[binding.type]?.track === track
        ) {
            return;
        }
        if (previousBinding) {
            this._clearConsumer(previousBinding.sessionId, previousBinding.type);
        }
        const consumers = this._consumers.get(binding.sessionId) ?? EMPTY_CONSUMERS();
        consumers[binding.type] = {
            closed: false,
            paused: !binding.active,
            track
        };
        this._consumers.set(binding.sessionId, consumers);
        this._remoteTrackBindings.set(mid, {
            active: binding.active,
            sessionId: binding.sessionId,
            type: binding.type
        });
        this.dispatchEvent(
            new CustomEvent("update", {
                detail: {
                    name: CLIENT_UPDATE.TRACK,
                    payload: {
                        active: binding.active,
                        sessionId: binding.sessionId,
                        track,
                        type: binding.type
                    }
                }
            })
        );
    }

    private _clearConsumer(sessionId: SessionId, streamType: StreamType): void {
        const consumers = this._consumers.get(sessionId);
        if (!consumers) {
            return;
        }
        consumers[streamType] = null;
        if (!consumers.audio && !consumers.camera && !consumers.screen) {
            this._consumers.delete(sessionId);
        }
    }

    private _cancelTimer(id: number): void {
        const handle = this._timerHandles.get(id);
        if (!handle) {
            return;
        }
        this._clearTimer(handle);
        this._timerHandles.delete(id);
    }

    private _handleRuntimeError(error: unknown): void {
        const resolvedError = error instanceof Error ? error : new Error(String(error));
        for (const callbacks of this._pendingRequestResolvers.values()) {
            callbacks.reject(resolvedError);
        }
        this._pendingRequestResolvers.clear();
        for (const requestKind of Object.values(PENDING_REQUEST_KIND)) {
            const waiters = this._requestWaiters[requestKind];
            while (waiters.length > 0) {
                const callbacks = waiters.shift();
                callbacks?.reject(resolvedError);
            }
        }
        for (const timerId of [...this._timerHandles.keys()]) {
            this._cancelTimer(timerId);
        }
        this._closePeerConnection();
        if (this._webSocket && this._webSocket.readyState < 2) {
            this._webSocket.close(1011);
        }
        this._webSocket = null;
        this.dispatchEvent(
            new CustomEvent("handledError", {
                detail: {
                    error: resolvedError
                }
            })
        );
    }
}

function normalizeWebSocketUrl(url: string): string {
    return url.replace(/^http(s?):/i, (_match, secure) => (secure ? "wss:" : "ws:"));
}

function validateConnectOptions(options: ConnectOptions): void {
    if (options.channelUUID !== undefined && typeof options.channelUUID !== "string") {
        throw new Error("connect options channelUUID must be a string when provided");
    }
    if (options.iceServers === undefined) {
        return;
    }
    if (!Array.isArray(options.iceServers)) {
        throw new Error("connect options iceServers must be an array when provided");
    }
    for (const iceServer of options.iceServers) {
        if (!iceServer || typeof iceServer !== "object") {
            throw new Error("each ICE server entry must be an object");
        }
        const { urls } = iceServer;
        if (
            typeof urls !== "string" &&
            !(Array.isArray(urls) && urls.every((url) => typeof url === "string" && url.length > 0))
        ) {
            throw new Error("each ICE server must expose urls as a string or a string array");
        }
    }
}

function cloneIceServers(iceServers?: RTCIceServer[]): RTCIceServer[] | undefined {
    return iceServers?.map((server) => ({
        ...server,
        urls: Array.isArray(server.urls) ? [...server.urls] : server.urls
    }));
}

function validateDownloadStates(states: DownloadStates): void {
    for (const value of [states.audio, states.camera, states.screen]) {
        if (value !== undefined && typeof value !== "boolean") {
            throw new Error("download state flags must be booleans when provided");
        }
    }
}

function validateTrackForStreamType(type: StreamType, track: MediaStreamTrack | null): void {
    if (track === null) {
        return;
    }
    if (
        typeof track !== "object" ||
        typeof track.kind !== "string" ||
        typeof track.id !== "string"
    ) {
        throw new Error("upload track must be a MediaStreamTrack-compatible object");
    }
    if (track.kind !== STREAM_KIND[type]) {
        throw new Error(`${type} uploads require a ${STREAM_KIND[type]} track`);
    }
}
