import {
    CLIENT_UPDATE,
    CLIENT_LOG_LEVEL,
    SFU_CLIENT_STATE,
    type AvailableFeatures,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectOptions,
    type ConnectionState,
    type DownloadStates,
    type RecordingOptions,
    type RecordingState,
    type SessionId,
    type SessionInfo,
    type SourceDescriptor,
    type SfuStats,
    type StreamType,
    type UpdateInfoOptions
} from "./public_api.js";
import { BrowserRuntime } from "./internals/browser_runtime.js";
import {
    EMPTY_FEATURES,
    type ConsumersCompat,
    type SfuClientDependencies
} from "./internals/browser_types.js";
import {
    cloneIceServers,
    normalizeWebSocketUrl,
    validateConnectOptions,
    validateDownloadStates,
    validateTrackForStreamType
} from "./internals/validation.js";

const CLIENT_LOG_SOURCE = "sfu_client";

export class SfuClient extends EventTarget {
    public availableFeatures: AvailableFeatures = { ...EMPTY_FEATURES };
    public errors: Error[] = [];
    public recordingState: RecordingState = {};
    public sourceDescriptors: readonly SourceDescriptor[] = [];
    public readonly _consumers: ReadonlyMap<SessionId, ConsumersCompat>;

    private readonly _runtime: BrowserRuntime;

    private _state: ConnectionState = SFU_CLIENT_STATE.DISCONNECTED;

    constructor(dependencies: SfuClientDependencies = {}) {
        super();
        this._runtime = new BrowserRuntime(
            {
                onLog: (detail) => this._emitRuntimeLog(detail),
                onRuntimeError: (error) => this._handleRuntimeError(error),
                onPublicState: ({ state, features, recordingState, sourceDescriptors }) => {
                    this._state = state;
                    this.availableFeatures = features;
                    this.recordingState = recordingState;
                    this.sourceDescriptors = sourceDescriptors;
                },
                onStateChange: (state, cause) => this._emitStateChange(state, cause),
                onUpdate: (update) => this._emitUpdate(update)
            },
            dependencies
        );
        this._consumers = this._runtime.consumers;
    }

    get state(): ConnectionState {
        return this._state;
    }

    connect(url: string, jwt: string, options: ConnectOptions = {}): void {
        validateConnectOptions(options);
        const iceServers = cloneIceServers(options.iceServers);
        this.errors = [];
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            `connect requested for ${options.channelUUID ? `room ${options.channelUUID}` : "implicit room"}`
        );
        this._runtime.connect(
            normalizeWebSocketUrl(url),
            jwt,
            options.channelUUID ?? null,
            iceServers
        );
    }

    disconnect(): void {
        this.errors = [];
        this._emitLog(CLIENT_LOG_LEVEL.INFO, "disconnect requested");
        this._runtime.disconnect();
    }

    publish(type: StreamType, track: MediaStreamTrack | null | undefined): void {
        validateTrackForStreamType(type, track);
        this._runtime.publish(type, track ?? null);
    }

    subscribe(sessionId: SessionId, states: DownloadStates): void {
        validateDownloadStates(states);
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            `updating download states for user ${sessionId}: ${JSON.stringify(states)}`
        );
        this._runtime.subscribe(sessionId, states);
    }

    /** @deprecated Odoo compatibility alias. Use `publish()` for new code. */
    updateUpload(type: StreamType, track: MediaStreamTrack | null | undefined): void {
        this.publish(type, track);
    }

    /** @deprecated Odoo compatibility alias. Use `subscribe()` for new code. */
    updateDownload(sessionId: SessionId, states: DownloadStates): void {
        this.subscribe(sessionId, states);
    }

    updateInfo(info: SessionInfo, _options: UpdateInfoOptions = {}): void {
        this._emitLog(CLIENT_LOG_LEVEL.DEBUG, `updating user info: ${JSON.stringify(info)}`);
        this._runtime.updateInfo(info);
    }

    async getStats(): Promise<SfuStats> {
        return this._runtime.getStats();
    }

    broadcast(message: unknown): void {
        this._emitLog(CLIENT_LOG_LEVEL.DEBUG, `broadcast requested: ${JSON.stringify(message)}`);
        this._runtime.broadcast(message);
    }

    startRecording(options: RecordingOptions = {}): Promise<boolean> {
        return this._runtime.startRecording(options);
    }

    stopRecording(): Promise<boolean> {
        return this._runtime.stopRecording();
    }

    private _emitStateChange(state: ConnectionState, cause?: string): void {
        this._state = state;
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            cause ? `state changed to ${state} (cause: ${cause})` : `state changed to ${state}`
        );
        this.dispatchEvent(
            new CustomEvent("stateChange", {
                detail: {
                    cause,
                    state
                }
            })
        );
    }

    private _emitUpdate(update: ClientUpdateDetail): void {
        this._logUpdate(update);
        this.dispatchEvent(
            new CustomEvent("update", {
                detail: update
            })
        );
    }

    private _handleRuntimeError(error: Error): void {
        this.errors.push(error);
        this._emitLog(CLIENT_LOG_LEVEL.ERROR, `runtime error: ${error.message}`);
        this.dispatchEvent(
            new CustomEvent("handledError", {
                detail: {
                    error
                }
            })
        );
    }

    private _emitRuntimeLog(detail: ClientLogDetail): void {
        this.dispatchEvent(new CustomEvent("log", { detail }));
    }

    private _emitLog(level: ClientLogDetail["level"], message: string): void {
        this._emitRuntimeLog({
            id: CLIENT_LOG_SOURCE,
            level,
            message
        });
    }

    private _logUpdate(update: ClientUpdateDetail): void {
        switch (update.name) {
            case CLIENT_UPDATE.TRACK:
                this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `remote ${update.payload.type} track update for session ${update.payload.sessionId}: active=${update.payload.active}, muted=${update.payload.track.muted}, readyState=${update.payload.track.readyState}`
                );
                break;
            case CLIENT_UPDATE.SOURCE:
                this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received ${update.payload.sources.length} remote source descriptors`
                );
                break;
            case CLIENT_UPDATE.DISCONNECT:
                this._emitLog(
                    CLIENT_LOG_LEVEL.INFO,
                    `session ${update.payload.sessionId} disconnected`
                );
                break;
            case CLIENT_UPDATE.INFO_CHANGE:
                this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received remote user info update: ${JSON.stringify(update.payload)}`
                );
                break;
            case CLIENT_UPDATE.BROADCAST:
                this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received broadcast from user ${update.payload.senderId}`
                );
                break;
            case CLIENT_UPDATE.CHANNEL_INFO_CHANGE:
                this._emitLog(CLIENT_LOG_LEVEL.DEBUG, "received channel info update");
                break;
        }
    }
}
