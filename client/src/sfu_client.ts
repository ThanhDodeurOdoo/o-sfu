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
    type SfuStats,
    type SfuClientSurface,
    type StreamType,
    type UpdateInfoOptions
} from "./public_api.js";
import {
    createProtocolCore,
    wrapProtocolCoreBindings,
    type HostCommand,
    type ProtocolCoreBindings
} from "./runtime_contract.js";
import { BrowserRuntime, type BrowserRuntimeHooks } from "./sfu_client/browser_runtime.js";
import {
    EMPTY_FEATURES,
    type ConsumersCompat,
    type SfuClientDependencies
} from "./sfu_client/browser_types.js";
import { LocalUploads } from "./sfu_client/local_uploads.js";
import { PendingRequests } from "./sfu_client/pending_requests.js";
import { RemoteTracks } from "./sfu_client/remote_tracks.js";
import {
    cloneIceServers,
    normalizeWebSocketUrl,
    validateConnectOptions,
    validateDownloadStates,
    validateTrackForStreamType
} from "./sfu_client/validation.js";

export type { SfuClientDependencies } from "./sfu_client/browser_types.js";

const CLIENT_RECOVERABLE_CLOSE_CODE = 4000;
const CLIENT_LOG_SOURCE = "sfu_client";

export class SfuClient extends EventTarget implements SfuClientSurface {
    public availableFeatures: AvailableFeatures = { ...EMPTY_FEATURES };
    public errors: Error[] = [];
    public recordingState: RecordingState = {};
    public readonly _consumers: ReadonlyMap<SessionId, ConsumersCompat>;

    private readonly _localUploads = new LocalUploads();
    private readonly _pendingRequests = new PendingRequests();
    private readonly _protocolCore: ProtocolCoreBindings;
    private readonly _remoteTracks = new RemoteTracks();
    private readonly _runtime: BrowserRuntime;

    private _iceServers?: RTCIceServer[];
    private _state: ConnectionState = SFU_CLIENT_STATE.DISCONNECTED;

    constructor(dependencies: SfuClientDependencies = {}) {
        super();
        this._protocolCore = dependencies.createProtocolCore
            ? wrapProtocolCoreBindings(dependencies.createProtocolCore())
            : createProtocolCore();
        this._runtime = new BrowserRuntime(dependencies);
        this._consumers = this._remoteTracks.consumers;
        this._syncPublicState();
    }

    get state(): ConnectionState {
        return this._state;
    }

    connect(url: string, jwt: string, options: ConnectOptions = {}): void {
        validateConnectOptions(options);
        this.errors = [];
        this._iceServers = cloneIceServers(options.iceServers);
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            `connect requested for ${options.channelUUID ? `channel ${options.channelUUID}` : "implicit channel"}`
        );
        this._runtime.enqueueProtocolCommands(
            () =>
                this._protocolCore.connect(
                    normalizeWebSocketUrl(url),
                    jwt,
                    options.channelUUID ?? null
                ),
            this._runtimeHooks()
        );
    }

    disconnect(): void {
        this.errors = [];
        this._emitLog(CLIENT_LOG_LEVEL.INFO, "disconnect requested");
        this._runtime.enqueueProtocolCommands(
            () => this._protocolCore.disconnect(),
            this._runtimeHooks()
        );
    }

    publish(type: StreamType, track: MediaStreamTrack | null | undefined): void {
        validateTrackForStreamType(type, track);
        const normalizedTrack = track ?? null;
        const transition = this._localUploads.setTrack(type, normalizedTrack);
        const action =
            transition.hadTrack && transition.hasTrack
                ? "replacing"
                : transition.hadTrack
                  ? "removing"
                  : transition.hasTrack
                    ? "publishing"
                    : "keeping";
        this._emitLog(
            transition.hadTrack && transition.hasTrack
                ? CLIENT_LOG_LEVEL.DEBUG
                : CLIENT_LOG_LEVEL.INFO,
            `${action} ${type} track${transition.knownMid ? ` on mid ${transition.knownMid}` : ""}`
        );

        if (transition.hadTrack && transition.hasTrack) {
            this._runtime.enqueueLocalOperation(async () => {
                if (transition.knownMid) {
                    await this._runtime.attachTrack(transition.knownMid, type, this._localUploads);
                }
            }, this._runtimeHooks());
            return;
        }

        if (transition.hadTrack && !transition.hasTrack) {
            this._runtime.enqueueLocalOperation(async () => {
                await this._runtime.detachTrack(type, this._localUploads);
            }, this._runtimeHooks());
        }

        if (transition.hadTrack === transition.hasTrack) {
            return;
        }

        this._runtime.enqueueProtocolCommands(
            () => this._protocolCore.publish(type, transition.hasTrack),
            this._runtimeHooks()
        );
    }

    subscribe(sessionId: SessionId, states: DownloadStates): void {
        validateDownloadStates(states);
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            `updating download states for session ${sessionId}: ${JSON.stringify(states)}`
        );
        this._runtime.enqueueLocalOperation(async () => {
            this._remoteTracks.updateSubscriptionStates(sessionId, states, (update) => {
                this._emitUpdate(update);
            });
        }, this._runtimeHooks());
        this._runtime.enqueueProtocolCommands(
            () => this._protocolCore.subscribe(sessionId, states),
            this._runtimeHooks()
        );
    }

    /**
     * @deprecated Use `publish()` instead.
     */
    updateUpload(type: StreamType, track: MediaStreamTrack | null | undefined): void {
        this.publish(type, track);
    }

    /**
     * @deprecated Use `subscribe()` instead.
     */
    updateDownload(sessionId: SessionId, states: DownloadStates): void {
        this.subscribe(sessionId, states);
    }

    updateInfo(info: SessionInfo, _options: UpdateInfoOptions = {}): void {
        this._emitLog(CLIENT_LOG_LEVEL.DEBUG, `updating session info: ${JSON.stringify(info)}`);
        this._runtime.enqueueProtocolCommands(
            () => this._protocolCore.updateInfo(info),
            this._runtimeHooks()
        );
    }

    async getStats(): Promise<SfuStats> {
        return this._runtime.getStats(this._localUploads);
    }

    broadcast(message: unknown): void {
        this._emitLog(CLIENT_LOG_LEVEL.DEBUG, `broadcast requested: ${JSON.stringify(message)}`);
        this._runtime.enqueueProtocolCommands(
            () => this._protocolCore.broadcast(message),
            this._runtimeHooks()
        );
    }

    startRecording(options: RecordingOptions = {}): Promise<boolean> {
        return this._beginPendingRequest(() => this._protocolCore.startRecording(options));
    }

    stopRecording(): Promise<boolean> {
        return this._beginPendingRequest(() => this._protocolCore.stopRecording());
    }

    private _beginPendingRequest(getCommands: () => HostCommand[]): Promise<boolean> {
        return this._pendingRequests.begin(
            getCommands,
            (commands) => {
                this._runtime.enqueue(commands, this._runtimeHooks());
            },
            (error) => {
                this._handleRuntimeError(error);
            }
        );
    }

    private _syncPublicState(): void {
        this._state = this._protocolCore.state;
        this.availableFeatures = this._protocolCore.features;
        this.recordingState = this._protocolCore.recordingState;
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
        this._applyCompatUpdate(update);
        this._logUpdate(update);
        this.dispatchEvent(
            new CustomEvent("update", {
                detail: update
            })
        );
    }

    private _applyCompatUpdate(update: ClientUpdateDetail): void {
        if (
            update.name === CLIENT_UPDATE.CHANNEL_INFO_CHANGE &&
            update.payload &&
            typeof update.payload === "object" &&
            "state" in update.payload &&
            update.payload.state
        ) {
            this.recordingState = update.payload.state;
        }
    }

    private _handleRuntimeError(error: unknown): void {
        const resolvedError = error instanceof Error ? error : new Error(String(error));
        this.errors.push(resolvedError);
        this._emitLog(CLIENT_LOG_LEVEL.ERROR, `runtime error: ${resolvedError.message}`);
        this._protocolCore.disconnect();
        this._pendingRequests.rejectAll(resolvedError);
        this._runtime.teardown(this._runtimeHooks(), CLIENT_RECOVERABLE_CLOSE_CODE);
        this._syncPublicState();
        this.dispatchEvent(
            new CustomEvent("handledError", {
                detail: {
                    error: resolvedError
                }
            })
        );
    }

    private _runtimeHooks(): BrowserRuntimeHooks {
        return {
            iceServers: this._iceServers,
            localUploads: this._localUploads,
            onLog: (detail) => {
                this._emitRuntimeLog(detail);
            },
            onRuntimeError: (error) => {
                this._handleRuntimeError(error);
            },
            onStateChange: (state, cause) => {
                this._emitStateChange(state, cause);
            },
            onUpdate: (update) => {
                this._emitUpdate(update);
            },
            pendingRequests: this._pendingRequests,
            protocolCore: this._protocolCore,
            remoteTracks: this._remoteTracks,
            syncPublicState: () => {
                this._syncPublicState();
            }
        };
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
            case CLIENT_UPDATE.DISCONNECT:
                this._emitLog(
                    CLIENT_LOG_LEVEL.INFO,
                    `session ${update.payload.sessionId} disconnected`
                );
                break;
            case CLIENT_UPDATE.INFO_CHANGE:
                this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received remote session info update: ${JSON.stringify(update.payload)}`
                );
                break;
            case CLIENT_UPDATE.BROADCAST:
                this._emitLog(
                    CLIENT_LOG_LEVEL.DEBUG,
                    `received broadcast from session ${update.payload.senderId}`
                );
                break;
            case CLIENT_UPDATE.CHANNEL_INFO_CHANGE:
                this._emitLog(CLIENT_LOG_LEVEL.DEBUG, "received channel info update");
                break;
        }
    }
}
