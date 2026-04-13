import {
    CLIENT_UPDATE,
    SFU_CLIENT_STATE,
    type AvailableFeatures,
    type ClientUpdateDetail,
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

export class SfuClient extends EventTarget implements SfuClientSurface {
    public availableFeatures: AvailableFeatures = { ...EMPTY_FEATURES };
    public recordingState: RecordingState = {};
    public _consumers: Map<SessionId, ConsumersCompat>;

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
        this._iceServers = cloneIceServers(options.iceServers);
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
        this._runtime.enqueueProtocolCommands(
            () => this._protocolCore.disconnect(),
            this._runtimeHooks()
        );
    }

    publish(type: StreamType, track: MediaStreamTrack | null): void {
        validateTrackForStreamType(type, track);
        const transition = this._localUploads.setTrack(type, track);

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
        this._runtime.enqueueProtocolCommands(
            () => this._protocolCore.subscribe(sessionId, states),
            this._runtimeHooks()
        );
    }

    /**
     * @deprecated Use `publish()` instead.
     */
    updateUpload(type: StreamType, track: MediaStreamTrack | null): void {
        this.publish(type, track);
    }

    /**
     * @deprecated Use `subscribe()` instead.
     */
    updateDownload(sessionId: SessionId, states: DownloadStates): void {
        this.subscribe(sessionId, states);
    }

    updateInfo(info: SessionInfo, _options: UpdateInfoOptions = {}): void {
        this._runtime.enqueueProtocolCommands(
            () => this._protocolCore.updateInfo(info),
            this._runtimeHooks()
        );
    }

    broadcast(message: unknown): void {
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
        this._remoteTracks.syncAll(this._protocolCore, (update) => {
            this._emitUpdate(update);
        });
    }

    private _emitStateChange(state: ConnectionState, cause?: string): void {
        this._state = state;
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
        this.dispatchEvent(
            new CustomEvent("update", {
                detail: update
            })
        );
    }

    private _applyCompatUpdate(update: ClientUpdateDetail): void {
        this._remoteTracks.applyCompatUpdate(update);
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
        this._protocolCore.disconnect();
        this._pendingRequests.rejectAll(resolvedError);
        this._runtime.teardown(this._runtimeHooks(), 1011);
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
}
