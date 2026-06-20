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
    type SfuClientSurface,
    type StreamType,
    type UpdateInfoOptions
} from "./public_api.js";
import {
    COMMAND_KIND,
    createProtocolCore,
    wrapProtocolCoreBindings,
    type HostCommand,
    type ProtocolCoreBindings
} from "./runtime_contract.js";
import { BrowserRuntime, CLIENT_RECOVERABLE_CLOSE_CODE } from "./internals/browser_runtime.js";
import {
    EMPTY_FEATURES,
    type ConsumersCompat,
    type SfuClientDependencies
} from "./internals/browser_types.js";
import { LocalUploads } from "./internals/local_uploads.js";
import { PendingRequests } from "./internals/pending_requests.js";
import { RemoteTracks } from "./internals/remote_tracks.js";
import {
    cloneIceServers,
    normalizeWebSocketUrl,
    validateConnectOptions,
    validateDownloadStates,
    validateTrackForStreamType
} from "./internals/validation.js";

const CLIENT_LOG_SOURCE = "sfu_client";

export class SfuClient extends EventTarget implements SfuClientSurface {
    public availableFeatures: AvailableFeatures = { ...EMPTY_FEATURES };
    public errors: Error[] = [];
    public recordingState: RecordingState = {};
    public sourceDescriptors: readonly SourceDescriptor[] = [];
    /**
     * Compatibility/debug view of the remote consumer map kept for Discuss
     * diagnostics and the bundle contract (exposed to odoo).
     */
    public readonly _consumers: ReadonlyMap<SessionId, ConsumersCompat>;

    private readonly _localUploads = new LocalUploads();
    private readonly _pendingRequests = new PendingRequests();
    private readonly _protocolCore: ProtocolCoreBindings;
    private readonly _remoteTracks = new RemoteTracks();
    private readonly _runtime: BrowserRuntime;

    private _state: ConnectionState = SFU_CLIENT_STATE.DISCONNECTED;

    constructor();
    constructor(dependencies: SfuClientDependencies = {}) {
        super();
        this._protocolCore = dependencies.createProtocolCore
            ? wrapProtocolCoreBindings(dependencies.createProtocolCore())
            : createProtocolCore();
        this._runtime = new BrowserRuntime(
            {
                localUploads: this._localUploads,
                onLog: (detail) => this._emitRuntimeLog(detail),
                onRuntimeError: (error) => this._handleRuntimeError(error),
                onStateChange: (state, cause) => this._emitStateChange(state, cause),
                onUpdate: (update) => this._emitUpdate(update),
                pendingRequests: this._pendingRequests,
                protocolCore: this._protocolCore,
                remoteTracks: this._remoteTracks,
                syncPublicState: () => this._syncPublicState()
            },
            dependencies
        );
        this._consumers = this._remoteTracks.consumers;
        this._syncPublicState();
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
        this._runtime.enqueueProtocolCommands(() => {
            const commands = this._protocolCore.connect(
                normalizeWebSocketUrl(url),
                jwt,
                options.channelUUID ?? null
            );
            if (commands.some((command) => command.kind === COMMAND_KIND.CONNECT)) {
                this._runtime.setIceServers(iceServers);
            }
            return commands;
        });
    }

    disconnect(): void {
        this.errors = [];
        this._emitLog(CLIENT_LOG_LEVEL.INFO, "disconnect requested");
        this._runtime.enqueueProtocolCommands(() => this._protocolCore.disconnect());
    }

    publish(type: StreamType, track: MediaStreamTrack | null | undefined): void {
        validateTrackForStreamType(type, track);
        const transition = this._localUploads.setTrack(type, track ?? null);
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
                    await this._runtime.attachTrack(transition.knownMid, type);
                }
            });
            return;
        }

        if (transition.hadTrack && !transition.hasTrack) {
            this._runtime.enqueueLocalOperation(async () => {
                await this._runtime.detachTrack(type);
            });
        }

        if (transition.hadTrack === transition.hasTrack) {
            return;
        }

        this._runtime.enqueueProtocolCommands(() =>
            this._protocolCore.publish(type, transition.hasTrack)
        );
    }

    subscribe(sessionId: SessionId, states: DownloadStates): void {
        validateDownloadStates(states);
        this._emitLog(
            CLIENT_LOG_LEVEL.INFO,
            `updating download states for user ${sessionId}: ${JSON.stringify(states)}`
        );
        this._runtime.enqueueLocalOperation(() => {
            this._remoteTracks.updateSubscriptionStates(sessionId, states, (update) => {
                this._emitUpdate(update);
            });
        });
        this._runtime.enqueueProtocolCommands(() =>
            this._protocolCore.subscribe(sessionId, states)
        );
    }

    updateUpload(type: StreamType, track: MediaStreamTrack | null | undefined): void {
        this.publish(type, track);
    }

    updateDownload(sessionId: SessionId, states: DownloadStates): void {
        this.subscribe(sessionId, states);
    }

    updateInfo(info: SessionInfo, _options: UpdateInfoOptions = {}): void {
        this._emitLog(CLIENT_LOG_LEVEL.DEBUG, `updating user info: ${JSON.stringify(info)}`);
        this._runtime.enqueueProtocolCommands(() => this._protocolCore.updateInfo(info));
    }

    async getStats(): Promise<SfuStats> {
        return this._runtime.getStats();
    }

    broadcast(message: unknown): void {
        this._emitLog(CLIENT_LOG_LEVEL.DEBUG, `broadcast requested: ${JSON.stringify(message)}`);
        this._runtime.enqueueProtocolCommands(() => this._protocolCore.broadcast(message));
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
            (commands) => this._runtime.enqueue(commands),
            (error) => this._handleRuntimeError(error)
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
        if (update.name === CLIENT_UPDATE.CHANNEL_INFO_CHANGE) {
            this.recordingState = update.payload.state;
            return;
        }
        if (update.name === CLIENT_UPDATE.SOURCE) {
            this.sourceDescriptors = update.payload.sources;
        }
    }

    private _handleRuntimeError(error: unknown): void {
        const resolvedError = error instanceof Error ? error : new Error(String(error));
        this.errors.push(resolvedError);
        this.sourceDescriptors = [];
        this._emitLog(CLIENT_LOG_LEVEL.ERROR, `runtime error: ${resolvedError.message}`);
        this._protocolCore.disconnect();
        this._pendingRequests.rejectAll(resolvedError);
        this._runtime.teardown(CLIENT_RECOVERABLE_CLOSE_CODE);
        this._syncPublicState();
        this.dispatchEvent(
            new CustomEvent("handledError", {
                detail: {
                    error: resolvedError
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
