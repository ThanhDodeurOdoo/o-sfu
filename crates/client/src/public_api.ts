export type SessionId = number | string;

export const SFU_CLIENT_STATE = {
    DISCONNECTED: "disconnected",
    CONNECTING: "connecting",
    AUTHENTICATED: "authenticated",
    CONNECTED: "connected",
    RECOVERING: "recovering",
    CLOSED: "closed"
} as const;

export type ConnectionState = (typeof SFU_CLIENT_STATE)[keyof typeof SFU_CLIENT_STATE];

export const STREAM_TYPES = ["audio", "camera", "screen"] as const;

export type StreamType = (typeof STREAM_TYPES)[number];

export type PublishedSourceId = string;

export type SourceEncodingId = string;

export const VIDEO_LAYOUT_INTENTS = [
    "featured",
    "pinned",
    "visible_thumbnail",
    "hidden",
    "overflow"
] as const;

export type VideoLayoutIntent = (typeof VIDEO_LAYOUT_INTENTS)[number];

export interface ConnectOptions {
    channelUUID?: string;
    iceServers?: RTCIceServer[];
}

export interface DownloadStates {
    audio?: boolean;
    camera?: boolean;
    screen?: boolean;
    cameraLayout?: VideoLayoutIntent;
    screenLayout?: VideoLayoutIntent;
}

export const SOURCE_ENCODING_POLICY_ROLES = ["featured", "thumbnail", "degradedThumbnail"] as const;

export type SourceEncodingPolicyRole = (typeof SOURCE_ENCODING_POLICY_ROLES)[number];

export interface SourceEncodingDescriptor {
    encodingId: SourceEncodingId;
    rid?: string;
    maxBitrate?: number;
    resolutionScale?: number;
    maxFramerate?: number;
    policyRole?: SourceEncodingPolicyRole;
}

export interface SourceDescriptor {
    sourceId: PublishedSourceId;
    sessionId: SessionId;
    type: StreamType;
    active: boolean;
    mid?: string;
    encodings: SourceEncodingDescriptor[];
}

export interface SessionInfo {
    isTalking?: boolean;
    isFeatured?: boolean;
    isCameraOn?: boolean;
    isScreenSharingOn?: boolean;
    isSelfMuted?: boolean;
    isDeaf?: boolean;
    isRaisingHand?: boolean;
}

export interface UpdateInfoOptions {
    /**
     * @deprecated The welcome payload already carries the current peer snapshot,
     * so the browser shell keeps this legacy flag as a compatibility no-op.
     */
    needRefresh?: boolean;
}

export interface AvailableFeatures {
    rtc: boolean;
    transcription: boolean;
    audioRecording: boolean;
    videoRecording: boolean;
}

export interface RecordingState {
    recording?: boolean;
    audio?: boolean;
    video?: boolean;
    transcription?: boolean;
}

export interface RecordingOptions {
    audio?: boolean;
    video?: boolean;
    transcription?: boolean;
}

export const CLIENT_LOG_LEVEL = {
    DEBUG: "debug",
    INFO: "info",
    WARN: "warn",
    ERROR: "error"
} as const;

export type ClientLogLevel = (typeof CLIENT_LOG_LEVEL)[keyof typeof CLIENT_LOG_LEVEL];

export interface ClientLogDetail {
    id: string;
    level: ClientLogLevel;
    message: string;
}

export interface SfuStats {
    uploadStats?: RTCStatsReport;
    downloadStats?: RTCStatsReport;
    audio?: RTCStatsReport;
    camera?: RTCStatsReport;
    screen?: RTCStatsReport;
}

export const RECORDING_STOP_CODES = [
    "user_request",
    "channel_closed",
    "recording_timeout",
    "recording_failed",
    "disk_space_exhausted"
] as const;

export type RecordingStopCode = (typeof RECORDING_STOP_CODES)[number];

export const CLIENT_UPDATE = {
    TRACK: "track",
    SOURCE: "source",
    DISCONNECT: "disconnect",
    INFO_CHANGE: "info_change",
    BROADCAST: "broadcast",
    CHANNEL_INFO_CHANGE: "channel_info_change"
} as const;

export type SfuClientState = ConnectionState;

export type ClientUpdateName = (typeof CLIENT_UPDATE)[keyof typeof CLIENT_UPDATE];

export interface TrackUpdateDetail {
    sessionId: SessionId;
    type: StreamType;
    track: MediaStreamTrack;
    active: boolean;
}

export interface SourceUpdateDetail {
    sources: SourceDescriptor[];
}

export interface DisconnectUpdateDetail {
    sessionId: SessionId;
}

export type InfoChangeUpdateDetail = Record<string, SessionInfo>;

export interface BroadcastUpdateDetail {
    senderId: SessionId;
    message: unknown;
}

export interface ChannelInfoChangeDetail {
    state: RecordingState;
    stopCode?: RecordingStopCode;
}

export type ClientUpdateDetail =
    | { name: typeof CLIENT_UPDATE.TRACK; payload: TrackUpdateDetail }
    | { name: typeof CLIENT_UPDATE.SOURCE; payload: SourceUpdateDetail }
    | { name: typeof CLIENT_UPDATE.DISCONNECT; payload: DisconnectUpdateDetail }
    | { name: typeof CLIENT_UPDATE.INFO_CHANGE; payload: InfoChangeUpdateDetail }
    | { name: typeof CLIENT_UPDATE.BROADCAST; payload: BroadcastUpdateDetail }
    | {
          name: typeof CLIENT_UPDATE.CHANNEL_INFO_CHANGE;
          payload: ChannelInfoChangeDetail;
      };

export interface StateChangeDetail {
    state: ConnectionState;
    cause?: string;
}

export interface SfuClientSurface extends EventTarget {
    readonly state: ConnectionState;
    readonly errors: Error[];
    readonly availableFeatures: AvailableFeatures;
    readonly recordingState: RecordingState;
    readonly sourceDescriptors: readonly SourceDescriptor[];

    connect(url: string, jwt: string, options?: ConnectOptions): void;
    disconnect(): void;
    publish(type: StreamType, track: MediaStreamTrack | null | undefined): void;
    subscribe(sessionId: SessionId, states: DownloadStates): void;
    /**
     * @deprecated Odoo compatibility alias. Use `publish()` for new code.
     */
    updateUpload(type: StreamType, track: MediaStreamTrack | null | undefined): void;
    /**
     * @deprecated Odoo compatibility alias. Use `subscribe()` for new code.
     */
    updateDownload(sessionId: SessionId, states: DownloadStates): void;
    updateInfo(info: SessionInfo, options?: UpdateInfoOptions): void;
    getStats(): Promise<SfuStats>;
    broadcast(message: unknown): void;
    startRecording(options?: RecordingOptions): Promise<boolean>;
    stopRecording(): Promise<boolean>;
}
