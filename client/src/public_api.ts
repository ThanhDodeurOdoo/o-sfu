export type SessionId = number | string;

export type ConnectionState =
    | "disconnected"
    | "connecting"
    | "authenticated"
    | "connected"
    | "recovering"
    | "closed";

export type StreamType = "audio" | "camera" | "screen";

export interface ConnectOptions {
    channelUUID?: string;
    iceServers?: RTCIceServer[];
}

export interface DownloadStates {
    audio?: boolean;
    camera?: boolean;
    screen?: boolean;
}

export interface SessionInfo {
    isTalking?: boolean;
    isCameraOn?: boolean;
    isScreenSharingOn?: boolean;
    isSelfMuted?: boolean;
    isDeaf?: boolean;
    isRaisingHand?: boolean;
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

export type RecordingStopCode =
    | "user_request"
    | "channel_closed"
    | "recording_timeout"
    | "recording_failed"
    | "disk_space_exhausted";

export const CLIENT_UPDATE = {
    TRACK: "TRACK",
    DISCONNECT: "DISCONNECT",
    INFO_CHANGE: "INFO_CHANGE",
    BROADCAST: "BROADCAST",
    CHANNEL_INFO_CHANGE: "CHANNEL_INFO_CHANGE"
} as const;

export type ClientUpdateName = (typeof CLIENT_UPDATE)[keyof typeof CLIENT_UPDATE];

export interface TrackUpdateDetail {
    sessionId: SessionId;
    type: StreamType;
    track: MediaStreamTrack;
    active: boolean;
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
    | { name: typeof CLIENT_UPDATE.DISCONNECT; payload: DisconnectUpdateDetail }
    | { name: typeof CLIENT_UPDATE.INFO_CHANGE; payload: InfoChangeUpdateDetail }
    | { name: typeof CLIENT_UPDATE.BROADCAST; payload: BroadcastUpdateDetail }
    | { name: typeof CLIENT_UPDATE.CHANNEL_INFO_CHANGE; payload: ChannelInfoChangeDetail };

export interface StateChangeDetail {
    state: ConnectionState;
    cause?: string;
}

export interface SfuClientSurface extends EventTarget {
    readonly state: ConnectionState;
    readonly availableFeatures: AvailableFeatures;
    readonly recordingState: RecordingState;

    connect(url: string, jwt: string, options?: ConnectOptions): void;
    disconnect(): void;
    updateUpload(type: StreamType, track: MediaStreamTrack | null): void;
    updateDownload(sessionId: SessionId, states: DownloadStates): void;
    updateInfo(info: SessionInfo): void;
    broadcast(message: unknown): void;
    startRecording(options?: RecordingOptions): Promise<boolean>;
    stopRecording(): Promise<boolean>;
}
