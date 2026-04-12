import type {
    AvailableFeatures,
    DownloadStates,
    RecordingOptions as PublicRecordingOptions,
    RecordingState,
    RecordingStopCode,
    SessionId,
    SessionInfo,
    StreamType
} from "./public_api.js";

export type RequestId = string;

export interface MessageEnvelope<TType extends string, TPayload> {
    t: TType;
    p: TPayload;
}

export interface RequestEnvelope<TType extends string, TPayload = undefined> {
    t: TType;
    q: RequestId;
    p?: TPayload;
}

export interface ResponseEnvelope<TType extends string, TPayload = undefined> {
    t: TType;
    r: RequestId;
    p?: TPayload;
}

export type Envelope =
    | ClientOutboundEnvelope
    | ServerOutboundEnvelope
    | RequestEnvelope<string, unknown>
    | ResponseEnvelope<string, unknown>;

export type EnvelopeBatch = Envelope[];

export interface AuthPayload {
    jwt: string;
    channel?: string;
}

export interface PeerSnapshot {
    sessionId: SessionId;
    info: SessionInfo;
}

export interface WelcomePayload {
    features: AvailableFeatures;
    recording: RecordingState;
    peers: PeerSnapshot[];
}

export interface SessionDescriptionPayload {
    sdp: string;
}

export interface StreamIntentPayload {
    type: StreamType;
}

export interface SubscribePayload extends DownloadStates {
    sessionId: SessionId;
}

export interface TrackBinding {
    mid: string;
    sessionId: SessionId;
    type: StreamType;
    active: boolean;
}

export interface PeerInfoPayload {
    sessionId: SessionId;
    info: SessionInfo;
}

export interface PeerLeftPayload {
    sessionId: SessionId;
}

export interface ClientBroadcastPayload {
    message: unknown;
}

export interface ServerBroadcastPayload {
    senderId: SessionId;
    message: unknown;
}

export interface RecordingActionResult {
    ok: boolean;
}

export interface RecordingChangePayload {
    state: RecordingState;
    stopCode?: RecordingStopCode;
}

export type ClientMessageEnvelope =
    | MessageEnvelope<"auth", AuthPayload>
    | MessageEnvelope<"publish", StreamIntentPayload>
    | MessageEnvelope<"unpublish", StreamIntentPayload>
    | MessageEnvelope<"subscribe", SubscribePayload>
    | MessageEnvelope<"info", SessionInfo>
    | MessageEnvelope<"broadcast", ClientBroadcastPayload>;

export type ClientRequestEnvelope =
    | RequestEnvelope<"startrecording", PublicRecordingOptions>
    | RequestEnvelope<"stoprecording">;

export type ClientResponseEnvelope =
    | ResponseEnvelope<"offer", SessionDescriptionPayload>
    | ResponseEnvelope<"renegotiate", SessionDescriptionPayload>
    | ResponseEnvelope<"ping">;

export type ClientOutboundEnvelope =
    | ClientMessageEnvelope
    | ClientRequestEnvelope
    | ClientResponseEnvelope;

export type ServerMessageEnvelope =
    | MessageEnvelope<"welcome", WelcomePayload>
    | MessageEnvelope<"tracks", TrackBinding[]>
    | MessageEnvelope<"peerinfo", PeerInfoPayload>
    | MessageEnvelope<"peerjoined", PeerInfoPayload>
    | MessageEnvelope<"peerleft", PeerLeftPayload>
    | MessageEnvelope<"broadcast", ServerBroadcastPayload>
    | MessageEnvelope<"recordingchange", RecordingChangePayload>;

export type ServerRequestEnvelope =
    | RequestEnvelope<"offer", SessionDescriptionPayload>
    | RequestEnvelope<"renegotiate", SessionDescriptionPayload>
    | RequestEnvelope<"ping">;

export type ServerResponseEnvelope =
    | ResponseEnvelope<"startrecording", RecordingActionResult>
    | ResponseEnvelope<"stoprecording", RecordingActionResult>;

export type ServerOutboundEnvelope =
    | ServerMessageEnvelope
    | ServerRequestEnvelope
    | ServerResponseEnvelope;

export const WS_CLOSE_CODE = {
    CLEAN: 1000,
    LEAVING: 1001,
    PROTOCOL_ERROR: 1002,
    ERROR: 1011,
    AUTH_FAILED: 4001,
    AUTH_TIMEOUT: 4002,
    KICKED: 4003,
    CHANNEL_FULL: 4004
} as const;
