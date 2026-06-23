import type {
    AvailableFeatures,
    DownloadStates,
    RecordingOptions,
    RecordingState,
    RecordingStopCode,
    SessionId,
    SessionInfo,
    SourceDescriptor,
    SourceEncodingDescriptor,
    StreamType
} from "./public_api.js";
export { RECORDING_STOP_CODES, SOURCE_ENCODING_POLICY_ROLES, STREAM_TYPES } from "./public_api.js";

export type {
    AvailableFeatures,
    DownloadStates,
    RecordingOptions,
    RecordingState,
    RecordingStopCode,
    SessionId,
    SessionInfo,
    SourceDescriptor,
    SourceEncodingDescriptor,
    SourceEncodingPolicyRole,
    StreamType,
    VideoLayoutIntent
} from "./public_api.js";

export type MediaKind = "audio" | "video";

export type UploadLayerPolicyRole = NonNullable<SourceEncodingDescriptor["policyRole"]>;

export type RequestId = string;

export type AuthPayload = {
    jwt: string;
    channel?: string;
};

export type RecordingStateUpdate = {
    state: RecordingState;
    stopCode?: RecordingStopCode;
};

export type RecordingChangePayload = RecordingStateUpdate;

export type PeerSnapshot = {
    sessionId: SessionId;
    info: SessionInfo;
};

export type WelcomePayload = {
    features: AvailableFeatures;
    recording: RecordingState;
    peers: PeerSnapshot[];
};

export type NegotiationUploadSlot = {
    mid: string;
    kind: MediaKind;
    codecs?: string[];
    simulcastEncodings?: NegotiationUploadEncoding[];
};

export type NegotiationUploadEncoding = {
    rid: string;
    maxBitrate?: number;
    resolutionScale?: number;
    maxFramerate?: number;
};

export type SessionDescriptionPayload = {
    sdp: string;
    uploadSlots?: NegotiationUploadSlot[];
};

export type StreamIntentPayload = {
    type: StreamType;
};

export type SubscribePayload = {
    sessionId: SessionId;
} & DownloadStates;

export type TrackBinding = {
    mid: string;
    sessionId: SessionId;
    type: StreamType;
    active: boolean;
    source?: SourceDescriptor;
};

export type PeerInfoPayload = {
    sessionId: SessionId;
    info: SessionInfo;
};

export type PeerLeftPayload = {
    sessionId: SessionId;
};

export type ClientBroadcastPayload = {
    message: unknown;
};

export type ServerBroadcastPayload = {
    senderId: SessionId;
    message: unknown;
};

export type RecordingActionResult = {
    ok: boolean;
};

export const UPLOAD_KINDS = ["audio", "video"] as const satisfies readonly MediaKind[];

export const NEGOTIATION_KIND = {
    OFFER: "offer",
    RENEGOTIATE: "renegotiate"
} as const;

export type NegotiationKind = (typeof NEGOTIATION_KIND)[keyof typeof NEGOTIATION_KIND];

export const PENDING_REQUEST_KIND = {
    START_RECORDING: "startRecording",
    STOP_RECORDING: "stopRecording"
} as const;

export type PendingRequestKind = (typeof PENDING_REQUEST_KIND)[keyof typeof PENDING_REQUEST_KIND];

export const COMMAND_KIND = {
    CONNECT: "connect",
    SEND_WEB_SOCKET: "sendWebSocket",
    SET_LOCAL_UPLOAD_INTENT: "setLocalUploadIntent",
    CLOSE_WEB_SOCKET: "closeWebSocket",
    APPLY_NEGOTIATION: "applyNegotiation",
    CREATE_PEER_CONNECTION: "createPeerConnection",
    CLOSE_PEER_CONNECTION: "closePeerConnection",
    ATTACH_TRACK: "attachTrack",
    DETACH_TRACK: "detachTrack",
    REPLACE_TRACK_BINDINGS: "replaceTrackBindings",
    REMOVE_SESSION_TRACKS: "removeSessionTracks",
    EMIT_STATE_CHANGE: "emitStateChange",
    EMIT_UPDATE: "emitUpdate",
    BEGIN_PENDING_REQUEST: "beginPendingRequest",
    RESOLVE_PENDING_REQUEST: "resolvePendingRequest",
    SCHEDULE_TIMER: "scheduleTimer",
    CANCEL_TIMER: "cancelTimer"
} as const;

export const WS_CLOSE_CODE = {
    CLEAN: 1000,
    LEAVING: 1001,
    PROTOCOL_ERROR: 1002,
    ERROR: 1011,
    AUTH_FAILED: 4106,
    AUTH_TIMEOUT: 4107,
    KICKED: 4108,
    CHANNEL_FULL: 4109
} as const;

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

export type ClientMessageEnvelope =
    | MessageEnvelope<"auth", AuthPayload>
    | MessageEnvelope<"publish", StreamIntentPayload>
    | MessageEnvelope<"unpublish", StreamIntentPayload>
    | MessageEnvelope<"subscribe", SubscribePayload>
    | MessageEnvelope<"info", SessionInfo>
    | MessageEnvelope<"broadcast", ClientBroadcastPayload>;

export type ClientRequestEnvelope =
    | RequestEnvelope<"startrecording", RecordingOptions>
    | RequestEnvelope<"stoprecording">;

export type ClientResponseEnvelope =
    | ResponseEnvelope<"offer", SessionDescriptionPayload>
    | ResponseEnvelope<"renegotiate", SessionDescriptionPayload>;

export type ClientOutboundEnvelope =
    | ClientMessageEnvelope
    | ClientRequestEnvelope
    | ClientResponseEnvelope;

export type ServerMessageEnvelope =
    | MessageEnvelope<"welcome", WelcomePayload>
    | MessageEnvelope<"tracks", TrackBinding[]>
    | MessageEnvelope<"sources", SourceDescriptor[]>
    | MessageEnvelope<"peerinfo", PeerInfoPayload>
    | MessageEnvelope<"peerjoined", PeerInfoPayload>
    | MessageEnvelope<"peerleft", PeerLeftPayload>
    | MessageEnvelope<"broadcast", ServerBroadcastPayload>
    | MessageEnvelope<"recordingchange", RecordingStateUpdate>;

export type ServerRequestEnvelope =
    | RequestEnvelope<"offer", SessionDescriptionPayload>
    | RequestEnvelope<"renegotiate", SessionDescriptionPayload>;

export type ServerResponseEnvelope =
    | ResponseEnvelope<"startrecording", RecordingActionResult>
    | ResponseEnvelope<"stoprecording", RecordingActionResult>;

export type ServerOutboundEnvelope =
    | ServerMessageEnvelope
    | ServerRequestEnvelope
    | ServerResponseEnvelope;

export type Envelope =
    | ClientOutboundEnvelope
    | ServerOutboundEnvelope
    | RequestEnvelope<string, unknown>
    | ResponseEnvelope<string, unknown>;

export type EnvelopeBatch = Envelope[];
