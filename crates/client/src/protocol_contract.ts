import type { SessionId, StreamType } from "./public_api.js";

export const UPLOAD_KINDS = ["audio", "video"] as const;

type MediaKind = (typeof UPLOAD_KINDS)[number];
type NegotiationUploadEncoding = {
    rid: string;
    maxBitrate?: number;
    resolutionScale?: number;
    maxFramerate?: number;
};

export type NegotiationUploadSlot = {
    mid: string;
    kind: MediaKind;
    codecs?: string[];
    simulcastEncodings?: NegotiationUploadEncoding[];
};

export type TrackBinding = {
    mid: string;
    sessionId: SessionId;
    type: StreamType;
    active: boolean;
};

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
