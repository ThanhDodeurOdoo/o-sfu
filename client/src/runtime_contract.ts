import type {
    AvailableFeatures,
    ClientUpdateDetail,
    ConnectionState,
    DownloadStates,
    RecordingOptions,
    RecordingState,
    SessionId,
    SessionInfo,
    StreamType
} from "./public_api.js";
import type { TrackBinding } from "./protocol.js";
import { defaultProtocolCoreFactory } from "./wasm_runtime.js";

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

export type HostCommand =
    | { kind: "sendWebSocket"; frame: string }
    | {
          kind: "applyNegotiation";
          requestId: string;
          negotiationKind: NegotiationKind;
          sdp: string;
      }
    | { kind: "attachTrack"; mid: string; streamType: StreamType }
    | { kind: "detachTrack"; streamType: StreamType }
    | { kind: "createPeerConnection" }
    | { kind: "closePeerConnection" }
    | { kind: "closeWebSocket"; code: number }
    | { kind: "emitStateChange"; state: ConnectionState; cause?: string }
    | { kind: "emitUpdate"; update: ClientUpdateDetail }
    | {
          kind: "registerPendingRequest";
          requestId: string;
          requestKind: PendingRequestKind;
      }
    | { kind: "resolvePendingRequest"; requestId: string; ok: boolean }
    | { kind: "scheduleTimer"; id: number; ms: number }
    | { kind: "cancelTimer"; id: number }
    | { kind: "connect"; url: string };

export interface ProtocolCoreBindings {
    readonly state: ConnectionState;
    readonly features: AvailableFeatures;
    readonly recordingState: RecordingState;

    connect(url: string, jwt: string, channel?: string | null): HostCommand[];
    onWsOpen(): HostCommand[];
    onWsMessage(frame: string): HostCommand[];
    onTransportReady(): HostCommand[];
    onWsClose(code: number): HostCommand[];
    onTimer(timerId: number): HostCommand[];
    updateUpload(type: StreamType, active: boolean): HostCommand[];
    updateDownload(sessionId: SessionId, states: DownloadStates): HostCommand[];
    updateInfo(info: SessionInfo): HostCommand[];
    broadcast(message: unknown): HostCommand[];
    startRecording(options?: RecordingOptions): HostCommand[];
    stopRecording(): HostCommand[];
    submitNegotiationAnswer(
        requestId: string,
        negotiationKind: NegotiationKind,
        sdp: string
    ): HostCommand[];
    disconnect(): HostCommand[];
    trackBinding(mid: string): TrackBinding | null | undefined;
}

export type ProtocolCoreFactory = () => ProtocolCoreBindings;

let protocolCoreFactory: ProtocolCoreFactory | undefined;

export function configureProtocolCoreFactory(factory: ProtocolCoreFactory): void {
    protocolCoreFactory = factory;
}

export function createProtocolCore(): ProtocolCoreBindings {
    return (protocolCoreFactory ?? defaultProtocolCoreFactory)();
}
