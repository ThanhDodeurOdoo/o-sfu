import type {
    COMMAND_KIND,
    NegotiationKind,
    NegotiationUploadSlot,
    TrackBinding
} from "./protocol_contract.js";
import type {
    AvailableFeatures,
    ClientUpdateDetail,
    ConnectionState,
    SfuRecordingState
} from "./public_api.js";

export const REMOTE_MEDIA_UPDATE = "remote_media";

type RemoteMediaUpdate = {
    name: typeof REMOTE_MEDIA_UPDATE;
    payload: { bindings: TrackBinding[] };
};

type HostUpdate = ClientUpdateDetail | RemoteMediaUpdate;

export type PendingRequest = {
    requestId: string;
    timeoutTimerId: number;
    timeoutMs: number;
};

export type HostCommand =
    | { kind: typeof COMMAND_KIND.SEND_WEB_SOCKET; frame: string }
    | {
          kind: typeof COMMAND_KIND.APPLY_NEGOTIATION;
          requestId: string;
          negotiationKind: NegotiationKind;
          sdp: string;
          uploadSlots: NegotiationUploadSlot[];
      }
    | { kind: typeof COMMAND_KIND.CLOSE_PEER_CONNECTION }
    | { kind: typeof COMMAND_KIND.CLOSE_WEB_SOCKET; code: number }
    | { kind: typeof COMMAND_KIND.SET_AVAILABLE_FEATURES; features: AvailableFeatures }
    | { kind: typeof COMMAND_KIND.SET_RECORDING_STATE; state: SfuRecordingState }
    | { kind: typeof COMMAND_KIND.EMIT_STATE_CHANGE; state: ConnectionState; cause?: string }
    | { kind: typeof COMMAND_KIND.EMIT_UPDATE; update: HostUpdate }
    | { kind: typeof COMMAND_KIND.BEGIN_PENDING_REQUEST; request: PendingRequest }
    | {
          kind: typeof COMMAND_KIND.COMPLETE_PENDING_REQUEST;
          requestId: string;
          timeoutTimerId: number;
          ok: boolean;
      }
    | { kind: typeof COMMAND_KIND.SCHEDULE_TIMER; id: number; ms: number }
    | { kind: typeof COMMAND_KIND.CANCEL_TIMER; id: number }
    | { kind: typeof COMMAND_KIND.CONNECT; url: string };
