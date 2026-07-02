import {
    COMMAND_KIND,
    NEGOTIATION_KIND,
    PENDING_REQUEST_KIND,
    UPLOAD_KINDS,
    type NegotiationKind,
    type NegotiationUploadSlot,
    type PendingRequestKind,
    type TrackBinding
} from "./protocol_contract.js";
import type { ClientUpdateDetail, ConnectionState } from "./public_api.js";
import {
    asRecord,
    requireBoolean,
    requireNonNegativeInteger,
    requireOptionalFiniteNumber,
    requireOptionalNonNegativeInteger,
    requireOptionalString,
    requireString,
    validateArray,
    validateClientUpdate,
    validateConnectionState,
    validateOptionalArray,
    validateSessionId,
    validateStreamType,
    validateStringEnum
} from "./public_api_validation.js";

const REQUEST_TIMEOUT_TIMER_BASE = 10_000;
export const REMOTE_MEDIA_UPDATE = "remote_media";
const NEGOTIATION_KINDS = Object.values(NEGOTIATION_KIND);
const PENDING_REQUEST_KINDS = Object.values(PENDING_REQUEST_KIND);

type RemoteMediaUpdate = {
    name: typeof REMOTE_MEDIA_UPDATE;
    payload: { bindings: TrackBinding[] };
};

type HostUpdate = ClientUpdateDetail | RemoteMediaUpdate;

export type PendingRequest = {
    requestId: string;
    kind: PendingRequestKind;
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
    | { kind: typeof COMMAND_KIND.CREATE_PEER_CONNECTION }
    | { kind: typeof COMMAND_KIND.CLOSE_PEER_CONNECTION }
    | { kind: typeof COMMAND_KIND.CLOSE_WEB_SOCKET; code: number }
    | { kind: typeof COMMAND_KIND.EMIT_STATE_CHANGE; state: ConnectionState; cause?: string }
    | { kind: typeof COMMAND_KIND.EMIT_UPDATE; update: HostUpdate }
    | { kind: typeof COMMAND_KIND.BEGIN_PENDING_REQUEST; request: PendingRequest }
    | { kind: typeof COMMAND_KIND.RESOLVE_PENDING_REQUEST; requestId: string; ok: boolean }
    | { kind: typeof COMMAND_KIND.SCHEDULE_TIMER; id: number; ms: number }
    | { kind: typeof COMMAND_KIND.CANCEL_TIMER; id: number }
    | { kind: typeof COMMAND_KIND.CONNECT; url: string };

export function validateHostCommandShapes(
    value: unknown,
    context: string,
    requestMethod = false
): HostCommand[] {
    if (!Array.isArray(value)) {
        throw new Error(`${context} must return an array of host commands`);
    }
    const commands: HostCommand[] = [];
    for (let index = 0; index < value.length; index += 1) {
        if (!Object.hasOwn(value, index)) {
            throw new Error(`${context} command #${index} must be a host command`);
        }
        const command = validateHostCommand(value[index], `${context} command #${index}`);
        if (command.kind === COMMAND_KIND.BEGIN_PENDING_REQUEST) {
            if (!requestMethod || index !== 0) {
                throw new Error(`${context} command #${index} cannot begin a pending request here`);
            }
        }
        commands.push(command);
    }
    return commands;
}

function validateHostCommand(value: unknown, context: string): HostCommand {
    const command = asRecord(value, context);
    const kind = requireString(command.kind, `${context}.kind`);
    switch (kind) {
        case COMMAND_KIND.SEND_WEB_SOCKET:
            requireString(command.frame, `${context}.frame`);
            break;
        case COMMAND_KIND.APPLY_NEGOTIATION:
            requireString(command.requestId, `${context}.requestId`);
            validateStringEnum(
                command.negotiationKind,
                NEGOTIATION_KINDS,
                `${context}.negotiationKind`
            );
            requireString(command.sdp, `${context}.sdp`);
            validateArray(
                command.uploadSlots,
                `${context}.uploadSlots`,
                validateNegotiationUploadSlot
            );
            break;
        case COMMAND_KIND.CREATE_PEER_CONNECTION:
        case COMMAND_KIND.CLOSE_PEER_CONNECTION:
            break;
        case COMMAND_KIND.CLOSE_WEB_SOCKET:
            requireNonNegativeInteger(command.code, `${context}.code`);
            break;
        case COMMAND_KIND.EMIT_STATE_CHANGE:
            validateConnectionState(command.state, `${context}.state`);
            requireOptionalString(command.cause, `${context}.cause`);
            break;
        case COMMAND_KIND.EMIT_UPDATE:
            return {
                kind,
                update: validateHostUpdate(command.update, `${context}.update`)
            };
        case COMMAND_KIND.BEGIN_PENDING_REQUEST:
            return {
                kind,
                request: validatePendingRequest(command.request, `${context}.request`)
            };
        case COMMAND_KIND.RESOLVE_PENDING_REQUEST:
            requireString(command.requestId, `${context}.requestId`);
            requireBoolean(command.ok, `${context}.ok`);
            break;
        case COMMAND_KIND.SCHEDULE_TIMER:
            requireNonNegativeInteger(command.id, `${context}.id`);
            requireNonNegativeInteger(command.ms, `${context}.ms`);
            break;
        case COMMAND_KIND.CANCEL_TIMER:
            requireNonNegativeInteger(command.id, `${context}.id`);
            break;
        case COMMAND_KIND.CONNECT:
            requireString(command.url, `${context}.url`);
            break;
        default:
            throw new Error(`${context}.kind is invalid: ${String(kind)}`);
    }
    return command as HostCommand;
}

function validateHostUpdate(value: unknown, context: string): HostUpdate {
    const update = asRecord(value, context);
    if (update.name !== REMOTE_MEDIA_UPDATE) {
        return validateClientUpdate(value, context);
    }
    const payload = asRecord(update.payload, `${context}.payload`);
    validateArray(payload.bindings, `${context}.payload.bindings`, validateTrackBinding);
    return update as RemoteMediaUpdate;
}

function validatePendingRequest(value: unknown, context: string): PendingRequest {
    const request = asRecord(value, context);
    requireString(request.requestId, `${context}.requestId`);
    validateStringEnum(request.kind, PENDING_REQUEST_KINDS, `${context}.kind`);
    const timeoutTimerId = requireNonNegativeInteger(
        request.timeoutTimerId,
        `${context}.timeoutTimerId`
    );
    if (timeoutTimerId < REQUEST_TIMEOUT_TIMER_BASE) {
        throw new Error(`${context}.timeoutTimerId must be a request timeout timer id`);
    }
    if (requireNonNegativeInteger(request.timeoutMs, `${context}.timeoutMs`) === 0) {
        throw new Error(`${context}.timeoutMs must be a positive browser timer delay`);
    }
    return request as PendingRequest;
}

function validateTrackBinding(value: unknown, context: string): void {
    const binding = asRecord(value, context);
    requireString(binding.mid, `${context}.mid`);
    validateSessionId(binding.sessionId, `${context}.sessionId`);
    validateStreamType(binding.type, `${context}.type`);
    requireBoolean(binding.active, `${context}.active`);
}

function validateNegotiationUploadSlot(value: unknown, context: string): void {
    const uploadSlot = asRecord(value, context);
    requireString(uploadSlot.mid, `${context}.mid`);
    validateStringEnum(uploadSlot.kind, UPLOAD_KINDS, `${context}.kind`);
    validateOptionalArray(uploadSlot.codecs, `${context}.codecs`, requireString);
    validateOptionalArray(
        uploadSlot.simulcastEncodings,
        `${context}.simulcastEncodings`,
        validateNegotiationUploadEncoding
    );
}

function validateNegotiationUploadEncoding(value: unknown, context: string): void {
    const uploadEncoding = asRecord(value, context);
    requireString(uploadEncoding.rid, `${context}.rid`);
    requireOptionalNonNegativeInteger(uploadEncoding.maxBitrate, `${context}.maxBitrate`);
    requireOptionalFiniteNumber(uploadEncoding.resolutionScale, `${context}.resolutionScale`);
    requireOptionalNonNegativeInteger(uploadEncoding.maxFramerate, `${context}.maxFramerate`);
}
