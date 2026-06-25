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
import type { ClientUpdateDetail, ConnectionState, SessionId, StreamType } from "./public_api.js";
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
    validateOptionalArray,
    validateSessionId,
    validateStreamType,
    validateStringEnum,
    validateConnectionState
} from "./public_api_validation.js";

const REQUEST_TIMEOUT_TIMER_BASE = 10_000;
const NEGOTIATION_KINDS = Object.values(NEGOTIATION_KIND);
const PENDING_REQUEST_KINDS = Object.values(PENDING_REQUEST_KIND);

export type HostCommand =
    | { kind: typeof COMMAND_KIND.SEND_WEB_SOCKET; frame: string }
    | {
          kind: typeof COMMAND_KIND.SET_LOCAL_UPLOAD_INTENT;
          streamType: StreamType;
          active: boolean;
      }
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
    | { kind: typeof COMMAND_KIND.REPLACE_TRACK_BINDINGS; bindings: TrackBinding[] }
    | { kind: typeof COMMAND_KIND.REMOVE_SESSION_TRACKS; sessionId: SessionId }
    | { kind: typeof COMMAND_KIND.EMIT_UPDATE; update: ClientUpdateDetail }
    | {
          kind: typeof COMMAND_KIND.BEGIN_PENDING_REQUEST;
          requestId: string;
          requestKind: PendingRequestKind;
          timeoutTimerId: number;
          timeoutMs: number;
      }
    | { kind: typeof COMMAND_KIND.RESOLVE_PENDING_REQUEST; requestId: string; ok: boolean }
    | { kind: typeof COMMAND_KIND.SCHEDULE_TIMER; id: number; ms: number }
    | { kind: typeof COMMAND_KIND.CANCEL_TIMER; id: number }
    | { kind: typeof COMMAND_KIND.CONNECT; url: string };

export function validateHostCommandShapes(value: unknown, context: string): HostCommand[] {
    if (!Array.isArray(value)) {
        throw new Error(`${context} must return an array of host commands`);
    }
    const commands: HostCommand[] = [];
    for (let index = 0; index < value.length; index += 1) {
        if (!Object.hasOwn(value, index)) {
            throw new Error(`${context} command #${index} must be a host command`);
        }
        commands.push(validateHostCommand(value[index], `${context} command #${index}`));
    }
    return commands;
}

export function validateHostCommandBatch(value: unknown, context: string): HostCommand[] {
    const commands = validateHostCommandShapes(value, context);
    validateHostCommandOrder(commands, context);
    return commands;
}

function validateHostCommandOrder(commands: HostCommand[], context: string): void {
    let closeWebSocketIndex = -1;
    let closePeerConnectionIndex = -1;
    let recoveryTimerIndex = -1;
    let unknownResolvedRequestIndex = -1;
    let unknownResolvedRequestId = "";
    const startedPendingRequestIds = new Set<string>();
    for (let index = 0; index < commands.length; index += 1) {
        const command = commands[index];
        const previous = commands[index - 1];
        if (command.kind === COMMAND_KIND.CLOSE_WEB_SOCKET && closeWebSocketIndex < 0) {
            closeWebSocketIndex = index;
        }
        if (command.kind === COMMAND_KIND.CLOSE_PEER_CONNECTION && closePeerConnectionIndex < 0) {
            closePeerConnectionIndex = index;
        }
        if (
            command.kind === COMMAND_KIND.SCHEDULE_TIMER &&
            command.id === 1 &&
            recoveryTimerIndex < 0
        ) {
            recoveryTimerIndex = index;
        }
        if (command.kind === COMMAND_KIND.BEGIN_PENDING_REQUEST) {
            startedPendingRequestIds.add(command.requestId);
        }
        if (
            command.kind === COMMAND_KIND.RESOLVE_PENDING_REQUEST &&
            unknownResolvedRequestIndex < 0 &&
            !startedPendingRequestIds.has(command.requestId) &&
            !(
                previous?.kind === COMMAND_KIND.CANCEL_TIMER &&
                previous.id >= REQUEST_TIMEOUT_TIMER_BASE
            )
        ) {
            unknownResolvedRequestIndex = index;
            unknownResolvedRequestId = command.requestId;
        }
        if (command.kind !== COMMAND_KIND.APPLY_NEGOTIATION) {
            continue;
        }
        if (command.negotiationKind === NEGOTIATION_KIND.OFFER) {
            if (!previous || previous.kind !== COMMAND_KIND.CREATE_PEER_CONNECTION) {
                throw new Error(
                    `${context} command #${index} initial negotiation must immediately follow createPeerConnection`
                );
            }
        } else if (previous?.kind === COMMAND_KIND.CREATE_PEER_CONNECTION) {
            throw new Error(
                `${context} command #${index} renegotiation must not recreate the peer connection`
            );
        }
    }

    if (
        closeWebSocketIndex >= 0 &&
        closePeerConnectionIndex >= 0 &&
        closeWebSocketIndex > closePeerConnectionIndex
    ) {
        throw new Error(
            `${context} must close the websocket before the peer connection when both are in one batch`
        );
    }

    if (
        recoveryTimerIndex >= 0 &&
        closePeerConnectionIndex >= 0 &&
        closePeerConnectionIndex > recoveryTimerIndex
    ) {
        throw new Error(`${context} must close the peer connection before scheduling recovery`);
    }

    if (closePeerConnectionIndex < 0 && unknownResolvedRequestIndex >= 0) {
        throw new Error(
            `${context} command #${unknownResolvedRequestIndex} resolves unknown pending request ${unknownResolvedRequestId}`
        );
    }
}

function validateHostCommand(value: unknown, context: string): HostCommand {
    const command = asRecord(value, context);
    const kind = requireString(command.kind, `${context}.kind`);
    switch (kind) {
        case COMMAND_KIND.SEND_WEB_SOCKET:
            requireString(command.frame, `${context}.frame`);
            break;
        case COMMAND_KIND.SET_LOCAL_UPLOAD_INTENT:
            validateStreamType(command.streamType, `${context}.streamType`);
            requireBoolean(command.active, `${context}.active`);
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
        case COMMAND_KIND.REPLACE_TRACK_BINDINGS:
            validateArray(command.bindings, `${context}.bindings`, validateTrackBinding);
            break;
        case COMMAND_KIND.REMOVE_SESSION_TRACKS:
            validateSessionId(command.sessionId, `${context}.sessionId`);
            break;
        case COMMAND_KIND.EMIT_UPDATE:
            return {
                kind,
                update: validateClientUpdate(command.update, `${context}.update`)
            };
        case COMMAND_KIND.BEGIN_PENDING_REQUEST: {
            requireString(command.requestId, `${context}.requestId`);
            validateStringEnum(
                command.requestKind,
                PENDING_REQUEST_KINDS,
                `${context}.requestKind`
            );
            const timeoutTimerId = requireNonNegativeInteger(
                command.timeoutTimerId,
                `${context}.timeoutTimerId`
            );
            if (timeoutTimerId < REQUEST_TIMEOUT_TIMER_BASE) {
                throw new Error(`${context}.timeoutTimerId must be a request timeout timer id`);
            }
            if (requireNonNegativeInteger(command.timeoutMs, `${context}.timeoutMs`) === 0) {
                throw new Error(`${context}.timeoutMs must be a positive browser timer delay`);
            }
            break;
        }
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
