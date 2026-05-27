import {
    CLIENT_UPDATE,
    SFU_CLIENT_STATE,
    type AvailableFeatures,
    type ClientUpdateDetail,
    type ConnectionState,
    type DownloadStates,
    type InfoChangeUpdateDetail,
    type RecordingOptions,
    type RecordingState,
    type SessionId,
    type SessionInfo,
    type SourceDescriptor,
    type StreamType
} from "./public_api.js";
import type { NegotiationUploadSlot, TrackBinding } from "./protocol.js";
import {
    COMMAND_KIND,
    NEGOTIATION_KIND,
    PENDING_REQUEST_KIND,
    RECORDING_STOP_CODES,
    SOURCE_ENCODING_POLICY_ROLES,
    STREAM_TYPES,
    UPLOAD_KINDS
} from "./generated/protocol_contract.js";

const MIN_TEMPORAL_LAYER_ID = 0;
const MAX_TEMPORAL_LAYER_ID = 7;
const FEATURE_BOOLEAN_FIELDS = ["rtc", "transcription", "audioRecording", "videoRecording"];
const RECORDING_BOOLEAN_FIELDS = ["recording", "audio", "video", "transcription"];
const SESSION_INFO_BOOLEAN_FIELDS = [
    "isTalking",
    "isFeatured",
    "isCameraOn",
    "isScreenSharingOn",
    "isSelfMuted",
    "isDeaf",
    "isRaisingHand"
];
export { NEGOTIATION_KIND, PENDING_REQUEST_KIND };

const NEGOTIATION_KINDS = Object.values(NEGOTIATION_KIND);

export type NegotiationKind = (typeof NEGOTIATION_KIND)[keyof typeof NEGOTIATION_KIND];

const PENDING_REQUEST_KINDS = Object.values(PENDING_REQUEST_KIND);

export type PendingRequestKind = (typeof PENDING_REQUEST_KIND)[keyof typeof PENDING_REQUEST_KIND];

export const CommandKind = COMMAND_KIND;

export type HostCommandKind = (typeof CommandKind)[keyof typeof CommandKind];

export type HostCommand =
    | { kind: typeof CommandKind.SEND_WEB_SOCKET; frame: string }
    | {
          kind: typeof CommandKind.SET_LOCAL_UPLOAD_INTENT;
          streamType: StreamType;
          active: boolean;
      }
    | {
          kind: typeof CommandKind.APPLY_NEGOTIATION;
          requestId: string;
          negotiationKind: NegotiationKind;
          sdp: string;
          uploadSlots: NegotiationUploadSlot[];
      }
    | { kind: typeof CommandKind.ATTACH_TRACK; mid: string; streamType: StreamType }
    | { kind: typeof CommandKind.DETACH_TRACK; streamType: StreamType }
    | { kind: typeof CommandKind.CREATE_PEER_CONNECTION }
    | { kind: typeof CommandKind.CLOSE_PEER_CONNECTION }
    | { kind: typeof CommandKind.CLOSE_WEB_SOCKET; code: number }
    | { kind: typeof CommandKind.EMIT_STATE_CHANGE; state: ConnectionState; cause?: string }
    | { kind: typeof CommandKind.REPLACE_TRACK_BINDINGS; bindings: TrackBinding[] }
    | { kind: typeof CommandKind.REPLACE_SOURCE_DESCRIPTORS; sources: SourceDescriptor[] }
    | { kind: typeof CommandKind.REMOVE_SESSION_TRACKS; sessionId: SessionId }
    | { kind: typeof CommandKind.EMIT_UPDATE; update: ClientUpdateDetail }
    | {
          kind: typeof CommandKind.REGISTER_PENDING_REQUEST;
          requestId: string;
          requestKind: PendingRequestKind;
      }
    | { kind: typeof CommandKind.RESOLVE_PENDING_REQUEST; requestId: string; ok: boolean }
    | { kind: typeof CommandKind.SCHEDULE_TIMER; id: number; ms: number }
    | { kind: typeof CommandKind.CANCEL_TIMER; id: number }
    | { kind: typeof CommandKind.CONNECT; url: string };

export interface ProtocolCoreBindings {
    readonly state: ConnectionState;
    readonly features: AvailableFeatures;
    readonly recordingState: RecordingState;

    connect(url: string, jwt: string, room?: string | null): HostCommand[];
    onWsOpen(): HostCommand[];
    onWsMessage(frame: string): HostCommand[];
    onTransportReady(): HostCommand[];
    onWsClose(code: number): HostCommand[];
    onTimer(timerId: number): HostCommand[];
    publish(type: StreamType, active: boolean): HostCommand[];
    subscribe(sessionId: SessionId, states: DownloadStates): HostCommand[];
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

export type ProtocolCoreProvider = () => ProtocolCoreBindings;

let defaultProtocolCoreProvider: ProtocolCoreProvider | undefined;
let protocolCoreProvider: ProtocolCoreProvider | undefined;

export function configureDefaultProtocolCoreProvider(provider: ProtocolCoreProvider): void {
    defaultProtocolCoreProvider = provider;
}

export function configureProtocolCoreProvider(provider: ProtocolCoreProvider): void {
    protocolCoreProvider = provider;
}

export function wrapProtocolCoreBindings(bindings: ProtocolCoreBindings): ProtocolCoreBindings {
    return {
        get state(): ConnectionState {
            return validateConnectionState(bindings.state, "protocol core state");
        },
        get features(): AvailableFeatures {
            return validateAvailableFeatures(bindings.features, "protocol core features");
        },
        get recordingState(): RecordingState {
            return validateRecordingState(bindings.recordingState, "protocol core recordingState");
        },
        connect(url: string, jwt: string, room?: string | null): HostCommand[] {
            return validateHostCommands(
                bindings.connect(url, jwt, room),
                "protocol core connect()"
            );
        },
        onWsOpen(): HostCommand[] {
            return validateHostCommands(bindings.onWsOpen(), "protocol core onWsOpen()");
        },
        onWsMessage(frame: string): HostCommand[] {
            return validateHostCommands(bindings.onWsMessage(frame), "protocol core onWsMessage()");
        },
        onTransportReady(): HostCommand[] {
            return validateHostCommands(
                bindings.onTransportReady(),
                "protocol core onTransportReady()"
            );
        },
        onWsClose(code: number): HostCommand[] {
            return validateHostCommands(bindings.onWsClose(code), "protocol core onWsClose()");
        },
        onTimer(timerId: number): HostCommand[] {
            return validateHostCommands(bindings.onTimer(timerId), "protocol core onTimer()");
        },
        publish(type: StreamType, active: boolean): HostCommand[] {
            return validateHostCommands(bindings.publish(type, active), "protocol core publish()");
        },
        subscribe(sessionId: SessionId, states: DownloadStates): HostCommand[] {
            return validateHostCommands(
                bindings.subscribe(sessionId, states),
                "protocol core subscribe()"
            );
        },
        updateInfo(info: SessionInfo): HostCommand[] {
            return validateHostCommands(bindings.updateInfo(info), "protocol core updateInfo()");
        },
        broadcast(message: unknown): HostCommand[] {
            return validateHostCommands(bindings.broadcast(message), "protocol core broadcast()");
        },
        startRecording(options?: RecordingOptions): HostCommand[] {
            return validateHostCommands(
                bindings.startRecording(options),
                "protocol core startRecording()"
            );
        },
        stopRecording(): HostCommand[] {
            return validateHostCommands(bindings.stopRecording(), "protocol core stopRecording()");
        },
        submitNegotiationAnswer(
            requestId: string,
            negotiationKind: NegotiationKind,
            sdp: string
        ): HostCommand[] {
            return validateHostCommands(
                bindings.submitNegotiationAnswer(requestId, negotiationKind, sdp),
                "protocol core submitNegotiationAnswer()"
            );
        },
        disconnect(): HostCommand[] {
            return validateHostCommands(bindings.disconnect(), "protocol core disconnect()");
        },
        trackBinding(mid: string): TrackBinding | null | undefined {
            return validateOptionalTrackBinding(
                bindings.trackBinding(mid),
                "protocol core trackBinding()"
            );
        }
    };
}

export function createProtocolCore(): ProtocolCoreBindings {
    return wrapProtocolCoreBindings(
        (protocolCoreProvider ?? requireDefaultProtocolCoreProvider())()
    );
}

function requireDefaultProtocolCoreProvider(): ProtocolCoreProvider {
    if (!defaultProtocolCoreProvider) {
        throw new Error(
            "default protocol core provider is not configured; import the package entrypoint or configure one explicitly"
        );
    }
    return defaultProtocolCoreProvider;
}

function validateHostCommands(value: unknown, context: string): HostCommand[] {
    if (!Array.isArray(value)) {
        throw new Error(`${context} must return an array of host commands`);
    }
    const commands = value.map((command, index) =>
        validateHostCommand(command, `${context} command #${index}`)
    );
    validateHostCommandOrder(commands, context);
    return commands;
}

function validateHostCommandOrder(commands: HostCommand[], context: string): void {
    for (let index = 0; index < commands.length; index += 1) {
        const command = commands[index];
        if (command.kind !== CommandKind.APPLY_NEGOTIATION) {
            continue;
        }
        const previous = commands[index - 1];
        if (command.negotiationKind === NEGOTIATION_KIND.OFFER) {
            if (!previous || previous.kind !== CommandKind.CREATE_PEER_CONNECTION) {
                throw new Error(
                    `${context} command #${index} initial negotiation must immediately follow createPeerConnection`
                );
            }
        } else if (previous?.kind === CommandKind.CREATE_PEER_CONNECTION) {
            throw new Error(
                `${context} command #${index} renegotiation must not recreate the peer connection`
            );
        }
    }

    const closeWebSocketIndex = commands.findIndex(
        (command) => command.kind === CommandKind.CLOSE_WEB_SOCKET
    );
    const closePeerConnectionIndex = commands.findIndex(
        (command) => command.kind === CommandKind.CLOSE_PEER_CONNECTION
    );
    if (
        closeWebSocketIndex >= 0 &&
        closePeerConnectionIndex >= 0 &&
        closeWebSocketIndex > closePeerConnectionIndex
    ) {
        throw new Error(
            `${context} must close the websocket before the peer connection when both are in one batch`
        );
    }

    const recoveryTimerIndex = commands.findIndex(
        (command) => command.kind === CommandKind.SCHEDULE_TIMER && command.id === 1
    );
    if (
        recoveryTimerIndex >= 0 &&
        closePeerConnectionIndex >= 0 &&
        closePeerConnectionIndex > recoveryTimerIndex
    ) {
        throw new Error(`${context} must close the peer connection before scheduling recovery`);
    }
}

function validateHostCommand(value: unknown, context: string): HostCommand {
    const command = asRecord(value, context);
    const kind = requireString(command.kind, `${context}.kind`);
    switch (kind) {
        case CommandKind.SEND_WEB_SOCKET:
            requireString(command.frame, `${context}.frame`);
            return command as HostCommand;
        case CommandKind.SET_LOCAL_UPLOAD_INTENT:
            validateStreamType(command.streamType, `${context}.streamType`);
            requireBoolean(command.active, `${context}.active`);
            return command as HostCommand;
        case CommandKind.APPLY_NEGOTIATION:
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
            return command as HostCommand;
        case CommandKind.ATTACH_TRACK:
            requireString(command.mid, `${context}.mid`);
            validateStreamType(command.streamType, `${context}.streamType`);
            return command as HostCommand;
        case CommandKind.DETACH_TRACK:
            validateStreamType(command.streamType, `${context}.streamType`);
            return command as HostCommand;
        case CommandKind.CREATE_PEER_CONNECTION:
        case CommandKind.CLOSE_PEER_CONNECTION:
            return command as HostCommand;
        case CommandKind.CLOSE_WEB_SOCKET:
            requireNonNegativeInteger(command.code, `${context}.code`);
            return command as HostCommand;
        case CommandKind.EMIT_STATE_CHANGE:
            validateConnectionState(command.state, `${context}.state`);
            requireOptionalString(command.cause, `${context}.cause`);
            return command as HostCommand;
        case CommandKind.REPLACE_TRACK_BINDINGS:
            validateArray(command.bindings, `${context}.bindings`, validateTrackBinding);
            return command as HostCommand;
        case CommandKind.REPLACE_SOURCE_DESCRIPTORS:
            validateArray(command.sources, `${context}.sources`, validateSourceDescriptor);
            return command as HostCommand;
        case CommandKind.REMOVE_SESSION_TRACKS:
            validateSessionId(command.sessionId, `${context}.sessionId`);
            return command as HostCommand;
        case CommandKind.EMIT_UPDATE:
            return {
                kind,
                update: validateClientUpdate(command.update, `${context}.update`)
            };
        case CommandKind.REGISTER_PENDING_REQUEST:
            requireString(command.requestId, `${context}.requestId`);
            validateStringEnum(
                command.requestKind,
                PENDING_REQUEST_KINDS,
                `${context}.requestKind`
            );
            return command as HostCommand;
        case CommandKind.RESOLVE_PENDING_REQUEST:
            requireString(command.requestId, `${context}.requestId`);
            requireBoolean(command.ok, `${context}.ok`);
            return command as HostCommand;
        case CommandKind.SCHEDULE_TIMER:
            requireNonNegativeInteger(command.id, `${context}.id`);
            requireNonNegativeInteger(command.ms, `${context}.ms`);
            return command as HostCommand;
        case CommandKind.CANCEL_TIMER:
            requireNonNegativeInteger(command.id, `${context}.id`);
            return command as HostCommand;
        case CommandKind.CONNECT:
            requireString(command.url, `${context}.url`);
            return command as HostCommand;
        default:
            throw new Error(`${context}.kind is invalid: ${String(kind)}`);
    }
}

function validateClientUpdate(value: unknown, context: string): ClientUpdateDetail {
    const update = asRecord(value, context);
    const name = requireString(update.name, `${context}.name`);
    switch (name) {
        case CLIENT_UPDATE.TRACK: {
            const payload = asRecord(update.payload, `${context}.payload`);
            validateSessionId(payload.sessionId, `${context}.payload.sessionId`);
            validateStreamType(payload.type, `${context}.payload.type`);
            requireBoolean(payload.active, `${context}.payload.active`);
            if (payload.track === null || typeof payload.track !== "object") {
                throw new Error(`${context}.payload.track must be an object`);
            }
            return update as ClientUpdateDetail;
        }
        case CLIENT_UPDATE.SOURCE: {
            const payload = asRecord(update.payload, `${context}.payload`);
            validateArray(payload.sources, `${context}.payload.sources`, validateSourceDescriptor);
            return update as ClientUpdateDetail;
        }
        case CLIENT_UPDATE.DISCONNECT: {
            const payload = asRecord(update.payload, `${context}.payload`);
            validateSessionId(payload.sessionId, `${context}.payload.sessionId`);
            return update as ClientUpdateDetail;
        }
        case CLIENT_UPDATE.INFO_CHANGE: {
            const payload = toStringKeyedRecord(update.payload, `${context}.payload`);
            for (const [sessionId, info] of Object.entries(payload)) {
                validateSessionInfo(info, `${context}.payload.${sessionId}`);
            }
            return {
                name: CLIENT_UPDATE.INFO_CHANGE,
                payload: payload as InfoChangeUpdateDetail
            };
        }
        case CLIENT_UPDATE.BROADCAST: {
            const payload = asRecord(update.payload, `${context}.payload`);
            validateSessionId(payload.senderId, `${context}.payload.senderId`);
            return update as ClientUpdateDetail;
        }
        case CLIENT_UPDATE.CHANNEL_INFO_CHANGE: {
            const payload = asRecord(update.payload, `${context}.payload`);
            validateRecordingState(payload.state, `${context}.payload.state`);
            if (payload.stopCode !== undefined) {
                validateStringEnum(
                    payload.stopCode,
                    RECORDING_STOP_CODES,
                    `${context}.payload.stopCode`
                );
            }
            return update as ClientUpdateDetail;
        }
        default:
            throw new Error(`${context}.name is invalid: ${String(name)}`);
    }
}

function validateOptionalTrackBinding(
    value: unknown,
    context: string
): TrackBinding | null | undefined {
    if (value === null || value === undefined) {
        return value;
    }
    const binding = asRecord(value, context);
    requireString(binding.mid, `${context}.mid`);
    validateSessionId(binding.sessionId, `${context}.sessionId`);
    validateStreamType(binding.type, `${context}.type`);
    requireBoolean(binding.active, `${context}.active`);
    if (binding.source !== undefined) {
        requirePresent(
            validateOptionalSourceDescriptor(binding.source, `${context}.source`),
            `${context}.source`,
            "source descriptor when provided"
        );
    }
    return value as TrackBinding;
}

function validateTrackBinding(value: unknown, context: string): TrackBinding {
    return requirePresent(validateOptionalTrackBinding(value, context), context, "track binding");
}

function validateOptionalSourceDescriptor(
    value: unknown,
    context: string
): SourceDescriptor | null | undefined {
    if (value === null || value === undefined) {
        return value;
    }
    const source = asRecord(value, context);
    requireString(source.sourceId, `${context}.sourceId`);
    validateSessionId(source.sessionId, `${context}.sessionId`);
    validateStreamType(source.type, `${context}.type`);
    requireBoolean(source.active, `${context}.active`);
    requireOptionalString(source.mid, `${context}.mid`);
    validateArray(source.encodings, `${context}.encodings`, validateSourceEncodingDescriptor);
    return value as SourceDescriptor;
}

function validateSourceDescriptor(value: unknown, context: string): SourceDescriptor {
    return requirePresent(
        validateOptionalSourceDescriptor(value, context),
        context,
        "source descriptor"
    );
}

function validateSourceEncodingDescriptor(value: unknown, context: string): void {
    const descriptor = asRecord(value, context);
    requireString(descriptor.encodingId, `${context}.encodingId`);
    requireOptionalString(descriptor.rid, `${context}.rid`);
    requireOptionalNonNegativeInteger(descriptor.maxBitrate, `${context}.maxBitrate`);
    requireOptionalPositiveNumber(descriptor.resolutionScale, `${context}.resolutionScale`);
    requireOptionalNonNegativeInteger(descriptor.maxFramerate, `${context}.maxFramerate`);
    validateOptionalPolicyRole(descriptor.policyRole, `${context}.policyRole`);
    requireOptionalTemporalLayerId(descriptor.maxTemporalLayerId, `${context}.maxTemporalLayerId`);
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

function requireOptionalPositiveNumber(value: unknown, context: string): void {
    requireOptionalNumber(
        value,
        context,
        "must be a positive number",
        (number) => Number.isFinite(number) && number > 0
    );
}

function requireOptionalFiniteNumber(value: unknown, context: string): void {
    requireOptionalNumber(value, context, "must be a finite number", Number.isFinite);
}

function validateOptionalPolicyRole(value: unknown, context: string): void {
    if (value !== undefined) {
        validateStringEnum(
            value,
            SOURCE_ENCODING_POLICY_ROLES,
            context,
            `${context} must be a supported upload layer policy role`
        );
    }
}

function validateAvailableFeatures(value: unknown, context: string): AvailableFeatures {
    const features = asRecord(value, context);
    requireBooleanFields(features, FEATURE_BOOLEAN_FIELDS, context);
    return value as AvailableFeatures;
}

function validateRecordingState(value: unknown, context: string): RecordingState {
    const state = asRecord(value, context);
    requireBooleanFields(state, RECORDING_BOOLEAN_FIELDS, context, true);
    return value as RecordingState;
}

function validateSessionInfo(value: unknown, context: string): SessionInfo {
    const info = asRecord(value, context);
    requireBooleanFields(info, SESSION_INFO_BOOLEAN_FIELDS, context, true);
    return value as SessionInfo;
}

function validateConnectionState(value: unknown, context: string): ConnectionState {
    return validateStringEnum(value, Object.values(SFU_CLIENT_STATE), context);
}

function validateStreamType(value: unknown, context: string): StreamType {
    return validateStringEnum(value, STREAM_TYPES, context);
}

function validateSessionId(value: unknown, context: string): SessionId {
    if (typeof value !== "string" && typeof value !== "number") {
        throw new Error(`${context} must be a string or number session ID`);
    }
    if (typeof value === "number" && !Number.isFinite(value)) {
        throw new Error(`${context} number session ID must be finite`);
    }
    return value;
}

function asRecord(value: unknown, context: string): Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
        throw new Error(`${context} must be an object`);
    }
    return value as Record<string, unknown>;
}

function toStringKeyedRecord(value: unknown, context: string): Record<string, unknown> {
    if (value instanceof Map) {
        return Object.fromEntries(
            [...value.entries()].map(([key, entryValue]) => [
                requireString(key, `${context} map key`),
                entryValue
            ])
        );
    }
    return asRecord(value, context);
}

function requireString(value: unknown, context: string): string {
    if (typeof value !== "string") {
        throw new Error(`${context} must be a string`);
    }
    return value;
}

function requireOptionalString(value: unknown, context: string): void {
    if (value !== undefined && typeof value !== "string") {
        throw new Error(`${context} must be a string when provided`);
    }
}

function requireBoolean(value: unknown, context: string, optional = false): boolean {
    if (typeof value !== "boolean") {
        throw new Error(`${context} must be a boolean${optional ? " when provided" : ""}`);
    }
    return value;
}

function requireBooleanFields(
    record: Record<string, unknown>,
    fields: readonly string[],
    context: string,
    optional = false
): void {
    for (const field of fields) {
        const value = record[field];
        if (!optional || value !== undefined) {
            requireBoolean(value, `${context}.${field}`, optional);
        }
    }
}

function requireNonNegativeInteger(value: unknown, context: string): number {
    if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
        throw new Error(`${context} must be a non-negative integer`);
    }
    return value;
}

function requireOptionalNonNegativeInteger(value: unknown, context: string): void {
    requireOptionalNumber(
        value,
        context,
        "must be a non-negative integer when provided",
        (number) => Number.isInteger(number) && number >= 0
    );
}

function requireOptionalTemporalLayerId(value: unknown, context: string): void {
    requireOptionalNumber(
        value,
        context,
        `must be an integer from ${MIN_TEMPORAL_LAYER_ID} through ${MAX_TEMPORAL_LAYER_ID} when provided`,
        (number) =>
            Number.isInteger(number) &&
            number >= MIN_TEMPORAL_LAYER_ID &&
            number <= MAX_TEMPORAL_LAYER_ID
    );
}

function validateArray<T>(
    value: unknown,
    context: string,
    itemValidator: (item: unknown, context: string) => T,
    arrayExpectation = "must be an array"
): T[] {
    if (!Array.isArray(value)) {
        throw new Error(`${context} ${arrayExpectation}`);
    }
    return value.map((item, index) => itemValidator(item, `${context}[${index}]`));
}

function validateOptionalArray<T>(
    value: unknown,
    context: string,
    itemValidator: (item: unknown, context: string) => T
): T[] | undefined {
    if (value === undefined) {
        return undefined;
    }
    return validateArray(value, context, itemValidator, "must be an array when provided");
}

function validateStringEnum<T extends string>(
    value: unknown,
    allowed: readonly T[],
    context: string,
    invalidMessage = `${context} is invalid: ${String(value)}`
): T {
    if (typeof value !== "string" || !allowed.includes(value as T)) {
        throw new Error(invalidMessage);
    }
    return value as T;
}

function requirePresent<T>(value: T | null | undefined, context: string, label: string): T {
    if (value === null || value === undefined) {
        throw new Error(`${context} must be a ${label}`);
    }
    return value;
}

function requireOptionalNumber(
    value: unknown,
    context: string,
    expectation: string,
    isValid: (value: number) => boolean
): void {
    if (value !== undefined && (typeof value !== "number" || !isValid(value))) {
        throw new Error(`${context} ${expectation}`);
    }
}
