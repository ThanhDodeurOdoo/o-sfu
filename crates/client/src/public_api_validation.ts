import {
    CLIENT_UPDATE,
    RECORDING_STOP_CODES,
    SFU_CLIENT_STATE,
    SOURCE_ENCODING_POLICY_ROLES,
    STREAM_TYPES,
    type AvailableFeatures,
    type ClientUpdateDetail,
    type ConnectionState,
    type InfoChangeUpdateDetail,
    type RecordingState,
    type SessionInfo
} from "./public_api.js";

const FEATURE_BOOLEAN_FIELDS = [
    "rtc",
    "transcription",
    "audioRecording",
    "videoRecording"
] as const satisfies readonly (keyof AvailableFeatures)[];
const RECORDING_BOOLEAN_FIELDS = [
    "recording",
    "audio",
    "video",
    "transcription"
] as const satisfies readonly (keyof RecordingState)[];
const SESSION_INFO_BOOLEAN_FIELDS = [
    "isTalking",
    "isFeatured",
    "isCameraOn",
    "isScreenSharingOn",
    "isSelfMuted",
    "isDeaf",
    "isRaisingHand"
] as const satisfies readonly (keyof SessionInfo)[];
const CONNECTION_STATES = Object.values(SFU_CLIENT_STATE);

export function validateClientUpdate(value: unknown, context: string): ClientUpdateDetail {
    const update = asRecord(value, context);
    const name = requireString(update.name, `${context}.name`);
    let payload: Record<string, unknown>;
    switch (name) {
        case CLIENT_UPDATE.TRACK:
            payload = asRecord(update.payload, `${context}.payload`);
            validateSessionId(payload.sessionId, `${context}.payload.sessionId`);
            validateStreamType(payload.type, `${context}.payload.type`);
            requireBoolean(payload.active, `${context}.payload.active`);
            if (payload.track === null || typeof payload.track !== "object") {
                throw new Error(`${context}.payload.track must be an object`);
            }
            break;
        case CLIENT_UPDATE.SOURCE:
            payload = asRecord(update.payload, `${context}.payload`);
            validateArray(payload.sources, `${context}.payload.sources`, validateSourceDescriptor);
            break;
        case CLIENT_UPDATE.DISCONNECT:
            payload = asRecord(update.payload, `${context}.payload`);
            validateSessionId(payload.sessionId, `${context}.payload.sessionId`);
            break;
        case CLIENT_UPDATE.INFO_CHANGE: {
            const infoPayload = toStringKeyedRecord(update.payload, `${context}.payload`);
            for (const [sessionId, info] of Object.entries(infoPayload)) {
                validateSessionInfo(info, `${context}.payload.${sessionId}`);
            }
            return {
                name: CLIENT_UPDATE.INFO_CHANGE,
                payload: infoPayload as InfoChangeUpdateDetail
            };
        }
        case CLIENT_UPDATE.BROADCAST:
            payload = asRecord(update.payload, `${context}.payload`);
            validateSessionId(payload.senderId, `${context}.payload.senderId`);
            break;
        case CLIENT_UPDATE.CHANNEL_INFO_CHANGE:
            payload = asRecord(update.payload, `${context}.payload`);
            validateRecordingState(payload.state, `${context}.payload.state`);
            if (payload.stopCode !== undefined) {
                validateStringEnum(
                    payload.stopCode,
                    RECORDING_STOP_CODES,
                    `${context}.payload.stopCode`
                );
            }
            break;
        default:
            throw new Error(`${context}.name is invalid: ${String(name)}`);
    }
    return update as ClientUpdateDetail;
}

export function validateSourceDescriptor(value: unknown, context: string): void {
    const source = asRecord(value, context);
    requireString(source.sourceId, `${context}.sourceId`);
    validateSessionId(source.sessionId, `${context}.sessionId`);
    validateStreamType(source.type, `${context}.type`);
    requireBoolean(source.active, `${context}.active`);
    requireOptionalString(source.mid, `${context}.mid`);
    validateArray(source.encodings, `${context}.encodings`, validateSourceEncodingDescriptor);
}

export function validateAvailableFeatures(value: unknown, context: string): AvailableFeatures {
    requireBooleanFields(asRecord(value, context), FEATURE_BOOLEAN_FIELDS, context);
    return value as AvailableFeatures;
}

export function validateRecordingState(value: unknown, context: string): RecordingState {
    requireBooleanFields(asRecord(value, context), RECORDING_BOOLEAN_FIELDS, context, true);
    return value as RecordingState;
}

function validateSessionInfo(value: unknown, context: string): void {
    requireBooleanFields(asRecord(value, context), SESSION_INFO_BOOLEAN_FIELDS, context, true);
}

export function validateConnectionState(value: unknown, context: string): ConnectionState {
    return validateStringEnum(value, CONNECTION_STATES, context);
}

export function validateStreamType(value: unknown, context: string): void {
    validateStringEnum(value, STREAM_TYPES, context);
}

export function validateSessionId(value: unknown, context: string): void {
    if (typeof value !== "string" && typeof value !== "number") {
        throw new Error(`${context} must be a string or number session ID`);
    }
    if (typeof value === "number" && !Number.isFinite(value)) {
        throw new Error(`${context} number session ID must be finite`);
    }
}

export function asRecord(value: unknown, context: string): Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
        throw new Error(`${context} must be an object`);
    }
    return value as Record<string, unknown>;
}

export function requireString(value: unknown, context: string): string {
    if (typeof value !== "string") {
        throw new Error(`${context} must be a string`);
    }
    return value;
}

export function requireOptionalString(value: unknown, context: string): void {
    if (value !== undefined && typeof value !== "string") {
        throw new Error(`${context} must be a string when provided`);
    }
}

export function requireBoolean(value: unknown, context: string, optional = false): void {
    if (typeof value !== "boolean") {
        throw new Error(`${context} must be a boolean${optional ? " when provided" : ""}`);
    }
}

export function requireNonNegativeInteger(value: unknown, context: string): number {
    if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
        throw new Error(`${context} must be a non-negative integer`);
    }
    return value;
}

export function requireOptionalNonNegativeInteger(value: unknown, context: string): void {
    requireOptionalNumber(
        value,
        context,
        "must be a non-negative integer when provided",
        (number) => Number.isInteger(number) && number >= 0
    );
}

export function requireOptionalFiniteNumber(value: unknown, context: string): void {
    requireOptionalNumber(value, context, "must be a finite number", Number.isFinite);
}

export function validateArray(
    value: unknown,
    context: string,
    itemValidator: (item: unknown, context: string) => void,
    arrayExpectation = "must be an array"
): void {
    if (!Array.isArray(value)) {
        throw new Error(`${context} ${arrayExpectation}`);
    }
    for (let index = 0; index < value.length; index += 1) {
        itemValidator(value[index], `${context}[${index}]`);
    }
}

export function validateOptionalArray(
    value: unknown,
    context: string,
    itemValidator: (item: unknown, context: string) => void
): void {
    if (value !== undefined) {
        validateArray(value, context, itemValidator, "must be an array when provided");
    }
}

export function validateStringEnum<T extends string>(
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

function validateSourceEncodingDescriptor(value: unknown, context: string): void {
    const descriptor = asRecord(value, context);
    requireString(descriptor.encodingId, `${context}.encodingId`);
    requireOptionalString(descriptor.rid, `${context}.rid`);
    requireOptionalNonNegativeInteger(descriptor.maxBitrate, `${context}.maxBitrate`);
    requireOptionalPositiveNumber(descriptor.resolutionScale, `${context}.resolutionScale`);
    requireOptionalNonNegativeInteger(descriptor.maxFramerate, `${context}.maxFramerate`);
    validateOptionalPolicyRole(descriptor.policyRole, `${context}.policyRole`);
}

function toStringKeyedRecord(value: unknown, context: string): Record<string, unknown> {
    if (value instanceof Map) {
        const record: Record<string, unknown> = {};
        for (const [key, entryValue] of value) {
            Object.defineProperty(record, requireString(key, `${context} map key`), {
                configurable: true,
                enumerable: true,
                value: entryValue,
                writable: true
            });
        }
        return record;
    }
    return asRecord(value, context);
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

function requireOptionalPositiveNumber(value: unknown, context: string): void {
    requireOptionalNumber(
        value,
        context,
        "must be a positive number",
        (number) => Number.isFinite(number) && number > 0
    );
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
