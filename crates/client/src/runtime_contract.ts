import {
    SFU_CLIENT_STATE,
    type AvailableFeatures,
    type ConnectionState,
    type DownloadStates,
    type RecordingOptions,
    type RecordingState,
    type SessionId,
    type SessionInfo,
    type StreamType
} from "./public_api.js";
import type { TrackBinding } from "./protocol.js";
import {
    COMMAND_KIND,
    HOST_COMMAND_SCHEMAS,
    NEGOTIATION_KIND,
    PENDING_REQUEST_KIND,
    RUNTIME_SCHEMAS,
    type HostCommand as GeneratedHostCommand,
    type HostCommandKind,
    type ProtocolValidationSchema
} from "./generated/protocol_contract.js";

export { NEGOTIATION_KIND, PENDING_REQUEST_KIND };

export type NegotiationKind = (typeof NEGOTIATION_KIND)[keyof typeof NEGOTIATION_KIND];

export type PendingRequestKind = (typeof PENDING_REQUEST_KIND)[keyof typeof PENDING_REQUEST_KIND];

export const CommandKind = COMMAND_KIND;

export type { HostCommandKind };

export type HostCommand = GeneratedHostCommand;

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
    const schema = (HOST_COMMAND_SCHEMAS as Record<string, ProtocolValidationSchema>)[kind];
    if (!schema) {
        throw new Error(`${context}.kind is invalid: ${String(kind)}`);
    }
    return validateSchema(schema, value, context) as HostCommand;
}

function validateOptionalTrackBinding(
    value: unknown,
    context: string
): TrackBinding | null | undefined {
    if (value === null || value === undefined) {
        return value;
    }
    return validateSchema(RUNTIME_SCHEMAS.trackBinding, value, context) as TrackBinding;
}

function validateAvailableFeatures(value: unknown, context: string): AvailableFeatures {
    return validateSchema(RUNTIME_SCHEMAS.availableFeatures, value, context) as AvailableFeatures;
}

function validateRecordingState(value: unknown, context: string): RecordingState {
    return validateSchema(RUNTIME_SCHEMAS.recordingState, value, context) as RecordingState;
}

function validateConnectionState(value: unknown, context: string): ConnectionState {
    return validateStringEnum(value, Object.values(SFU_CLIENT_STATE), context);
}

function validateSchema(
    schema: ProtocolValidationSchema,
    value: unknown,
    context: string,
    optional = false
): unknown {
    if (typeof schema === "string") {
        return validatePrimitiveSchema(schema, value, context, optional);
    }
    switch (schema.kind) {
        case "array":
            return validateArray(value, context, schema.items, optional);
        case "enum":
            return validateStringEnum(
                value,
                schema.values,
                context,
                schema.message ? `${context} ${schema.message}` : undefined
            );
        case "literal":
            return validateStringEnum(value, [schema.value], context);
        case "object":
            return validateObjectSchema(schema.fields, value, context);
        case "optional":
            if (value === undefined) {
                return undefined;
            }
            return validateSchema(schema.value, value, context, true);
        case "record":
            return validateRecordSchema(schema.values, value, context);
        case "taggedUnion":
            return validateTaggedUnionSchema(schema.tag, schema.variants, value, context);
    }
}

function validatePrimitiveSchema(
    schema: string,
    value: unknown,
    context: string,
    optional: boolean
): unknown {
    switch (schema) {
        case "boolean":
            return requireBoolean(value, context, optional);
        case "browserCloseCode":
            requireNumber(
                value,
                context,
                "must be 1000 or an integer from 3000 through 4999",
                isBrowserCloseCode,
                optional
            );
            return value;
        case "finiteNumber":
            requireNumber(value, context, "must be a finite number", Number.isFinite, optional);
            return value;
        case "nonNegativeInteger":
            requireNumber(
                value,
                context,
                "must be a non-negative integer",
                (number) => Number.isInteger(number) && number >= 0,
                optional
            );
            return value;
        case "positiveNumber":
            requireNumber(
                value,
                context,
                "must be a positive number",
                (number) => Number.isFinite(number) && number > 0,
                optional
            );
            return value;
        case "sessionId":
            return validateSessionId(value, context);
        case "string":
            return requireString(value, context, optional);
        case "temporalLayerId":
            requireNumber(
                value,
                context,
                "must be an integer from 0 through 7",
                (number) => Number.isInteger(number) && number >= 0 && number <= 7,
                optional
            );
            return value;
        case "unknown":
            return value;
        default:
            throw new Error(`unsupported protocol validation schema: ${schema}`);
    }
}

function validateObjectSchema(
    fields: Record<string, ProtocolValidationSchema>,
    value: unknown,
    context: string
): Record<string, unknown> {
    const record = asRecord(value, context);
    const result: Record<string, unknown> = { ...record };
    for (const [field, fieldSchema] of Object.entries(fields)) {
        if (!hasOwn(record, field)) {
            if (isOptionalSchema(fieldSchema)) {
                continue;
            }
            throw new Error(`${context}.${field} is required`);
        }
        const normalized = validateSchema(fieldSchema, record[field], `${context}.${field}`);
        result[field] = normalized;
    }
    return result;
}

function validateRecordSchema(
    schema: ProtocolValidationSchema,
    value: unknown,
    context: string
): Record<string, unknown> {
    const record = asStringKeyedRecord(value, context);
    const result: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(record)) {
        if (isUnsafeRecordKey(key)) {
            throw new Error(`${context}.${key} is not a supported record key`);
        }
        result[key] = validateSchema(schema, item, `${context}.${key}`);
    }
    return result;
}

function validateTaggedUnionSchema(
    tag: string,
    variants: Record<string, ProtocolValidationSchema>,
    value: unknown,
    context: string
): unknown {
    const record = asRecord(value, context);
    const tagValue = requireString(record[tag], `${context}.${tag}`);
    const variant = variants[tagValue];
    if (!variant) {
        throw new Error(`${context}.${tag} is invalid: ${String(tagValue)}`);
    }
    return validateSchema(variant, value, context);
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

function hasOwn(record: Record<string, unknown>, field: string): boolean {
    return Object.prototype.hasOwnProperty.call(record, field);
}

function isBrowserCloseCode(value: number): boolean {
    return Number.isInteger(value) && (value === 1000 || (value >= 3000 && value <= 4999));
}

function isOptionalSchema(schema: ProtocolValidationSchema): boolean {
    return typeof schema !== "string" && schema.kind === "optional";
}

function isUnsafeRecordKey(key: string): boolean {
    return key === "__proto__" || key === "prototype" || key === "constructor";
}

function asRecord(value: unknown, context: string): Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
        throw new Error(`${context} must be an object`);
    }
    return value as Record<string, unknown>;
}

function asStringKeyedRecord(value: unknown, context: string): Record<string, unknown> {
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

function requireString(value: unknown, context: string, optional = false): string {
    if (typeof value !== "string") {
        throw new Error(`${context} must be a string${optional ? " when provided" : ""}`);
    }
    return value;
}

function requireBoolean(value: unknown, context: string, optional = false): boolean {
    if (typeof value !== "boolean") {
        throw new Error(`${context} must be a boolean${optional ? " when provided" : ""}`);
    }
    return value;
}

function validateArray(
    value: unknown,
    context: string,
    itemSchema: ProtocolValidationSchema,
    optional: boolean
): unknown[] {
    if (!Array.isArray(value)) {
        throw new Error(`${context} must be an array${optional ? " when provided" : ""}`);
    }
    return value.map((item, index) => validateSchema(itemSchema, item, `${context}[${index}]`));
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

function requireNumber(
    value: unknown,
    context: string,
    expectation: string,
    isValid: (value: number) => boolean,
    optional: boolean
): void {
    if (typeof value !== "number" || !isValid(value)) {
        throw new Error(`${context} ${expectation}${optional ? " when provided" : ""}`);
    }
}
