/**
 * protocol and runtime contract definitions
 *
 * this module defines the interface between the platform-agnostic protocol
 * core (wasm) and the browser-specific host runtime. it includes command
 * schemas, state types, and the binding wrappers that enforce safety
 */

import {
    CLIENT_UPDATE,
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

const MIN_TEMPORAL_LAYER_ID = 0;
const MAX_TEMPORAL_LAYER_ID = 7;

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

export const CommandKind = {
    SEND_WEB_SOCKET: "sendWebSocket",
    APPLY_NEGOTIATION: "applyNegotiation",
    ATTACH_TRACK: "attachTrack",
    DETACH_TRACK: "detachTrack",
    CREATE_PEER_CONNECTION: "createPeerConnection",
    CLOSE_PEER_CONNECTION: "closePeerConnection",
    CLOSE_WEB_SOCKET: "closeWebSocket",
    EMIT_STATE_CHANGE: "emitStateChange",
    REPLACE_TRACK_BINDINGS: "replaceTrackBindings",
    REPLACE_SOURCE_DESCRIPTORS: "replaceSourceDescriptors",
    REMOVE_SESSION_TRACKS: "removeSessionTracks",
    EMIT_UPDATE: "emitUpdate",
    REGISTER_PENDING_REQUEST: "registerPendingRequest",
    RESOLVE_PENDING_REQUEST: "resolvePendingRequest",
    SCHEDULE_TIMER: "scheduleTimer",
    CANCEL_TIMER: "cancelTimer",
    CONNECT: "connect"
} as const;

export type HostCommandKind = (typeof CommandKind)[keyof typeof CommandKind];

export type HostCommand =
    | { kind: typeof CommandKind.SEND_WEB_SOCKET; frame: string }
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

/**
 * registers the entrypoint-owned default protocol-core provider
 *
 * browser bundles install this once so createProtocolCore() can stay
 * decoupled from a specific wasm bootstrap path
 *
 * @param provider callback that returns the protocol core bindings
 */
export function configureDefaultProtocolCoreProvider(provider: ProtocolCoreProvider): void {
    defaultProtocolCoreProvider = provider;
}

/**
 * configures the active protocol core provider
 *
 * @param provider callback that returns the protocol core bindings
 */
export function configureProtocolCoreProvider(provider: ProtocolCoreProvider): void {
    protocolCoreProvider = provider;
}

/**
 * wraps protocol core bindings with validation logic
 *
 * this ensures that values crossing the wasm boundary conform to the
 * expected types and constraints before they reach the rest of the client
 *
 * @param bindings raw protocol core bindings from the wasm module
 * @returns validated protocol core bindings
 */
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

/**
 * creates a new protocol core instance using the configured provider
 *
 * @returns validated protocol core bindings
 */
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

// Rust `CommandBatch` validation is canonical; this guard keeps browser hosts
// defensive around dynamically loaded or stale protocol bindings.
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
        case CommandKind.APPLY_NEGOTIATION:
            requireString(command.requestId, `${context}.requestId`);
            validateNegotiationKind(command.negotiationKind, `${context}.negotiationKind`);
            requireString(command.sdp, `${context}.sdp`);
            validateNegotiationUploadSlots(command.uploadSlots, `${context}.uploadSlots`);
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
            requireInteger(command.code, `${context}.code`);
            return command as HostCommand;
        case CommandKind.EMIT_STATE_CHANGE:
            validateConnectionState(command.state, `${context}.state`);
            requireOptionalString(command.cause, `${context}.cause`);
            return command as HostCommand;
        case CommandKind.REPLACE_TRACK_BINDINGS:
            if (!Array.isArray(command.bindings)) {
                throw new Error(`${context}.bindings must be an array`);
            }
            command.bindings.forEach((binding, index) => {
                const validated = validateOptionalTrackBinding(
                    binding,
                    `${context}.bindings[${index}]`
                );
                if (validated === null || validated === undefined) {
                    throw new Error(`${context}.bindings[${index}] must be a track binding`);
                }
            });
            return command as HostCommand;
        case CommandKind.REPLACE_SOURCE_DESCRIPTORS:
            if (!Array.isArray(command.sources)) {
                throw new Error(`${context}.sources must be an array`);
            }
            command.sources.forEach((source, index) => {
                const validated = validateOptionalSourceDescriptor(
                    source,
                    `${context}.sources[${index}]`
                );
                if (validated === null || validated === undefined) {
                    throw new Error(`${context}.sources[${index}] must be a source descriptor`);
                }
            });
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
            validatePendingRequestKind(command.requestKind, `${context}.requestKind`);
            return command as HostCommand;
        case CommandKind.RESOLVE_PENDING_REQUEST:
            requireString(command.requestId, `${context}.requestId`);
            requireBoolean(command.ok, `${context}.ok`);
            return command as HostCommand;
        case CommandKind.SCHEDULE_TIMER:
            requireInteger(command.id, `${context}.id`);
            requireInteger(command.ms, `${context}.ms`);
            return command as HostCommand;
        case CommandKind.CANCEL_TIMER:
            requireInteger(command.id, `${context}.id`);
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
            if (!Array.isArray(payload.sources)) {
                throw new Error(`${context}.payload.sources must be an array`);
            }
            payload.sources.forEach((source, index) => {
                const validated = validateOptionalSourceDescriptor(
                    source,
                    `${context}.payload.sources[${index}]`
                );
                if (validated === null || validated === undefined) {
                    throw new Error(
                        `${context}.payload.sources[${index}] must be a source descriptor`
                    );
                }
            });
            return update as ClientUpdateDetail;
        }
        case CLIENT_UPDATE.DISCONNECT: {
            const payload = asRecord(update.payload, `${context}.payload`);
            validateSessionId(payload.sessionId, `${context}.payload.sessionId`);
            return update as ClientUpdateDetail;
        }
        case CLIENT_UPDATE.INFO_CHANGE: {
            const payload = asStringKeyedRecord(update.payload, `${context}.payload`);
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
                validateRecordingStopCode(payload.stopCode, `${context}.payload.stopCode`);
            }
            return update as ClientUpdateDetail;
        }
        default:
            throw new Error(`${context}.name is invalid: ${String(name)}`);
    }
}

function validateOptionalTrackBinding(
    value: TrackBinding | null | undefined,
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
        const source = validateOptionalSourceDescriptor(binding.source, `${context}.source`);
        if (source === null) {
            throw new Error(`${context}.source must be a source descriptor when provided`);
        }
    }
    return value;
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
    if (!Array.isArray(source.encodings)) {
        throw new Error(`${context}.encodings must be an array`);
    }
    source.encodings.forEach((encoding, index) => {
        const descriptor = asRecord(encoding, `${context}.encodings[${index}]`);
        requireString(descriptor.encodingId, `${context}.encodings[${index}].encodingId`);
        requireOptionalString(descriptor.rid, `${context}.encodings[${index}].rid`);
        requireOptionalNonNegativeInteger(
            descriptor.maxBitrate,
            `${context}.encodings[${index}].maxBitrate`
        );
        requireOptionalPositiveNumber(
            descriptor.resolutionScale,
            `${context}.encodings[${index}].resolutionScale`
        );
        requireOptionalNonNegativeInteger(
            descriptor.maxFramerate,
            `${context}.encodings[${index}].maxFramerate`
        );
        validateOptionalPolicyRole(
            descriptor.policyRole,
            `${context}.encodings[${index}].policyRole`
        );
        requireOptionalTemporalLayerId(
            descriptor.maxTemporalLayerId,
            `${context}.encodings[${index}].maxTemporalLayerId`
        );
    });
    return value as SourceDescriptor;
}

function validateNegotiationUploadSlots(value: unknown, context: string): void {
    if (!Array.isArray(value)) {
        throw new Error(`${context} must be an array`);
    }
    value.forEach((slot, slotIndex) => {
        const uploadSlot = asRecord(slot, `${context}[${slotIndex}]`);
        requireString(uploadSlot.mid, `${context}[${slotIndex}].mid`);
        validateUploadKind(uploadSlot.kind, `${context}[${slotIndex}].kind`);
        validateOptionalStringArray(uploadSlot.codecs, `${context}[${slotIndex}].codecs`);
        if (
            uploadSlot.simulcastEncodings !== undefined &&
            !Array.isArray(uploadSlot.simulcastEncodings)
        ) {
            throw new Error(`${context}[${slotIndex}].simulcastEncodings must be an array`);
        }
        for (const [encodingIndex, encoding] of (
            uploadSlot.simulcastEncodings as unknown[] | undefined
        )?.entries() ?? []) {
            const uploadEncoding = asRecord(
                encoding,
                `${context}[${slotIndex}].simulcastEncodings[${encodingIndex}]`
            );
            requireString(
                uploadEncoding.rid,
                `${context}[${slotIndex}].simulcastEncodings[${encodingIndex}].rid`
            );
            requireOptionalNonNegativeInteger(
                uploadEncoding.maxBitrate,
                `${context}[${slotIndex}].simulcastEncodings[${encodingIndex}].maxBitrate`
            );
            requireOptionalFiniteNumber(
                uploadEncoding.resolutionScale,
                `${context}[${slotIndex}].simulcastEncodings[${encodingIndex}].resolutionScale`
            );
            requireOptionalNonNegativeInteger(
                uploadEncoding.maxFramerate,
                `${context}[${slotIndex}].simulcastEncodings[${encodingIndex}].maxFramerate`
            );
        }
    });
}

function requireOptionalPositiveNumber(value: unknown, context: string): void {
    if (
        value !== undefined &&
        (typeof value !== "number" || !Number.isFinite(value) || value <= 0)
    ) {
        throw new Error(`${context} must be a positive number`);
    }
}

function requireOptionalFiniteNumber(value: unknown, context: string): void {
    if (value !== undefined && (typeof value !== "number" || !Number.isFinite(value))) {
        throw new Error(`${context} must be a finite number`);
    }
}

function validateOptionalPolicyRole(value: unknown, context: string): void {
    if (
        value !== undefined &&
        value !== "featured" &&
        value !== "thumbnail" &&
        value !== "degradedThumbnail"
    ) {
        throw new Error(`${context} must be a supported upload layer policy role`);
    }
}

function validateAvailableFeatures(value: unknown, context: string): AvailableFeatures {
    const features = asRecord(value, context);
    requireBoolean(features.rtc, `${context}.rtc`);
    requireBoolean(features.transcription, `${context}.transcription`);
    requireBoolean(features.audioRecording, `${context}.audioRecording`);
    requireBoolean(features.videoRecording, `${context}.videoRecording`);
    return value as AvailableFeatures;
}

function validateRecordingState(value: unknown, context: string): RecordingState {
    const state = asRecord(value, context);
    requireOptionalBoolean(state.recording, `${context}.recording`);
    requireOptionalBoolean(state.audio, `${context}.audio`);
    requireOptionalBoolean(state.video, `${context}.video`);
    requireOptionalBoolean(state.transcription, `${context}.transcription`);
    return value as RecordingState;
}

function validateSessionInfo(value: unknown, context: string): SessionInfo {
    const info = asRecord(value, context);
    requireOptionalBoolean(info.isTalking, `${context}.isTalking`);
    requireOptionalBoolean(info.isFeatured, `${context}.isFeatured`);
    requireOptionalBoolean(info.isCameraOn, `${context}.isCameraOn`);
    requireOptionalBoolean(info.isScreenSharingOn, `${context}.isScreenSharingOn`);
    requireOptionalBoolean(info.isSelfMuted, `${context}.isSelfMuted`);
    requireOptionalBoolean(info.isDeaf, `${context}.isDeaf`);
    requireOptionalBoolean(info.isRaisingHand, `${context}.isRaisingHand`);
    return value as SessionInfo;
}

function validateConnectionState(value: unknown, context: string): ConnectionState {
    if (
        value !== "disconnected" &&
        value !== "connecting" &&
        value !== "authenticated" &&
        value !== "connected" &&
        value !== "recovering" &&
        value !== "closed"
    ) {
        throw new Error(`${context} is invalid: ${String(value)}`);
    }
    return value;
}

function validateNegotiationKind(value: unknown, context: string): NegotiationKind {
    if (value !== NEGOTIATION_KIND.OFFER && value !== NEGOTIATION_KIND.RENEGOTIATE) {
        throw new Error(`${context} is invalid: ${String(value)}`);
    }
    return value;
}

function validatePendingRequestKind(value: unknown, context: string): PendingRequestKind {
    if (
        value !== PENDING_REQUEST_KIND.START_RECORDING &&
        value !== PENDING_REQUEST_KIND.STOP_RECORDING
    ) {
        throw new Error(`${context} is invalid: ${String(value)}`);
    }
    return value;
}

function validateStreamType(value: unknown, context: string): StreamType {
    if (value !== "audio" && value !== "camera" && value !== "screen") {
        throw new Error(`${context} is invalid: ${String(value)}`);
    }
    return value;
}

function validateUploadKind(value: unknown, context: string): void {
    if (value !== "audio" && value !== "video") {
        throw new Error(`${context} is invalid: ${String(value)}`);
    }
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

function validateRecordingStopCode(value: unknown, context: string): void {
    if (
        value !== "user_request" &&
        value !== "channel_closed" &&
        value !== "recording_timeout" &&
        value !== "recording_failed" &&
        value !== "disk_space_exhausted"
    ) {
        throw new Error(`${context} is invalid: ${String(value)}`);
    }
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

function validateOptionalStringArray(value: unknown, context: string): void {
    if (value === undefined) {
        return;
    }
    if (!Array.isArray(value)) {
        throw new Error(`${context} must be an array when provided`);
    }
    value.forEach((entry, index) => {
        requireString(entry, `${context}[${index}]`);
    });
}

function requireBoolean(value: unknown, context: string): boolean {
    if (typeof value !== "boolean") {
        throw new Error(`${context} must be a boolean`);
    }
    return value;
}

function requireOptionalBoolean(value: unknown, context: string): void {
    if (value !== undefined && typeof value !== "boolean") {
        throw new Error(`${context} must be a boolean when provided`);
    }
}

function requireInteger(value: unknown, context: string): number {
    if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
        throw new Error(`${context} must be a non-negative integer`);
    }
    return value;
}

function requireOptionalNonNegativeInteger(value: unknown, context: string): void {
    if (value === undefined) {
        return;
    }
    if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
        throw new Error(`${context} must be a non-negative integer when provided`);
    }
}

function requireOptionalTemporalLayerId(value: unknown, context: string): void {
    if (value === undefined) {
        return;
    }
    if (
        typeof value !== "number" ||
        !Number.isInteger(value) ||
        value < MIN_TEMPORAL_LAYER_ID ||
        value > MAX_TEMPORAL_LAYER_ID
    ) {
        throw new Error(
            `${context} must be an integer from ${MIN_TEMPORAL_LAYER_ID} through ${MAX_TEMPORAL_LAYER_ID} when provided`
        );
    }
}
