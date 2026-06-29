import {
    type AvailableFeatures,
    type ConnectionState,
    type DownloadStates,
    type RecordingOptions,
    type RecordingState,
    type SessionId,
    type SessionInfo,
    type StreamType
} from "./public_api.js";
import {
    validateAvailableFeatures,
    validateConnectionState,
    validateRecordingState
} from "./public_api_validation.js";
import type { NegotiationKind } from "./protocol_contract.js";
import { type HostCommand, validateHostCommandShapes } from "./protocol_host_commands.js";

export type { HostCommand, PendingRequest } from "./protocol_host_commands.js";

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
}

export type ProtocolCoreProvider = () => ProtocolCoreBindings;

let defaultWasmProtocolCoreProvider: ProtocolCoreProvider | undefined;

export function configureDefaultWasmProtocolCoreProvider(provider: ProtocolCoreProvider): void {
    defaultWasmProtocolCoreProvider = provider;
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
        connect: (url, jwt, room) =>
            validateHostCommandShapes(bindings.connect(url, jwt, room), "protocol core connect()"),
        onWsOpen: () => validateHostCommandShapes(bindings.onWsOpen(), "protocol core onWsOpen()"),
        onWsMessage: (frame) =>
            validateHostCommandShapes(bindings.onWsMessage(frame), "protocol core onWsMessage()"),
        onTransportReady: () =>
            validateHostCommandShapes(
                bindings.onTransportReady(),
                "protocol core onTransportReady()"
            ),
        onWsClose: (code) =>
            validateHostCommandShapes(bindings.onWsClose(code), "protocol core onWsClose()"),
        onTimer: (timerId) =>
            validateHostCommandShapes(bindings.onTimer(timerId), "protocol core onTimer()"),
        publish: (type, active) =>
            validateHostCommandShapes(bindings.publish(type, active), "protocol core publish()"),
        subscribe: (sessionId, states) =>
            validateHostCommandShapes(
                bindings.subscribe(sessionId, states),
                "protocol core subscribe()"
            ),
        updateInfo: (info) =>
            validateHostCommandShapes(bindings.updateInfo(info), "protocol core updateInfo()"),
        broadcast: (message) =>
            validateHostCommandShapes(bindings.broadcast(message), "protocol core broadcast()"),
        startRecording: (options) =>
            validateHostCommandShapes(
                bindings.startRecording(options),
                "protocol core startRecording()",
                true
            ),
        stopRecording: () =>
            validateHostCommandShapes(
                bindings.stopRecording(),
                "protocol core stopRecording()",
                true
            ),
        submitNegotiationAnswer: (requestId, negotiationKind, sdp) =>
            validateHostCommandShapes(
                bindings.submitNegotiationAnswer(requestId, negotiationKind, sdp),
                "protocol core submitNegotiationAnswer()"
            ),
        disconnect: () =>
            validateHostCommandShapes(bindings.disconnect(), "protocol core disconnect()")
    };
}

export function createProtocolCore(): ProtocolCoreBindings {
    return wrapProtocolCoreBindings(requireDefaultWasmProtocolCoreProvider()());
}

function requireDefaultWasmProtocolCoreProvider(): ProtocolCoreProvider {
    if (!defaultWasmProtocolCoreProvider) {
        throw new Error(
            "default WASM protocol core provider is not configured; import the package entrypoint or configure one explicitly"
        );
    }
    return defaultWasmProtocolCoreProvider;
}
