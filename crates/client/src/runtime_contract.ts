import type {
    AvailableFeatures,
    ConnectionState,
    DownloadStates,
    RecordingOptions,
    RecordingState,
    SessionId,
    SessionInfo,
    StreamType
} from "./public_api.js";
import type { NegotiationKind } from "./protocol_contract.js";
import type { HostCommand } from "./protocol_host_commands.js";

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

export function createProtocolCore(): ProtocolCoreBindings {
    if (!defaultWasmProtocolCoreProvider) {
        throw new Error(
            "default WASM protocol core provider is not configured; import the package entrypoint or configure one explicitly"
        );
    }
    return defaultWasmProtocolCoreProvider();
}
