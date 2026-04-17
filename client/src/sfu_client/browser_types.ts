import type { TrackBinding } from "../protocol.js";
import type { StreamType } from "../public_api.js";
import type { ProtocolCoreFactory } from "../runtime_contract.js";

export type MediaTrack = MediaStreamTrack;

export type TimerHandle = ReturnType<typeof globalThis.setTimeout>;

export type PendingRequestCallbacks = {
    reject: (error: Error) => void;
    resolve: (ok: boolean) => void;
};

export type ConsumerCompat = {
    track: MediaTrack | null;
};

export type ConsumersCompat = {
    audio: ConsumerCompat | null;
    camera: ConsumerCompat | null;
    screen: ConsumerCompat | null;
};

export type AppliedTrackBinding = Pick<TrackBinding, "active" | "sessionId" | "type">;

export interface ClientWebSocket {
    close(code?: number): void;
    onclose: ((event: { code: number }) => void) | null;
    onerror: ((event: Event) => void) | null;
    onmessage: ((event: { data: unknown }) => void) | null;
    onopen: ((event: Event) => void) | null;
    readonly readyState: number;
    send(data: string): void;
}

export interface PeerConnectionSender {
    getStats?(): Promise<RTCStatsReport>;
    replaceTrack(track: MediaTrack | null): Promise<void>;
    track?: MediaTrack | null;
}

export type PeerConnectionTransceiverDirection = "sendrecv" | "sendonly" | "recvonly" | "inactive";

export interface PeerConnectionTransceiver {
    mid: string | null;
    currentDirection?: PeerConnectionTransceiverDirection | null;
    direction?: PeerConnectionTransceiverDirection;
    receiver?: {
        track?: MediaTrack | null;
    };
    sender: PeerConnectionSender;
}

export interface PeerConnectionTrackEvent {
    track: MediaTrack;
    transceiver: {
        mid: string | null;
    };
}

export type ClientIceGatheringState = "new" | "gathering" | "complete";

export type ClientIceConnectionState =
    | "new"
    | "checking"
    | "connected"
    | "completed"
    | "disconnected"
    | "failed"
    | "closed";

export type ClientPeerConnectionState =
    | "new"
    | "connecting"
    | "connected"
    | "disconnected"
    | "failed"
    | "closed";

export interface ClientPeerConnection {
    close(): void;
    connectionState?: ClientPeerConnectionState;
    createAnswer(): Promise<{ sdp: string; type: "answer" }>;
    getStats?(): Promise<RTCStatsReport>;
    getTransceivers(): PeerConnectionTransceiver[];
    iceConnectionState?: ClientIceConnectionState;
    iceGatheringState?: ClientIceGatheringState;
    localDescription?: { sdp: string; type: "answer" } | null;
    onicecandidate: ((event: { candidate: { candidate: string } | null }) => void) | null;
    oniceconnectionstatechange: (() => void) | null;
    onicegatheringstatechange: (() => void) | null;
    onconnectionstatechange: (() => void) | null;
    ontrack: ((event: PeerConnectionTrackEvent) => void) | null;
    setLocalDescription(description: { sdp: string; type: "answer" }): Promise<void>;
    setRemoteDescription(description: { sdp: string; type: "offer" }): Promise<void>;
}

export interface SfuClientDependencies {
    clearTimer?: (handle: TimerHandle) => void;
    createPeerConnection?: (config: RTCConfiguration) => ClientPeerConnection;
    createProtocolCore?: ProtocolCoreFactory;
    createWebSocket?: (url: string) => ClientWebSocket;
    setTimer?: (callback: () => void, ms: number) => TimerHandle;
}

export const EMPTY_FEATURES = {
    rtc: false,
    transcription: false,
    audioRecording: false,
    videoRecording: false
};

export function createEmptyConsumers(): ConsumersCompat {
    return {
        audio: null,
        camera: null,
        screen: null
    };
}

export const STREAM_KIND: Record<StreamType, "audio" | "video"> = {
    audio: "audio",
    camera: "video",
    screen: "video"
};
