import type { TrackBinding } from "../protocol.js";
import type { StreamType } from "../public_api.js";
import type { ProtocolCoreFactory } from "../runtime_contract.js";

export type TrackLike = MediaStreamTrack & { muted?: boolean };

export type TimerHandle = ReturnType<typeof globalThis.setTimeout>;

export type PendingRequestCallbacks = {
    reject: (error: Error) => void;
    resolve: (ok: boolean) => void;
};

export type ConsumerCompat = {
    track: TrackLike | null;
};

export type ConsumersCompat = {
    audio: ConsumerCompat | null;
    camera: ConsumerCompat | null;
    screen: ConsumerCompat | null;
};

export type AppliedTrackBinding = Pick<TrackBinding, "active" | "sessionId" | "type">;

export type WebSocketLike = {
    close(code?: number): void;
    onclose: ((event: { code: number }) => void) | null;
    onerror: ((event: Event) => void) | null;
    onmessage: ((event: { data: unknown }) => void) | null;
    onopen: ((event: Event) => void) | null;
    readonly readyState: number;
    send(data: string): void;
};

export type RtcSenderLike = {
    replaceTrack(track: TrackLike | null): Promise<void>;
    track?: TrackLike | null;
};

export type RtcTransceiverLike = {
    mid: string | null;
    sender: RtcSenderLike;
};

export type RtcTrackEventLike = {
    track: TrackLike;
    transceiver: {
        mid: string | null;
    };
};

export type PeerConnectionLike = {
    close(): void;
    createAnswer(): Promise<{ sdp: string; type: "answer" }>;
    getTransceivers(): RtcTransceiverLike[];
    ontrack: ((event: RtcTrackEventLike) => void) | null;
    setLocalDescription(description: { sdp: string; type: "answer" }): Promise<void>;
    setRemoteDescription(description: { sdp: string; type: "offer" }): Promise<void>;
};

export interface SfuClientDependencies {
    clearTimer?: (handle: TimerHandle) => void;
    createPeerConnection?: (config: RTCConfiguration) => PeerConnectionLike;
    createProtocolCore?: ProtocolCoreFactory;
    createWebSocket?: (url: string) => WebSocketLike;
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
