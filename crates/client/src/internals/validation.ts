import {
    STREAM_TYPES,
    VIDEO_LAYOUT_INTENTS,
    type ConnectOptions,
    type DownloadStates,
    type StreamType
} from "../public_api.js";
import { STREAM_KIND } from "./browser_types.js";

const STREAM_TYPE_SET = new Set<StreamType>(STREAM_TYPES);
const VIDEO_LAYOUT_INTENT_SET = new Set(VIDEO_LAYOUT_INTENTS);

export function normalizeWebSocketUrl(url: string): string {
    return url.replace(/^http(s?):/i, (_match, secure) => (secure ? "wss:" : "ws:"));
}

export function validateConnectOptions(options: ConnectOptions): void {
    if (options.channelUUID !== undefined && typeof options.channelUUID !== "string") {
        throw new Error("connect options channelUUID must be a string when provided");
    }
    if (options.iceServers === undefined) {
        return;
    }
    if (!Array.isArray(options.iceServers)) {
        throw new Error("connect options iceServers must be an array when provided");
    }
    for (const iceServer of options.iceServers) {
        if (!iceServer || typeof iceServer !== "object") {
            throw new Error("each ICE server entry must be an object");
        }
        const { urls } = iceServer;
        if (
            typeof urls !== "string" &&
            !(Array.isArray(urls) && urls.every((url) => typeof url === "string" && url.length > 0))
        ) {
            throw new Error("each ICE server must expose urls as a string or a string array");
        }
    }
}

export function cloneIceServers(iceServers?: RTCIceServer[]): RTCIceServer[] | undefined {
    return iceServers?.map((server) => ({
        ...server,
        urls: Array.isArray(server.urls) ? [...server.urls] : server.urls
    }));
}

export function validateDownloadStates(states: DownloadStates): void {
    for (const value of [states.audio, states.camera, states.screen]) {
        if (value !== undefined && typeof value !== "boolean") {
            throw new Error("download state flags must be booleans when provided");
        }
    }
    for (const [name, value] of [
        ["cameraLayout", states.cameraLayout],
        ["screenLayout", states.screenLayout]
    ] as const) {
        if (value !== undefined && !VIDEO_LAYOUT_INTENT_SET.has(value)) {
            throw new Error(`${name} must be a valid video layout intent when provided`);
        }
    }
}

export function validateTrackForStreamType(
    type: StreamType,
    track: MediaStreamTrack | null | undefined
): void {
    if (!STREAM_TYPE_SET.has(type)) {
        throw new Error("stream type must be audio, camera, or screen");
    }
    if (track == null) {
        return;
    }
    if (
        typeof track !== "object" ||
        typeof track.kind !== "string" ||
        typeof track.id !== "string"
    ) {
        throw new Error("upload track must be a MediaStreamTrack-compatible object");
    }
    if (track.kind !== STREAM_KIND[type]) {
        throw new Error(`${type} uploads require a ${STREAM_KIND[type]} track`);
    }
}
