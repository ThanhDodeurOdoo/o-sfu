import {
    STREAM_TYPES,
    VIDEO_LAYOUT_INTENTS,
    type ConnectOptions,
    type DownloadStates,
    type StreamType
} from "../public_api.js";
import { STREAM_KIND } from "./browser_types.js";

const STREAM_TYPE_SET = new Set<StreamType>(STREAM_TYPES);
const DOWNLOAD_LAYOUT_FIELDS = ["cameraLayout", "screenLayout"] as const;
const DOWNLOAD_STATE_FIELDS = [...STREAM_TYPES, ...DOWNLOAD_LAYOUT_FIELDS] as const;
const DOWNLOAD_STATE_FIELD_SET = new Set<string>(DOWNLOAD_STATE_FIELDS);

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
    for (const field of Object.keys(states)) {
        if (!DOWNLOAD_STATE_FIELD_SET.has(field)) {
            throw new Error(`download state field ${field} is invalid`);
        }
    }
    if (
        STREAM_TYPES.some((type) => states[type] !== undefined && typeof states[type] !== "boolean")
    ) {
        throw new Error("download state flags must be booleans when provided");
    }
    for (const field of DOWNLOAD_LAYOUT_FIELDS) {
        const value = states[field];
        if (value !== undefined && !VIDEO_LAYOUT_INTENTS.includes(value)) {
            throw new Error(`${field} must be a valid video layout intent when provided`);
        }
    }
}

export function mergeDownloadStates(
    previous: DownloadStates | undefined,
    next: DownloadStates
): DownloadStates {
    return Object.fromEntries(
        DOWNLOAD_STATE_FIELDS.flatMap((field) => {
            const value = next[field] ?? previous?.[field];
            return value === undefined ? [] : [[field, value]];
        })
    ) as DownloadStates;
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
