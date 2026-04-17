import { spawn } from "node:child_process";
import { createHmac, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const TEST_AUTH_KEY = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
const TEST_SFU_HTTP_BASE_URL = "http://127.0.0.1:18080";
export const TEST_SFU_WS_URL = "ws://127.0.0.1:18080/";
const HARNESS_URL = "/playwright/fixtures/harness.html";

export async function createChannel({
    authKey = TEST_AUTH_KEY,
    httpBaseUrl = TEST_SFU_HTTP_BASE_URL
} = {}) {
    const response = await fetch(`${httpBaseUrl}/v1/channel`, {
        headers: {
            Authorization: `Bearer ${signJwt(
                {
                    iss: `playwright-${randomUUID()}`
                },
                authKey
            )}`
        }
    });
    if (!response.ok) {
        throw new Error(`expected channel creation to succeed, got HTTP ${response.status}`);
    }
    const payload = await response.json();
    return payload.uuid;
}

export function createConnectToken(channelUuid, sessionId, authKey = TEST_AUTH_KEY) {
    return signJwt(
        {
            sfu_channel_uuid: channelUuid,
            session_id: sessionId
        },
        authKey
    );
}

export async function createPeerPage(context) {
    const page = await context.newPage();
    await page.goto(HARNESS_URL);
    await page.evaluate(() => {
        globalThis.__liveHarness = {
            client: null,
            errors: [],
            localTrack: null,
            localTrackTicker: null,
            stateChanges: [],
            updates: []
        };
    });
    return page;
}

export async function connectPeer(page, { channelUuid, jwt, url = TEST_SFU_WS_URL }) {
    await page.evaluate(
        async ({ channelUuid: nextChannelUuid, jwt: nextJwt, url: nextUrl }) => {
            const serializeTrack = (track) =>
                track
                    ? {
                          enabled: track.enabled,
                          id: track.id,
                          kind: track.kind,
                          muted: track.muted,
                          readyState: track.readyState
                      }
                    : null;
            const serializeUpdate = (detail) => {
                if (detail.name === "track") {
                    return {
                        name: detail.name,
                        payload: {
                            active: detail.payload.active,
                            sessionId: detail.payload.sessionId,
                            track: serializeTrack(detail.payload.track),
                            type: detail.payload.type
                        }
                    };
                }
                return structuredClone(detail);
            };

            const { SfuClient } = await import("/dist/index.js");
            const harness = globalThis.__liveHarness;
            const client = new SfuClient();
            harness.client = client;
            harness.errors = [];
            harness.stateChanges = [];
            harness.updates = [];
            client.addEventListener("handledError", (event) => {
                harness.errors.push(String(event.detail.error));
            });
            client.addEventListener("stateChange", (event) => {
                harness.stateChanges.push(structuredClone(event.detail));
            });
            client.addEventListener("update", (event) => {
                harness.updates.push(serializeUpdate(event.detail));
            });
            client.connect(nextUrl, nextJwt, {
                channelUUID: nextChannelUuid
            });
        },
        { channelUuid, jwt, url }
    );
}

export async function publishSyntheticCamera(page, label) {
    await page.evaluate((nextLabel) => {
        const harness = globalThis.__liveHarness;
        if (!harness.client) {
            throw new Error("browser harness client is not connected");
        }
        if (harness.localTrackTicker !== null) {
            clearInterval(harness.localTrackTicker);
            harness.localTrackTicker = null;
        }
        if (harness.localTrack) {
            harness.localTrack.stop();
            harness.localTrack = null;
        }

        const canvas = document.createElement("canvas");
        canvas.width = 96;
        canvas.height = 96;
        const context = canvas.getContext("2d");
        if (!context) {
            throw new Error("expected 2D canvas context for synthetic video track");
        }
        let frame = 0;
        const draw = () => {
            context.fillStyle = frame % 2 === 0 ? "#14324a" : "#5b2d1f";
            context.fillRect(0, 0, canvas.width, canvas.height);
            context.fillStyle = "#f3f4f6";
            context.font = "14px sans-serif";
            context.fillText(nextLabel, 8, 28);
            context.fillText(String(frame), 8, 56);
            frame += 1;
        };
        draw();
        harness.localTrackTicker = window.setInterval(draw, 100);
        const stream = canvas.captureStream(10);
        const [track] = stream.getVideoTracks();
        if (!track) {
            throw new Error("expected synthetic canvas capture to expose a video track");
        }
        harness.localTrack = track;
        harness.client.updateUpload("camera", track);
    }, label);
}

export async function setCameraDownload(page, targetSessionId, active) {
    await page.evaluate(
        ({ active: nextActive, targetSessionId: nextTargetSessionId }) => {
            const harness = globalThis.__liveHarness;
            if (!harness.client) {
                throw new Error("browser harness client is not connected");
            }
            harness.client.updateDownload(nextTargetSessionId, {
                camera: nextActive
            });
        },
        { active, targetSessionId }
    );
}

export async function unpublishCamera(page) {
    await page.evaluate(() => {
        const harness = globalThis.__liveHarness;
        if (!harness.client) {
            throw new Error("browser harness client is not connected");
        }
        harness.client.updateUpload("camera", null);
        if (harness.localTrackTicker !== null) {
            clearInterval(harness.localTrackTicker);
            harness.localTrackTicker = null;
        }
        if (harness.localTrack) {
            harness.localTrack.stop();
            harness.localTrack = null;
        }
    });
}

export async function peerSnapshot(page) {
    return page.evaluate(() => {
        const serializeTrack = (track) =>
            track
                ? {
                      enabled: track.enabled,
                      id: track.id,
                      kind: track.kind,
                      muted: track.muted,
                      readyState: track.readyState
                  }
                : null;
        const serializeConsumers = (consumers) =>
            Object.fromEntries(
                [...consumers.entries()].map(([sessionId, entry]) => [
                    String(sessionId),
                    {
                        audio: serializeTrack(entry.audio?.track ?? null),
                        camera: serializeTrack(entry.camera?.track ?? null),
                        screen: serializeTrack(entry.screen?.track ?? null)
                    }
                ])
            );
        const harness = globalThis.__liveHarness;
        const client = harness.client;
        return {
            consumers: client ? serializeConsumers(client._consumers) : {},
            errors: [...harness.errors],
            state: client?.state ?? null,
            stateChanges: [...harness.stateChanges],
            updates: [...harness.updates]
        };
    });
}

export async function peerLocalDescriptionSdp(page) {
    return page.evaluate(() => {
        const peerConnection = globalThis.__liveHarness.client?._runtime?._peerConnection;
        return peerConnection?.localDescription?.sdp ?? null;
    });
}

export async function spawnLiveServer({
    authKey = TEST_AUTH_KEY,
    bindPort,
    rtcMaxPort,
    rtcMinPort,
    codecFlags = {}
}) {
    const child = spawn(
        "cargo",
        ["run", "--quiet", "--manifest-path", "../Cargo.toml", "-p", "o-sfu"],
        {
            cwd: fileURLToPath(new URL("../", import.meta.url)),
            env: {
                ...process.env,
                AUTH_KEY: authKey,
                BIND_ADDRESS: `127.0.0.1:${bindPort}`,
                PUBLIC_IP: "127.0.0.1",
                RTC_MAX_PORT: String(rtcMaxPort),
                RTC_MIN_PORT: String(rtcMinPort),
                TRANSPORT_BACKEND: "rtc",
                ENABLE_CODEC_H264: String(Boolean(codecFlags.h264)),
                ENABLE_CODEC_VP9: String(Boolean(codecFlags.vp9))
            },
            stdio: "ignore"
        }
    );
    const httpBaseUrl = `http://127.0.0.1:${bindPort}`;
    for (let attempt = 0; attempt < 60; attempt += 1) {
        try {
            const response = await fetch(`${httpBaseUrl}/v1/noop`);
            if (response.ok) {
                return {
                    authKey,
                    child,
                    httpBaseUrl,
                    stop: async () => {
                        child.kill("SIGTERM");
                        await onceExit(child);
                    },
                    wsUrl: `ws://127.0.0.1:${bindPort}/`
                };
            }
        } catch (_error) {
            void _error;
        }
        await delay(500);
    }
    child.kill("SIGTERM");
    await onceExit(child);
    throw new Error(`o-sfu test server on port ${bindPort} did not become ready`);
}

function signJwt(payload, keyB64) {
    const encodedHeader = encodeJwtSegment({
        alg: "HS256",
        typ: "JWT"
    });
    const encodedPayload = encodeJwtSegment(payload);
    const signedData = `${encodedHeader}.${encodedPayload}`;
    const signature = createHmac("sha256", Buffer.from(keyB64, "base64"))
        .update(signedData)
        .digest("base64url");
    return `${signedData}.${signature}`;
}

function encodeJwtSegment(value) {
    return Buffer.from(JSON.stringify(value)).toString("base64url");
}

async function onceExit(child) {
    if (child.exitCode !== null) {
        return;
    }
    await new Promise((resolve) => {
        child.once("exit", resolve);
    });
}
