import { spawn } from "node:child_process";
import { createHmac, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const TEST_AUTH_KEY = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
const TEST_SFU_HTTP_BASE_URL = "http://127.0.0.1:18080";
export const TEST_SFU_WS_URL = "ws://127.0.0.1:18080/";
const DIAGNOSTICS_ROOM_PATH = "/internal/diagnostics/rooms";
const DIAGNOSTICS_POLL_INTERVAL_MS = 100;
const DIAGNOSTICS_POLL_TIMEOUT_MS = 5_000;
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
        throw new Error(`expected room creation to succeed, got HTTP ${response.status}`);
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

export async function setCameraDownload(page, targetSessionId, active, cameraLayout = undefined) {
    await page.evaluate(
        ({
            active: nextActive,
            cameraLayout: nextCameraLayout,
            targetSessionId: nextTargetSessionId
        }) => {
            const harness = globalThis.__liveHarness;
            if (!harness.client) {
                throw new Error("browser harness client is not connected");
            }
            const states = {
                camera: nextActive
            };
            if (nextCameraLayout !== undefined) {
                states.cameraLayout = nextCameraLayout;
            }
            harness.client.updateDownload(nextTargetSessionId, {
                ...states
            });
        },
        { active, cameraLayout, targetSessionId }
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

export async function latestTrackUpdate(page, targetSessionId, targetType) {
    return page.evaluate(
        ({ sessionId: nextSessionId, type: nextType }) => {
            const harness = globalThis.__liveHarness;
            return (
                harness.updates
                    .filter(
                        (update) =>
                            update.name === "track" &&
                            update.payload.sessionId === nextSessionId &&
                            update.payload.type === nextType
                    )
                    .at(-1) ?? null
            );
        },
        { sessionId: targetSessionId, type: targetType }
    );
}

export async function peerLocalDescriptionSdp(page) {
    return page.evaluate(() => {
        const peerConnection = globalThis.__liveHarness.client?._runtime?._peerConnection;
        return peerConnection?.localDescription?.sdp ?? null;
    });
}

export async function localCameraSenderEncodings(page) {
    return page.evaluate(() => {
        const harness = globalThis.__liveHarness;
        const peerConnection = harness.client?._runtime?._peerConnection;
        const localTrack = harness.localTrack;
        if (!peerConnection || !localTrack) {
            return [];
        }
        const transceiver = peerConnection
            .getTransceivers()
            .find((candidate) => candidate.sender.track === localTrack);
        return (transceiver?.sender.getParameters().encodings ?? []).map((encoding) => ({
            active: encoding.active,
            maxBitrate: encoding.maxBitrate,
            rid: encoding.rid,
            scaleResolutionDownBy: encoding.scaleResolutionDownBy
        }));
    });
}

export async function waitForUserMediaWorker({
    expectedMediaWorkerId,
    httpBaseUrl = TEST_SFU_HTTP_BASE_URL,
    roomId,
    userId
}) {
    return waitForDiagnosticsMatch(
        httpBaseUrl,
        roomId,
        (room) => {
            const user = room.users.find((candidate) => userIdsMatch(candidate.userId, userId));
            return user?.transport?.mediaWorkerId === expectedMediaWorkerId ? room : null;
        },
        `expected user ${String(userId)} on media worker ${expectedMediaWorkerId}`
    );
}

export async function waitForCameraSubscriptionSelectedRid({
    consumerSessionId,
    expectedRid,
    httpBaseUrl = TEST_SFU_HTTP_BASE_URL,
    producerSessionId,
    roomId
}) {
    return waitForDiagnosticsMatch(
        httpBaseUrl,
        roomId,
        (room) => {
            const subscription = cameraSubscription(room, consumerSessionId, producerSessionId);
            if (!subscription || subscription.state !== "active") {
                return null;
            }
            return cameraSubscriptionSelectedRid(room, subscription) === expectedRid ? room : null;
        },
        `expected camera subscription for ${String(consumerSessionId)} from ${String(
            producerSessionId
        )} to select RID ${expectedRid}`
    );
}

export async function waitForDecodedRemoteCameraFrame(page, targetSessionId) {
    return page.evaluate(
        async ({ sessionId }) => {
            const sleep = (ms) =>
                new Promise((resolve) => {
                    window.setTimeout(resolve, ms);
                });
            const nextVideoFrameMetadata = async (video, timeoutMs) => {
                if (typeof video.requestVideoFrameCallback !== "function") {
                    await sleep(Math.min(timeoutMs, 100));
                    return null;
                }
                return new Promise((resolve) => {
                    const timeout = window.setTimeout(
                        () => resolve(null),
                        Math.min(timeoutMs, 1_000)
                    );
                    video.requestVideoFrameCallback((_now, metadata) => {
                        window.clearTimeout(timeout);
                        resolve(metadata);
                    });
                });
            };
            const requestPlayback = (video) => {
                const playPromise = video.play();
                if (playPromise && typeof playPromise.catch === "function") {
                    playPromise.catch(() => null);
                }
            };
            const drawDecodedFrame = (video, usedVideoFrameCallback, decodedFrames) => {
                const canvas = document.createElement("canvas");
                canvas.width = 1;
                canvas.height = 1;
                const context = canvas.getContext("2d");
                if (!context) {
                    throw new Error("expected 2D canvas context for decoded frame check");
                }
                context.drawImage(video, 0, 0, 1, 1);
                const [red, green, blue, alpha] = context.getImageData(0, 0, 1, 1).data;
                return {
                    decodedFrames,
                    height: video.videoHeight,
                    pixel: { alpha, blue, green, red },
                    usedVideoFrameCallback,
                    width: video.videoWidth
                };
            };
            const harness = globalThis.__liveHarness;
            const client = harness.client;
            const track =
                client?._consumers?.get(sessionId)?.camera?.track ??
                client?._consumers?.get(String(sessionId))?.camera?.track ??
                null;
            if (!track) {
                throw new Error(`remote camera track for session ${String(sessionId)} is missing`);
            }

            const video = document.createElement("video");
            video.autoplay = true;
            video.muted = true;
            video.playsInline = true;
            video.srcObject = new MediaStream([track]);
            document.body.append(video);

            const startedAt = performance.now();
            const deadline = startedAt + 12_000;
            requestPlayback(video);
            const initialCurrentTime = video.currentTime;
            const initialDecodedFrames = video.getVideoPlaybackQuality?.().totalVideoFrames ?? 0;
            let usedVideoFrameCallback = false;

            while (performance.now() < deadline) {
                if (video.paused) {
                    requestPlayback(video);
                }
                const metadata = await nextVideoFrameMetadata(video, deadline - performance.now());
                usedVideoFrameCallback ||= metadata !== null;
                if (video.videoWidth > 0 && video.videoHeight > 0) {
                    const decodedFrames =
                        video.getVideoPlaybackQuality?.().totalVideoFrames ??
                        (metadata ? metadata.presentedFrames : 0);
                    const decodedFrameObserved =
                        decodedFrames > initialDecodedFrames ||
                        metadata !== null ||
                        video.currentTime > initialCurrentTime;
                    if (decodedFrameObserved) {
                        return drawDecodedFrame(video, usedVideoFrameCallback, decodedFrames);
                    }
                }
                await sleep(100);
            }

            throw new Error("remote camera video did not decode a frame before timeout");
        },
        { sessionId: targetSessionId }
    );
}

export async function spawnLiveServer({
    authKey = TEST_AUTH_KEY,
    bindHost = "127.0.0.1",
    bindPort,
    host = "127.0.0.1",
    publicIp = host,
    rtcMaxPort,
    rtcMinPort,
    codecFlags = {},
    spillover = {}
}) {
    const env = {
        ...process.env,
        AUTH_KEY: authKey,
        BIND_ADDRESS: `${bindHost}:${bindPort}`,
        PUBLIC_IP: publicIp,
        RTC_MAX_PORT: String(rtcMaxPort),
        RTC_MIN_PORT: String(rtcMinPort),
        CODEC_H264: String(Boolean(codecFlags.h264)),
        CODEC_VP9: String(Boolean(codecFlags.vp9)),
        ...spilloverEnv(spillover)
    };
    if (Object.hasOwn(codecFlags, "vp8")) {
        env.CODEC_VP8 = String(Boolean(codecFlags.vp8));
    }
    const child = spawn(
        "cargo",
        ["run", "--quiet", "--manifest-path", "../../Cargo.toml", "-p", "o-sfu"],
        {
            cwd: fileURLToPath(new URL("../", import.meta.url)),
            env,
            stdio: "ignore"
        }
    );
    const httpBaseUrl = `http://${host}:${bindPort}`;
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
                    wsUrl: `ws://${host}:${bindPort}/`
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

async function waitForDiagnosticsMatch(httpBaseUrl, roomId, matches, failureMessage) {
    const deadline = Date.now() + DIAGNOSTICS_POLL_TIMEOUT_MS;
    let lastRoom = null;
    while (Date.now() < deadline) {
        lastRoom = await fetchRoomDiagnostics(httpBaseUrl, roomId);
        const matched = lastRoom ? matches(lastRoom) : null;
        if (matched) {
            return matched;
        }
        await delay(DIAGNOSTICS_POLL_INTERVAL_MS);
    }
    throw new Error(`${failureMessage}; last diagnostics: ${JSON.stringify(lastRoom)}`);
}

async function fetchRoomDiagnostics(httpBaseUrl, roomId) {
    const response = await fetch(`${httpBaseUrl}${DIAGNOSTICS_ROOM_PATH}/${roomId}`);
    if (!response.ok) {
        return null;
    }
    return response.json();
}

function cameraSubscription(room, consumerSessionId, producerSessionId) {
    return room.users
        .find((user) => userIdsMatch(user.userId, consumerSessionId))
        ?.subscriptions.find(
            (subscription) =>
                userIdsMatch(subscription.producerUserId, producerSessionId) &&
                subscription.streamId === "camera"
        );
}

function cameraSubscriptionSelectedRid(room, subscription) {
    if (subscription.selection?.selectedRid) {
        return subscription.selection.selectedRid;
    }
    const policyRole = policyRoleForLayoutRole(subscription.layoutRole);
    if (!policyRole) {
        return null;
    }
    return (
        room.sources
            .find((source) => source.sourceId === subscription.sourceId)
            ?.encodings.find((encoding) => encoding.policyRole === policyRole)?.rid ?? null
    );
}

function policyRoleForLayoutRole(layoutRole) {
    switch (layoutRole) {
        case "active_speaker":
        case "featured":
        case "pinned":
        case "readable_detail":
            return "featured";
        case "visible_thumbnail":
            return "thumbnail";
        default:
            return null;
    }
}

function userIdsMatch(actual, expected) {
    return String(actual) === String(expected);
}

function spilloverEnv({
    activationWindow,
    minReceivers,
    mode,
    roomMaxLocalRouters,
    rtcMediaWorkerCount
}) {
    const env = {};
    if (rtcMediaWorkerCount !== undefined) {
        env.RTC_MEDIA_WORKER_COUNT = String(rtcMediaWorkerCount);
    }
    if (roomMaxLocalRouters !== undefined) {
        env.ROOM_MAX_LOCAL_ROUTERS = String(roomMaxLocalRouters);
    }
    if (mode !== undefined) {
        env.ROOM_SPILLOVER_MODE = mode;
    }
    if (minReceivers !== undefined) {
        env.ROOM_SPILLOVER_MIN_RECEIVERS = String(minReceivers);
    }
    if (activationWindow !== undefined) {
        env.ROOM_SPILLOVER_ACTIVATION_WINDOW = String(activationWindow);
    }
    return env;
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
