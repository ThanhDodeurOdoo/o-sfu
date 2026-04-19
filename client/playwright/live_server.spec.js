import { expect, test } from "@playwright/test";

import {
    connectPeer,
    createChannel,
    createConnectToken,
    createPeerPage,
    latestTrackUpdate,
    peerLocalDescriptionSdp,
    peerSnapshot,
    publishSyntheticCamera,
    setCameraDownload,
    spawnLiveServer,
    unpublishCamera
} from "./live_server_helpers.mjs";

const PUBLISHER_SESSION_ID = 41;
const SUBSCRIBER_SESSION_ID = 42;

test("browser compatibility upload and download flows survive live-server replacement", async ({
    context
}) => {
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const subscriber = await createPeerPage(context);

    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

    await publishSyntheticCamera(publisher, "initial-camera");

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);

    await setCameraDownload(subscriber, PUBLISHER_SESSION_ID, false);

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, false);

    const replacement = await createPeerPage(context);
    await connectPeer(replacement, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(replacement)).state).toBe("connected");
    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(publisher);
            return snapshot.stateChanges.at(-1);
        })
        .toEqual({
            cause: "kicked",
            state: "closed"
        });
    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "disconnect" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID
            ).length;
        })
        .toBeGreaterThan(0);

    await publishSyntheticCamera(replacement, "replacement-camera");

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, false);

    await setCameraDownload(subscriber, PUBLISHER_SESSION_ID, true);

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);

    await unpublishCamera(replacement);

    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.camera ?? null)
        .toBeNull();
});

test("late-joining subscriber receives the already-live publication", async ({ context }) => {
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await publishSyntheticCamera(publisher, "live-before-join");

    const subscriber = await createPeerPage(context);
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");
    await expect
        .poll(async () => latestTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "camera"))
        .toMatchObject(cameraTrackUpdateExpectation(PUBLISHER_SESSION_ID, true));
    await expect
        .poll(async () => {
            return (
                (await peerSnapshot(subscriber)).consumers[String(PUBLISHER_SESSION_ID)]?.camera ??
                null
            );
        })
        .toMatchObject({
            enabled: true,
            kind: "video",
            readyState: "live"
        });
});

test("live browser negotiation keeps RTX pairs when optional codecs are enabled", async ({
    browserName,
    context
}) => {
    const liveServerPorts =
        browserName === "firefox"
            ? {
                  bindPort: 18083,
                  rtcMaxPort: 58263,
                  rtcMinPort: 58232
              }
            : {
                  bindPort: 18082,
                  rtcMaxPort: 58231,
                  rtcMinPort: 58200
              };
    const server = await spawnLiveServer({
        bindPort: liveServerPorts.bindPort,
        rtcMinPort: liveServerPorts.rtcMinPort,
        rtcMaxPort: liveServerPorts.rtcMaxPort,
        codecFlags: { h264: true, vp9: true }
    });
    try {
        const channelUuid = await createChannel({
            authKey: server.authKey,
            httpBaseUrl: server.httpBaseUrl
        });
        const peer = await createPeerPage(context);
        await connectPeer(peer, {
            channelUuid,
            jwt: createConnectToken(channelUuid, 77, server.authKey),
            url: server.wsUrl
        });

        await expect.poll(async () => (await peerSnapshot(peer)).state).toBe("connected");
        await expect.poll(async () => peerLocalDescriptionSdp(peer)).not.toBeNull();
        const sdp = await peerLocalDescriptionSdp(peer);
        const codecs = parseVideoCodecAnswer(sdp);

        expect(codecs.videoCodecPayloadTypes.size).toBeGreaterThan(0);
        if (codecs.h264PayloadTypes.size > 0) {
            expect(codecs.h264Variants).toEqual(
                new Set([
                    "packetization-mode=0;profile-level-id=42001f",
                    "packetization-mode=0;profile-level-id=42e01f",
                    "packetization-mode=0;profile-level-id=4d001f",
                    "packetization-mode=1;profile-level-id=42001f",
                    "packetization-mode=1;profile-level-id=42e01f",
                    "packetization-mode=1;profile-level-id=4d001f"
                ])
            );
        }
        if (codecs.vp9Profiles.size > 0) {
            expect(codecs.vp9Profiles).toEqual(new Set(["0", "2"]));
        }
        for (const payloadType of codecs.videoCodecPayloadTypes) {
            expect(codecs.rtxAssociations.has(payloadType)).toBeTruthy();
        }
    } finally {
        await server.stop();
    }
});

async function expectCameraTrackUpdate(page, sessionId, active) {
    await expect
        .poll(async () => latestTrackUpdate(page, sessionId, "camera"))
        .toMatchObject(cameraTrackUpdateExpectation(sessionId, active));
}

function cameraTrackUpdateExpectation(sessionId, active) {
    return {
        name: "track",
        payload: {
            active,
            sessionId,
            track: {
                enabled: true,
                id: expect.any(String),
                kind: "video",
                readyState: "live"
            },
            type: "camera"
        }
    };
}

function parseVideoCodecAnswer(sdp) {
    const lines = sdp.split(/\r?\n/);
    const h264Variants = new Set();
    const h264PayloadTypes = new Set();
    const formatParametersByPayloadType = new Map();
    const rtxAssociations = new Set();
    const videoCodecPayloadTypes = new Set();
    const videoPayloadTypes = new Map();
    const vp9PayloadTypes = new Set();
    const vp9Profiles = new Set();
    let currentMediaKind = null;

    for (const line of lines) {
        const mediaDescriptionMatch = /^m=([^ ]+)/.exec(line);
        if (mediaDescriptionMatch) {
            [, currentMediaKind] = mediaDescriptionMatch;
            continue;
        }
        if (currentMediaKind !== "video") {
            continue;
        }
        const rtpmapMatch = /^a=rtpmap:(\d+) ([^/]+)\/\d+/.exec(line);
        if (rtpmapMatch) {
            const [, payloadType, codecName] = rtpmapMatch;
            videoPayloadTypes.set(payloadType, codecName);
            if (codecName !== "rtx") {
                videoCodecPayloadTypes.add(payloadType);
            }
            if (codecName === "H264") {
                h264PayloadTypes.add(payloadType);
            } else if (codecName === "VP9") {
                vp9PayloadTypes.add(payloadType);
            }
            continue;
        }
        const fmtpMatch = /^a=fmtp:(\d+) (.+)$/.exec(line);
        if (!fmtpMatch) {
            continue;
        }
        const [, payloadType, formatParams] = fmtpMatch;
        formatParametersByPayloadType.set(payloadType, parseFmtpParameters(formatParams));
    }

    for (const [payloadType, codecName] of videoPayloadTypes) {
        const params = formatParametersByPayloadType.get(payloadType);
        if (!params) {
            continue;
        }
        if (codecName === "H264") {
            h264Variants.add(
                `packetization-mode=${params["packetization-mode"]};profile-level-id=${params["profile-level-id"]}`
            );
            continue;
        }
        if (codecName === "VP9" && params["profile-id"]) {
            vp9Profiles.add(params["profile-id"]);
        }
    }

    for (const [payloadType, params] of formatParametersByPayloadType) {
        const codecName = videoPayloadTypes.get(payloadType);
        if (codecName === "rtx") {
            if (params.apt) {
                rtxAssociations.add(params.apt);
            }
        }
    }

    return {
        h264PayloadTypes,
        h264Variants,
        rtxAssociations,
        videoCodecPayloadTypes,
        vp9PayloadTypes,
        vp9Profiles
    };
}

function parseFmtpParameters(formatParams) {
    return Object.fromEntries(
        formatParams
            .split(";")
            .map((entry) => entry.trim())
            .filter(Boolean)
            .map((entry) => {
                const [key, value] = entry.split("=");
                return [key, value];
            })
    );
}
