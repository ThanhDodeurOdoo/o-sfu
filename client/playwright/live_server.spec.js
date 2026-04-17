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

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera"
            );
        })
        .toContainEqual({
            name: "track",
            payload: {
                active: true,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    id: expect.any(String),
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });

    await setCameraDownload(subscriber, PUBLISHER_SESSION_ID, false);

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera"
            );
        })
        .toContainEqual({
            name: "track",
            payload: {
                active: false,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    id: expect.any(String),
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });

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

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera"
            );
        })
        .toContainEqual({
            name: "track",
            payload: {
                active: false,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    id: expect.any(String),
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });

    await setCameraDownload(subscriber, PUBLISHER_SESSION_ID, true);

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera"
            );
        })
        .toContainEqual({
            name: "track",
            payload: {
                active: true,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    id: expect.any(String),
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });

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
        .toMatchObject({
            name: "track",
            payload: {
                active: true,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });
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
            muted: false,
            readyState: "live"
        });
});

test("live browser negotiation exposes optional H264 and VP9 RTX pairs when enabled", async ({
    context
}) => {
    const server = await spawnLiveServer({
        bindPort: 18082,
        rtcMinPort: 58200,
        rtcMaxPort: 58231,
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
        expect(codecs.vp9Profiles).toEqual(new Set(["0", "2"]));
        for (const payloadType of codecs.h264PayloadTypes) {
            expect(codecs.rtxAssociations.has(payloadType)).toBeTruthy();
        }
        for (const payloadType of codecs.vp9PayloadTypes) {
            expect(codecs.rtxAssociations.has(payloadType)).toBeTruthy();
        }
    } finally {
        await server.stop();
    }
});

function parseVideoCodecAnswer(sdp) {
    const lines = sdp.split(/\r?\n/);
    const h264Variants = new Set();
    const h264PayloadTypes = new Set();
    const rtxAssociations = new Set();
    const videoPayloadTypes = new Map();
    const vp9PayloadTypes = new Set();
    const vp9Profiles = new Set();

    for (const line of lines) {
        const rtpmapMatch = /^a=rtpmap:(\d+) ([^/]+)\/\d+/.exec(line);
        if (rtpmapMatch) {
            const [, payloadType, codecName] = rtpmapMatch;
            videoPayloadTypes.set(payloadType, codecName);
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
        const codecName = videoPayloadTypes.get(payloadType);
        if (codecName === "H264") {
            const params = parseFmtpParameters(formatParams);
            h264Variants.add(
                `packetization-mode=${params["packetization-mode"]};profile-level-id=${params["profile-level-id"]}`
            );
            continue;
        }
        if (codecName === "VP9") {
            const params = parseFmtpParameters(formatParams);
            vp9Profiles.add(params["profile-id"]);
            continue;
        }
        if (codecName === "rtx") {
            const params = parseFmtpParameters(formatParams);
            if (params.apt) {
                rtxAssociations.add(params.apt);
            }
        }
    }

    return {
        h264PayloadTypes,
        h264Variants,
        rtxAssociations,
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
