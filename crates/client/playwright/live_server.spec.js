import { createSocket } from "node:dgram";
import { once } from "node:events";

import { expect, test } from "@playwright/test";

import {
    broadcast,
    cameraPublicationActive,
    cameraSubscriptionRid,
    connectPeer,
    createChannel,
    createConnectToken,
    createPeerPage,
    disconnectPeer,
    forceRecoverableClose,
    latestBroadcastUpdate,
    latestInfoUpdate,
    latestTrackUpdate,
    localSenderEncodings,
    observeNegotiationNeeded,
    observeNegotiations,
    pauseStream,
    peerLocalDescriptionSdp,
    peerSnapshot,
    publishSyntheticAudio,
    publishSyntheticCamera,
    publishSyntheticScreen,
    roomUserInfo,
    setStreamDownload,
    spawnLiveServer,
    streamDiagnostics,
    updateInfo,
    waitForDecodedRemoteVideoFrame
} from "./live_server_helpers.mjs";

const PUBLISHER_SESSION_ID = 41;
const SUBSCRIBER_SESSION_ID = 42;
const STUN_MAGIC_COOKIE = 0x2112a442;

test("unresponsive STUN does not block the initial answer", async ({ browserName, context }) => {
    test.skip(browserName !== "chromium", "Chromium-specific ICE gathering regression");
    test.setTimeout(15_000);
    const stun = createSocket("udp4");
    let listening = false;
    let stunRequests = 0;
    stun.on("message", (message) => {
        if (message.length >= 20 && message.readUInt32BE(4) === STUN_MAGIC_COOKIE) {
            stunRequests += 1;
        }
    });

    try {
        stun.bind(0, "127.0.0.1");
        await once(stun, "listening");
        listening = true;
        const channelUuid = await createChannel();
        const peer = await createPeerPage(context);

        await connectPeer(peer, {
            channelUuid,
            iceServers: [{ urls: `stun:127.0.0.1:${stun.address().port}` }],
            jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
        });

        await expect.poll(() => stunRequests).toBeGreaterThan(0);
        await expect
            .poll(async () => (await peerSnapshot(peer)).peerConnectionState, { timeout: 8_000 })
            .toBe("connected");
        const snapshot = await peerSnapshot(peer);
        expect(snapshot.state).toBe("connected");
    } finally {
        if (listening) {
            const closed = once(stun, "close");
            stun.close();
            await closed;
        }
    }
});

test("default VP8 camera pauses and resumes without renegotiation", async ({
    browserName,
    context
}) => {
    test.setTimeout(60_000);
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const negotiations = observeNegotiations(publisher);
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

    await expectCommittedPauseResume({
        browserName,
        channelUuid,
        firstLabel: "camera-one",
        negotiations,
        publisher,
        resumedLabel: "camera-two",
        streamType: "camera",
        subscriber
    });
    await expect
        .poll(async () => localSenderEncodings(publisher, "camera"))
        .toEqual([
            {
                active: true,
                maxBitrate: 150000,
                rid: "lo",
                scaleResolutionDownBy: 4
            },
            {
                active: true,
                maxBitrate: 4000000,
                rid: "hi",
                scaleResolutionDownBy: 1
            }
        ]);
    await expect.poll(async () => peerLocalDescriptionSdp(publisher)).not.toBeNull();
    const sdp = await peerLocalDescriptionSdp(publisher);
    const video = parseVideoCodecAnswer(sdp);

    expect(video.vp8PayloadTypes.size).toBeGreaterThan(0);
    expect(video.hasSendRidLo).toBeTruthy();
    expect(video.hasSendRidHi).toBeTruthy();
    expect(video.hasSendSimulcastLoHi).toBeTruthy();
});

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

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", false);

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

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", true);

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);

    await disconnectPeer(replacement);

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

test("audio and screen streams publish, pause and clean up independently", async ({
    browserName,
    context
}) => {
    test.setTimeout(60_000);
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const negotiations = observeNegotiations(publisher);
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

    await publishSyntheticAudio(publisher, "synthetic-audio");

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "audio", true, "audio");
    await expect
        .poll(async () => {
            return (
                (await peerSnapshot(subscriber)).consumers[String(PUBLISHER_SESSION_ID)]?.audio ??
                null
            );
        })
        .toMatchObject({
            enabled: true,
            kind: "audio",
            readyState: "live"
        });

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "audio", false);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "audio", false, "audio");

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "audio", true);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "audio", true, "audio");

    await expectCommittedPauseResume({
        browserName,
        channelUuid,
        firstLabel: "screen-one",
        negotiations,
        publisher,
        resumedLabel: "screen-two",
        streamType: "screen",
        subscriber
    });
    await expect
        .poll(async () => localSenderEncodings(publisher, "screen"))
        .toEqual([
            {
                active: true,
                maxBitrate: 150000,
                rid: "lo",
                scaleResolutionDownBy: 4
            },
            {
                active: true,
                maxBitrate: 4000000,
                rid: "hi",
                scaleResolutionDownBy: 1
            }
        ]);

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", false);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "screen", false, "video");

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", true);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "screen", true, "video");

    await disconnectPeer(publisher);

    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.audio ?? null)
        .toBeNull();
    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.screen ?? null)
        .toBeNull();
});

test("broadcast and info fanout through the browser bundle", async ({ context }) => {
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

    await broadcast(publisher, {
        kind: "sequence",
        sequence: 17
    });

    await expect
        .poll(async () => latestBroadcastUpdate(subscriber, PUBLISHER_SESSION_ID))
        .toMatchObject({
            name: "broadcast",
            payload: {
                message: {
                    kind: "sequence",
                    sequence: 17
                },
                senderId: PUBLISHER_SESSION_ID
            }
        });

    await updateInfo(
        publisher,
        {
            isRaisingHand: true,
            isTalking: true
        },
        { needRefresh: true }
    );

    await expect
        .poll(async () => latestInfoUpdate(subscriber, PUBLISHER_SESSION_ID))
        .toMatchObject({
            name: "info_change",
            payload: {
                [String(PUBLISHER_SESSION_ID)]: {
                    isRaisingHand: true,
                    isTalking: true
                }
            }
        });
});

test("live recovery replays sticky publish subscribe and info intents", async ({ context }) => {
    test.setTimeout(45_000);
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

    await publishSyntheticCamera(publisher, "recovery-publisher-camera");
    await publishSyntheticCamera(subscriber, "recovery-subscriber-camera");
    await setStreamDownload(publisher, SUBSCRIBER_SESSION_ID, "camera", true, "featured");
    await updateInfo(
        publisher,
        {
            isCameraOn: true,
            isRaisingHand: true
        },
        { needRefresh: true }
    );

    await expect
        .poll(() =>
            cameraPublicationActive({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID })
        )
        .toBeTruthy();
    await expect
        .poll(() =>
            cameraSubscriptionRid({
                consumerSessionId: PUBLISHER_SESSION_ID,
                producerSessionId: SUBSCRIBER_SESSION_ID,
                roomId: channelUuid
            })
        )
        .toBe("hi");
    await expect
        .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }))
        .toMatchObject({
            isCameraOn: true,
            isRaisingHand: true
        });

    await forceRecoverableClose(publisher);

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(publisher);
            return snapshot.stateChanges.some((change) => change.state === "recovering");
        })
        .toBeTruthy();
    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");

    await expect
        .poll(
            () =>
                cameraPublicationActive({
                    roomId: channelUuid,
                    sessionId: PUBLISHER_SESSION_ID
                }),
            { timeout: 15_000 }
        )
        .toBeTruthy();
    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", true, "featured");
    await expect
        .poll(
            () =>
                cameraSubscriptionRid({
                    consumerSessionId: SUBSCRIBER_SESSION_ID,
                    producerSessionId: PUBLISHER_SESSION_ID,
                    roomId: channelUuid
                }),
            { timeout: 15_000 }
        )
        .toBe("hi");
    await expect
        .poll(
            () =>
                cameraSubscriptionRid({
                    consumerSessionId: PUBLISHER_SESSION_ID,
                    producerSessionId: SUBSCRIBER_SESSION_ID,
                    roomId: channelUuid
                }),
            { timeout: 15_000 }
        )
        .toBe("hi");
    await expect
        .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }), {
            timeout: 15_000
        })
        .toMatchObject({
            isCameraOn: true,
            isRaisingHand: true
        });
});

test("H264-only live publish applies RID simulcast and renders when supported", async ({
    browserName,
    context
}) => {
    test.skip(
        browserName === "firefox",
        "the bundled Playwright Firefox build does not render the current H264-only live flow"
    );
    const server = await spawnLiveServer({
        bindPort: 18084,
        rtcMinPort: 58264,
        rtcMaxPort: 58295,
        codecFlags: { h264: true, vp8: false }
    });
    try {
        const channelUuid = await createChannel({
            authKey: server.authKey,
            httpBaseUrl: server.httpBaseUrl
        });
        const publisher = await createPeerPage(context);
        const subscriber = await createPeerPage(context);

        await connectPeer(publisher, {
            channelUuid,
            jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID),
            url: server.wsUrl
        });
        await connectPeer(subscriber, {
            channelUuid,
            jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID),
            url: server.wsUrl
        });

        await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
        await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

        await publishSyntheticCamera(publisher, "h264-simulcast");

        await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);
        await expect
            .poll(async () => localSenderEncodings(publisher, "camera"))
            .toEqual([
                {
                    active: true,
                    maxBitrate: 150000,
                    rid: "lo",
                    scaleResolutionDownBy: undefined
                },
                {
                    active: true,
                    maxBitrate: 4000000,
                    rid: "hi",
                    scaleResolutionDownBy: undefined
                }
            ]);
        await expect.poll(async () => peerLocalDescriptionSdp(publisher)).not.toBeNull();
        const sdp = await peerLocalDescriptionSdp(publisher);
        const video = parseVideoCodecAnswer(sdp);

        expect(video.h264PayloadTypes.size).toBeGreaterThan(0);
        expect(video.vp8PayloadTypes.size).toBe(0);
        expect(video.hasSendRidLo).toBeTruthy();
        expect(video.hasSendRidHi).toBeTruthy();
        expect(video.hasSendSimulcastLoHi).toBeTruthy();
    } finally {
        await server.stop();
    }
});

test("live browser negotiation keeps upload repair out of consumer media", async ({
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
        const publisher = await createPeerPage(context);
        await connectPeer(publisher, {
            channelUuid,
            jwt: createConnectToken(channelUuid, 77),
            url: server.wsUrl
        });

        await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
        await publishSyntheticCamera(publisher, "upload-repair");
        await expect
            .poll(async () => {
                const sdp = await peerLocalDescriptionSdp(publisher);
                return sdp ? videoMediaSectionsByDirection(sdp, "sendonly").length : 0;
            })
            .toBeGreaterThan(0);
        const publisherSdp = await peerLocalDescriptionSdp(publisher);
        const codecs = parseVideoCodecAnswer(publisherSdp);

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
        const uploadSections = videoMediaSectionsByDirection(publisherSdp, "sendonly");
        expect(uploadSections.length).toBeGreaterThan(0);
        for (const section of uploadSections) {
            expect(section).toMatch(/^a=rtpmap:\d+ rtx\//im);
            expect(section).toMatch(/^a=fmtp:\d+ .*\bapt=/im);
            expect(section).toMatch(/^a=rtcp-fb:(?:\*|\d+) nack\s*$/im);
        }

        const subscriber = await createPeerPage(context);
        await connectPeer(subscriber, {
            channelUuid,
            jwt: createConnectToken(channelUuid, 78),
            url: server.wsUrl
        });
        await expectCameraTrackUpdate(subscriber, 77, true);
        await expect
            .poll(async () => {
                const sdp = await peerLocalDescriptionSdp(subscriber);
                return sdp ? videoMediaSectionsByDirection(sdp, "recvonly").length : 0;
            })
            .toBeGreaterThan(0);
        const subscriberSdp = await peerLocalDescriptionSdp(subscriber);
        const consumerSections = videoMediaSectionsByDirection(subscriberSdp, "recvonly");
        for (const section of consumerSections) {
            expect(section).not.toMatch(/^a=rtpmap:\d+ rtx\//im);
            expect(section).not.toMatch(/^a=fmtp:\d+ .*\bapt=/im);
            expect(section).not.toMatch(/^a=rtcp-fb:(?:\*|\d+) nack\s*$/im);
            expect(section).not.toContain("urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id");
            expect(section).not.toMatch(/^a=ssrc-group:FID\b/im);
        }
    } finally {
        await server.stop();
    }
});

async function expectCommittedPauseResume({
    browserName,
    channelUuid,
    firstLabel,
    httpBaseUrl,
    negotiations,
    publisher,
    resumedLabel,
    streamType,
    subscriber
}) {
    const firstTrack = await publishSyntheticStream(publisher, streamType, firstLabel);
    const activeDiagnostics = await expectStreamActivity(
        subscriber,
        channelUuid,
        streamType,
        true,
        httpBaseUrl
    );

    const negotiationNeeded = await observeNegotiationNeeded(publisher);
    const negotiationNeededCount = await negotiationNeeded();
    const negotiationCount = negotiations.count();
    const identity = streamIdentity(activeDiagnostics);

    await pauseStream(publisher, streamType);
    const pausedDiagnostics = await expectStreamActivity(
        subscriber,
        channelUuid,
        streamType,
        false,
        httpBaseUrl
    );

    expect(streamIdentity(pausedDiagnostics)).toEqual(identity);
    expect(await negotiationNeeded()).toBe(negotiationNeededCount);
    expect(negotiations.count()).toBe(negotiationCount);

    const resumedTrack = await publishSyntheticStream(publisher, streamType, resumedLabel);
    const resumedDiagnostics = await expectStreamActivity(
        subscriber,
        channelUuid,
        streamType,
        true,
        httpBaseUrl
    );

    expect(streamIdentity(resumedDiagnostics)).toEqual(identity);
    expect(await negotiationNeeded()).toBe(negotiationNeededCount);
    expect(negotiations.count()).toBe(negotiationCount);

    if (browserName === "chromium") {
        const frame = await waitForDecodedRemoteVideoFrame(
            subscriber,
            PUBLISHER_SESSION_ID,
            streamType,
            {
                expectedPixel: resumedTrack.fillPixel
            }
        );
        expect(frame.width).toBeGreaterThan(0);
        expect(frame.height).toBeGreaterThan(0);
        expect(frame.pixel.alpha).toBe(255);
        expect(pixelDistance(frame.pixel, firstTrack.fillPixel)).toBeGreaterThan(96);
    }
}

async function expectStreamActivity(subscriber, roomId, streamType, active, httpBaseUrl) {
    const state = active ? "active" : "inactive";
    await expect
        .poll(() => streamState(roomId, streamType, httpBaseUrl))
        .toMatchObject({
            publication: { active },
            source: {
                active,
                encodings: expect.arrayContaining([
                    expect.objectContaining({ encodingId: expect.any(Number) })
                ]),
                mid: expect.any(String),
                sourceId: expect.any(Number)
            },
            subscription: { state }
        });
    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, streamType, active, "video");

    const presenceField = streamType === "camera" ? "isCameraOn" : "isScreenSharingOn";
    await expect
        .poll(() => roomUserInfo({ httpBaseUrl, roomId, sessionId: PUBLISHER_SESSION_ID }))
        .toMatchObject({
            [presenceField]: active
        });
    await expect
        .poll(() => latestInfoUpdate(subscriber, PUBLISHER_SESSION_ID))
        .toMatchObject({
            payload: {
                [String(PUBLISHER_SESSION_ID)]: {
                    [presenceField]: active
                }
            }
        });

    return streamState(roomId, streamType, httpBaseUrl);
}

function streamState(roomId, streamType, httpBaseUrl) {
    return streamDiagnostics({
        consumerSessionId: SUBSCRIBER_SESSION_ID,
        httpBaseUrl,
        producerSessionId: PUBLISHER_SESSION_ID,
        roomId,
        streamType
    });
}

function publishSyntheticStream(page, streamType, label) {
    return streamType === "camera"
        ? publishSyntheticCamera(page, label)
        : publishSyntheticScreen(page, label);
}

function streamIdentity({ publication, source, subscription }) {
    return {
        consumerTransportMediaId: subscription.consumerTransportMediaId,
        publicationSourceId: publication.sourceId,
        publicationTransportMediaId: publication.transportMediaId,
        sourceId: source.sourceId,
        sourceMid: source.mid,
        sourceTransportMediaId: source.transportMediaId,
        subscriptionSourceId: subscription.sourceId,
        subscriptionSourceTransportMediaId: subscription.sourceTransportMediaId
    };
}

async function expectCameraTrackUpdate(page, sessionId, active) {
    await expectTrackUpdate(page, sessionId, "camera", active, "video");
}

async function expectTrackUpdate(page, sessionId, type, active, kind) {
    await expect
        .poll(async () => latestTrackUpdate(page, sessionId, type))
        .toMatchObject(trackUpdateExpectation(sessionId, type, active, kind));
}

function cameraTrackUpdateExpectation(sessionId, active) {
    return trackUpdateExpectation(sessionId, "camera", active, "video");
}

function trackUpdateExpectation(sessionId, type, active, kind) {
    return {
        name: "track",
        payload: {
            active,
            sessionId,
            track: {
                enabled: true,
                id: expect.any(String),
                kind,
                readyState: "live"
            },
            type
        }
    };
}

function pixelDistance(left, right) {
    return Math.hypot(left.red - right.red, left.green - right.green, left.blue - right.blue);
}

function videoMediaSectionsByDirection(sdp, direction) {
    return sdp
        .split(/\r?\n(?=m=)/)
        .filter(
            (section) =>
                section.startsWith("m=video ") && section.split(/\r?\n/).includes(`a=${direction}`)
        );
}

function parseVideoCodecAnswer(sdp) {
    const lines = sdp.split(/\r?\n/);
    const h264Variants = new Set();
    const h264PayloadTypes = new Set();
    const fmtpByPayloadType = new Map();
    const hasSendRidHi = lines.some((line) => /^a=rid:hi send(?: |$)/.test(line));
    const hasSendRidLo = lines.some((line) => /^a=rid:lo send(?: |$)/.test(line));
    const hasSendSimulcastLoHi = lines.some((line) => /^a=simulcast:send lo[;,]hi$/.test(line));
    const rtxAssociations = new Set();
    const videoCodecPayloadTypes = new Set();
    const videoPayloadTypes = new Map();
    const vp8PayloadTypes = new Set();
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
            } else if (codecName === "VP8") {
                vp8PayloadTypes.add(payloadType);
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
        fmtpByPayloadType.set(payloadType, parseFmtpParameters(formatParams));
    }

    for (const [payloadType, codecName] of videoPayloadTypes) {
        const params = fmtpByPayloadType.get(payloadType);
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

    for (const [payloadType, params] of fmtpByPayloadType) {
        const codecName = videoPayloadTypes.get(payloadType);
        if (codecName === "rtx") {
            if (params.apt) {
                rtxAssociations.add(params.apt);
            }
        }
    }

    return {
        hasSendRidHi,
        hasSendRidLo,
        hasSendSimulcastLoHi,
        h264PayloadTypes,
        h264Variants,
        rtxAssociations,
        videoCodecPayloadTypes,
        vp8PayloadTypes,
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
