import { expect, test } from "@playwright/test";

import {
    broadcast,
    cameraPublicationActive,
    cameraSubscriptionRid,
    connectPeer,
    createChannel,
    createConnectToken,
    createPeerPage,
    forceRecoverableClose,
    latestBroadcastUpdate,
    latestInfoUpdate,
    latestTrackUpdate,
    localCameraSenderEncodings,
    localSenderEncodings,
    localSenderTrackId,
    peerLocalDescriptionSdp,
    peerSnapshot,
    publishSyntheticAudio,
    publishSyntheticCamera,
    publishSyntheticScreen,
    roomUserInfo,
    setCameraDownload,
    setStreamDownload,
    spawnLiveServer,
    unpublishCamera,
    unpublishStream,
    updateInfo,
    waitForCameraSubscriptionSelectedRid,
    waitForDecodedRemoteCameraFrame,
    waitForDecodedRemoteVideoFrame,
    waitForUserMediaWorker
} from "./live_server_helpers.mjs";

const PUBLISHER_SESSION_ID = 41;
const SUBSCRIBER_SESSION_ID = 42;

test("default VP8 live publish applies RID simulcast and renders remotely", async ({
    browserName,
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

    await publishSyntheticCamera(publisher, "vp8-simulcast");

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);
    await expect
        .poll(async () => localCameraSenderEncodings(publisher))
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

    if (browserName === "chromium") {
        const decodedFrame = await waitForDecodedRemoteCameraFrame(
            subscriber,
            PUBLISHER_SESSION_ID
        );
        expect(decodedFrame.width).toBeGreaterThan(0);
        expect(decodedFrame.height).toBeGreaterThan(0);
        expect(decodedFrame.pixel.alpha).toBe(255);
    }
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

test("audio and screen streams publish, replace and clean up independently", async ({
    browserName,
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

    const firstScreen = await publishSyntheticScreen(publisher, "screen-one");

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "screen", true, "video");
    await expectScreenSource(subscriber, PUBLISHER_SESSION_ID);
    const canAssertScreenPixels = browserName === "chromium";
    if (canAssertScreenPixels) {
        await waitForDecodedRemoteVideoFrame(subscriber, PUBLISHER_SESSION_ID, "screen", {
            expectedPixel: firstScreen.fillPixel
        });
    }
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
    await expect
        .poll(async () => localSenderTrackId(publisher, "screen"))
        .toBe(firstScreen.trackId);

    const secondScreen = await publishSyntheticScreen(publisher, "screen-two");

    expect(secondScreen.trackId).not.toBe(firstScreen.trackId);
    await expect
        .poll(async () => localSenderTrackId(publisher, "screen"))
        .toBe(secondScreen.trackId);
    if (canAssertScreenPixels) {
        const secondScreenFrame = await waitForDecodedRemoteVideoFrame(
            subscriber,
            PUBLISHER_SESSION_ID,
            "screen",
            {
                expectedPixel: secondScreen.fillPixel
            }
        );
        expect(pixelDistance(secondScreenFrame.pixel, firstScreen.fillPixel)).toBeGreaterThan(96);
    }
    await expect
        .poll(async () => {
            return (
                (await peerSnapshot(subscriber)).consumers[String(PUBLISHER_SESSION_ID)]?.screen ??
                null
            );
        })
        .toMatchObject({
            enabled: true,
            kind: "video",
            readyState: "live"
        });

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", false);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "screen", false, "video");

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", true);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "screen", true, "video");

    await unpublishStream(publisher, "screen");

    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.screen ?? null)
        .toBeNull();
    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.audio ?? null)
        .toMatchObject({
            enabled: true,
            kind: "audio",
            readyState: "live"
        });

    await unpublishStream(publisher, "audio");

    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.audio ?? null)
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
    await setCameraDownload(publisher, SUBSCRIBER_SESSION_ID, true, "featured");
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
    await setCameraDownload(subscriber, PUBLISHER_SESSION_ID, true, "featured");
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

test("load-triggered spillover relays VP8 camera between real browsers", async ({
    browserName,
    context
}) => {
    test.skip(
        browserName === "firefox",
        "the bundled Playwright Firefox build does not decode the current VP8 live relay flow"
    );
    test.setTimeout(45_000);
    const liveServerPorts =
        browserName === "firefox"
            ? {
                  bindPort: 18087,
                  rtcMaxPort: 58455,
                  rtcMinPort: 58392
              }
            : {
                  bindPort: 18086,
                  rtcMaxPort: 58391,
                  rtcMinPort: 58328
              };
    const server = await spawnLiveServer({
        bindPort: liveServerPorts.bindPort,
        codecFlags: { vp8: true },
        rtcMaxPort: liveServerPorts.rtcMaxPort,
        rtcMinPort: liveServerPorts.rtcMinPort,
        spillover: {
            activationWindow: 1,
            minReceivers: 2,
            mode: "load-triggered",
            roomMaxLocalRouters: 2,
            rtcMediaWorkerCount: 2
        }
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
        await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
        await waitForUserMediaWorker({
            expectedMediaWorkerId: 0,
            httpBaseUrl: server.httpBaseUrl,
            roomId: channelUuid,
            userId: PUBLISHER_SESSION_ID
        });

        await connectPeer(subscriber, {
            channelUuid,
            jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID),
            url: server.wsUrl
        });
        await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");
        await waitForUserMediaWorker({
            expectedMediaWorkerId: 1,
            httpBaseUrl: server.httpBaseUrl,
            roomId: channelUuid,
            userId: SUBSCRIBER_SESSION_ID
        });

        await publishSyntheticCamera(publisher, "load-spillover-vp8");
        await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);
        await setCameraDownload(subscriber, PUBLISHER_SESSION_ID, true, "featured");
        await waitForCameraSubscriptionSelectedRid({
            consumerSessionId: SUBSCRIBER_SESSION_ID,
            expectedRid: "hi",
            httpBaseUrl: server.httpBaseUrl,
            producerSessionId: PUBLISHER_SESSION_ID,
            roomId: channelUuid
        });

        const decodedFrame = await waitForDecodedRemoteCameraFrame(
            subscriber,
            PUBLISHER_SESSION_ID
        );
        expect(decodedFrame.width).toBeGreaterThan(0);
        expect(decodedFrame.height).toBeGreaterThan(0);
        expect(decodedFrame.pixel.alpha).toBe(255);
    } finally {
        await server.stop();
    }
});

test("H264-only live publish applies RID simulcast and renders when supported", async ({
    browserName,
    context
}) => {
    test.skip(
        browserName === "firefox",
        "the bundled Playwright Firefox build does not render the current H264-only live flow"
    );
    const liveServerPorts =
        browserName === "firefox"
            ? {
                  bindPort: 18085,
                  rtcMaxPort: 58327,
                  rtcMinPort: 58296
              }
            : {
                  bindPort: 18084,
                  rtcMaxPort: 58295,
                  rtcMinPort: 58264
              };
    const server = await spawnLiveServer({
        bindPort: liveServerPorts.bindPort,
        rtcMinPort: liveServerPorts.rtcMinPort,
        rtcMaxPort: liveServerPorts.rtcMaxPort,
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
            .poll(async () => localCameraSenderEncodings(publisher))
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

test("live browser negotiation keeps RTX pairs only for eligible optional codecs", async ({
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
            jwt: createConnectToken(channelUuid, 77),
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
        for (const payloadType of codecs.h264PayloadTypes) {
            expect(codecs.rtxAssociations.has(payloadType)).toBeFalsy();
        }
        for (const payloadType of codecs.vp9PayloadTypes) {
            expect(codecs.rtxAssociations.has(payloadType)).toBeTruthy();
        }
    } finally {
        await server.stop();
    }
});

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

async function expectScreenSource(page, sessionId) {
    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(page);
            return (
                snapshot.updates
                    .filter((update) => update.name === "source")
                    .flatMap((update) => update.payload.sources)
                    .find(
                        (source) =>
                            source.active &&
                            source.sessionId === sessionId &&
                            source.type === "screen"
                    ) ?? null
            );
        })
        .toMatchObject({
            active: true,
            encodings: expect.any(Array),
            sessionId,
            sourceId: expect.any(String),
            type: "screen"
        });
}

function parseVideoCodecAnswer(sdp) {
    const lines = sdp.split(/\r?\n/);
    const h264Variants = new Set();
    const h264PayloadTypes = new Set();
    const formatParametersByPayloadType = new Map();
    const hasAnySendRid = lines.some((line) => /^a=rid:[^ ]+ send(?: |$)/.test(line));
    const hasAnySendSimulcast = lines.some((line) => /^a=simulcast:send /.test(line));
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
        hasAnySendRid,
        hasAnySendSimulcast,
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
