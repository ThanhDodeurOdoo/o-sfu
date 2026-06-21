import assert from "node:assert/strict";
import test from "node:test";

import { CLIENT_UPDATE } from "../dist/index.js";
import { WS_CLOSE_CODE } from "../dist/protocol_contract.js";
import { PENDING_REQUEST_KIND, createProtocolCore } from "../dist/runtime_contract.js";
import { PendingRequests } from "../dist/internals/pending_requests.js";
import { FakeMediaTrack, FakePeerConnection, FakeSender } from "./support/browser_fakes.mjs";
import {
    EMPTY_FEATURES,
    FakeProtocolCore,
    buildWelcomeFrame,
    createManualTimers,
    decodeSentFrame,
    tick
} from "./support/protocol_fakes.mjs";
import {
    createCameraTrack,
    createSfuClientHarness,
    createScreenTrack
} from "./support/sfu_client_harness.mjs";
import {
    audioMedia,
    audioUploadSlot,
    sdp,
    videoMedia,
    videoUploadSlot
} from "./support/negotiation_fixtures.mjs";

test("connect normalizes the URL and sends auth on WebSocket open", async () => {
    const { sockets, connect, open } = createSfuClientHarness();

    await connect("https://example.test/ws", "jwt-token", {
        channelUUID: "channel-a",
        iceServers: [{ urls: "stun:stun.example.test" }]
    });

    assert.equal(sockets[0].url, "wss://example.test/ws");

    await open();

    assert.deepEqual(sockets[0].sent, ["auth-frame"]);
});

test("connect emits public client and runtime log events", async () => {
    const { client, connect } = createSfuClientHarness();
    const logs = [];
    client.addEventListener("log", (event) => {
        logs.push(event.detail);
    });

    await connect("ws://example.test/ws", "jwt-token");

    assert.ok(
        logs.some(
            (log) =>
                log.id === "sfu_client" &&
                log.level === "info" &&
                log.message === "connect requested for implicit room"
        )
    );
    assert.ok(
        logs.some(
            (log) =>
                log.id === "browser_runtime" &&
                log.level === "info" &&
                log.message.includes("opening websocket connection")
        )
    );
});

test("ignored duplicate connect keeps the accepted ICE server config", async () => {
    const harness = createRecoveryHarness();
    const { client, connect, emitMessage, open, peerConnections } = harness;
    const iceServers = [{ urls: "stun:first.example.test" }];

    await connect("ws://example.test/ws", "jwt-token", {
        channelUUID: "channel-a",
        iceServers
    });
    client.connect("ws://example.test/ws", "jwt-token", {
        channelUUID: "channel-a",
        iceServers: [{ urls: "stun:second.example.test" }]
    });
    await tick();
    await open();
    await emitMessage(buildWelcomeFrame());
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    assert.deepEqual(peerConnections[0].config.iceServers, iceServers);
});

test("oversized server text frames close before protocol decoding", async () => {
    const core = new FakeProtocolCore();
    let decoded = false;
    core.onWsMessage = () => {
        decoded = true;
        return [];
    };
    const { connect, open, sockets } = createSfuClientHarness({ protocolCore: core });

    await connect();
    await open();

    sockets[0].emitMessage("x".repeat(256 * 1024 + 1));

    assert.equal(decoded, false);
    assert.notEqual(sockets[0].closeCode, WS_CLOSE_CODE.PROTOCOL_ERROR);
    assert.equal(sockets[0].closeCode >= 3000 && sockets[0].closeCode <= 4999, true);
    assert.deepEqual(core.wsCloseCodes, [WS_CLOSE_CODE.PROTOCOL_ERROR]);
});

for (const [name, startRequest, ok, expected] of [
    [
        "startRecording resolves through the protocol request lifecycle",
        (client) => client.startRecording({ audio: true }),
        true,
        true
    ],
    [
        "stopRecording resolves through the protocol request lifecycle",
        (client) => client.stopRecording(),
        true,
        true
    ],
    [
        "recording request refusal resolves false",
        (client) => client.startRecording({ audio: true }),
        false,
        false
    ]
]) {
    test(name, async () => {
        assert.equal(await resolveRealRecordingRequest(startRequest, ok), expected);
    });
}

test("recording requests without protocol registration resolve false", async () => {
    const core = new FakeProtocolCore();
    core.startRecording = () => [];
    core.stopRecording = () => [];
    const { client } = createSfuClientHarness({ protocolCore: core });

    assert.equal(await client.startRecording({ audio: true }), false);
    assert.equal(await client.stopRecording(), false);
});

test("duplicate recording request begin is handled as a runtime error", async () => {
    const core = new FakeProtocolCore();
    core.startRecording = () => [
        beginPendingRequest("record-1", PENDING_REQUEST_KIND.START_RECORDING, 10000),
        beginPendingRequest("record-2", PENDING_REQUEST_KIND.START_RECORDING, 10001)
    ];
    const { client, handledErrors } = createSfuClientHarness({ protocolCore: core });

    await assert.rejects(client.startRecording({ audio: true }), Error);

    assert.equal(client.errors.length, 1);
    assert.equal(handledErrors[0], client.errors[0]);
});

test("runtime errors reject registered recording requests", async () => {
    const core = new FakeProtocolCore();
    const onWsMessage = core.onWsMessage.bind(core);
    core.onWsMessage = (frame) => {
        if (frame === "recording-runtime-failure") {
            throw new Error("recording runtime failure");
        }
        return onWsMessage(frame);
    };
    const { client, emitMessage, connectWithWelcome } = createSfuClientHarness({
        protocolCore: core
    });

    await connectWithWelcome();

    const registeredPromise = client.startRecording({ audio: true });
    await tick();
    const recordingRejection = assert.rejects(registeredPromise, Error);

    await emitMessage("recording-runtime-failure");
    await recordingRejection;
});

test("pending request rejection includes registered recordings", async () => {
    const pendingRequests = new PendingRequests();
    const registeredPromise = pendingRequests.begin(
        () => [
            {
                ...beginPendingRequest(
                    "start-recording",
                    PENDING_REQUEST_KIND.START_RECORDING,
                    10000
                )
            }
        ],
        () => undefined,
        () => undefined
    );
    pendingRequests.rejectAll(new Error("recording runtime failure"));

    await assert.rejects(registeredPromise, Error);
});

test("default runtime creates the protocol core from generated wasm bindings", () => {
    const core = createProtocolCore();

    const connectCommands = core.connect("ws://example.test/ws", "jwt-token", "channel-a");
    const authCommands = core.onWsOpen();

    assert.deepEqual(connectCommands, [
        { kind: "emitStateChange", cause: undefined, state: "connecting" },
        { kind: "connect", url: "ws://example.test/ws" }
    ]);
    assert.equal(authCommands.length, 1);
    assert.equal(authCommands[0].kind, "sendWebSocket");
    assert.deepEqual(JSON.parse(authCommands[0].frame), [
        {
            t: "auth",
            p: {
                channel: "channel-a",
                jwt: "jwt-token"
            }
        }
    ]);
});

function createRecoveryHarness(options = {}) {
    const timers = createManualTimers();
    return {
        ...createSfuClientHarness({
            clearTimer: timers.clearTimer,
            createProtocolCore: () => createProtocolCore(),
            setTimer: timers.setTimer,
            ...options
        }),
        timers
    };
}

function beginPendingRequest(requestId, requestKind, timeoutTimerId) {
    return {
        kind: "beginPendingRequest",
        requestId,
        requestKind,
        timeoutTimerId,
        timeoutMs: 5000
    };
}

async function resolveRealRecordingRequest(startRequest, ok) {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets, timers } = harness;

    await connectRealWithWelcome(harness);

    const resultPromise = startRequest(client);
    await tick();
    timers.fireByDelay(100);
    await tick();
    assert.equal(timers.hasDelay(5000), true);

    const [request] = decodeSentFrame(sockets[0], sockets[0].sent.length - 1);
    await emitMessage(JSON.stringify([{ t: request.t, r: request.q, p: { ok } }]));
    return resultPromise;
}

async function connectRealWithWelcome(harness) {
    await harness.connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await harness.open();
    await harness.emitMessage(buildWelcomeFrame());
}

async function finishRecovery({ emitMessage, open, sockets, timers }) {
    timers.fireByDelay(1000);
    await tick();

    assert.equal(sockets.length, 2);
    await open(1);
    await emitMessage(buildWelcomeFrame(), 1);
}

function buildNegotiationFrame(tag, requestId, payloadOrUploadMid) {
    const payload =
        typeof payloadOrUploadMid === "string"
            ? {
                  sdp: sdp(audioMedia("0"), videoMedia(payloadOrUploadMid)),
                  uploadSlots: [videoUploadSlot(payloadOrUploadMid)]
              }
            : payloadOrUploadMid;
    return JSON.stringify([
        {
            t: tag,
            q: requestId,
            p: payload
        }
    ]);
}

function buildVideoRenegotiationFrame(
    requestId,
    { codecs, mid = "2", payloadType = 96, rtpmap = null, simulcastEncodings } = {}
) {
    return buildNegotiationFrame("renegotiate", requestId, {
        sdp: sdp(videoMedia(mid, { payloadType, rtpmap })),
        uploadSlots: [videoUploadSlot(mid, { codecs, simulcastEncodings })]
    });
}

function lastSentEnvelope(socket) {
    return decodeSentFrame(socket, socket.sent.length - 1).at(-1);
}

function assertLastNegotiationResponse(socket, tag, responseTo) {
    assert.deepEqual(lastSentEnvelope(socket), {
        t: tag,
        r: responseTo,
        p: {
            sdp: "answer-sdp"
        }
    });
}

async function emitOfferWithBinding({ core, emitMessage }, binding = {}) {
    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera",
        ...binding
    });
    await emitMessage("offer");
}

test("real protocol core replays sticky publish after recovery transport readiness", async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, connect, emitMessage, open, peerConnections } = harness;

    const cameraTrack = createCameraTrack("camera-track-1");

    await connect("ws://example.test/ws", "jwt-token", {
        channelUUID: "channel-a"
    });

    await open();
    await emitMessage(buildWelcomeFrame());
    await emitMessage(buildNegotiationFrame("offer", "server-initial", "1"));

    client.publish("camera", cameraTrack);
    await tick();
    await emitMessage(buildNegotiationFrame("renegotiate", "server-publish", "2"));
    client.subscribe(7, { audio: true, camera: false });
    client.updateInfo({ isCameraOn: true, isRaisingHand: true });
    await tick();

    sockets[0].emitClose(1011);
    await tick();
    await finishRecovery(harness);

    assert.deepEqual(decodeSentFrame(sockets[1], 0), [
        {
            t: "auth",
            p: {
                jwt: "jwt-token",
                channel: "channel-a"
            }
        }
    ]);
    assert.deepEqual(decodeSentFrame(sockets[1], 1), [
        {
            t: "subscribe",
            p: {
                sessionId: 7,
                audio: true,
                camera: false
            }
        },
        {
            t: "info",
            p: {
                isCameraOn: true,
                isRaisingHand: true
            }
        }
    ]);

    await emitMessage(buildNegotiationFrame("offer", "server-0", "1"), 1);

    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .some((section) => section.senderTrack === cameraTrack),
        false
    );
    assert.deepEqual(decodeSentFrame(sockets[1], 3), [
        {
            t: "publish",
            p: {
                type: "camera"
            }
        }
    ]);
});

test("real protocol core replays the latest sticky intents changed while recovering", async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, connect, emitMessage, open } = harness;

    await connect();

    await open();
    await emitMessage(buildWelcomeFrame());

    client.publish("camera", createCameraTrack("camera-track-2"));
    client.subscribe(7, { audio: true });
    await tick();

    sockets[0].emitClose(1011);
    await tick();

    client.publish("camera", null);
    client.subscribe(7, { audio: false, camera: true });
    client.updateInfo({ isSelfMuted: true });
    await tick();

    await finishRecovery(harness);

    assert.deepEqual(decodeSentFrame(sockets[1], 1), [
        {
            t: "subscribe",
            p: {
                sessionId: 7,
                audio: false,
                camera: true
            }
        },
        {
            t: "info",
            p: {
                isSelfMuted: true
            }
        }
    ]);
});

test("explicit disconnect neutralizes a stale recovery timer", async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, connect, emitMessage, open, timers } = harness;

    await connect();
    await open();
    await emitMessage(buildWelcomeFrame());

    sockets[0].emitClose(1011);
    await tick();
    assert.equal(timers.hasDelay(1000), true);

    client.disconnect();
    await tick();
    assert.equal(timers.hasDelay(1000), false);

    timers.fireLastByDelay(1000);
    await tick();

    assert.equal(sockets.length, 1);
});

test("new connect neutralizes a stale recovery timer", async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, connect, emitMessage, open, timers } = harness;

    await connect("ws://example.test/old", "old-token");
    await open();
    await emitMessage(buildWelcomeFrame());

    sockets[0].emitClose(1011);
    await tick();
    assert.equal(timers.hasDelay(1000), true);

    client.connect("ws://example.test/new", "new-token");
    await tick();
    assert.equal(sockets.length, 2);
    assert.equal(sockets[1].url, "ws://example.test/new");
    assert.equal(timers.hasDelay(1000), false);

    timers.fireLastByDelay(1000);
    await tick();

    assert.equal(sockets.length, 2);
});

test("new connect closes the previous socket without feeding a stale close to the protocol core", async () => {
    const core = new FakeProtocolCore();
    const { client, sockets, connect, open } = createSfuClientHarness({ protocolCore: core });

    await connect("ws://example.test/old", "old-token");
    await open();

    client.connect("ws://example.test/new", "new-token");
    await tick();

    assert.equal(sockets[0].closeCode, 1000);
    assert.equal(sockets[1].url, "ws://example.test/new");
    assert.deepEqual(core.wsCloseCodes, []);
});

test("negotiation creates a peer connection and emits lowercase track updates", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome({
        connectOptions: {
            iceServers: [{ urls: ["stun:one.example.test", "stun:two.example.test"] }]
        }
    });

    await emitOfferWithBinding({ core, emitMessage });

    assert.equal(peerConnections.length, 1);
    assert.deepEqual(peerConnections[0].config, {
        iceServers: [{ urls: ["stun:one.example.test", "stun:two.example.test"] }]
    });
    assert.deepEqual(core.submittedAnswers, [
        {
            negotiationKind: "offer",
            requestId: "7",
            sdp: "answer-sdp"
        }
    ]);
    assert.equal(client.state, "connected");

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("offer waits for the ICE-complete local description before replying", async () => {
    const { core, emitMessage, peerConnections, connectWithWelcome } = createSfuClientHarness({
        peerConnectionOptions: {
            answerSdp: "answer-sdp",
            gatheredAnswerSdp:
                "answer-sdp\r\na=candidate:1 1 udp 2113937151 127.0.0.1 54400 typ host"
        }
    });

    await connectWithWelcome();
    await emitMessage("offer");

    assert.equal(peerConnections.length, 1);
    assert.deepEqual(core.submittedAnswers, [
        {
            negotiationKind: "offer",
            requestId: "7",
            sdp: "answer-sdp\r\na=candidate:1 1 udp 2113937151 127.0.0.1 54400 typ host"
        }
    ]);
});

test("info_change map payloads are normalized into plain objects", async () => {
    const { emitMessage, updates, connect } = createSfuClientHarness();

    await connect();
    await emitMessage("info-change-map");

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.INFO_CHANGE,
            payload: {
                31: {
                    isRaisingHand: true
                }
            }
        }
    ]);
});

test("info_change map payloads preserve __proto__ as an own property", async () => {
    const { emitMessage, updates, connect } = createSfuClientHarness();

    await connect();
    await emitMessage("info-change-map-proto");

    assert.equal(Object.hasOwn(updates[0].payload, "__proto__"), true);
    assert.deepEqual(Object.keys(updates[0].payload), ["__proto__"]);
    assert.deepEqual(updates[0].payload.__proto__, {
        isRaisingHand: true
    });
});

test("source descriptor updates are exposed as additive client state", async () => {
    const { client, emitMessage, updates, connect } = createSfuClientHarness();

    await connect();
    await emitMessage("source-descriptors");

    const expectedSources = [
        {
            active: true,
            encodings: [
                { encodingId: "encoding-1", maxBitrate: 150000, rid: "lo" },
                { encodingId: "encoding-2", maxBitrate: 900000, rid: "hi" }
            ],
            mid: "0",
            sessionId: 42,
            sourceId: "source-1",
            type: "camera"
        }
    ];
    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.SOURCE,
            payload: {
                sources: expectedSources
            }
        }
    ]);
    assert.deepEqual(client.sourceDescriptors, expectedSources);

    client.disconnect();
    await tick();

    assert.deepEqual(client.sourceDescriptors, []);
});

test("renegotiation attaches pending audio only to upload-eligible mids", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    const localAudioTrack = new FakeMediaTrack({
        id: "local-audio",
        kind: "audio"
    });
    client.publish("audio", localAudioTrack);
    await tick();

    await emitMessage(
        buildNegotiationFrame("renegotiate", "10", {
            sdp: sdp(audioMedia("consumer-audio", "sendonly"), audioMedia("producer-audio")),
            uploadSlots: [audioUploadSlot("producer-audio")]
        })
    );

    assertLastNegotiationResponse(sockets[0], "renegotiate", "10");

    const producerTransceiver = peerConnections[0].transceivers.find(
        (transceiver) => transceiver.mid === "producer-audio"
    );
    const consumerTransceiver = peerConnections[0].transceivers.find(
        (transceiver) => transceiver.mid === "consumer-audio"
    );
    assert.ok(producerTransceiver);
    assert.ok(consumerTransceiver);
    assert.equal(producerTransceiver.sender.track, localAudioTrack);
    assert.equal(consumerTransceiver.sender.track, null);
});

test("track metadata updates re-emit track state for existing remote tracks", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    await emitMessage("track-inactive");

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track,
                type: "camera"
            }
        },
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("subscribe overlays local download state onto existing remote tracks", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    client.subscribe(42, { camera: false });
    await tick();
    await tick();
    client.subscribe(42, { camera: undefined });
    await tick();
    await tick();
    client.subscribe(42, { camera: true });
    await tick();
    await tick();

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track,
                type: "camera"
            }
        },
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        },
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
});

test("subscribe forwards additive video layout intent to the protocol core", async () => {
    const { client, core } = createSfuClientHarness();

    client.subscribe(42, {
        camera: true,
        cameraLayout: "pinned",
        screenLayout: "hidden"
    });

    assert.deepEqual(core.subscriptionUpdates, [
        {
            sessionId: 42,
            states: {
                camera: true,
                cameraLayout: "pinned",
                screenLayout: "hidden"
            }
        }
    ]);
});

test("subscribe rejects invalid download state fields", () => {
    const { client, core } = createSfuClientHarness();

    for (const states of [{ cameraLayout: "floating" }, { camera: true, video: false }]) {
        assert.throws(() => client.subscribe(42, states), Error);
    }
    assert.deepEqual(core.subscriptionUpdates, []);
});

test("subscribe preferences apply to future remote track bindings", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    client.subscribe(42, { camera: false });
    await tick();
    await tick();

    await emitOfferWithBinding({ core, emitMessage });

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: false,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("offer waits for peer connection transport readiness before emitting connected", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness({
            peerConnectionOptions: { autoConnect: false }
        });

    await connectWithWelcome();
    assert.equal(client.state, "authenticated");

    await emitMessage("offer");

    assert.equal(core.transportReadyCalls, 0);
    assert.equal(client.state, "authenticated");
    assert.deepEqual(core.submittedAnswers, [
        {
            negotiationKind: "offer",
            requestId: "7",
            sdp: "answer-sdp"
        }
    ]);

    peerConnections[0].emitConnectionState("connected");
    await tick();

    assert.equal(core.transportReadyCalls, 1);
    assert.equal(client.state, "connected");
});

test("initial offer with only inactive media enters connected without waiting for rtc transport", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness({
            peerConnectionOptions: {
                answerSdp: [
                    "v=0",
                    "o=- 1 1 IN IP4 0.0.0.0",
                    "s=-",
                    "t=0 0",
                    "m=audio 9 UDP/TLS/RTP/SAVPF 111",
                    "a=inactive",
                    "a=candidate:1 1 udp 2113937151 127.0.0.1 54400 typ host",
                    "m=video 9 UDP/TLS/RTP/SAVPF 96",
                    "a=inactive"
                ].join("\r\n"),
                autoConnect: false
            }
        });

    await connectWithWelcome();
    await emitMessage("offer");

    assert.equal(peerConnections.length, 1);
    assert.equal(core.transportReadyCalls, 1);
    assert.equal(client.state, "connected");
});

test("peer connection failed closes the websocket and enters recovery", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const { client, sockets, emitMessage, open, peerConnections, connect } = createSfuClientHarness(
        {
            protocolCore: core
        }
    );

    await connect();
    await open();
    await emitMessage("welcome");
    await emitMessage("offer");

    peerConnections[0].emitConnectionState("failed");
    await tick();

    assert.equal(sockets[0].readyState, 3);
    assert.equal(sockets[0].closeCode, 4000);
    assert.deepEqual(core.wsCloseCodes, [4000]);
    assert.equal(client.state, "recovering");
});

test("peer connection disconnected does not tear down the websocket session", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const { client, sockets, emitMessage, open, peerConnections, connect } = createSfuClientHarness(
        {
            protocolCore: core
        }
    );

    await connect();
    await open();
    await emitMessage("welcome");
    await emitMessage("offer");

    peerConnections[0].emitConnectionState("disconnected");
    await tick();

    assert.equal(sockets[0].readyState, 1);
    assert.equal(sockets[0].closeCode, null);
    assert.deepEqual(core.wsCloseCodes, []);
    assert.equal(client.state, "connected");
});

test("stale peer connection callbacks cannot affect the active session", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const { client, emitMessage, open, peerConnections, sockets, updates, connect } =
        createSfuClientHarness({
            protocolCore: core
        });

    await connect();
    await open();
    await emitMessage("welcome");

    await emitOfferWithBinding({ core, emitMessage });

    const stalePeerConnection = peerConnections[0];
    assert.equal(client.state, "connected");

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 84,
        type: "screen"
    });
    await emitMessage("offer");

    assert.equal(peerConnections.length, 2);
    assert.equal(stalePeerConnection.closed, true);

    stalePeerConnection.emitTrack(createCameraTrack("stale-camera"), "0");
    stalePeerConnection.emitConnectionState("connected");
    stalePeerConnection.emitConnectionState("failed");
    await tick();

    assert.deepEqual(updates, []);
    assert.equal(core.transportReadyCalls, 2);
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0].closeCode, null);
    assert.equal(client.state, "connected");
    assert.equal(client._consumers.size, 0);
});

test("track rebinding waits for a fresh track event before re-emitting state", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const firstTrack = createCameraTrack("track-1");
    peerConnections[0].emitTrack(firstTrack, "0");
    await tick();

    await emitMessage("track-rebind");

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track: firstTrack,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.has(42), false);
    assert.equal(client._consumers.has(84), false);

    const reboundTrack = createScreenTrack("track-2");
    peerConnections[0].emitTrack(reboundTrack, "0");
    await tick();

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track: firstTrack,
                type: "camera"
            }
        },
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 84,
                track: reboundTrack,
                type: "screen"
            }
        }
    ]);
    assert.equal(client._consumers.get(84).screen.track, reboundTrack);
});

test("peer departure clears remote-track state through the host cleanup command", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    await emitMessage("peer-left");

    assert.equal(client._consumers.has(42), false);
    assert.deepEqual(updates.at(-1), {
        name: CLIENT_UPDATE.DISCONNECT,
        payload: {
            sessionId: 42
        }
    });
});

test("peer connection teardown clears stale remote consumer state", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    peerConnections[0].emitTrack(createCameraTrack("track-1"), "0");
    await tick();

    assert.equal(client._consumers.get(42).camera.track.id, "track-1");

    await emitMessage("close-peer-connection");

    assert.equal(peerConnections[0].closed, true);
    assert.equal(client._consumers.size, 0);
});

test("remote track lifecycle updates re-emit when the browser unmutes the track", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    const track = new FakeMediaTrack({
        id: "track-1",
        kind: "video",
        muted: true
    });
    peerConnections[0].emitTrack(track, "0");
    await tick();

    track.setMuted(false);
    await tick();

    assert.deepEqual(updates.at(-1), {
        name: CLIENT_UPDATE.TRACK,
        payload: {
            active: true,
            sessionId: 42,
            track,
            type: "camera"
        }
    });
    assert.equal(client._consumers.get(42).camera.track, track);
    assert.equal(client._consumers.get(42).camera.track.muted, false);
});

test("publish replaces an already attached local sender track without re-publishing", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness();

    const firstTrack = createCameraTrack("camera-track-1");
    const secondTrack = createCameraTrack("camera-track-2");

    await connectWithWelcome();

    client.publish("camera", firstTrack);
    await tick();

    await emitMessage("offer-with-attach-camera");

    assert.equal(peerConnections[0].transceivers[1].sender.track, firstTrack);
    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);

    client.publish("camera", secondTrack);
    await tick();

    assert.equal(peerConnections[0].transceivers[1].sender.track, secondTrack);
    assert.deepEqual(
        core.publicationUpdates,
        [{ active: true, type: "camera" }],
        "replacing a live local track should stay local once the sender is bound"
    );
});

test("publish detaches the local sender before signaling unpublish", async () => {
    const { client, core, emitMessage, peerConnections, connectWithWelcome } =
        createSfuClientHarness();

    const track = createCameraTrack("camera-track-1");

    await connectWithWelcome();

    client.publish("camera", track);
    await tick();
    await emitMessage("offer-with-attach-camera");

    assert.equal(peerConnections[0].transceivers[1].sender.track, track);
    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);

    client.publish("camera", null);
    await tick();

    assert.equal(peerConnections[0].transceivers[1].sender.track, null);
    assert.deepEqual(core.publicationUpdates, [
        { active: true, type: "camera" },
        { active: false, type: "camera" }
    ]);
});

test("renegotiation binds a newly published local track before answering", async () => {
    const { peerConnections, track } = await renegotiateCamera(
        buildVideoRenegotiationFrame("9", { simulcastEncodings: [] }),
        "camera-track-1"
    );

    assert.equal(peerConnections[0].transceivers[2].sender.track, track);
    assert.equal(peerConnections[0].transceivers[2].direction, "sendonly");
    assert.equal(
        peerConnections[0].answerSnapshots.at(-1)[2].senderTrack,
        track,
        "the browser must bind the track before generating the renegotiation answer"
    );
});

const EXPECTED_RID_ENCODINGS = [
    {
        active: true,
        maxBitrate: 150000,
        rid: "lo",
        scaleResolutionDownBy: 4
    },
    {
        active: true,
        maxBitrate: 900000,
        rid: "hi",
        scaleResolutionDownBy: 1
    }
];

async function renegotiateCamera(frame, trackId, harnessOptions = {}) {
    const harness = createRecoveryHarness(harnessOptions);
    const track = createCameraTrack(trackId);

    await connectRealWithWelcome(harness);
    await harness.emitMessage(buildNegotiationFrame("offer", "7", "1"));

    harness.client.publish("camera", track);
    await tick();
    await harness.emitMessage(frame);
    const transceiver = harness.peerConnections[0].transceivers.find(
        (candidate) => candidate.mid === "2"
    );
    assert.ok(transceiver);
    assertLastNegotiationResponse(harness.sockets[0], "renegotiate", JSON.parse(frame)[0].q);

    return {
        ...harness,
        track,
        transceiver
    };
}

function assertSenderEncodings(peerConnection, transceiver, expected) {
    const snapshot = peerConnection.answerSnapshots
        .at(-1)
        .find((candidate) => candidate.mid === transceiver.mid);
    assert.ok(snapshot);
    assert.deepEqual(snapshot.senderParameters, {
        encodings: expected
    });
}

test("renegotiation configures RID simulcast before answering supported video publishes", async () => {
    const { peerConnections, track, transceiver } = await renegotiateCamera(
        buildVideoRenegotiationFrame("12", {
            rtpmap: "VP8/90000"
        }),
        "camera-track-simulcast"
    );

    assert.equal(transceiver.sender.track, track);
    assertSenderEncodings(peerConnections[0], transceiver, EXPECTED_RID_ENCODINGS);
});

test("renegotiation configures RID simulcast from server-defined upload slots", async () => {
    const { peerConnections, track, transceiver } = await renegotiateCamera(
        buildVideoRenegotiationFrame("13", {
            codecs: ["H264"],
            payloadType: 102,
            rtpmap: "H264/90000"
        }),
        "camera-track-single"
    );

    assert.equal(transceiver.sender.track, track);
    assertSenderEncodings(peerConnections[0], transceiver, EXPECTED_RID_ENCODINGS);
});

test("renegotiation falls back to single encoding when the server ladder is invalid", async () => {
    const { peerConnections, transceiver } = await renegotiateCamera(
        buildVideoRenegotiationFrame("14", {
            rtpmap: "VP8/90000",
            simulcastEncodings: [
                {
                    maxBitrate: 150000,
                    rid: "lo",
                    resolutionScale: 0
                },
                {
                    maxBitrate: 900000,
                    rid: "hi",
                    resolutionScale: 1
                }
            ]
        }),
        "camera-track-invalid-profile"
    );

    assertSenderEncodings(peerConnections[0], transceiver, []);
});

test("renegotiation falls back to single encoding when sender parameters are rejected", async () => {
    const { peerConnections, transceiver } = await renegotiateCamera(
        buildVideoRenegotiationFrame("12", {
            rtpmap: "VP8/90000"
        }),
        "camera-track-rejected-profile",
        {
            peerConnectionOptions: {
                senderOptionsByMid: {
                    2: { rejectSetParameters: true }
                }
            }
        }
    );

    assertSenderEncodings(peerConnections[0], transceiver, []);
});

test("initial offer binds a pending local track before answering", async () => {
    const { client, emitMessage, peerConnections, connectWithWelcome } = createSfuClientHarness({
        peerConnectionOptions: { autoConnect: false }
    });

    const track = createCameraTrack("camera-track-pending-offer");

    await connectWithWelcome();

    client.publish("camera", track);
    await tick();
    await emitMessage("offer");

    assert.equal(peerConnections[0].transceivers[1].sender.track, track);
    assert.equal(
        peerConnections[0].answerSnapshots.at(-1)[1].senderTrack,
        track,
        "the browser must bind the pending upload before generating the initial answer"
    );
});

test("offer waits for ice gathering completion before submitting the final answer", async () => {
    const { core, emitMessage, connectWithWelcome } = createSfuClientHarness({
        peerConnectionOptions: {
            autoConnect: false,
            gatheredAnswerSdp: "gathered-answer-sdp",
            preCompleteAnswerSdp: "candidate-answer-sdp"
        }
    });

    await connectWithWelcome();
    await emitMessage("offer");

    assert.deepEqual(core.submittedAnswers, [
        {
            negotiationKind: "offer",
            requestId: "7",
            sdp: "gathered-answer-sdp"
        }
    ]);
});

test("renegotiation binds pending camera and screen tracks to distinct offer-ordered mids", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections } = harness;

    const cameraTrack = createCameraTrack("camera-track-distinct-mid");
    const screenTrack = createScreenTrack("screen-track-distinct-mid");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("camera", cameraTrack);
    await tick();
    client.publish("screen", screenTrack);
    await tick();
    await emitMessage(
        buildNegotiationFrame("renegotiate", "11", {
            sdp: sdp(videoMedia("2"), videoMedia("3")),
            uploadSlots: [
                videoUploadSlot("2", { simulcastEncodings: [] }),
                videoUploadSlot("3", { simulcastEncodings: [] })
            ]
        })
    );

    assert.equal(peerConnections[0].transceivers[2].sender.track, cameraTrack);
    assert.equal(peerConnections[0].transceivers[3].sender.track, screenTrack);
    assert.equal(peerConnections[0].answerSnapshots.at(-1)[2].senderTrack, cameraTrack);
    assert.equal(peerConnections[0].answerSnapshots.at(-1)[3].senderTrack, screenTrack);
});

test("getStats exposes compatibility-shaped transport and producer stats", async () => {
    const peerConnectionStats = new Map([["transport", { type: "transport" }]]);
    const cameraProducerStats = new Map([["outbound-rtp", { type: "outbound-rtp" }]]);
    const { client, peerConnections, emitMessage, connectWithWelcome } = createSfuClientHarness({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, {
                peerConnectionStats
            });
            peerConnection.transceivers[1].sender = new FakeSender(cameraProducerStats);
            return peerConnection;
        }
    });

    await connectWithWelcome();

    client.updateUpload("camera", createCameraTrack("camera-track-compat"));
    await tick();

    await emitMessage("offer-with-attach-camera");

    const stats = await client.getStats();

    assert.equal(peerConnections.length, 1);
    assert.equal(stats.uploadStats, peerConnectionStats);
    assert.equal(stats.downloadStats, peerConnectionStats);
    assert.equal(stats.camera, cameraProducerStats);
    assert.equal(stats.audio, undefined);
    assert.equal(stats.screen, undefined);
});

test("updateInfo keeps the legacy needRefresh option as a compatibility no-op", async () => {
    const { client, core } = createSfuClientHarness();

    client.updateInfo({ isCameraOn: true, isRaisingHand: true }, { needRefresh: true });
    await tick();

    assert.deepEqual(core.updateInfoCalls, [
        {
            isCameraOn: true,
            isRaisingHand: true
        }
    ]);
});

test("fatal runtime errors reset the public client surface", async () => {
    const { client, core, emitMessage, handledErrors, open, peerConnections, sockets, connect } =
        createSfuClientHarness();

    await connect();
    await open();
    await emitMessage("welcome");

    await emitOfferWithBinding({ core, emitMessage });
    peerConnections[0].emitTrack(createCameraTrack("track-1"), "0");
    await emitMessage("source-descriptors");

    await emitMessage("explode");

    assert.equal(core.disconnectCalls, 1);
    assert.equal(client.state, "disconnected");
    assert.deepEqual(client.availableFeatures, EMPTY_FEATURES);
    assert.deepEqual(client.recordingState, {});
    assert.deepEqual(client.sourceDescriptors, []);
    assert.equal(client._consumers.size, 0);
    assert.equal(client.errors.length, 1);
    assert.equal(client.errors[0] instanceof Error, true);
    assert.equal(handledErrors[0], client.errors[0]);
    assert.equal(sockets[0].closeCode, 4000);
    assert.equal(sockets[0].readyState, 3);
    assert.deepEqual(core.wsCloseCodes, []);
});

test("fatal runtime errors drop already queued browser commands", async () => {
    const { client, connect, handledErrors, open, peerConnections, sockets } =
        createSfuClientHarness({
            createPeerConnection(config) {
                const peerConnection = new FakePeerConnection(config);
                peerConnection.setRemoteDescription = async () => {
                    throw new Error("broken remote offer");
                };
                return peerConnection;
            }
        });

    await connect();
    await open();

    sockets[0].emitMessage("offer");
    sockets[0].emitMessage("source-descriptors");
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.equal(peerConnections[0].closed, true);
    assert.deepEqual(client.sourceDescriptors, []);
});

test("publish rejects stream-kind mismatches", () => {
    const { client } = createSfuClientHarness();

    assert.throws(() => {
        client.publish("camera", {
            id: "audio-track",
            kind: "audio"
        });
    }, Error);
});

test("publish rejects invalid stream types", () => {
    const { client, core } = createSfuClientHarness();

    assert.throws(() => {
        client.publish("slides", null);
    }, Error);
    assert.deepEqual(core.publicationUpdates, []);
});

test("deprecated updateUpload and updateDownload delegate to publish and subscribe", async () => {
    const { client, core } = createSfuClientHarness();

    client.updateUpload("camera", createCameraTrack("camera-track-compat"));
    client.updateDownload(7, { audio: true });
    await tick();

    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);
    assert.equal(core.subscriptionUpdates.length, 1);
});

test("deprecated updateUpload treats undefined like the legacy no-track sentinel", async () => {
    const { client, core } = createSfuClientHarness();

    client.updateUpload("camera", undefined);
    await tick();

    assert.deepEqual(core.publicationUpdates, []);
});
