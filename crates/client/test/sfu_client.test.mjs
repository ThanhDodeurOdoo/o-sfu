import assert from "node:assert/strict";
import test from "node:test";

import { CLIENT_UPDATE } from "../dist/index.js";
import { COMMAND_KIND, PENDING_REQUEST_KIND, WS_CLOSE_CODE } from "../dist/protocol_contract.js";
import { createProtocolCore } from "../dist/runtime_contract.js";
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

test("immediate disconnect prevents pending connect and subscription", async () => {
    const core = new FakeProtocolCore();
    core.disconnect = () => [];
    const { client, sockets } = createSfuClientHarness({ protocolCore: core });

    client.connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    client.subscribe(42, { camera: false });
    client.disconnect();
    await tick();

    assert.deepEqual(sockets, []);
    assert.deepEqual(core.subscriptionUpdates, []);
});

test("last same-turn disconnect drops a queued reconnect without canceling cleanup", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.disconnect();
    client.connect("ws://other.example.test/ws", "jwt-token", { channelUUID: "channel-b" });
    client.disconnect();
    await tick();

    assert.equal(client.state, "disconnected");
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0].closeCode, WS_CLOSE_CODE.CLEAN);
    assert.equal(peerConnections[0].closed, true);
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
    core.startRecording = (options) => {
        assert.deepEqual(options, { audio: true });
        return [];
    };
    core.stopRecording = () => [];
    const { client } = createSfuClientHarness({ protocolCore: core });
    const options = { audio: true };

    const recording = client.startRecording(options);
    options.audio = false;
    assert.equal(await recording, false);
    assert.equal(await client.stopRecording(), false);
});

test("disconnect resolves a registered recording request", { timeout: 2_000 }, async () => {
    const harness = createRecoveryHarness();
    const { client, timers } = harness;

    await connectRealWithWelcome(harness);
    const recording = client.startRecording({ audio: true });
    await tick();

    assert.equal(timers.hasDelay(5000), true);
    client.disconnect();

    assert.equal(await recording, false);
    assert.equal(timers.hasDelay(5000), false);
});

test("socket recovery resolves a registered recording request", { timeout: 2_000 }, async () => {
    const harness = createRecoveryHarness();
    const { client, sockets, timers } = harness;

    await connectRealWithWelcome(harness);
    const recording = client.startRecording({ audio: true });
    await tick();

    assert.equal(timers.hasDelay(5000), true);
    sockets[0].emitClose(1011);

    assert.equal(await recording, false);
    await tick();
    assert.equal(timers.hasDelay(5000), false);
    assert.equal(client.state, "recovering");
});

test("duplicate recording request id is handled as a runtime error", async () => {
    const core = new FakeProtocolCore();
    core.startRecording = () => [
        {
            kind: COMMAND_KIND.BEGIN_PENDING_REQUEST,
            request: {
                kind: PENDING_REQUEST_KIND.START_RECORDING,
                requestId: "record-1",
                timeoutMs: 5000,
                timeoutTimerId: 10000
            }
        }
    ];
    const { client, handledErrors } = createSfuClientHarness({ protocolCore: core });

    const registeredPromise = client.startRecording({ audio: true });
    const registeredRejection = assert.rejects(registeredPromise, Error);
    await tick();
    await assert.rejects(client.startRecording({ audio: true }), Error);

    assert.equal(client.errors.length, 1);
    assert.equal(handledErrors[0], client.errors[0]);
    await registeredRejection;
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
    const recordingRejection = assert.rejects(registeredPromise, /recording runtime failure/);

    await emitMessage("recording-runtime-failure");
    await recordingRejection;
});

test("runtime aborts reject stale queued recording requests", async () => {
    const { client, connectWithWelcome, sockets } = createSfuClientHarness({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnection.setRemoteDescription = async () => {
                throw new Error("broken remote offer");
            };
            return peerConnection;
        }
    });

    await connectWithWelcome();

    sockets[0].emitMessage("offer");
    const recordingRejection = assert.rejects(
        client.startRecording({ audio: true }),
        /broken remote offer/
    );
    await tick();
    await recordingRejection;
});

test("recording request timer setup failures reject through the public promise", async (t) => {
    const core = new FakeProtocolCore();
    const unhandledRejections = [];
    const trackUnhandledRejection = (reason) => {
        unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", trackUnhandledRejection);
    t.after(() => process.off("unhandledRejection", trackUnhandledRejection));
    const { client, handledErrors } = createSfuClientHarness({
        protocolCore: core,
        setTimer: () => {
            throw new Error("timer setup failed");
        }
    });

    await assert.rejects(client.startRecording({ audio: true }), /timer setup failed/);
    await tick();
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.match(handledErrors[0].message, /timer setup failed/);
    assert.deepEqual(unhandledRejections, []);
});

test("startRecording rejects when the protocol core throws undefined", async () => {
    const core = new FakeProtocolCore();
    core.startRecording = () => {
        throw undefined;
    };
    const { client } = createSfuClientHarness({ protocolCore: core });

    await assert.rejects(client.startRecording(), (error) => error === undefined);
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

function sentPublishCount(socket) {
    return socket.sent
        .flatMap((_, index) => decodeSentFrame(socket, index))
        .filter((envelope) => envelope.t === "publish").length;
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

test("authenticated publication waits for transport readiness", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets } = harness;

    await connectRealWithWelcome(harness);
    client.publish("camera", createCameraTrack("camera-track"));
    await tick();

    assert.equal(sentPublishCount(sockets[0]), 0);
    await emitMessage(buildNegotiationFrame("offer", "server-initial", "1"));
    assert.equal(sentPublishCount(sockets[0]), 1);
});

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

    await emitMessage(buildNegotiationFrame("renegotiate", "server-republish", "2"), 1);

    const replayTransceiver = peerConnections
        .at(-1)
        .transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(replayTransceiver);
    assert.equal(replayTransceiver.sender.track, cameraTrack);
    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .find((snapshot) => snapshot.mid === "2")?.senderTrack,
        cameraTrack
    );
});

test("real protocol core waits for transport-ready replay before binding recovery publish", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;
    const track = createCameraTrack("camera-track-recovery-pending");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "server-initial", "1"));

    sockets[0].emitClose(1011);
    await tick();

    client.publish("camera", track);
    await tick();

    await finishRecovery(harness);
    await emitMessage(buildNegotiationFrame("offer", "server-recovery", "1"), 1);

    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .some((section) => section.senderTrack === track),
        false
    );
    assert.equal(sentPublishCount(sockets[1]), 1);

    await emitMessage(buildVideoRenegotiationFrame("server-republish", { mid: "2" }), 1);

    const transceiver = peerConnections
        .at(-1)
        .transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, track);
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

test("protocol inputs wait for an in-flight negotiation to finish", async () => {
    const { promise: answerGate, resolve: releaseAnswer } = Promise.withResolvers();
    const callOrder = [];
    class GatedPeerConnection extends FakePeerConnection {
        async createAnswer() {
            await answerGate;
            return super.createAnswer();
        }
    }
    class LoggingProtocolCore extends FakeProtocolCore {
        onWsMessage(frame) {
            callOrder.push(`onWsMessage:${frame}`);
            return super.onWsMessage(frame);
        }

        submitNegotiationAnswer(...args) {
            callOrder.push("submitNegotiationAnswer");
            super.submitNegotiationAnswer(...args);
            return [{ kind: "sendWebSocket", frame: "answer-feedback" }];
        }
    }
    const { connect, emitMessage, open, sockets } = createSfuClientHarness({
        createPeerConnection: (config) => new GatedPeerConnection(config),
        protocolCore: new LoggingProtocolCore()
    });

    await connect();
    await open();
    await emitMessage("welcome");
    const send = sockets[0].send.bind(sockets[0]);
    sockets[0].send = (frame) => {
        callOrder.push(`send:${frame}`);
        send(frame);
    };
    await emitMessage("offer");
    sockets[0].emitMessage("peer-left");
    await tick();

    assert.equal(callOrder.includes("onWsMessage:peer-left"), false);

    releaseAnswer();
    await tick();

    assert.deepEqual(callOrder.slice(-3), [
        "submitNegotiationAnswer",
        "send:answer-feedback",
        "onWsMessage:peer-left"
    ]);
});

test("reentrant disconnect cannot reorder a subscription behind cleanup", async () => {
    const core = new FakeProtocolCore();
    const calls = [];
    const subscribe = core.subscribe.bind(core);
    core.subscribe = (...args) => {
        calls.push("subscribe");
        return subscribe(...args);
    };
    const { client, connectWithWelcome, emitMessage, peerConnections } = createSfuClientHarness({
        protocolCore: core
    });

    await connectWithWelcome();
    await emitOfferWithBinding({ core, emitMessage });
    peerConnections[0].emitTrack(createCameraTrack("camera-track"), "0");
    await tick();

    client.addEventListener("update", (event) => {
        if (event.detail.name === CLIENT_UPDATE.TRACK && !event.detail.payload.active) {
            calls.push("disconnect");
            client.disconnect();
        }
    });
    client.subscribe(42, { camera: false });
    await tick();

    assert.deepEqual(calls, ["subscribe", "disconnect"]);
    assert.equal(core.disconnectCalls, 1);
});

test("reentrant disconnect cancels publication signaling", async () => {
    const core = new FakeProtocolCore();
    const { client, connectWithWelcome } = createSfuClientHarness({ protocolCore: core });

    await connectWithWelcome();
    client.addEventListener("log", () => client.disconnect(), { once: true });
    client.publish("camera", createCameraTrack("camera-track"));
    await tick();

    assert.equal(core.disconnectCalls, 1);
    assert.deepEqual(core.publicationUpdates, []);
});

test("socket close cancels an in-flight negotiation before recovery", async () => {
    const { promise: answerGate, resolve: releaseAnswer } = Promise.withResolvers();
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const onWsClose = core.onWsClose.bind(core);
    core.onWsClose = (code) => [{ kind: "closePeerConnection" }, ...onWsClose(code)];
    class GatedPeerConnection extends FakePeerConnection {
        async createAnswer() {
            await answerGate;
            return super.createAnswer();
        }
    }
    const { client, connectWithWelcome, emitMessage, handledErrors, peerConnections, sockets } =
        createSfuClientHarness({
            createPeerConnection: (config) => new GatedPeerConnection(config),
            protocolCore: core
        });

    await connectWithWelcome();
    await emitMessage("offer");
    const socket = sockets[0];
    socket.close = (code) => {
        socket.closeCode = code;
        socket.readyState = 2;
    };
    peerConnections[0].emitConnectionState("failed");
    releaseAnswer();
    await tick();

    assert.equal(client.state, "recovering");
    assert.equal(peerConnections[0].closed, true);
    assert.deepEqual(core.wsCloseCodes, [4000]);
    assert.deepEqual(core.submittedAnswers, []);
    assert.deepEqual(handledErrors, []);

    socket.readyState = 3;
    socket.onclose?.({ code: socket.closeCode });
    await tick();

    assert.deepEqual(core.wsCloseCodes, [4000]);
});

test("same-turn recovery retains queued sticky inputs", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const { client, connectWithWelcome, sockets } = createSfuClientHarness({ protocolCore: core });

    await connectWithWelcome();
    client.publish("camera", createCameraTrack("camera-before-recovery"));
    client.subscribe(42, { camera: false });
    client.updateInfo({ isCameraOn: false });
    client.broadcast({ dropped: true });
    sockets[0].emitClose(1011);
    await tick();

    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);
    assert.deepEqual(core.subscriptionUpdates, [{ sessionId: 42, states: { camera: false } }]);
    assert.deepEqual(core.updateInfoCalls, [{ isCameraOn: false }]);
    assert.deepEqual(core.broadcasts, []);
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
    const sourceSnapshots = [];
    client.addEventListener("update", (event) => {
        if (event.detail.name === CLIENT_UPDATE.SOURCE) {
            sourceSnapshots.push(client.sourceDescriptors);
        }
    });

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
    assert.deepEqual(sourceSnapshots, [expectedSources]);

    client.disconnect();
    await tick();

    assert.deepEqual(client.sourceDescriptors, []);
    assert.deepEqual(sourceSnapshots.at(-1), []);
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

    await emitMessage("inactive-track-binding");

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

test("track events wait for later binding snapshots before publishing", async () => {
    const { client, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();
    await emitMessage("offer");

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    assert.deepEqual(updates, []);
    assert.equal(client._consumers.size, 0);

    await emitMessage("inactive-track-binding");

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

test("initial peer creation keeps earlier binding snapshots", async () => {
    const core = new FakeProtocolCore();
    const onWsMessage = core.onWsMessage.bind(core);
    core.onWsMessage = (frame) => {
        if (frame === "offer-without-track-bindings") {
            return core._withPendingNegotiationKind([
                { kind: COMMAND_KIND.CREATE_PEER_CONNECTION },
                {
                    kind: COMMAND_KIND.APPLY_NEGOTIATION,
                    negotiationKind: "offer",
                    requestId: "7",
                    sdp: sdp(audioMedia("0"), videoMedia("1")),
                    uploadSlots: [audioUploadSlot("0"), videoUploadSlot("1")]
                }
            ]);
        }
        return onWsMessage(frame);
    };
    const { client, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness({ protocolCore: core });

    await connectWithWelcome();
    await emitMessage("inactive-track-binding");
    await emitMessage("offer-without-track-bindings");

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

test("track-only slots survive empty binding snapshots before publishing", async () => {
    const { client, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome();
    await emitMessage("offer");

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    await emitMessage("clear-track-bindings");
    await emitMessage("inactive-track-binding");

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
    const states = {
        camera: true,
        cameraLayout: "pinned",
        screenLayout: "hidden"
    };

    client.subscribe(42, states);
    states.camera = false;
    await tick();

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

test("fresh connect clears subscription overlays from the previous session", async () => {
    const { client, core, connectWithWelcome, emitMessage, peerConnections, updates } =
        createSfuClientHarness();

    client.subscribe(42, { camera: false });
    await tick();
    await connectWithWelcome();
    await emitOfferWithBinding({ core, emitMessage });
    peerConnections[0].emitTrack(createCameraTrack("camera-track"), "0");
    await tick();

    assert.equal(updates.at(-1).payload.active, true);
});

test("recovery retains subscription overlays for rebound tracks", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const onWsClose = core.onWsClose.bind(core);
    core.onWsClose = (code) => [
        { kind: "closePeerConnection" },
        ...onWsClose(code),
        { kind: "connect", url: "ws://example.test/recovery" }
    ];
    const { client, connectWithWelcome, emitMessage, open, peerConnections, sockets, updates } =
        createSfuClientHarness({ protocolCore: core });

    await connectWithWelcome();
    client.subscribe(42, { camera: false });
    await tick();
    sockets[0].emitClose(1011);
    await tick();
    await open(1);
    await emitMessage("welcome", 1);
    core.trackBindings.set("0", { active: true, mid: "0", sessionId: 42, type: "camera" });
    await emitMessage("offer", 1);
    peerConnections[0].emitTrack(createCameraTrack("rebound-camera"), "0");
    await tick();

    assert.equal(updates.at(-1).payload.active, false);
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

    await emitOfferWithBinding({ core, emitMessage }, { sessionId: 84, type: "screen" });

    assert.equal(peerConnections.length, 2);
    assert.equal(stalePeerConnection.closed, true);
    const transportReadyCalls = core.transportReadyCalls;

    stalePeerConnection.emitTrack(createCameraTrack("stale-camera"), "0");
    stalePeerConnection.emitConnectionState("connected");
    stalePeerConnection.emitConnectionState("failed");
    await tick();

    assert.deepEqual(updates, []);
    assert.equal(core.transportReadyCalls, transportReadyCalls);
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0].closeCode, null);
    assert.equal(client.state, "connected");
    assert.equal(client._consumers.size, 0);
});

test("peer teardown clears bindings before the next peer can emit tracks", async () => {
    const core = new FakeProtocolCore();
    const earlyTrack = new FakeMediaTrack({
        id: "fresh-before-binding",
        kind: "video"
    });
    const { client, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness({
            protocolCore: core,
            createPeerConnection(config, index) {
                const peerConnection = new FakePeerConnection(config);
                if (index === 1) {
                    const setRemoteDescription =
                        peerConnection.setRemoteDescription.bind(peerConnection);
                    peerConnection.setRemoteDescription = async (description) => {
                        await setRemoteDescription(description);
                        peerConnection.emitTrack(earlyTrack, "0");
                    };
                }
                return peerConnection;
            }
        });

    await connectWithWelcome();

    await emitOfferWithBinding({ core, emitMessage });

    peerConnections[0].emitTrack(createCameraTrack("track-1"), "0");
    await tick();
    updates.length = 0;

    await emitOfferWithBinding({ core, emitMessage }, { sessionId: 84, type: "screen" });

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 84,
                track: earlyTrack,
                type: "screen"
            }
        }
    ]);
    assert.equal(client._consumers.has(42), false);
    assert.equal(client._consumers.get(84).screen.track, earlyTrack);
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

test("peer departure clears remote-track state before disconnect update", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();
    const consumerStateAtDisconnect = [];
    client.addEventListener("update", (event) => {
        if (event.detail.name === CLIENT_UPDATE.DISCONNECT) {
            consumerStateAtDisconnect.push({
                hasConsumer: client._consumers.has(42),
                sourceDescriptors: client.sourceDescriptors
            });
        }
    });

    await connectWithWelcome();

    await emitMessage("source-descriptors");
    await emitOfferWithBinding({ core, emitMessage });

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");
    await tick();

    await emitMessage("peer-left");

    assert.deepEqual(consumerStateAtDisconnect, [{ hasConsumer: false, sourceDescriptors: [] }]);
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

test("duplicate remote track events keep one lifecycle listener", async () => {
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
    peerConnections[0].emitTrack(track, "0");
    await tick();

    assert.equal(updates.length, 1);

    track.setMuted(false);
    await tick();

    assert.equal(updates.length, 2);
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

    await emitMessage("offer");

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

test("cancelled track replacement cannot retain a stale peer binding", async () => {
    const { promise: replacementGate, resolve: releaseReplacement } = Promise.withResolvers();
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;
    const firstTrack = createCameraTrack("camera-before-recovery");
    const secondTrack = createCameraTrack("camera-after-recovery");

    await connectRealWithWelcome(harness);
    client.publish("camera", firstTrack);
    await tick();
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    const sender = peerConnections[0].transceivers[1].sender;
    const replaceTrack = sender.replaceTrack.bind(sender);
    let replacementStarted = false;
    sender.replaceTrack = async (track) => {
        if (track === secondTrack) {
            replacementStarted = true;
            await replacementGate;
        }
        await replaceTrack(track);
    };

    client.publish("camera", secondTrack);
    await tick();
    assert.equal(replacementStarted, true);
    sockets[0].emitClose(1011);
    await tick();
    releaseReplacement();
    await tick();

    await finishRecovery(harness);
    await emitMessage(buildNegotiationFrame("offer", "recovery-offer", "1"), 1);
    await emitMessage(
        buildVideoRenegotiationFrame("recovery-renegotiation", {
            simulcastEncodings: []
        }),
        1
    );

    assert.equal(
        peerConnections[1].transceivers.find((transceiver) => transceiver.mid === "2").sender.track,
        secondTrack
    );
});

test("cancelled track detach cannot clear a recovered peer binding", async () => {
    const { promise: detachGate, resolve: releaseDetach } = Promise.withResolvers();
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;
    const secondTrack = createCameraTrack("camera-after-recovery");
    const thirdTrack = createCameraTrack("camera-after-stale-detach");

    await connectRealWithWelcome(harness);
    client.publish("camera", createCameraTrack("camera-before-recovery"));
    await tick();
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    const sender = peerConnections[0].transceivers[1].sender;
    const replaceTrack = sender.replaceTrack.bind(sender);
    let detachStarted = false;
    sender.replaceTrack = async (track) => {
        if (track === null) {
            detachStarted = true;
            await detachGate;
        }
        await replaceTrack(track);
    };

    client.publish("camera", null);
    await tick();
    assert.equal(detachStarted, true);
    sockets[0].emitClose(1011);
    await tick();
    client.publish("camera", secondTrack);
    await tick();

    await finishRecovery(harness);
    await emitMessage(buildNegotiationFrame("offer", "recovery-offer", "1"), 1);
    await emitMessage(buildVideoRenegotiationFrame("recovery-renegotiation", { mid: "2" }), 1);
    releaseDetach();
    await tick();

    client.publish("camera", thirdTrack);
    await tick();

    const recoveredSender = peerConnections[1].transceivers.find(
        (transceiver) => transceiver.mid === "2"
    ).sender;
    assert.equal(recoveredSender.track, thirdTrack);
});

test("publish detaches the local sender before signaling unpublish", async () => {
    const track = createCameraTrack("camera-track-1");
    const { client, core, emitMessage, open, peerConnections, sockets, connect } =
        createSfuClientHarness();
    const originalPublish = core.publish.bind(core);
    core.publish = (type, active) => {
        const commands = originalPublish(type, active);
        if (!active) {
            commands.push({ frame: `unpublish:${type}`, kind: "sendWebSocket" });
        }
        return commands;
    };

    await connect();
    await open();
    await emitMessage("welcome");

    client.publish("camera", track);
    await tick();
    await emitMessage("offer");

    assert.equal(peerConnections[0].transceivers[1].sender.track, track);
    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);

    const unpublishOrder = [];
    const socket = sockets[0];
    const originalSend = socket.send.bind(socket);
    socket.send = (frame) => {
        unpublishOrder.push(`send:${frame}`);
        originalSend(frame);
    };
    const sender = peerConnections[0].transceivers[1].sender;
    const originalReplaceTrack = sender.replaceTrack.bind(sender);
    sender.replaceTrack = async (replacementTrack) => {
        assert.equal(replacementTrack, null);
        await originalReplaceTrack(replacementTrack);
        unpublishOrder.push("detach");
    };

    client.publish("camera", null);
    await tick();

    assert.deepEqual(unpublishOrder, ["detach", "send:unpublish:camera"]);
    assert.equal(peerConnections[0].transceivers[1].sender.track, null);
    assert.deepEqual(core.publicationUpdates, [
        { active: true, type: "camera" },
        { active: false, type: "camera" }
    ]);
});

test("duplicate unpublish keeps later re-publish eligible for a new upload mid", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections } = harness;
    const firstTrack = createCameraTrack("camera-track-first");
    const secondTrack = createCameraTrack("camera-track-second");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("camera", firstTrack);
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("9", { mid: "2", simulcastEncodings: [] }));

    client.publish("camera", null);
    client.publish("camera", null);
    await tick();

    client.publish("camera", secondTrack);
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("10", { mid: "3", simulcastEncodings: [] }));

    const transceiver = peerConnections[0].transceivers.find((candidate) => candidate.mid === "3");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, secondTrack);
    assert.equal(
        peerConnections[0].answerSnapshots
            .at(-1)
            .find((snapshot) => snapshot.mid === transceiver.mid)?.senderTrack,
        secondTrack
    );
});

test("same-turn unpublish and republish keeps the new track upload-eligible", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections } = harness;
    const secondTrack = createCameraTrack("camera-track-same-turn-second");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("camera", createCameraTrack("camera-track-same-turn-first"));
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("9", { mid: "2", simulcastEncodings: [] }));

    client.publish("camera", null);
    client.publish("camera", secondTrack);
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("10", { mid: "3", simulcastEncodings: [] }));

    const transceiver = peerConnections[0].transceivers.find((candidate) => candidate.mid === "3");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, secondTrack);
});

test("explicit disconnect clears publication intent before reconnect", async () => {
    const harness = createRecoveryHarness();
    const { client, connect, emitMessage, open, peerConnections, sockets, timers } = harness;
    const track = createCameraTrack("camera-track-after-disconnect");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("camera", track);
    await tick();
    timers.fireByDelay(100);
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("9", { mid: "2", simulcastEncodings: [] }));

    client.disconnect();
    await tick();

    await connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await open(1);
    await emitMessage(buildWelcomeFrame(), 1);
    await emitMessage(buildNegotiationFrame("offer", "restart-offer", "1"), 1);

    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .some((section) => section.senderTrack === track),
        false
    );
    assert.equal(sentPublishCount(sockets[1]), 0);

    client.publish("camera", track);
    await tick();
    timers.fireByDelay(100);
    await tick();

    assert.equal(sentPublishCount(sockets[1]), 1);

    await emitMessage(buildVideoRenegotiationFrame("10", { mid: "2", simulcastEncodings: [] }), 1);

    const transceiver = peerConnections
        .at(-1)
        .transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, track);
});

test("pre-connect publish does not survive a fresh connect", async () => {
    const harness = createRecoveryHarness();
    const { client, connect, emitMessage, open, peerConnections, sockets } = harness;
    const track = createCameraTrack("camera-track-before-connect");

    client.publish("camera", track);
    await tick();

    await connect("ws://example.test/ws", "jwt-token", { channelUUID: "channel-a" });
    await open();
    await emitMessage(buildWelcomeFrame());
    await emitMessage(buildNegotiationFrame("offer", "server-initial", "1"));

    assert.equal(
        peerConnections
            .at(-1)
            .answerSnapshots.at(-1)
            .some((section) => section.senderTrack === track),
        false
    );
    assert.equal(sentPublishCount(sockets[0]), 0);
});

test("canceling pending camera publish does not detach an attached screen sender", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections } = harness;
    const screenTrack = createScreenTrack("screen-track");
    const cameraTrack = createCameraTrack("camera-track");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("screen", screenTrack);
    await tick();
    await emitMessage(buildVideoRenegotiationFrame("9", { mid: "2", simulcastEncodings: [] }));

    client.publish("camera", cameraTrack);
    client.publish("camera", null);
    await tick();

    const transceiver = peerConnections[0].transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, screenTrack);
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

    client.publish("camera", createCameraTrack("camera-track"));
    await tick();

    await emitMessage("offer");

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
    const info = { isCameraOn: true, isRaisingHand: true };

    client.updateInfo(info, { needRefresh: true });
    info.isCameraOn = false;
    await tick();

    assert.deepEqual(core.updateInfoCalls, [
        {
            isCameraOn: true,
            isRaisingHand: true
        }
    ]);
});

test("broadcast snapshots nested payloads at call time", async () => {
    const { client, core } = createSfuClientHarness();
    const message = { metadata: { label: "before" } };

    client.broadcast(message);
    message.metadata.label = "after";
    await tick();

    assert.deepEqual(core.broadcasts, [{ metadata: { label: "before" } }]);
});

test("broadcast clone failures use the runtime error boundary", async () => {
    const { client, handledErrors } = createSfuClientHarness();

    client.broadcast(() => undefined);
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.equal(handledErrors[0].name, "DataCloneError");
});

test("same-turn disconnect preserves fatal cleanup effects", async () => {
    const harness = createRecoveryHarness();
    const { client, handledErrors } = harness;
    const stateChanges = [];
    client.addEventListener("stateChange", (event) => stateChanges.push(event.detail.state));

    await connectRealWithWelcome(harness);
    client.broadcast(() => undefined);
    client.disconnect();
    await tick();

    assert.equal(handledErrors.length, 1);
    assert.equal(stateChanges.at(-1), "disconnected");
});

test("fatal cleanup runs before a reconnect requested by teardown callbacks", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, sockets } = harness;

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));
    client.addEventListener("log", (event) => {
        if (event.detail.message === "closed RTCPeerConnection") {
            client.connect("ws://other.example.test/ws", "jwt-token", {
                channelUUID: "channel-b"
            });
        }
    });

    client.broadcast(() => undefined);
    await tick();

    assert.equal(sockets.length, 2);
    assert.equal(sockets[1].closeCode, null);
});

test("repeated fatal inputs preserve installed cleanup effects", async () => {
    const harness = createRecoveryHarness();
    const { client, handledErrors } = harness;
    const stateChanges = [];
    client.addEventListener("stateChange", (event) => stateChanges.push(event.detail.state));

    await connectRealWithWelcome(harness);
    client.broadcast(() => undefined);
    client.broadcast(() => undefined);
    await tick();

    assert.equal(handledErrors.length, 2);
    assert.equal(stateChanges.at(-1), "disconnected");
});

test("fatal teardown clears the active peer before logging", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, handledErrors } = harness;
    let closeLogs = 0;

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));
    client.addEventListener("log", (event) => {
        if (event.detail.message !== "closed RTCPeerConnection") {
            return;
        }
        closeLogs += 1;
        if (closeLogs === 1) {
            client.broadcast(() => undefined);
        }
    });

    client.broadcast(() => undefined);
    await tick();

    assert.equal(closeLogs, 1);
    assert.equal(handledErrors.length, 2);
});

test("fatal runtime errors reset the public client surface", async () => {
    const {
        client,
        core,
        emitMessage,
        handledErrors,
        open,
        peerConnections,
        sockets,
        updates,
        connect
    } = createSfuClientHarness();
    const stateChanges = [];
    const sourcesAtError = [];
    client.addEventListener("stateChange", (event) => {
        stateChanges.push(event.detail);
    });
    client.addEventListener("handledError", () => {
        sourcesAtError.push(client.sourceDescriptors);
    });

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
    const sourceUpdates = updates.filter((update) => update.name === CLIENT_UPDATE.SOURCE);
    assert.deepEqual(sourceUpdates.at(-1), {
        name: CLIENT_UPDATE.SOURCE,
        payload: {
            sources: []
        }
    });
    assert.equal(stateChanges.at(-1).state, "disconnected");
    assert.equal(client.errors.length, 1);
    assert.equal(client.errors[0] instanceof Error, true);
    assert.equal(handledErrors[0], client.errors[0]);
    assert.deepEqual(sourcesAtError, [[]]);
    assert.equal(sockets[0].closeCode, 4000);
    assert.equal(sockets[0].readyState, 3);
    assert.deepEqual(core.wsCloseCodes, []);
});

test(
    "disconnect cancels a stalled negotiation and ignores late failures",
    { timeout: 2_000 },
    async () => {
        const { promise: remoteDescription, reject: rejectRemoteDescription } =
            Promise.withResolvers();
        const harness = createRecoveryHarness({
            createPeerConnection(config) {
                const peerConnection = new FakePeerConnection(config);
                peerConnection.setRemoteDescription = () => remoteDescription;
                return peerConnection;
            }
        });
        const { client, emitMessage, handledErrors, peerConnections, sockets } = harness;

        await connectRealWithWelcome(harness);
        await emitMessage(buildNegotiationFrame("offer", "7", "1"));

        client.disconnect();
        await tick();

        assert.equal(client.state, "disconnected");
        assert.equal(peerConnections[0].closed, true);
        assert.equal(sockets[0].closeCode, WS_CLOSE_CODE.CLEAN);
        assert.equal(handledErrors.length, 0);

        rejectRemoteDescription(new Error("late negotiation failure"));
        await tick();

        assert.equal(handledErrors.length, 0);
    }
);

test("fatal abort ignores late negotiation failures", async () => {
    const { promise: remoteDescription, reject: rejectRemoteDescription } = Promise.withResolvers();
    const { client, connectWithWelcome, emitMessage, handledErrors } = createSfuClientHarness({
        createPeerConnection(config) {
            const peerConnection = new FakePeerConnection(config);
            peerConnection.setRemoteDescription = () => remoteDescription;
            return peerConnection;
        }
    });

    await connectWithWelcome();
    await emitMessage("offer");
    client.broadcast(() => undefined);
    rejectRemoteDescription(new Error("late negotiation failure"));
    await tick();

    assert.equal(handledErrors.length, 1);
});

test("fatal runtime errors keep the original error when protocol disconnect fails", async () => {
    const core = new FakeProtocolCore();
    const disconnect = core.disconnect.bind(core);
    core.disconnect = () => {
        disconnect();
        throw new Error("disconnect failure");
    };
    const { client, connect, emitMessage, handledErrors, open, sockets, updates } =
        createSfuClientHarness({
            protocolCore: core
        });

    await connect();
    await open();
    await emitMessage("source-descriptors");

    assert.notDeepEqual(client.sourceDescriptors, []);

    await emitMessage("explode");

    assert.equal(client.errors.length, 1);
    assert.equal(client.errors[0].message, "boom");
    assert.equal(handledErrors[0], client.errors[0]);
    assert.equal(core.disconnectCalls, 1);
    assert.deepEqual(client.sourceDescriptors, []);
    assert.equal(
        updates.some(
            (update) => update.name === CLIENT_UPDATE.SOURCE && update.payload.sources.length === 0
        ),
        false
    );
    assert.equal(sockets[0].closeCode, 4000);
    assert.equal(sockets[0].readyState, 3);
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
