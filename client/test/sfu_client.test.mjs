import assert from "node:assert/strict";
import test from "node:test";

import { CLIENT_UPDATE, SfuClient, createProtocolCore } from "../dist/index.js";
import {
    FakeMediaTrack,
    FakePeerConnection,
    FakeSender,
    FakeWebSocket
} from "./support/browser_fakes.mjs";
import {
    EMPTY_FEATURES,
    FakeProtocolCore,
    buildWelcomeFrame,
    createManualTimers,
    decodeSentFrame,
    tick
} from "./support/protocol_fakes.mjs";

test("connect normalizes the URL and sends auth on WebSocket open", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const client = new SfuClient({
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("https://example.test/ws", "jwt-token", {
        channelUUID: "channel-a",
        iceServers: [{ urls: "stun:stun.example.test" }]
    });
    await tick();

    assert.equal(core.connectCall.url, "wss://example.test/ws");
    assert.equal(sockets[0].url, "wss://example.test/ws");

    sockets[0].open();
    await tick();

    assert.deepEqual(sockets[0].sent, ["auth-frame"]);
});

test("startRecording resolves through the protocol request lifecycle", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const client = new SfuClient({
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    const resultPromise = client.startRecording({ audio: true });
    await tick();
    sockets[0].emitMessage("recording-ok");

    assert.equal(await resultPromise, true);
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

test("real protocol core replays sticky intents after recovery welcome", async () => {
    const sockets = [];
    const timers = createManualTimers();
    const client = new SfuClient({
        clearTimer: (handle) => timers.clearTimer(handle),
        createProtocolCore: () => createProtocolCore(),
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        },
        setTimer: (callback, ms) => timers.setTimer(callback, ms)
    });

    const cameraTrack = {
        enabled: true,
        id: "camera-track-1",
        kind: "video",
        muted: false
    };

    client.connect("ws://example.test/ws", "jwt-token", {
        channelUUID: "channel-a"
    });
    await tick();

    sockets[0].open();
    await tick();
    sockets[0].emitMessage(buildWelcomeFrame());
    await tick();

    client.publish("camera", cameraTrack);
    client.subscribe(7, { audio: true, camera: false });
    client.updateInfo({ isCameraOn: true, isRaisingHand: true });
    await tick();

    assert.equal(sockets[0].sent.length, 1);

    sockets[0].close(1011);
    await tick();
    timers.fireByDelay(1000);
    await tick();

    assert.equal(sockets.length, 2);
    sockets[1].open();
    await tick();
    sockets[1].emitMessage(buildWelcomeFrame());
    await tick();

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
            t: "publish",
            p: {
                type: "camera"
            }
        },
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
});

test("real protocol core replays the latest sticky intents changed while recovering", async () => {
    const sockets = [];
    const timers = createManualTimers();
    const client = new SfuClient({
        clearTimer: (handle) => timers.clearTimer(handle),
        createProtocolCore: () => createProtocolCore(),
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        },
        setTimer: (callback, ms) => timers.setTimer(callback, ms)
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();

    sockets[0].open();
    await tick();
    sockets[0].emitMessage(buildWelcomeFrame());
    await tick();

    client.publish("camera", {
        enabled: true,
        id: "camera-track-2",
        kind: "video",
        muted: false
    });
    client.subscribe(7, { audio: true });
    await tick();

    sockets[0].close(1011);
    await tick();

    client.publish("camera", null);
    client.subscribe(7, { audio: false, camera: true });
    client.updateInfo({ isSelfMuted: true });
    await tick();

    timers.fireByDelay(1000);
    await tick();

    assert.equal(sockets.length, 2);
    sockets[1].open();
    await tick();
    sockets[1].emitMessage(buildWelcomeFrame());
    await tick();

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

test("negotiation creates a peer connection and emits lowercase track updates", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const receivedUpdates = [];
    client.addEventListener("update", (event) => {
        receivedUpdates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token", {
        iceServers: [{ urls: ["stun:one.example.test", "stun:two.example.test"] }]
    });
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera"
    });
    sockets[0].emitMessage("offer");
    await tick();

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

    const track = {
        enabled: true,
        id: "track-1",
        kind: "video",
        muted: false
    };
    peerConnections[0].emitTrack(track, "0");

    assert.deepEqual(receivedUpdates, [
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
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, {
                answerSdp: "answer-sdp",
                gatheredAnswerSdp:
                    "answer-sdp\r\na=candidate:1 1 udp 2113937151 127.0.0.1 54400 typ host"
            });
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

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
    const core = new FakeProtocolCore();
    const sockets = [];
    const client = new SfuClient({
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const receivedUpdates = [];
    client.addEventListener("update", (event) => {
        receivedUpdates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("info-change-map");
    await tick();

    assert.deepEqual(receivedUpdates, [
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

test("source descriptor updates are exposed as additive client state", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const client = new SfuClient({
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const receivedUpdates = [];
    client.addEventListener("update", (event) => {
        receivedUpdates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("source-descriptors");
    await tick();

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
    assert.deepEqual(receivedUpdates, [
        {
            name: CLIENT_UPDATE.SOURCE,
            payload: {
                sources: expectedSources
            }
        }
    ]);
    assert.deepEqual(client.sourceDescriptors, expectedSources);
});

test("renegotiation attaches pending audio only to upload-eligible mids", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    const localAudioTrack = new FakeMediaTrack({
        id: "local-audio",
        kind: "audio"
    });
    client.publish("audio", localAudioTrack);
    await tick();

    sockets[0].emitMessage("renegotiate-with-pending-audio");
    await tick();

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
    assert.deepEqual(core.submittedAnswers.at(-1), {
        negotiationKind: "renegotiate",
        requestId: "10",
        sdp: "answer-sdp"
    });
});

test("track metadata updates re-emit track state for existing remote tracks", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const receivedUpdates = [];
    client.addEventListener("update", (event) => {
        receivedUpdates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera"
    });
    sockets[0].emitMessage("offer");
    await tick();

    const track = {
        enabled: true,
        id: "track-1",
        kind: "video",
        muted: false
    };
    peerConnections[0].emitTrack(track, "0");
    await tick();

    sockets[0].emitMessage("track-inactive");
    await tick();

    assert.deepEqual(receivedUpdates, [
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
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const receivedUpdates = [];
    client.addEventListener("update", (event) => {
        receivedUpdates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera"
    });
    sockets[0].emitMessage("offer");
    await tick();

    const track = {
        enabled: true,
        id: "track-1",
        kind: "video",
        muted: false
    };
    peerConnections[0].emitTrack(track, "0");
    await tick();

    client.subscribe(42, { camera: false });
    await tick();
    await tick();
    client.subscribe(42, { camera: true });
    await tick();
    await tick();

    assert.deepEqual(receivedUpdates, [
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
    const core = new FakeProtocolCore();
    const client = new SfuClient({
        createProtocolCore: () => core,
        createWebSocket: () => new FakeWebSocket("ws://example.test/ws")
    });

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

test("subscribe preferences apply to future remote track bindings", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const receivedUpdates = [];
    client.addEventListener("update", (event) => {
        receivedUpdates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    client.subscribe(42, { camera: false });
    await tick();
    await tick();

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera"
    });
    sockets[0].emitMessage("offer");
    await tick();

    const track = {
        enabled: true,
        id: "track-1",
        kind: "video",
        muted: false
    };
    peerConnections[0].emitTrack(track, "0");
    await tick();

    assert.deepEqual(receivedUpdates, [
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
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, { autoConnect: false });
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    assert.equal(client.state, "authenticated");

    sockets[0].emitMessage("offer");
    await tick();

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
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, {
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
            });
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    sockets[0].emitMessage("offer");
    await tick();

    assert.equal(peerConnections.length, 1);
    assert.equal(core.transportReadyCalls, 1);
    assert.equal(client.state, "connected");
});

test("peer connection failed closes the websocket and enters recovery", async () => {
    const core = new FakeProtocolCore();
    core.transportFailureState = "recovering";
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].open();
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

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
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].open();
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    peerConnections[0].emitConnectionState("disconnected");
    await tick();

    assert.equal(sockets[0].readyState, 1);
    assert.equal(sockets[0].closeCode, null);
    assert.deepEqual(core.wsCloseCodes, []);
    assert.equal(client.state, "connected");
});

test("track rebinding waits for a fresh track event before re-emitting state", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const receivedUpdates = [];
    client.addEventListener("update", (event) => {
        receivedUpdates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera"
    });
    sockets[0].emitMessage("offer");
    await tick();

    const firstTrack = {
        enabled: true,
        id: "track-1",
        kind: "video",
        muted: false
    };
    peerConnections[0].emitTrack(firstTrack, "0");
    await tick();

    sockets[0].emitMessage("track-rebind");
    await tick();

    assert.deepEqual(receivedUpdates, [
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

    const reboundTrack = {
        enabled: true,
        id: "track-2",
        kind: "video",
        muted: false
    };
    peerConnections[0].emitTrack(reboundTrack, "0");
    await tick();

    assert.deepEqual(receivedUpdates, [
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
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const receivedUpdates = [];
    client.addEventListener("update", (event) => {
        receivedUpdates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera"
    });
    sockets[0].emitMessage("offer");
    await tick();

    const track = {
        enabled: true,
        id: "track-1",
        kind: "video",
        muted: false
    };
    peerConnections[0].emitTrack(track, "0");
    await tick();

    sockets[0].emitMessage("peer-left");
    await tick();

    assert.equal(client._consumers.has(42), false);
    assert.deepEqual(receivedUpdates.at(-1), {
        name: CLIENT_UPDATE.DISCONNECT,
        payload: {
            sessionId: 42
        }
    });
});

test("peer connection teardown clears stale remote consumer state", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera"
    });
    sockets[0].emitMessage("offer");
    await tick();

    peerConnections[0].emitTrack(
        {
            enabled: true,
            id: "track-1",
            kind: "video",
            muted: false
        },
        "0"
    );
    await tick();

    assert.equal(client._consumers.get(42).camera.track.id, "track-1");

    sockets[0].emitMessage("close-peer-connection");
    await tick();

    assert.equal(peerConnections[0].closed, true);
    assert.equal(client._consumers.size, 0);
});

test("remote track lifecycle updates re-emit when the browser unmutes the track", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const updates = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.addEventListener("update", (event) => {
        updates.push(event.detail);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    core.trackBindings.set("0", {
        active: true,
        mid: "0",
        sessionId: 42,
        type: "camera"
    });
    sockets[0].emitMessage("offer");
    await tick();

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
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const firstTrack = {
        enabled: true,
        id: "camera-track-1",
        kind: "video",
        muted: false
    };
    const secondTrack = {
        enabled: true,
        id: "camera-track-2",
        kind: "video",
        muted: false
    };

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    client.publish("camera", firstTrack);
    await tick();

    sockets[0].emitMessage("offer-with-attach-camera");
    await tick();

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
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const track = {
        enabled: true,
        id: "camera-track-1",
        kind: "video",
        muted: false
    };

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    client.publish("camera", track);
    await tick();
    sockets[0].emitMessage("offer-with-attach-camera");
    await tick();

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
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const track = {
        enabled: true,
        id: "camera-track-1",
        kind: "video",
        muted: false
    };

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    client.publish("camera", track);
    await tick();
    sockets[0].emitMessage("renegotiate-with-unbound-camera");
    await tick();

    assert.equal(peerConnections[0].transceivers[2].sender.track, track);
    assert.equal(peerConnections[0].transceivers[2].direction, "sendonly");
    assert.equal(
        peerConnections[0].answerSnapshots.at(-1)[2].senderTrack,
        track,
        "the browser must bind the track before generating the renegotiation answer"
    );
    assert.deepEqual(core.submittedAnswers.at(-1), {
        negotiationKind: "renegotiate",
        requestId: "9",
        sdp: "answer-sdp"
    });
});

test("renegotiation configures RID simulcast before answering supported video publishes", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const logs = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });
    client.addEventListener("log", (event) => {
        logs.push(event.detail);
    });

    const track = {
        enabled: true,
        id: "camera-track-simulcast",
        kind: "video",
        muted: false
    };

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    client.publish("camera", track);
    await tick();
    sockets[0].emitMessage("renegotiate-with-pending-simulcast-camera");
    await tick();

    const transceiver = peerConnections[0].transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, track);
    assert.deepEqual(transceiver.sender.setParametersCalls, [
        {
            encodings: [
                {
                    active: true,
                    maxBitrate: 150000,
                    rid: "lo",
                    scaleResolutionDownBy: 2
                },
                {
                    active: true,
                    maxBitrate: 900000,
                    rid: "hi",
                    scaleResolutionDownBy: 1
                }
            ]
        }
    ]);
    assert.deepEqual(peerConnections[0].answerSnapshots.at(-1)[2].senderParameters, {
        encodings: [
            {
                active: true,
                maxBitrate: 150000,
                rid: "lo",
                scaleResolutionDownBy: 2
            },
            {
                active: true,
                maxBitrate: 900000,
                rid: "hi",
                scaleResolutionDownBy: 1
            }
        ]
    });
    assert.deepEqual(core.submittedAnswers.at(-1), {
        negotiationKind: "renegotiate",
        requestId: "12",
        sdp: "answer-sdp"
    });
    assert.ok(
        logs.some(
            (entry) =>
                entry.id === "browser_runtime" &&
                entry.level === "info" &&
                entry.message === "enabled RID simulcast for camera on mid 2"
        )
    );
});

test("renegotiation falls back to single encoding when the codec path is unsupported", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const track = {
        enabled: true,
        id: "camera-track-single",
        kind: "video",
        muted: false
    };

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    client.publish("camera", track);
    await tick();
    sockets[0].emitMessage("renegotiate-with-pending-h264-simulcast-camera");
    await tick();

    const transceiver = peerConnections[0].transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.equal(transceiver.sender.track, track);
    assert.deepEqual(transceiver.sender.setParametersCalls, []);
    assert.deepEqual(peerConnections[0].answerSnapshots.at(-1)[2].senderParameters, {
        encodings: []
    });
    assert.deepEqual(core.submittedAnswers.at(-1), {
        negotiationKind: "renegotiate",
        requestId: "13",
        sdp: "answer-sdp"
    });
});

test("renegotiation falls back to single encoding when the server ladder is invalid", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    client.publish("camera", {
        enabled: true,
        id: "camera-track-invalid-profile",
        kind: "video",
        muted: false
    });
    await tick();
    sockets[0].emitMessage("renegotiate-with-invalid-simulcast-camera");
    await tick();

    const transceiver = peerConnections[0].transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.deepEqual(transceiver.sender.setParametersCalls, []);
    assert.deepEqual(peerConnections[0].answerSnapshots.at(-1)[2].senderParameters, {
        encodings: []
    });
});

test("renegotiation falls back to single encoding when sender parameters are rejected", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, {
                senderOptionsByMid: {
                    2: { rejectSetParameters: true }
                }
            });
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    client.publish("camera", {
        enabled: true,
        id: "camera-track-rejected-profile",
        kind: "video",
        muted: false
    });
    await tick();
    sockets[0].emitMessage("renegotiate-with-pending-simulcast-camera");
    await tick();

    const transceiver = peerConnections[0].transceivers.find((candidate) => candidate.mid === "2");
    assert.ok(transceiver);
    assert.deepEqual(transceiver.sender.setParametersCalls, []);
    assert.deepEqual(peerConnections[0].answerSnapshots.at(-1)[2].senderParameters, {
        encodings: []
    });
});

test("initial offer binds a pending local track before answering", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, { autoConnect: false });
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const track = {
        enabled: true,
        id: "camera-track-pending-offer",
        kind: "video",
        muted: false
    };

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    client.publish("camera", track);
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    assert.equal(peerConnections[0].transceivers[1].sender.track, track);
    assert.equal(
        peerConnections[0].answerSnapshots.at(-1)[1].senderTrack,
        track,
        "the browser must bind the pending upload before generating the initial answer"
    );
});

test("offer waits for ice gathering completion before submitting the final answer", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, {
                autoConnect: false,
                gatheredAnswerSdp: "gathered-answer-sdp",
                preCompleteAnswerSdp: "candidate-answer-sdp"
            });
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    sockets[0].emitMessage("offer");
    await tick();

    assert.deepEqual(core.submittedAnswers, [
        {
            negotiationKind: "offer",
            requestId: "7",
            sdp: "gathered-answer-sdp"
        }
    ]);
});

test("renegotiation binds pending camera and screen tracks to distinct offer-ordered mids", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    const cameraTrack = {
        enabled: true,
        id: "camera-track-distinct-mid",
        kind: "video",
        muted: false
    };
    const screenTrack = {
        enabled: true,
        id: "screen-track-distinct-mid",
        kind: "video",
        muted: false
    };

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();
    sockets[0].emitMessage("offer");
    await tick();

    client.publish("camera", cameraTrack);
    await tick();
    client.publish("screen", screenTrack);
    await tick();
    sockets[0].emitMessage("renegotiate-with-pending-camera-and-screen");
    await tick();

    assert.equal(peerConnections[0].transceivers[2].sender.track, cameraTrack);
    assert.equal(peerConnections[0].transceivers[3].sender.track, screenTrack);
    assert.equal(peerConnections[0].answerSnapshots.at(-1)[2].senderTrack, cameraTrack);
    assert.equal(peerConnections[0].answerSnapshots.at(-1)[3].senderTrack, screenTrack);
});

test("getStats exposes compatibility-shaped transport and producer stats", async () => {
    const core = new FakeProtocolCore();
    const sockets = [];
    const peerConnections = [];
    const peerConnectionStats = new Map([["transport", { type: "transport" }]]);
    const cameraProducerStats = new Map([["outbound-rtp", { type: "outbound-rtp" }]]);
    const client = new SfuClient({
        createPeerConnection: (config) => {
            const peerConnection = new FakePeerConnection(config, {
                peerConnectionStats
            });
            peerConnection.transceivers[1].sender = new FakeSender(cameraProducerStats);
            peerConnections.push(peerConnection);
            return peerConnection;
        },
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    client.updateUpload("camera", {
        enabled: true,
        id: "camera-track-compat",
        kind: "video",
        muted: false
    });
    await tick();

    sockets[0].emitMessage("offer-with-attach-camera");
    await tick();

    const stats = await client.getStats();

    assert.equal(peerConnections.length, 1);
    assert.equal(stats.uploadStats, peerConnectionStats);
    assert.equal(stats.downloadStats, peerConnectionStats);
    assert.equal(stats.camera, cameraProducerStats);
    assert.equal(stats.audio, undefined);
    assert.equal(stats.screen, undefined);
});

test("updateInfo keeps the legacy needRefresh option as a compatibility no-op", async () => {
    const core = new FakeProtocolCore();
    const client = new SfuClient({
        createProtocolCore: () => core
    });

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
    const core = new FakeProtocolCore();
    const sockets = [];
    const handledErrors = [];
    const client = new SfuClient({
        createProtocolCore: () => core,
        createWebSocket: (url) => {
            const socket = new FakeWebSocket(url);
            sockets.push(socket);
            return socket;
        }
    });
    client.addEventListener("handledError", (event) => {
        handledErrors.push(event.detail.error);
    });

    client.connect("ws://example.test/ws", "jwt-token");
    await tick();
    sockets[0].open();
    await tick();
    sockets[0].emitMessage("welcome");
    await tick();

    client._consumers.set(42, {
        audio: null,
        camera: {
            track: { id: "track-1", kind: "video" }
        },
        screen: null
    });

    sockets[0].emitMessage("explode");
    await tick();

    assert.equal(core.disconnectCalls, 1);
    assert.equal(client.state, "disconnected");
    assert.deepEqual(client.availableFeatures, EMPTY_FEATURES);
    assert.deepEqual(client.recordingState, {});
    assert.equal(client._consumers.size, 0);
    assert.equal(client.errors.length, 1);
    assert.match(client.errors[0].message, /boom/);
    assert.equal(handledErrors[0], client.errors[0]);
    assert.equal(sockets[0].closeCode, 4000);
    assert.equal(sockets[0].readyState, 3);
});

test("publish rejects stream-kind mismatches", () => {
    const client = new SfuClient({
        createProtocolCore: () => new FakeProtocolCore()
    });

    assert.throws(() => {
        client.publish("camera", {
            id: "audio-track",
            kind: "audio"
        });
    }, /camera uploads require a video track/);
});

test("deprecated updateUpload and updateDownload delegate to publish and subscribe", async () => {
    const core = new FakeProtocolCore();
    const client = new SfuClient({
        createProtocolCore: () => core
    });

    client.updateUpload("camera", {
        enabled: true,
        id: "camera-track-compat",
        kind: "video",
        muted: false
    });
    client.updateDownload(7, { audio: true });
    await tick();

    assert.deepEqual(core.publicationUpdates, [{ active: true, type: "camera" }]);
    assert.equal(core.subscriptionUpdates.length, 1);
});

test("deprecated updateUpload treats undefined like the legacy no-track sentinel", async () => {
    const core = new FakeProtocolCore();
    const client = new SfuClient({
        createProtocolCore: () => core
    });

    client.updateUpload("camera", undefined);
    await tick();

    assert.deepEqual(core.publicationUpdates, []);
});
