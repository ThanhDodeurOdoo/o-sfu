import assert from "node:assert/strict";
import test from "node:test";

import { CLIENT_UPDATE, SfuClient, createProtocolCore } from "../dist/index.js";

const EMPTY_FEATURES = {
    rtc: false,
    transcription: false,
    audioRecording: false,
    videoRecording: false
};

class FakeWebSocket {
    constructor(url) {
        this.url = url;
        this.readyState = 0;
        this.sent = [];
        this.onclose = null;
        this.onerror = null;
        this.onmessage = null;
        this.onopen = null;
    }

    open() {
        this.readyState = 1;
        this.onopen?.(new Event("open"));
    }

    send(data) {
        this.sent.push(data);
    }

    emitMessage(data) {
        this.onmessage?.({ data });
    }

    close(code = 1000) {
        if (this.readyState >= 2) {
            return;
        }
        this.readyState = 3;
        this.onclose?.({ code });
    }
}

class FakeSender {
    constructor() {
        this.track = null;
    }

    async replaceTrack(track) {
        this.track = track;
    }
}

class FakePeerConnection {
    constructor(config) {
        this.config = config;
        this.localDescriptions = [];
        this.ontrack = null;
        this.remoteDescriptions = [];
        this.transceivers = [
            { mid: "0", sender: new FakeSender() },
            { mid: "1", sender: new FakeSender() }
        ];
    }

    async createAnswer() {
        return { sdp: "answer-sdp", type: "answer" };
    }

    async setLocalDescription(description) {
        this.localDescriptions.push(description);
    }

    async setRemoteDescription(description) {
        this.remoteDescriptions.push(description);
    }

    getTransceivers() {
        return this.transceivers;
    }

    close() {
        this.closed = true;
    }

    emitTrack(track, mid) {
        this.ontrack?.({
            track,
            transceiver: { mid }
        });
    }
}

class FakeProtocolCore {
    constructor() {
        this.features = { ...EMPTY_FEATURES };
        this.recordingState = {};
        this.state = "disconnected";
        this.disconnectCalls = 0;
        this.subscriptionUpdates = [];
        this.submittedAnswers = [];
        this.publicationUpdates = [];
        this.trackBindings = new Map();
    }

    broadcast() {
        return [];
    }

    connect(url, jwt, channel) {
        this.connectCall = { channel, jwt, url };
        this.state = "connecting";
        return [{ kind: "connect", url }];
    }

    disconnect() {
        this.disconnectCalls += 1;
        this.state = "disconnected";
        this.features = { ...EMPTY_FEATURES };
        this.recordingState = {};
        this.trackBindings.clear();
        return [{ kind: "emitStateChange", state: "disconnected" }];
    }

    onTimer() {
        return [];
    }

    onTransportReady() {
        this.state = "connected";
        return [{ kind: "emitStateChange", state: "connected" }];
    }

    onWsClose() {
        return [];
    }

    onWsMessage(frame) {
        switch (frame) {
            case "welcome":
                this.state = "authenticated";
                this.features = {
                    rtc: true,
                    transcription: false,
                    audioRecording: true,
                    videoRecording: false
                };
                return [{ kind: "emitStateChange", state: "authenticated" }];
            case "offer":
                return [
                    { kind: "createPeerConnection" },
                    {
                        kind: "applyNegotiation",
                        negotiationKind: "offer",
                        requestId: "7",
                        sdp: "offer-sdp"
                    },
                    ...this._replaceTrackBindings()
                ];
            case "offer-with-attach-camera":
                return [
                    { kind: "createPeerConnection" },
                    {
                        kind: "applyNegotiation",
                        negotiationKind: "offer",
                        requestId: "8",
                        sdp: "offer-sdp"
                    },
                    {
                        kind: "attachTrack",
                        mid: "1",
                        streamType: "camera"
                    },
                    ...this._replaceTrackBindings()
                ];
            case "track-inactive":
                this.trackBindings.set("0", {
                    active: false,
                    mid: "0",
                    sessionId: 42,
                    type: "camera"
                });
                return this._replaceTrackBindings();
            case "track-rebind":
                this.trackBindings.set("0", {
                    active: true,
                    mid: "0",
                    sessionId: 84,
                    type: "screen"
                });
                return this._replaceTrackBindings();
            case "peer-left":
                this.trackBindings.delete("0");
                return [
                    { kind: "removeSessionTracks", sessionId: 42 },
                    {
                        kind: "emitUpdate",
                        update: {
                            name: CLIENT_UPDATE.DISCONNECT,
                            payload: { sessionId: 42 }
                        }
                    }
                ];
            case "close-peer-connection":
                return [{ kind: "closePeerConnection" }];
            case "recording-ok":
                return [{ kind: "resolvePendingRequest", ok: true, requestId: "record-1" }];
            case "explode":
                throw new Error("boom");
            default:
                return [];
        }
    }

    onWsOpen() {
        return [{ kind: "sendWebSocket", frame: "auth-frame" }];
    }

    startRecording() {
        return [
            {
                kind: "registerPendingRequest",
                requestId: "record-1",
                requestKind: "startRecording"
            }
        ];
    }

    stopRecording() {
        return [];
    }

    submitNegotiationAnswer(requestId, negotiationKind, sdp) {
        this.submittedAnswers.push({ negotiationKind, requestId, sdp });
        return [];
    }

    _replaceTrackBindings() {
        return [
            {
                bindings: [...this.trackBindings.values()],
                kind: "replaceTrackBindings"
            }
        ];
    }

    trackBinding(mid) {
        return this.trackBindings.get(mid) ?? null;
    }

    subscribe(sessionId, states) {
        this.subscriptionUpdates.push({ sessionId, states });
        return [];
    }

    updateInfo() {
        return [];
    }

    publish(type, active) {
        this.publicationUpdates.push({ active, type });
        this.lastPublicationUpdate = { active, type };
        return [];
    }
}

const tick = async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
};

const buildWelcomeFrame = (peers = []) =>
    JSON.stringify([
        {
            t: "welcome",
            p: {
                features: {
                    rtc: true,
                    transcription: false,
                    audioRecording: false,
                    videoRecording: true
                },
                recording: {
                    recording: false,
                    audio: false,
                    transcription: false,
                    video: false
                },
                peers
            }
        }
    ]);

const decodeSentFrame = (socket, index) => JSON.parse(socket.sent[index]);

const createManualTimers = () => {
    let nextHandleId = 1;
    const handles = new Map();
    return {
        clearTimer(handle) {
            handles.delete(handle.id);
        },
        fireByDelay(ms) {
            const handle = [...handles.values()].find((candidate) => candidate.ms === ms);
            assert.ok(handle, `expected timer with delay ${ms}`);
            handles.delete(handle.id);
            handle.callback();
        },
        setTimer(callback, ms) {
            const handle = {
                callback,
                id: nextHandleId++,
                ms
            };
            handles.set(handle.id, handle);
            return handle;
        }
    };
};

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

test("fatal runtime errors reset the public client surface", async () => {
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
