import assert from "node:assert/strict";
import test from "node:test";

import { CLIENT_UPDATE, SfuClient } from "../dist/index.js";

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
        this.submittedAnswers = [];
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
        this.state = "disconnected";
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
                    }
                ];
            case "recording-ok":
                return [{ kind: "resolvePendingRequest", ok: true, requestId: "record-1" }];
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

    trackBinding(mid) {
        return this.trackBindings.get(mid) ?? null;
    }

    updateDownload() {
        return [];
    }

    updateInfo() {
        return [];
    }

    updateUpload(type, active) {
        this.lastUploadUpdate = { active, type };
        return [];
    }
}

const tick = async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
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

test("updateUpload rejects stream-kind mismatches", () => {
    const client = new SfuClient({
        createProtocolCore: () => new FakeProtocolCore()
    });

    assert.throws(() => {
        client.updateUpload("camera", {
            id: "audio-track",
            kind: "audio"
        });
    }, /camera uploads require a video track/);
});
