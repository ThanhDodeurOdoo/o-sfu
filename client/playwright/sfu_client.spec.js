import { expect, test } from "@playwright/test";

const WELCOME_FRAME = JSON.stringify([
    {
        t: "welcome",
        p: {
            features: {
                rtc: true,
                transcription: false,
                audioRecording: true,
                videoRecording: false
            },
            recording: {
                recording: false,
                audio: false,
                transcription: false,
                video: false
            },
            peers: []
        }
    }
]);

test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
        const state = {
            peerConnections: [],
            sockets: []
        };

        class FakeWebSocket {
            constructor(url) {
                this.closeEvents = [];
                this.onclose = null;
                this.onerror = null;
                this.onmessage = null;
                this.onopen = null;
                this.readyState = 0;
                this.sent = [];
                this.url = url;
                state.sockets.push(this);
            }

            close(code = 1000) {
                if (this.readyState >= 2) {
                    return;
                }
                this.readyState = 3;
                this.closeEvents.push(code);
                this.onclose?.({ code });
            }

            emitMessage(data) {
                this.onmessage?.({ data });
            }

            open() {
                this.readyState = 1;
                this.onopen?.(new Event("open"));
            }

            send(data) {
                this.sent.push(data);
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
                this.closed = false;
                this.config = config;
                this.localDescriptions = [];
                this.ontrack = null;
                this.remoteDescriptions = [];
                this.transceivers = [
                    { mid: "0", sender: new FakeSender() },
                    { mid: "1", sender: new FakeSender() }
                ];
                state.peerConnections.push(this);
            }

            close() {
                this.closed = true;
            }

            async createAnswer() {
                return { sdp: "browser-answer-sdp", type: "answer" };
            }

            emitTrack(track, mid) {
                this.ontrack?.({
                    track,
                    transceiver: { mid }
                });
            }

            getTransceivers() {
                return this.transceivers;
            }

            async setLocalDescription(description) {
                this.localDescriptions.push(description);
            }

            async setRemoteDescription(description) {
                this.remoteDescriptions.push(description);
            }
        }

        globalThis.__browserHarness = {
            client: null,
            events: [],
            stateChanges: [],
            state
        };
        globalThis.RTCPeerConnection = FakePeerConnection;
        globalThis.WebSocket = FakeWebSocket;
    });
    await page.goto("/playwright/fixtures/harness.html");
});

test("default browser runtime negotiates and emits remote track updates", async ({ page }) => {
    await page.evaluate(async () => {
        const { SfuClient } = await import("/dist/index.js");
        const client = new SfuClient();
        globalThis.__browserHarness.client = client;
        client.addEventListener("stateChange", (event) => {
            globalThis.__browserHarness.stateChanges.push(structuredClone(event.detail));
        });
        client.addEventListener("update", (event) => {
            globalThis.__browserHarness.events.push(structuredClone(event.detail));
        });
        client.connect("https://example.test/ws", "jwt-token", {
            channelUUID: "channel-a",
            iceServers: [{ urls: ["stun:one.example.test", "stun:two.example.test"] }]
        });
    });

    await expect
        .poll(async () => page.evaluate(() => globalThis.__browserHarness.state.sockets.length))
        .toBe(1);
    await page.evaluate(() => globalThis.__browserHarness.state.sockets[0].open());
    await expect
        .poll(async () =>
            page.evaluate(() => JSON.parse(globalThis.__browserHarness.state.sockets[0].sent[0]))
        )
        .toEqual([
            {
                t: "auth",
                p: {
                    channel: "channel-a",
                    jwt: "jwt-token"
                }
            }
        ]);

    await page.evaluate((frame) => {
        globalThis.__browserHarness.state.sockets[0].emitMessage(frame);
    }, WELCOME_FRAME);
    await page.evaluate(() => {
        globalThis.__browserHarness.state.sockets[0].emitMessage(
            JSON.stringify([
                {
                    t: "tracks",
                    p: [{ active: true, mid: "0", sessionId: 42, type: "camera" }]
                }
            ])
        );
        globalThis.__browserHarness.state.sockets[0].emitMessage(
            JSON.stringify([{ t: "offer", q: "7", p: { sdp: "offer-sdp" } }])
        );
    });

    await expect
        .poll(async () =>
            page.evaluate(() => ({
                config: globalThis.__browserHarness.state.peerConnections[0]?.config ?? null,
                sent: globalThis.__browserHarness.state.sockets[0].sent.map((frame) =>
                    JSON.parse(frame)
                ),
                states: globalThis.__browserHarness.stateChanges
            }))
        )
        .toEqual({
            config: {
                iceServers: [{ urls: ["stun:one.example.test", "stun:two.example.test"] }]
            },
            sent: [
                [
                    {
                        t: "auth",
                        p: {
                            channel: "channel-a",
                            jwt: "jwt-token"
                        }
                    }
                ],
                [{ t: "offer", r: "7", p: { sdp: "browser-answer-sdp" } }]
            ],
            states: [
                { cause: undefined, state: "connecting" },
                { cause: undefined, state: "authenticated" },
                { cause: undefined, state: "connected" }
            ]
        });

    await page.evaluate(() => {
        globalThis.__browserHarness.state.peerConnections[0].emitTrack(
            {
                enabled: true,
                id: "remote-track-1",
                kind: "video",
                muted: false
            },
            "0"
        );
    });

    await expect
        .poll(async () => page.evaluate(() => globalThis.__browserHarness.events))
        .toEqual([
            {
                name: "track",
                payload: {
                    active: true,
                    sessionId: 42,
                    track: {
                        enabled: true,
                        id: "remote-track-1",
                        kind: "video",
                        muted: false
                    },
                    type: "camera"
                }
            }
        ]);
});

test("default browser runtime reconnects and replays sticky intents", async ({ page }) => {
    await page.evaluate(async () => {
        const { SfuClient } = await import("/dist/index.js");
        const client = new SfuClient();
        globalThis.__browserHarness.client = client;
        client.connect("ws://example.test/ws", "jwt-token");
    });

    await expect
        .poll(async () => page.evaluate(() => globalThis.__browserHarness.state.sockets.length))
        .toBe(1);
    await page.evaluate((frame) => {
        const socket = globalThis.__browserHarness.state.sockets[0];
        socket.open();
        socket.emitMessage(frame);
    }, WELCOME_FRAME);
    await page.evaluate(() => {
        globalThis.__browserHarness.client.publish("camera", {
            enabled: true,
            id: "camera-track-1",
            kind: "video",
            muted: false
        });
        globalThis.__browserHarness.client.subscribe(7, {
            audio: true,
            camera: false
        });
        globalThis.__browserHarness.client.updateInfo({
            isCameraOn: true,
            isRaisingHand: true
        });
        globalThis.__browserHarness.state.sockets[0].close(1011);
    });

    await expect
        .poll(async () => page.evaluate(() => globalThis.__browserHarness.state.sockets.length))
        .toBe(2);
    await page.evaluate((frame) => {
        const socket = globalThis.__browserHarness.state.sockets[1];
        socket.open();
        socket.emitMessage(frame);
    }, WELCOME_FRAME);

    await expect
        .poll(async () =>
            page.evaluate(() =>
                globalThis.__browserHarness.state.sockets[1].sent.map((frame) => JSON.parse(frame))
            )
        )
        .toEqual([
            [
                {
                    t: "auth",
                    p: {
                        channel: undefined,
                        jwt: "jwt-token"
                    }
                }
            ],
            [
                {
                    t: "publish",
                    p: {
                        type: "camera"
                    }
                },
                {
                    t: "subscribe",
                    p: {
                        audio: true,
                        camera: false,
                        sessionId: 7
                    }
                },
                {
                    t: "info",
                    p: {
                        isCameraOn: true,
                        isRaisingHand: true
                    }
                }
            ]
        ]);
});
