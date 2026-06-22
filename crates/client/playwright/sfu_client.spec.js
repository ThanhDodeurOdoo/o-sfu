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
const SOURCE = {
    active: true,
    encodings: [{ encodingId: "encoding-1", maxBitrate: 150000, rid: "lo" }],
    mid: "0",
    sessionId: 42,
    sourceId: "source-1",
    type: "camera"
};
const SOURCE_FRAME = JSON.stringify([
    {
        t: "sources",
        p: [SOURCE]
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
                this.emitClose(code);
            }

            emitClose(code) {
                if (this.readyState >= 2) {
                    return;
                }
                this.readyState = 3;
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
                this.iceGatheringState = "new";
                this.localDescription = null;
                this.onicecandidate = null;
                this.onicegatheringstatechange = null;
                this.ontrack = null;
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
                this.localDescription = description;
                this.iceGatheringState = "complete";
            }

            async setRemoteDescription() {}
        }

        globalThis.__browserHarness = {
            client: null,
            events: [],
            logs: [],
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
        client.addEventListener("log", (event) => {
            globalThis.__browserHarness.logs.push(structuredClone(event.detail));
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
                    p: [
                        {
                            active: true,
                            mid: "0",
                            sessionId: 42,
                            type: "camera"
                        }
                    ]
                },
                {
                    t: "sources",
                    p: [
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
                    ]
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
    await expect
        .poll(async () =>
            page.evaluate(() => ({
                hasClientLog: globalThis.__browserHarness.logs.some(
                    (log) =>
                        log.id === "sfu_client" &&
                        log.level === "info" &&
                        log.message === "connect requested for room channel-a"
                ),
                hasRuntimeLog: globalThis.__browserHarness.logs.some(
                    (log) =>
                        log.id === "browser_runtime" &&
                        log.level === "info" &&
                        log.message.includes("opening websocket connection")
                )
            }))
        )
        .toEqual({
            hasClientLog: true,
            hasRuntimeLog: true
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
                name: "source",
                payload: {
                    sources: [
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
                    ]
                }
            },
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

test("odoo bundle embeds wasm and drives the browser runtime", async ({ page }) => {
    try {
        await page.evaluate(async () => {
            const harness = globalThis.__browserHarness;
            harness.fetchCalls = [];
            harness.originalFetch = globalThis.fetch;
            globalThis.fetch = (...args) => {
                harness.fetchCalls.push(args.map((arg) => String(arg)));
                return Promise.reject(new Error("unexpected Odoo bundle fetch"));
            };
            const { CLIENT_UPDATE, SFU_CLIENT_STATE, SfuClient } =
                await import("/dist/odoo_sfu.js");
            if (CLIENT_UPDATE.TRACK !== "track") {
                throw new Error("unexpected client update export");
            }
            if (CLIENT_UPDATE.SOURCE !== "source") {
                throw new Error("unexpected source update export");
            }
            if (SFU_CLIENT_STATE.CONNECTED !== "connected") {
                throw new Error("unexpected state export");
            }
            const client = new SfuClient();
            harness.client = client;
            client.addEventListener("stateChange", (event) => {
                harness.stateChanges.push(structuredClone(event.detail));
            });
            client.addEventListener("update", (event) => {
                harness.events.push(structuredClone(event.detail));
            });
            client.connect("https://example.test/ws", "jwt-token", {
                channelUUID: "channel-a"
            });
        });

        await expect
            .poll(async () => page.evaluate(() => globalThis.__browserHarness.state.sockets.length))
            .toBe(1);
        await page.evaluate((frame) => {
            const socket = globalThis.__browserHarness.state.sockets[0];
            socket.open();
            socket.emitMessage(frame);
            socket.emitMessage(JSON.stringify([{ t: "offer", q: "7", p: { sdp: "offer-sdp" } }]));
        }, WELCOME_FRAME);
        await page.evaluate((frame) => {
            globalThis.__browserHarness.state.sockets[0].emitMessage(frame);
        }, SOURCE_FRAME);

        await expect
            .poll(async () =>
                page.evaluate(() => ({
                    events: globalThis.__browserHarness.events,
                    fetchCalls: globalThis.__browserHarness.fetchCalls,
                    peerConnections: globalThis.__browserHarness.state.peerConnections.length,
                    sent: globalThis.__browserHarness.state.sockets[0].sent.map((payload) =>
                        JSON.parse(payload)
                    ),
                    sourceDescriptors: globalThis.__browserHarness.client.sourceDescriptors,
                    states: globalThis.__browserHarness.stateChanges
                }))
            )
            .toEqual({
                events: [
                    {
                        name: "source",
                        payload: {
                            sources: [SOURCE]
                        }
                    }
                ],
                fetchCalls: [],
                peerConnections: 1,
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
                ],
                sourceDescriptors: [SOURCE]
            });
    } finally {
        await page.evaluate(() => {
            if (globalThis.__browserHarness.originalFetch) {
                globalThis.fetch = globalThis.__browserHarness.originalFetch;
                delete globalThis.__browserHarness.originalFetch;
            }
        });
    }
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
        globalThis.__browserHarness.state.sockets[0].emitClose(1011);
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
                        jwt: "jwt-token"
                    }
                }
            ],
            [
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

    await page.evaluate(() => {
        globalThis.__browserHarness.state.sockets[1].emitMessage(
            JSON.stringify([
                {
                    t: "offer",
                    q: "recovery-offer",
                    p: {
                        sdp: "recovered-offer-sdp",
                        uploadSlots: [{ mid: "0", kind: "video", codecs: ["vp8"] }]
                    }
                }
            ])
        );
    });

    await expect
        .poll(async () =>
            page.evaluate(() =>
                globalThis.__browserHarness.state.sockets[1].sent.map((frame) => JSON.parse(frame))
            )
        )
        .toEqual(
            expect.arrayContaining([
                [{ t: "offer", r: "recovery-offer", p: { sdp: "browser-answer-sdp" } }],
                [
                    {
                        t: "publish",
                        p: {
                            type: "camera"
                        }
                    }
                ]
            ])
        );
});
