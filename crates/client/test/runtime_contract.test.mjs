import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { CLIENT_UPDATE } from "../dist/index.js";
import { NEGOTIATION_KIND, PENDING_REQUEST_KIND } from "../dist/protocol_contract.js";
import {
    configureDefaultWasmProtocolCoreProvider,
    createProtocolCore,
    wrapProtocolCoreBindings
} from "../dist/runtime_contract.js";

const VALID_FEATURES = {
    rtc: true,
    transcription: false,
    audioRecording: false,
    videoRecording: false
};
const REQUIRED_FEATURE_FIELDS = ["rtc", "transcription", "audioRecording", "videoRecording"];
const OPTIONAL_RECORDING_FIELDS = ["recording", "audio", "video", "transcription"];
const OPTIONAL_SESSION_INFO_FIELDS = [
    "isTalking",
    "isFeatured",
    "isCameraOn",
    "isScreenSharingOn",
    "isSelfMuted",
    "isDeaf",
    "isRaisingHand"
];

function assertThrowsError(callback) {
    assert.throws(callback, Error);
}

function validCore(overrides = {}) {
    return {
        state: "disconnected",
        features: { ...VALID_FEATURES },
        recordingState: {
            recording: false
        },
        connect() {
            return [{ kind: "connect", url: "ws://example.test/" }];
        },
        onWsOpen() {
            return [{ kind: "sendWebSocket", frame: "auth" }];
        },
        onWsMessage() {
            return [];
        },
        onTransportReady() {
            return [];
        },
        onWsClose() {
            return [];
        },
        onTimer() {
            return [];
        },
        publish() {
            return [];
        },
        subscribe() {
            return [];
        },
        updateInfo() {
            return [];
        },
        broadcast() {
            return [];
        },
        startRecording() {
            return [];
        },
        stopRecording() {
            return [];
        },
        submitNegotiationAnswer() {
            return [];
        },
        disconnect() {
            return [];
        },
        ...overrides
    };
}

function assertInjectedCoreThrows(overrides, read) {
    const core = wrapProtocolCoreBindings(validCore(overrides));
    assertThrowsError(() => read(core));
}

function pendingRequest(overrides = {}) {
    return {
        requestId: "request-1",
        kind: PENDING_REQUEST_KIND.START_RECORDING,
        timeoutMs: 5000,
        timeoutTimerId: 10_000,
        ...overrides
    };
}

function beginPendingRequest(overrides = {}) {
    return {
        kind: "beginPendingRequest",
        request: pendingRequest(overrides)
    };
}

function negotiationCommand(negotiationKind) {
    return {
        kind: "applyNegotiation",
        requestId: negotiationKind,
        negotiationKind,
        sdp: "v=0\r\n",
        uploadSlots: []
    };
}

function sparseHostCommandBatch() {
    const commands = [];
    commands.length = 1;
    return commands;
}

function validSourceDescriptor(encodingOverrides = {}) {
    return {
        active: true,
        encodings: [{ encodingId: "encoding-1", ...encodingOverrides }],
        sessionId: 7,
        sourceId: "source-1",
        type: "camera"
    };
}

function sourceUpdate(sources) {
    return {
        kind: "emitUpdate",
        update: {
            name: CLIENT_UPDATE.SOURCE,
            payload: { sources }
        }
    };
}

const remoteMediaUpdate = (bindings) => ({
    kind: "emitUpdate",
    update: { name: "remote_media", payload: { bindings } }
});

test("source tree does not own generated protocol manifest", () => {
    const sourceManifestPath = fileURLToPath(
        new URL("../src/generated/protocol_manifest.json", import.meta.url)
    );

    assert.equal(existsSync(sourceManifestPath), false);
});

test("default WASM protocol core validates host command shape only", () => {
    const misorderedHostCommands = [negotiationCommand(NEGOTIATION_KIND.OFFER)];
    configureDefaultWasmProtocolCoreProvider(() =>
        validCore({
            connect: () => [{ kind: "emitStateChange", state: "broken" }],
            onTimer: () => sparseHostCommandBatch(),
            onWsMessage: () => misorderedHostCommands
        })
    );
    const core = createProtocolCore();

    assert.deepEqual(core.onWsMessage("offer"), misorderedHostCommands);
    assertThrowsError(() => core.connect("ws://example.test", "jwt", null));
    assertThrowsError(() => core.onTimer(1));
});

test("injected protocol core rejects malformed host commands", () => {
    const commandsWithHostileMap = [{ kind: "sendWebSocket", frame: "auth" }];
    commandsWithHostileMap.map = () => [{ kind: "closeWebSocket", code: "bad" }];
    const core = wrapProtocolCoreBindings(
        validCore({
            connect: () => [{ kind: "emitStateChange", state: "broken" }],
            onTimer: () => sparseHostCommandBatch(),
            onWsOpen: () => commandsWithHostileMap
        })
    );

    assertThrowsError(() => core.connect("ws://example.test", "jwt", null));
    assertThrowsError(() => core.onTimer(1));
    assert.deepEqual(core.onWsOpen(), [{ kind: "sendWebSocket", frame: "auth" }]);
});

test("injected protocol core rejects obsolete direct media host commands", () => {
    for (const command of [
        { kind: "attachTrack", mid: "0", streamType: "camera" },
        { kind: "detachTrack", streamType: "camera" },
        { kind: "replaceTrackBindings", bindings: [] },
        { kind: "removeSessionTracks", sessionId: 7 }
    ]) {
        assertInjectedCoreThrows(
            {
                connect: () => [command]
            },
            (core) => core.connect("ws://example.test", "jwt", null)
        );
    }
});

test("injected protocol core validates host command ordering", () => {
    for (const [method, commands, args = []] of [
        ["onWsMessage", [negotiationCommand(NEGOTIATION_KIND.OFFER)], ["offer"]],
        [
            "onWsMessage",
            [{ kind: "createPeerConnection" }, negotiationCommand(NEGOTIATION_KIND.RENEGOTIATE)],
            ["renegotiate"]
        ],
        ["disconnect", [{ kind: "closePeerConnection" }, { kind: "closeWebSocket", code: 1000 }]],
        [
            "onWsClose",
            [{ kind: "scheduleTimer", id: 1, ms: 1000 }, { kind: "closePeerConnection" }],
            [1011]
        ]
    ]) {
        assertInjectedCoreThrows(
            {
                [method]: () => commands
            },
            (core) => core[method](...args)
        );
    }
});

test("injected protocol core validates pending request commands", () => {
    for (const [method, commands, args = []] of [
        ["startRecording", [beginPendingRequest({ timeoutTimerId: 1 })]],
        ["startRecording", [beginPendingRequest({ kind: "unknown" })]],
        ["startRecording", [{ kind: "resolvePendingRequest", requestId: "missing", ok: false }]],
        ["connect", [beginPendingRequest()], ["ws://example.test", "jwt", null]],
        [
            "startRecording",
            [{ kind: "sendWebSocket", frame: "before-request" }, beginPendingRequest()]
        ],
        ["startRecording", [beginPendingRequest(), beginPendingRequest({ requestId: "request-2" })]]
    ]) {
        assertInjectedCoreThrows(
            {
                [method]: () => commands
            },
            (core) => core[method](...args)
        );
    }
});

test("injected protocol core validates remote media host updates", () => {
    assertInjectedCoreThrows(
        {
            connect: () => [
                remoteMediaUpdate([{ active: "yes", mid: "0", sessionId: 7, type: "camera" }])
            ]
        },
        (core) => core.connect("ws://example.test", "jwt", null)
    );
});

test("injected protocol core validates source descriptors", () => {
    const sparseSources = [];
    sparseSources.length = 1;
    for (const sources of [[validSourceDescriptor({ maxBitrate: -1 })], sparseSources]) {
        assertInjectedCoreThrows(
            {
                connect: () => [sourceUpdate(sources)]
            },
            (core) => core.connect("ws://example.test", "jwt", null)
        );
    }
});

test("injected protocol core accepts valid temporal layer ids", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            connect: () => [sourceUpdate([validSourceDescriptor({ maxTemporalLayerId: 7 })])]
        })
    );

    assert.doesNotThrow(() => core.connect("ws://example.test", "jwt", null));
});

for (const maxTemporalLayerId of [-1, 8, 1.5, "2", Number.NaN]) {
    test(`injected protocol core rejects invalid temporal layer id ${String(maxTemporalLayerId)}`, () => {
        const core = wrapProtocolCoreBindings(
            validCore({
                connect: () => [sourceUpdate([validSourceDescriptor({ maxTemporalLayerId })])]
            })
        );

        assertThrowsError(() => core.connect("ws://example.test", "jwt", null));
    });
}

test("injected protocol core rejects NaN and infinite numeric session IDs", () => {
    for (const sessionId of [Number.NaN, Number.POSITIVE_INFINITY]) {
        assertInjectedCoreThrows(
            {
                connect: () => [
                    remoteMediaUpdate([{ active: true, mid: "0", sessionId, type: "camera" }])
                ]
            },
            (core) => core.connect("ws://example.test", "jwt", null)
        );
    }
});

test("injected protocol core validates boolean fields", () => {
    for (const field of REQUIRED_FEATURE_FIELDS) {
        assertInjectedCoreThrows(
            { features: { ...VALID_FEATURES, [field]: "yes" } },
            (core) => core.features
        );
    }

    for (const field of OPTIONAL_RECORDING_FIELDS) {
        assertInjectedCoreThrows(
            { recordingState: { [field]: "yes" } },
            (core) => core.recordingState
        );
    }

    for (const field of OPTIONAL_SESSION_INFO_FIELDS) {
        assertInjectedCoreThrows(
            {
                connect: () => [
                    {
                        kind: "emitUpdate",
                        update: {
                            name: CLIENT_UPDATE.INFO_CHANGE,
                            payload: { 7: { [field]: "yes" } }
                        }
                    }
                ]
            },
            (core) => core.connect("ws://example.test", "jwt", null)
        );
    }
});
