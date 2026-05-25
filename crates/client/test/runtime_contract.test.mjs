import assert from "node:assert/strict";
import test from "node:test";

import { CLIENT_UPDATE } from "../dist/index.js";
import {
    NEGOTIATION_KIND,
    PENDING_REQUEST_KIND,
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

function assertThrowsMessage(callback, expectedMessage) {
    assert.throws(callback, (error) => {
        assert.equal(error?.message, expectedMessage);
        return true;
    });
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
            return [
                {
                    kind: "registerPendingRequest",
                    requestId: "request-1",
                    requestKind: PENDING_REQUEST_KIND.START_RECORDING
                }
            ];
        },
        stopRecording() {
            return [];
        },
        submitNegotiationAnswer() {
            return [
                {
                    kind: "applyNegotiation",
                    requestId: "request-2",
                    negotiationKind: NEGOTIATION_KIND.OFFER,
                    sdp: "v=0\r\n",
                    uploadSlots: []
                }
            ];
        },
        disconnect() {
            return [];
        },
        trackBinding() {
            return {
                active: true,
                mid: "0",
                sessionId: 7,
                type: "camera"
            };
        },
        ...overrides
    };
}

function assertWrappedCoreThrows(overrides, read, expectedMessage) {
    const core = wrapProtocolCoreBindings(validCore(overrides));
    assertThrowsMessage(() => read(core), expectedMessage);
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

test("wrapped protocol core rejects malformed host commands", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [{ kind: "emitStateChange", state: "broken" }];
            }
        })
    );

    assertThrowsMessage(
        () => core.connect("ws://example.test", "jwt", null),
        "protocol core connect() command #0.state is invalid: broken"
    );
});

test("wrapped protocol core rejects missing required unknown fields", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [
                    {
                        kind: "emitUpdate",
                        update: {
                            name: CLIENT_UPDATE.BROADCAST,
                            payload: { senderId: 7 }
                        }
                    }
                ];
            }
        })
    );

    assertThrowsMessage(
        () => core.connect("ws://example.test", "jwt", null),
        "protocol core connect() command #0.update.payload.message is required"
    );
});

test("wrapped protocol core requires initial negotiation after peer connection creation", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            onWsMessage() {
                return [
                    {
                        kind: "applyNegotiation",
                        requestId: "offer-1",
                        negotiationKind: NEGOTIATION_KIND.OFFER,
                        sdp: "v=0\r\n",
                        uploadSlots: []
                    }
                ];
            }
        })
    );

    assertThrowsMessage(
        () => core.onWsMessage("offer"),
        "protocol core onWsMessage() command #0 initial negotiation must immediately follow createPeerConnection"
    );
});

test("wrapped protocol core rejects peer connection recreation during renegotiation", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            onWsMessage() {
                return [
                    { kind: "createPeerConnection" },
                    {
                        kind: "applyNegotiation",
                        requestId: "renegotiate-1",
                        negotiationKind: NEGOTIATION_KIND.RENEGOTIATE,
                        sdp: "v=0\r\n",
                        uploadSlots: []
                    }
                ];
            }
        })
    );

    assertThrowsMessage(
        () => core.onWsMessage("renegotiate"),
        "protocol core onWsMessage() command #1 renegotiation must not recreate the peer connection"
    );
});

test("wrapped protocol core validates close and recovery ordering", () => {
    const closeOrderCore = wrapProtocolCoreBindings(
        validCore({
            disconnect() {
                return [{ kind: "closePeerConnection" }, { kind: "closeWebSocket", code: 1000 }];
            }
        })
    );

    assertThrowsMessage(
        () => closeOrderCore.disconnect(),
        "protocol core disconnect() must close the websocket before the peer connection when both are in one batch"
    );

    const recoveryOrderCore = wrapProtocolCoreBindings(
        validCore({
            onWsClose() {
                return [
                    { kind: "scheduleTimer", id: 1, ms: 1000 },
                    { kind: "closePeerConnection" }
                ];
            }
        })
    );

    assertThrowsMessage(
        () => recoveryOrderCore.onWsClose(1011),
        "protocol core onWsClose() must close the peer connection before scheduling recovery"
    );
});

for (const code of [999, 1002, 2999, 5000]) {
    test(`wrapped protocol core rejects browser-invalid close code ${String(code)}`, () => {
        const core = wrapProtocolCoreBindings(
            validCore({
                disconnect() {
                    return [{ kind: "closeWebSocket", code }];
                }
            })
        );

        assertThrowsMessage(
            () => core.disconnect(),
            "protocol core disconnect() command #0.code must be 1000 or an integer from 3000 through 4999"
        );
    });
}

test("wrapped protocol core rejects malformed track bindings", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            trackBinding() {
                return {
                    active: "yes",
                    mid: "0",
                    sessionId: 7,
                    type: "camera"
                };
            }
        })
    );

    assertThrowsMessage(
        () => core.trackBinding("0"),
        "protocol core trackBinding().active must be a boolean"
    );
});

test("wrapped protocol core validates replaceTrackBindings host commands", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [
                    {
                        bindings: [{ active: "yes", mid: "0", sessionId: 7, type: "camera" }],
                        kind: "replaceTrackBindings"
                    }
                ];
            }
        })
    );

    assertThrowsMessage(
        () => core.connect("ws://example.test", "jwt", null),
        "protocol core connect() command #0.bindings[0].active must be a boolean"
    );
});

test("wrapped protocol core validates source descriptors", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [
                    {
                        kind: "replaceSourceDescriptors",
                        sources: [validSourceDescriptor({ maxBitrate: -1 })]
                    }
                ];
            }
        })
    );

    assertThrowsMessage(
        () => core.connect("ws://example.test", "jwt", null),
        "protocol core connect() command #0.sources[0].encodings[0].maxBitrate must be a non-negative integer when provided"
    );
});

test("wrapped protocol core accepts valid temporal layer ids", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [
                    {
                        kind: "replaceSourceDescriptors",
                        sources: [validSourceDescriptor({ maxTemporalLayerId: 7 })]
                    }
                ];
            }
        })
    );

    assert.doesNotThrow(() => core.connect("ws://example.test", "jwt", null));
});

for (const maxTemporalLayerId of [-1, 8, 1.5, "2", Number.NaN]) {
    test(`wrapped protocol core rejects invalid temporal layer id ${String(maxTemporalLayerId)}`, () => {
        const core = wrapProtocolCoreBindings(
            validCore({
                connect() {
                    return [
                        {
                            kind: "replaceSourceDescriptors",
                            sources: [validSourceDescriptor({ maxTemporalLayerId })]
                        }
                    ];
                }
            })
        );

        assertThrowsMessage(
            () => core.connect("ws://example.test", "jwt", null),
            "protocol core connect() command #0.sources[0].encodings[0].maxTemporalLayerId must be an integer from 0 through 7 when provided"
        );
    });
}

test("wrapped protocol core rejects NaN and infinite numeric session IDs", () => {
    const nanSessionIdCore = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [{ kind: "removeSessionTracks", sessionId: Number.NaN }];
            }
        })
    );

    assertThrowsMessage(
        () => nanSessionIdCore.connect("ws://example.test", "jwt", null),
        "protocol core connect() command #0.sessionId number session ID must be finite"
    );

    const infiniteSessionIdCore = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [{ kind: "removeSessionTracks", sessionId: Number.POSITIVE_INFINITY }];
            }
        })
    );

    assertThrowsMessage(
        () => infiniteSessionIdCore.connect("ws://example.test", "jwt", null),
        "protocol core connect() command #0.sessionId number session ID must be finite"
    );
});

test("wrapped protocol core validates boolean fields", () => {
    for (const field of REQUIRED_FEATURE_FIELDS) {
        assertWrappedCoreThrows(
            { features: { ...VALID_FEATURES, [field]: "yes" } },
            (core) => core.features,
            `protocol core features.${field} must be a boolean`
        );
    }

    for (const field of OPTIONAL_RECORDING_FIELDS) {
        assertWrappedCoreThrows(
            { recordingState: { [field]: "yes" } },
            (core) => core.recordingState,
            `protocol core recordingState.${field} must be a boolean when provided`
        );
    }

    for (const field of OPTIONAL_SESSION_INFO_FIELDS) {
        assertWrappedCoreThrows(
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
            (core) => core.connect("ws://example.test", "jwt", null),
            `protocol core connect() command #0.update.payload.7.${field} must be a boolean when provided`
        );
    }
});

test("wrapped protocol core rejects prototype-sensitive info keys", () => {
    const payload = JSON.parse('{"__proto__":{"isTalking":true}}');

    assertWrappedCoreThrows(
        {
            connect: () => [
                {
                    kind: "emitUpdate",
                    update: {
                        name: CLIENT_UPDATE.INFO_CHANGE,
                        payload
                    }
                }
            ]
        },
        (core) => core.connect("ws://example.test", "jwt", null),
        "protocol core connect() command #0.update.payload.__proto__ is not a supported record key"
    );
});
