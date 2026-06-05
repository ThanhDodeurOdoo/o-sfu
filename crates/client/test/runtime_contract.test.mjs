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

function assertWrappedCoreThrows(overrides, read) {
    const core = wrapProtocolCoreBindings(validCore(overrides));
    assertThrowsError(() => read(core));
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

    assertThrowsError(() => core.connect("ws://example.test", "jwt", null));
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

    assertThrowsError(() => core.onWsMessage("offer"));
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

    assertThrowsError(() => core.onWsMessage("renegotiate"));
});

test("wrapped protocol core validates close and recovery ordering", () => {
    const closeOrderCore = wrapProtocolCoreBindings(
        validCore({
            disconnect() {
                return [{ kind: "closePeerConnection" }, { kind: "closeWebSocket", code: 1000 }];
            }
        })
    );

    assertThrowsError(() => closeOrderCore.disconnect());

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

    assertThrowsError(() => recoveryOrderCore.onWsClose(1011));
});

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

    assertThrowsError(() => core.trackBinding("0"));
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

    assertThrowsError(() => core.connect("ws://example.test", "jwt", null));
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

    assertThrowsError(() => core.connect("ws://example.test", "jwt", null));
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

        assertThrowsError(() => core.connect("ws://example.test", "jwt", null));
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

    assertThrowsError(() => nanSessionIdCore.connect("ws://example.test", "jwt", null));

    const infiniteSessionIdCore = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [{ kind: "removeSessionTracks", sessionId: Number.POSITIVE_INFINITY }];
            }
        })
    );

    assertThrowsError(() => infiniteSessionIdCore.connect("ws://example.test", "jwt", null));
});

test("wrapped protocol core validates boolean fields", () => {
    for (const field of REQUIRED_FEATURE_FIELDS) {
        assertWrappedCoreThrows(
            { features: { ...VALID_FEATURES, [field]: "yes" } },
            (core) => core.features
        );
    }

    for (const field of OPTIONAL_RECORDING_FIELDS) {
        assertWrappedCoreThrows(
            { recordingState: { [field]: "yes" } },
            (core) => core.recordingState
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
            (core) => core.connect("ws://example.test", "jwt", null)
        );
    }
});
