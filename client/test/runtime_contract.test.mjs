import assert from "node:assert/strict";
import test from "node:test";

import {
    NEGOTIATION_KIND,
    PENDING_REQUEST_KIND,
    configureProtocolCoreFactory,
    createProtocolCore,
    wrapProtocolCoreBindings
} from "../dist/runtime_contract.js";

function validCore(overrides = {}) {
    return {
        state: "disconnected",
        features: {
            rtc: true,
            transcription: false,
            audioRecording: false,
            videoRecording: false
        },
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
        updateUpload() {
            return [];
        },
        updateDownload() {
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
                    sdp: "v=0\r\n"
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

test("wrapped protocol core rejects malformed host commands", () => {
    const core = wrapProtocolCoreBindings(
        validCore({
            connect() {
                return [{ kind: "emitStateChange", state: "broken" }];
            }
        })
    );

    assert.throws(() => core.connect("ws://example.test", "jwt", null), {
        message: /protocol core connect\(\) command #0\.state is invalid/
    });
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

    assert.throws(() => core.trackBinding("0"), {
        message: /protocol core trackBinding\(\)\.active must be a boolean/
    });
});

test("createProtocolCore validates factory output at runtime", () => {
    configureProtocolCoreFactory(() =>
        validCore({
            get features() {
                return {
                    rtc: true
                };
            }
        })
    );

    try {
        const core = createProtocolCore();
        assert.throws(() => core.features, {
            message: /protocol core features\.transcription must be a boolean/
        });
    } finally {
        configureProtocolCoreFactory(() => validCore());
    }
});
