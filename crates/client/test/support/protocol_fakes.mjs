import assert from "node:assert/strict";

import { CLIENT_UPDATE } from "../../dist/index.js";
import {
    audioMedia,
    audioUploadSlot,
    sdp,
    videoMedia,
    videoUploadSlot
} from "./negotiation_fixtures.mjs";

export const EMPTY_FEATURES = {
    rtc: false,
    transcription: false,
    audioRecording: false,
    videoRecording: false
};

const initialOfferCommand = (requestId) => ({
    kind: "applyNegotiation",
    negotiationKind: "offer",
    requestId,
    sdp: sdp(audioMedia("0"), videoMedia("1")),
    uploadSlots: [audioUploadSlot("0"), videoUploadSlot("1")]
});

const sourceUpdate = (sources) => ({
    kind: "emitUpdate",
    update: {
        name: CLIENT_UPDATE.SOURCE,
        payload: { sources }
    }
});

export class FakeProtocolCore {
    constructor() {
        this.features = { ...EMPTY_FEATURES };
        this.recordingState = {};
        this.state = "disconnected";
        this.disconnectCalls = 0;
        this.pendingNegotiationKind = null;
        this.subscriptionUpdates = [];
        this.submittedAnswers = [];
        this.publicationUpdates = [];
        this.sourceDescriptors = new Map();
        this.trackBindings = new Map();
        this.transportReadyCalls = 0;
        this.transportFailureState = null;
        this.updateInfoCalls = [];
        this.wsCloseCodes = [];
    }

    broadcast() {
        return [];
    }

    connect(url) {
        this.state = "connecting";
        return [{ kind: "connect", url }];
    }

    disconnect() {
        this.disconnectCalls += 1;
        this.state = "disconnected";
        this.features = { ...EMPTY_FEATURES };
        this.recordingState = {};
        this.sourceDescriptors.clear();
        this.trackBindings.clear();
        return [sourceUpdate([]), { kind: "emitStateChange", state: "disconnected" }];
    }

    onTimer() {
        return [];
    }

    onTransportReady() {
        if (this.pendingNegotiationKind === "offer") {
            return [];
        }
        this.transportReadyCalls += 1;
        this.state = "connected";
        return [{ kind: "emitStateChange", state: "connected" }];
    }

    onWsClose(code) {
        this.wsCloseCodes.push(code);
        if (this.transportFailureState) {
            this.state = this.transportFailureState;
            return [{ kind: "emitStateChange", state: this.transportFailureState }];
        }
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
                return this._withPendingNegotiationKind([
                    { kind: "createPeerConnection" },
                    initialOfferCommand("7"),
                    ...this._replaceTrackBindings()
                ]);
            case "offer-with-attach-camera":
                return this._withPendingNegotiationKind([
                    { kind: "createPeerConnection" },
                    initialOfferCommand("8"),
                    {
                        kind: "attachTrack",
                        mid: "1",
                        streamType: "camera"
                    },
                    ...this._replaceTrackBindings()
                ]);
            case "info-change-map":
                return [
                    {
                        kind: "emitUpdate",
                        update: {
                            name: CLIENT_UPDATE.INFO_CHANGE,
                            payload: new Map([["31", { isRaisingHand: true }]])
                        }
                    }
                ];
            case "info-change-map-proto":
                return [
                    {
                        kind: "emitUpdate",
                        update: {
                            name: CLIENT_UPDATE.INFO_CHANGE,
                            payload: new Map([["__proto__", { isRaisingHand: true }]])
                        }
                    }
                ];
            case "source-descriptors":
                this.sourceDescriptors.set("source-1", {
                    active: true,
                    encodings: [
                        { encodingId: "encoding-1", maxBitrate: 150000, rid: "lo" },
                        { encodingId: "encoding-2", maxBitrate: 900000, rid: "hi" }
                    ],
                    mid: "0",
                    sessionId: 42,
                    sourceId: "source-1",
                    type: "camera"
                });
                return [sourceUpdate([...this.sourceDescriptors.values()])];
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
        return beginRecordingRequest("startRecording");
    }

    stopRecording() {
        return beginRecordingRequest("stopRecording");
    }

    submitNegotiationAnswer(requestId, negotiationKind, sdp) {
        this.submittedAnswers.push({ negotiationKind, requestId, sdp });
        this.pendingNegotiationKind = null;
        return [];
    }

    subscribe(sessionId, states) {
        this.subscriptionUpdates.push({ sessionId, states });
        return [];
    }

    updateInfo(info) {
        this.updateInfoCalls.push(info);
        return [];
    }

    publish(type, active) {
        this.publicationUpdates.push({ active, type });
        return [
            {
                active,
                kind: "setLocalUploadIntent",
                streamType: type
            }
        ];
    }

    _withPendingNegotiationKind(commands) {
        this.pendingNegotiationKind =
            commands.find((command) => command.kind === "applyNegotiation")?.negotiationKind ??
            null;
        return commands;
    }

    _replaceTrackBindings() {
        return [
            {
                bindings: [...this.trackBindings.values()],
                kind: "replaceTrackBindings"
            }
        ];
    }
}

const beginRecordingRequest = (requestKind) => [
    {
        kind: "beginPendingRequest",
        requestId: "record-1",
        requestKind,
        timeoutMs: 5000,
        timeoutTimerId: 10000
    }
];

export const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

export const buildWelcomeFrame = (peers = []) =>
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

export const decodeSentFrame = (socket, index) => JSON.parse(socket.sent[index]);

export const createManualTimers = () => {
    let nextHandleId = 1;
    const allHandles = [];
    const handles = new Map();
    return {
        clearTimer(handle) {
            handles.delete(handle.id);
        },
        fireLastByDelay(ms) {
            const handle = allHandles.findLast((candidate) => candidate.ms === ms);
            assert.ok(handle, `expected timer with delay ${ms}`);
            handle.callback();
        },
        fireByDelay(ms) {
            const handle = [...handles.values()].find((candidate) => candidate.ms === ms);
            assert.ok(handle, `expected timer with delay ${ms}`);
            handles.delete(handle.id);
            handle.callback();
        },
        hasDelay(ms) {
            return [...handles.values()].some((candidate) => candidate.ms === ms);
        },
        setTimer(callback, ms) {
            const handle = {
                callback,
                id: nextHandleId++,
                ms
            };
            handles.set(handle.id, handle);
            allHandles.push(handle);
            return handle;
        }
    };
};
