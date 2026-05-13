import assert from "node:assert/strict";

import { CLIENT_UPDATE } from "../../dist/index.js";
import {
    audioMedia,
    audioUploadSlot,
    negotiationCommand,
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

const initialOfferCommand = (requestId) =>
    negotiationCommand({
        negotiationKind: "offer",
        requestId,
        sdp: sdp(audioMedia("0"), videoMedia("1")),
        uploadSlots: [audioUploadSlot("0"), videoUploadSlot("1")]
    });

const videoRenegotiationCommand = ({
    codecs,
    mid,
    payloadType = 96,
    requestId,
    rtpmap = null,
    simulcastEncodings
}) =>
    negotiationCommand({
        negotiationKind: "renegotiate",
        requestId,
        sdp: sdp(videoMedia(mid, { payloadType, rtpmap })),
        uploadSlots: [videoUploadSlot(mid, { codecs, simulcastEncodings })]
    });

export class FakeProtocolCore {
    constructor() {
        this.features = { ...EMPTY_FEATURES };
        this.recordingState = {};
        this.state = "disconnected";
        this.disconnectCalls = 0;
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
        this.sourceDescriptors.clear();
        this.trackBindings.clear();
        return [{ kind: "emitStateChange", state: "disconnected" }];
    }

    onTimer() {
        return [];
    }

    onTransportReady() {
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
                return [
                    { kind: "createPeerConnection" },
                    initialOfferCommand("7"),
                    ...this._replaceTrackBindings()
                ];
            case "offer-with-attach-camera":
                return [
                    { kind: "createPeerConnection" },
                    initialOfferCommand("8"),
                    {
                        kind: "attachTrack",
                        mid: "1",
                        streamType: "camera"
                    },
                    ...this._replaceTrackBindings()
                ];
            case "renegotiate-with-unbound-camera":
                return [
                    videoRenegotiationCommand({
                        mid: "2",
                        requestId: "9",
                        simulcastEncodings: []
                    })
                ];
            case "renegotiate-with-pending-camera-and-screen":
                return [
                    negotiationCommand({
                        negotiationKind: "renegotiate",
                        requestId: "11",
                        sdp: sdp(videoMedia("2"), videoMedia("3")),
                        uploadSlots: [
                            videoUploadSlot("2", { simulcastEncodings: [] }),
                            videoUploadSlot("3", { simulcastEncodings: [] })
                        ]
                    })
                ];
            case "renegotiate-with-pending-simulcast-camera":
                return [
                    videoRenegotiationCommand({
                        mid: "2",
                        requestId: "12",
                        rtpmap: "VP8/90000"
                    })
                ];
            case "renegotiate-with-pending-h264-simulcast-camera":
                return [
                    videoRenegotiationCommand({
                        codecs: ["H264"],
                        mid: "2",
                        payloadType: 102,
                        requestId: "13",
                        rtpmap: "H264/90000"
                    })
                ];
            case "renegotiate-with-invalid-simulcast-camera":
                return [
                    videoRenegotiationCommand({
                        mid: "2",
                        requestId: "14",
                        rtpmap: "VP8/90000",
                        simulcastEncodings: [
                            {
                                maxBitrate: 150000,
                                rid: "lo",
                                resolutionScale: 0
                            },
                            {
                                maxBitrate: 900000,
                                rid: "hi",
                                resolutionScale: 1
                            }
                        ]
                    })
                ];
            case "renegotiate-with-pending-audio":
                return [
                    negotiationCommand({
                        negotiationKind: "renegotiate",
                        requestId: "10",
                        sdp: sdp(
                            audioMedia("consumer-audio", "sendonly"),
                            audioMedia("producer-audio")
                        ),
                        uploadSlots: [audioUploadSlot("producer-audio")]
                    })
                ];
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
                return [
                    {
                        kind: "replaceSourceDescriptors",
                        sources: [...this.sourceDescriptors.values()]
                    }
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

    trackBinding(mid) {
        return this.trackBindings.get(mid) ?? null;
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
        this.lastPublicationUpdate = { active, type };
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
}

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
