import assert from "node:assert/strict";
import test from "node:test";

import { CLIENT_UPDATE, SFU_CLIENT_STATE } from "../dist/index.js";
import {
    COMMAND_KIND,
    NEGOTIATION_KIND,
    PENDING_REQUEST_KIND,
    WS_CLOSE_CODE
} from "../dist/protocol_contract.js";
import { createProtocolCore } from "../dist/runtime_contract.js";
import { audioMedia, sdp, videoMedia, videoUploadSlot } from "./support/negotiation_fixtures.mjs";

const EMPTY_FEATURES = {
    rtc: false,
    transcription: false,
    audioRecording: false,
    videoRecording: false
};
const AVAILABLE_FEATURES = {
    rtc: true,
    transcription: false,
    audioRecording: true,
    videoRecording: false
};
const RECORDING_STATE = {
    recording: false,
    audio: false,
    transcription: false,
    video: false
};

const stateChange = (state) => ({
    kind: COMMAND_KIND.EMIT_STATE_CHANGE,
    state,
    cause: undefined
});
const encodeFrame = (envelope) => JSON.stringify([envelope]);
const sendWebSocket = (envelope) => ({
    kind: COMMAND_KIND.SEND_WEB_SOCKET,
    frame: encodeFrame(envelope)
});

function requireCommand(commands, kind) {
    const command = commands.find((candidate) => candidate.kind === kind);
    assert.ok(command, `expected ${kind} command`);
    return command;
}

test("generated Wasm conforms to the complete host-command contract", () => {
    const seenKinds = new Set();
    const core = createProtocolCore();
    const assertCommands = (actual, expected) => {
        actual.forEach((command) => seenKinds.add(command.kind));
        assert.deepEqual(actual, expected);
        return actual;
    };

    assert.equal(core.state, SFU_CLIENT_STATE.DISCONNECTED);
    assert.deepEqual(core.features, EMPTY_FEATURES);
    assert.deepEqual(core.recordingState, {});

    assertCommands(core.connect("ws://example.test/ws", "jwt-token", "channel-a"), [
        stateChange(SFU_CLIENT_STATE.CONNECTING),
        { kind: COMMAND_KIND.CONNECT, url: "ws://example.test/ws" }
    ]);
    assertCommands(core.onWsOpen(), [
        sendWebSocket({ t: "auth", p: { channel: "channel-a", jwt: "jwt-token" } })
    ]);

    const peerInfo = { isTalking: true, isRaisingHand: false };
    const welcomeCommands = assertCommands(
        core.onWsMessage(
            encodeFrame({
                t: "welcome",
                p: {
                    features: AVAILABLE_FEATURES,
                    recording: RECORDING_STATE,
                    peers: [{ sessionId: "peer-7", info: peerInfo }]
                }
            })
        ),
        [
            stateChange(SFU_CLIENT_STATE.AUTHENTICATED),
            {
                kind: COMMAND_KIND.EMIT_UPDATE,
                update: {
                    name: CLIENT_UPDATE.INFO_CHANGE,
                    payload: { "peer-7": peerInfo }
                }
            }
        ]
    );
    assert.equal(Object.getPrototypeOf(welcomeCommands[1].update.payload), Object.prototype);
    assert.equal(core.state, SFU_CLIENT_STATE.AUTHENTICATED);
    assert.deepEqual(core.features, AVAILABLE_FEATURES);
    assert.deepEqual(core.recordingState, RECORDING_STATE);

    const offerSdp = sdp(audioMedia("0"), videoMedia("1"));
    const uploadSlot = videoUploadSlot("1");
    assertCommands(
        core.onWsMessage(
            encodeFrame({
                t: "offer",
                q: "offer-7",
                p: { sdp: offerSdp, uploadSlots: [uploadSlot] }
            })
        ),
        [
            { kind: COMMAND_KIND.CREATE_PEER_CONNECTION },
            {
                kind: COMMAND_KIND.APPLY_NEGOTIATION,
                requestId: "offer-7",
                negotiationKind: NEGOTIATION_KIND.OFFER,
                sdp: offerSdp,
                uploadSlots: [uploadSlot]
            }
        ]
    );

    const answerSdp = "v=0\r\nanswer";
    assertCommands(core.submitNegotiationAnswer("offer-7", NEGOTIATION_KIND.OFFER, answerSdp), [
        sendWebSocket({ t: "offer", p: { sdp: answerSdp }, r: "offer-7" })
    ]);
    assertCommands(core.onTransportReady(), [stateChange(SFU_CLIENT_STATE.CONNECTED)]);
    assert.equal(core.state, SFU_CLIENT_STATE.CONNECTED);

    const recordingCommands = core.startRecording({ audio: true });
    const { requestId, timeoutTimerId } = requireCommand(
        recordingCommands,
        COMMAND_KIND.BEGIN_PENDING_REQUEST
    ).request;
    const flushTimerId = requireCommand(recordingCommands, COMMAND_KIND.SCHEDULE_TIMER).id;
    assert.equal(typeof requestId, "string");
    assert.equal(Number.isInteger(timeoutTimerId), true);
    assert.equal(Number.isInteger(flushTimerId), true);
    assert.notEqual(flushTimerId, timeoutTimerId);
    assertCommands(recordingCommands, [
        {
            kind: COMMAND_KIND.BEGIN_PENDING_REQUEST,
            request: {
                requestId,
                kind: PENDING_REQUEST_KIND.START_RECORDING,
                timeoutTimerId,
                timeoutMs: 5000
            }
        },
        { kind: COMMAND_KIND.SCHEDULE_TIMER, id: flushTimerId, ms: 100 }
    ]);
    assertCommands(core.onTimer(flushTimerId), [
        sendWebSocket({
            t: "startrecording",
            p: { audio: true },
            q: requestId
        })
    ]);
    assertCommands(
        core.onWsMessage(
            encodeFrame({
                t: "startrecording",
                r: requestId,
                p: { ok: true }
            })
        ),
        [
            { kind: COMMAND_KIND.CANCEL_TIMER, id: timeoutTimerId },
            {
                kind: COMMAND_KIND.RESOLVE_PENDING_REQUEST,
                requestId,
                ok: true
            }
        ]
    );

    const recoveryCommands = core.onWsClose(WS_CLOSE_CODE.ERROR);
    const recoveryTimerId = requireCommand(recoveryCommands, COMMAND_KIND.SCHEDULE_TIMER).id;
    assert.equal(Number.isInteger(recoveryTimerId), true);
    assertCommands(recoveryCommands, [
        { kind: COMMAND_KIND.CLOSE_PEER_CONNECTION },
        stateChange(SFU_CLIENT_STATE.RECOVERING),
        { kind: COMMAND_KIND.SCHEDULE_TIMER, id: recoveryTimerId, ms: 1000 }
    ]);
    assertCommands(core.onTimer(recoveryTimerId), [
        stateChange(SFU_CLIENT_STATE.CONNECTING),
        { kind: COMMAND_KIND.CONNECT, url: "ws://example.test/ws" }
    ]);
    assertCommands(core.disconnect(), [
        { kind: COMMAND_KIND.CANCEL_TIMER, id: recoveryTimerId },
        { kind: COMMAND_KIND.CLOSE_WEB_SOCKET, code: WS_CLOSE_CODE.CLEAN },
        { kind: COMMAND_KIND.CLOSE_PEER_CONNECTION },
        stateChange(SFU_CLIENT_STATE.DISCONNECTED)
    ]);

    assert.equal(core.state, SFU_CLIENT_STATE.DISCONNECTED);
    assert.deepEqual(core.features, EMPTY_FEATURES);
    assert.deepEqual(core.recordingState, {});
    assert.deepEqual([...seenKinds].sort(), Object.values(COMMAND_KIND).sort());
});
