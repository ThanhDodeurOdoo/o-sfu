// @ts-expect-error Protocol internals are not part of the released module.
import { createProtocolCore } from "../../dist/odoo_sfu.js";
// @ts-expect-error Internal stream constants are not released runtime values.
import { STREAM_TYPES } from "../../dist/odoo_sfu.js";
// @ts-expect-error Internal layout constants are not released runtime values.
import { VIDEO_LAYOUT_INTENTS } from "../../dist/odoo_sfu.js";
// @ts-expect-error Internal log constants are not released runtime values.
import { CLIENT_LOG_LEVEL } from "../../dist/odoo_sfu.js";
// @ts-expect-error Internal recording constants are not released runtime values.
import { RECORDING_STOP_CODES } from "../../dist/odoo_sfu.js";
import {
    CLIENT_UPDATE,
    SFU_CLIENT_STATE,
    SfuClient,
    __info__,
    type AvailableFeatures,
    type BundleInfo,
    type ConnectionState,
    type ConsumersCompat,
    type JsonValue,
    type SfuRecordingState
} from "../../dist/odoo_sfu.js";

const client = new SfuClient();
const state: ConnectionState = client.state;
const features: AvailableFeatures = client.availableFeatures;
const recordingState: SfuRecordingState = client.recordingState;
const consumers: ConsumersCompat | undefined = client._consumers.get("remote-session");
const bundleInfo: BundleInfo = __info__;
const message: JsonValue = { type: "reaction", value: "raised-hand" };

client.broadcast(message);

client.addEventListener("stateChange", (event) => {
    const nextState: ConnectionState = event.detail.state;
    const cause: string | undefined = event.detail.cause;
    void [nextState, cause];
});

client.addEventListener("update", (event) => {
    if (event.detail.name === CLIENT_UPDATE.TRACK) {
        const track: MediaStreamTrack = event.detail.payload.track;
        void track;
    }
});

client.addEventListener("handledError", (event) => {
    const error: Error = event.detail.error;
    void error;
});

client.addEventListener("log", (event) => {
    const message: string = event.detail.message;
    void message;
});

if (state === SFU_CLIENT_STATE.CONNECTED) {
    void [features, recordingState, consumers, bundleInfo];
}

if (consumers?.camera && !consumers.camera.closed && !consumers.camera.paused) {
    const track: MediaStreamTrack | null = consumers.camera.track;
    void track;
}

// @ts-expect-error Test dependencies are not part of the released constructor.
new SfuClient({});
void createProtocolCore;
void [STREAM_TYPES, VIDEO_LAYOUT_INTENTS, CLIENT_LOG_LEVEL, RECORDING_STOP_CODES];
