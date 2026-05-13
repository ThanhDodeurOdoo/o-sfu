# o-sfu Client API

The browser bundle exposes the Odoo-facing SFU client facade:

```js
import { CLIENT_UPDATE, SFU_CLIENT_STATE, SfuClient } from "/bundle/odoo_sfu.js";

const sfu = new SfuClient();
```

The compatibility bundle is built with:

```bash
npm --prefix crates/client run build:odoo
```

The bundle is intentionally close to the old Node SFU client API. New code should
prefer `publish()` and `subscribe()`. The legacy `updateUpload()` and
`updateDownload()` names are still available as direct aliases for compatibility.

## Connection State

`sfu.state` is one of:

```ts
type ConnectionState =
    | "disconnected"
    | "connecting"
    | "authenticated"
    | "connected"
    | "recovering"
    | "closed";
```

The same values are exported through `SFU_CLIENT_STATE`:

```js
sfu.state === SFU_CLIENT_STATE.CONNECTED;
```

State changes are emitted through the `"stateChange"` event:

```js
sfu.addEventListener("stateChange", ({ detail }) => {
    const { state, cause } = detail;
});
```

## connect(url, jwt, options)

Connects to an o-sfu websocket endpoint.

```js
sfu.connect("https://sfu.example.com/ws", jsonWebToken, {
    channelUUID: "mail-channel-uuid",
    iceServers: [{ urls: "stun:stun.example.com:3478" }],
});
```

The client accepts `http:` and `https:` URLs and normalizes them to `ws:` or
`wss:` before opening the websocket.

Options:

```ts
interface ConnectOptions {
    channelUUID?: string;
    iceServers?: RTCIceServer[];
}
```

`channelUUID` is optional when the token already identifies the channel.
`iceServers` is passed to `RTCPeerConnection`.

## disconnect()

Requests a clean SFU disconnect.

```js
sfu.disconnect();
```

The client eventually emits a `"stateChange"` event and moves back to a terminal
or disconnected state according to the protocol state machine.

## publish(type, track)

Publishes or unpublishes a local media track.

```js
const audioTrack = audioStream.getAudioTracks()[0];
const cameraTrack = cameraStream.getVideoTracks()[0];

sfu.publish("audio", audioTrack);
sfu.publish("camera", cameraTrack);
sfu.publish("screen", screenTrack);

sfu.publish("camera", null);
```

`type` must be one of:

```ts
type StreamType = "audio" | "camera" | "screen";
```

The supplied track must match the stream type:

- `audio` requires an audio `MediaStreamTrack`
- `camera` requires a video `MediaStreamTrack`
- `screen` requires a video `MediaStreamTrack`

Passing `null` or `undefined` stops publishing that stream type.

### updateUpload(type, track)

Deprecated compatibility alias for `publish(type, track)`.

```js
sfu.updateUpload("camera", cameraTrack);
sfu.updateUpload("camera", undefined);
```

## subscribe(sessionId, states)

Updates what the current browser wants to receive from a remote participant.

```js
sfu.subscribe(remoteSessionId, {
    audio: true,
    camera: true,
    screen: false,
});
```

`states` is a partial object. Omitted fields keep their previous value for that
remote session.

```ts
interface DownloadStates {
    audio?: boolean;
    camera?: boolean;
    screen?: boolean;
    cameraLayout?: VideoLayoutIntent;
    screenLayout?: VideoLayoutIntent;
}
```

The boolean fields control whether the receiver wants the stream:

- `audio`: receive or stop receiving remote audio
- `camera`: receive or stop receiving the remote camera
- `screen`: receive or stop receiving the remote screen share

The layout fields tell the SFU how important each video route is for this
receiver. They are additive policy hints, not new tracks and not mandatory for
basic audio/video subscription.

```ts
type VideoLayoutIntent =
    | "featured"
    | "pinned"
    | "visible_thumbnail"
    | "hidden"
    | "overflow";
```

### Layout Intent

Use `cameraLayout` for the participant camera tile and `screenLayout` for the
participant screen-share tile.

```js
sfu.subscribe(remoteSessionId, {
    camera: true,
    cameraLayout: "pinned",
});
```

The SFU uses the resolved layout role when selecting video quality for this
receiver:

- `pinned`: the user explicitly pinned this video; highest priority with
  explicit featured treatment.
- `featured`: the UI explicitly treats this video as featured without
  necessarily being user-pinned.
- `visible_thumbnail`: the video is visible as a normal thumbnail.
- `hidden`: the video is subscribed but not currently visible.
- `overflow`: the video is outside the visible grid or in an overflow list.

For camera video, if `cameraLayout` is omitted, the SFU keeps compatibility
behavior: an active speaker can receive featured-quality treatment, and other
cameras are treated as visible thumbnails.

For screen share, if `screenLayout` is omitted, the SFU treats the stream as a
screen-share readability route. A `screenLayout` value of `"visible_thumbnail"`
keeps that screen-specific policy rather than turning the screen share into a
camera thumbnail.

Layout-only updates are valid:

```js
sfu.subscribe(remoteSessionId, {
    cameraLayout: "hidden",
});
```

That call keeps the current audio/camera/screen download flags and only changes
the server-side video priority for this receiver.

### Common Subscription Patterns

Receive a remote participant normally:

```js
sfu.subscribe(remoteSessionId, {
    audio: true,
    camera: true,
    cameraLayout: "visible_thumbnail",
});
```

Pin one participant camera:

```js
sfu.subscribe(pinnedSessionId, {
    camera: true,
    cameraLayout: "pinned",
});
```

Move an offscreen camera out of the visible budget:

```js
sfu.subscribe(remoteSessionId, {
    cameraLayout: "overflow",
});
```

Keep a screen share readable:

```js
sfu.subscribe(remoteSessionId, {
    screen: true,
});
```

Hide a screen share while preserving the existing subscription state:

```js
sfu.subscribe(remoteSessionId, {
    screenLayout: "hidden",
});
```

### updateDownload(sessionId, states)

Deprecated compatibility alias for `subscribe(sessionId, states)`.

The new layout-intent fields are available through the old name as well:

```js
sfu.updateDownload(remoteSessionId, {
    camera: true,
    cameraLayout: "pinned",
});
```

New code should call `subscribe()` directly.

## updateInfo(info, options)

Updates the local participant metadata sent to other participants.

```js
sfu.updateInfo({
    isTalking: false,
    isFeatured: true,
    isCameraOn: true,
    isScreenSharingOn: false,
    isSelfMuted: false,
    isDeaf: false,
    isRaisingHand: false,
});
```

Supported fields:

```ts
interface SessionInfo {
    isTalking?: boolean;
    isFeatured?: boolean;
    isCameraOn?: boolean;
    isScreenSharingOn?: boolean;
    isSelfMuted?: boolean;
    isDeaf?: boolean;
    isRaisingHand?: boolean;
}
```

`options.needRefresh` is accepted for legacy callers and is currently a
compatibility no-op.

## broadcast(message)

Sends an application-level message to the channel.

```js
sfu.broadcast({ type: "reaction", value: "raised-hand" });
```

Other clients receive it through the `"update"` event with
`CLIENT_UPDATE.BROADCAST`.

## getStats()

Returns WebRTC stats reports for the peer connection and local senders when
available.

```js
const stats = await sfu.getStats();

const {
    uploadStats,
    downloadStats,
    audio,
    camera,
    screen,
} = stats;
```

Shape:

```ts
interface SfuStats {
    uploadStats?: RTCStatsReport;
    downloadStats?: RTCStatsReport;
    audio?: RTCStatsReport;
    camera?: RTCStatsReport;
    screen?: RTCStatsReport;
}
```

`uploadStats` and `downloadStats` are compatibility names for the peer
connection stats report.

## Recording

Recording capabilities are exposed through `availableFeatures`:

```js
if (sfu.availableFeatures.videoRecording) {
    const allowed = await sfu.startRecording({ video: true });
}
```

Recording methods:

```js
const allowed = await sfu.startRecording({
    audio: true,
    video: true,
    transcription: false,
});

const stopped = await sfu.stopRecording();
```

Options:

```ts
interface RecordingOptions {
    audio?: boolean;
    video?: boolean;
    transcription?: boolean;
}
```

Current recording state is exposed through `sfu.recordingState` and updated by
`CLIENT_UPDATE.CHANNEL_INFO_CHANGE` events.

## Events

The client emits four event types:

- `"stateChange"` for connection-state transitions
- `"update"` for SFU protocol updates
- `"handledError"` after a recoverable runtime error is captured by the client
- `"log"` for client/runtime diagnostics

### update

```js
sfu.addEventListener("update", ({ detail }) => {
    switch (detail.name) {
        case CLIENT_UPDATE.TRACK: {
            const { sessionId, type, track, active } = detail.payload;
            break;
        }
        case CLIENT_UPDATE.SOURCE: {
            const { sources } = detail.payload;
            break;
        }
        case CLIENT_UPDATE.DISCONNECT: {
            const { sessionId } = detail.payload;
            break;
        }
        case CLIENT_UPDATE.INFO_CHANGE: {
            const sessions = detail.payload;
            break;
        }
        case CLIENT_UPDATE.BROADCAST: {
            const { senderId, message } = detail.payload;
            break;
        }
        case CLIENT_UPDATE.CHANNEL_INFO_CHANGE: {
            const { state, stopCode } = detail.payload;
            break;
        }
    }
});
```

`CLIENT_UPDATE` values:

```ts
const CLIENT_UPDATE = {
    TRACK: "track",
    SOURCE: "source",
    DISCONNECT: "disconnect",
    INFO_CHANGE: "info_change",
    BROADCAST: "broadcast",
    CHANNEL_INFO_CHANGE: "channel_info_change",
} as const;
```

Track update payload:

```ts
interface TrackUpdateDetail {
    sessionId: SessionId;
    type: StreamType;
    track: MediaStreamTrack;
    active: boolean;
}
```

Source descriptor payload:

```ts
interface SourceDescriptor {
    sourceId: string;
    sessionId: SessionId;
    type: StreamType;
    active: boolean;
    mid?: string;
    encodings: SourceEncodingDescriptor[];
}

interface SourceEncodingDescriptor {
    encodingId: string;
    rid?: string;
    maxBitrate?: number;
    resolutionScale?: number;
    maxFramerate?: number;
    policyRole?: "featured" | "thumbnail" | "degradedThumbnail";
    maxTemporalLayerId?: number;
}
```

The latest source descriptors are also available as `sfu.sourceDescriptors`.

### handledError

```js
sfu.addEventListener("handledError", ({ detail }) => {
    console.error(detail.error);
});
```

The same errors are retained in `sfu.errors`.

### log

```js
sfu.addEventListener("log", ({ detail }) => {
    const { id, level, message } = detail;
});
```

`level` is one of `"debug"`, `"info"`, `"warn"`, or `"error"`.

## Public Properties

```ts
interface SfuClientSurface extends EventTarget {
    readonly state: ConnectionState;
    readonly errors: Error[];
    readonly availableFeatures: AvailableFeatures;
    readonly recordingState: RecordingState;
    readonly sourceDescriptors: readonly SourceDescriptor[];
}
```

`availableFeatures` has this shape:

```ts
interface AvailableFeatures {
    rtc: boolean;
    transcription: boolean;
    audioRecording: boolean;
    videoRecording: boolean;
}
```

## Compatibility Notes

- `updateUpload()` is retained and delegates to `publish()`.
- `updateDownload()` is retained and delegates to `subscribe()`.
- `updateDownload()` therefore supports `cameraLayout` and `screenLayout` exactly
  like `subscribe()`.
- `_consumers` remains available as a compatibility/debug view for Odoo Discuss
  diagnostics. New integrations should consume `"update"` events and
  `sourceDescriptors` instead.
