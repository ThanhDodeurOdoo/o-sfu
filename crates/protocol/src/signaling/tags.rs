pub(super) const AUTH: &str = "auth";
pub(super) const BROADCAST: &str = "broadcast";
pub(super) const INFO: &str = "info";
pub(super) const OFFER: &str = "offer";
pub(super) const PEER_INFO: &str = "peerinfo";
pub(super) const PEER_JOINED: &str = "peerjoined";
pub(super) const PEER_LEFT: &str = "peerleft";
pub(super) const PUBLISH: &str = "publish";
pub(super) const RECORDING_CHANGE: &str = "recordingchange";
pub(super) const RENEGOTIATE: &str = "renegotiate";
pub(super) const START_RECORDING: &str = "startrecording";
pub(super) const STOP_RECORDING: &str = "stoprecording";
pub(super) const SUBSCRIBE: &str = "subscribe";
pub(super) const TRACKS: &str = "tracks";
pub(super) const UNPUBLISH: &str = "unpublish";
pub(super) const WELCOME: &str = "welcome";

#[cfg(feature = "ts-bindings")]
pub(crate) const WIRE_TAGS: &[(&str, &str)] = &[
    ("AUTH", AUTH),
    ("BROADCAST", BROADCAST),
    ("INFO", INFO),
    ("OFFER", OFFER),
    ("PEER_INFO", PEER_INFO),
    ("PEER_JOINED", PEER_JOINED),
    ("PEER_LEFT", PEER_LEFT),
    ("PUBLISH", PUBLISH),
    ("RECORDING_CHANGE", RECORDING_CHANGE),
    ("RENEGOTIATE", RENEGOTIATE),
    ("START_RECORDING", START_RECORDING),
    ("STOP_RECORDING", STOP_RECORDING),
    ("SUBSCRIBE", SUBSCRIBE),
    ("TRACKS", TRACKS),
    ("UNPUBLISH", UNPUBLISH),
    ("WELCOME", WELCOME),
];
