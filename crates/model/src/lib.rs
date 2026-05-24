//! Shared business model for the Odoo Discuss SFU contract.
//!
//! This crate owns the pure business-layer types that multiple `o-sfu` crates
//! must interpret identically. These are the Odoo Discuss call concepts that
//! are more specific than RFC vocabulary but less specific than any one
//! runtime subsystem.
//!
//! The model crate intentionally depends only on serialization support. It does
//! not own sockets, async work, media transports, router topology, metrics
//! registries, server configuration or JSON envelope parsing. Those concerns
//! stay in the runtime, core, router, telemetry and protocol crates.
//!
//! # Compatibility
//!
//! Several types preserve the old SFU and Odoo browser contract. They should
//! remain small data types with explicit serde shapes and local normalization
//! helpers. Runtime callers should normalize compatibility input at ingress
//! before storing it in room state, diagnostics indexes or subscription maps.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Opaque compatibility payload carried through legacy broadcast paths.
///
/// Prefer explicit business structs for new flows. This alias exists for
/// payloads where Odoo owns the shape and the SFU only relays the JSON value.
pub type JsonPayload = Value;

/// User identity as accepted by the Odoo-facing call contract.
///
/// Odoo normally uses integer user ids, while legacy and test callers may send
/// string ids. The runtime canonicalizes numeric strings before indexing room
/// state so `"42"` and `42` cannot become two live users in the same call.
///
/// Non-numeric strings remain valid compatibility ids.
///
/// Compatibility: The old SFU allowed strings and integers. We keep this
/// property in the new SFU, (if I remember correctly, it is the collaborative
/// web editor that uses strings)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(rename = "SessionId"))]
#[serde(untagged)]
pub enum UserId {
    Integer(i64),
    String(String),
}

impl From<i64> for UserId {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for UserId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for UserId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl UserId {
    /// Return the runtime key form for this user id.
    ///
    /// Numeric strings are parsed into [`Self::Integer`] so all room state,
    /// diagnostics lookup, disconnect handling and subscription logic use one
    /// canonical key. Non-numeric strings are preserved because they may be
    /// externally meaningful compatibility identities.
    #[must_use]
    pub fn normalized_for_runtime(self) -> Self {
        match self {
            Self::String(value) => value
                .parse::<i64>()
                .map_or(Self::String(value), Self::Integer),
            Self::Integer(value) => Self::Integer(value),
        }
    }

    /// Borrowing variant of [`Self::normalized_for_runtime`].
    ///
    /// Use this when the caller owns a borrowed auth or protocol payload and
    /// needs the canonical runtime key without consuming that payload.
    #[must_use]
    pub fn runtime_normalized(&self) -> Self {
        self.clone().normalized_for_runtime()
    }
}

/// Room capabilities advertised to a newly connected browser client.
///
/// These are business capabilities, not permission checks. The room advertises
/// which features exist for the call, then per-user permissions decide who may
/// actually start or change a restricted feature.
#[allow(
    clippy::struct_excessive_bools,
    reason = "feature flags mirror the compatibility startup surface with explicit optional room capabilities"
)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub struct AvailableFeatures {
    /// Whether the room supports real-time media.
    /// if not, this is just a websocket relay, it was a compatibility target for web editor.
    pub rtc: bool,
    /// Whether the room allows transcription flag during recording
    pub transcription: bool,
    /// Whether the room allows audio recording
    pub audio_recording: bool,
    /// Whether the room allows video recording
    pub video_recording: bool,
}

/// Current room recording state as shown to call participants.
///
/// The fields are optional because the compatibility surface may carry sparse
/// updates. Authoritative room snapshots should fill the fields they know.
/// Consumers must treat a missing field as "not asserted by this payload"
/// rather than as `false`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(optional_fields))]
#[serde(rename_all = "camelCase")]
pub struct RecordingState {
    /// Whether the recording is active.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recording: Option<bool>,
    /// Whether the active recording includes audio.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    /// Whether the active recording includes transcription.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    /// Whether the active recording includes video.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
}

/// Business reason attached to a recording stop update.
///
/// This code is shown to clients and diagnostics as the reason recording became
/// inactive. It does not describe transport failures or upload service details.
///
/// TODO: should probably rename it ``RecordingStopCode``
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(rename = "RecordingStopCode"))]
pub enum StopCode {
    /// A participant with permission requested the stop.
    #[serde(rename = "user_request")]
    UserRequest,
    /// The room closed before recording could continue.
    #[serde(rename = "channel_closed")]
    ChannelClosed,
    /// Recording exceeded the configured room timeout.
    #[serde(rename = "recording_timeout")]
    RecordingTimeout,
    /// The recording backend failed after the room accepted recording.
    #[serde(rename = "recording_failed")]
    RecordingFailed,
    /// Local storage capacity prevented recording from continuing.
    #[serde(rename = "disk_space_exhausted")]
    DiskSpaceExhausted,
}

/// Recording state update emitted to clients and observers.
///
/// The state carries the new room-visible recording flags. `stop_code` is only
/// present when the update explains why a recording session stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct RecordingStateUpdate {
    /// New room-visible recording state.
    pub state: RecordingState,
    /// Optional business reason for a transition to an inactive recording
    /// state.
    #[serde(rename = "stopCode", skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts-bindings", ts(optional))]
    pub stop_code: Option<StopCode>,
}

/// User-level permissions supplied by the Odoo authentication path.
///
/// These values answer what the authenticated user may control inside this
/// call. Missing values are treated as denied by the room runtime so omitted
/// permissions never grant access by accident.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(optional_fields))]
#[serde(rename_all = "camelCase")]
pub struct UserPermissions {
    /// Whether the user may toggle transcription.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    /// Whether the user may start audio recording.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_recording: Option<bool>,
    /// Whether the user may start video recording.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_recording: Option<bool>,
}

/// Presence and call UI state associated with one room participant.
///
/// This is business state visible to other clients. It does not own media
/// routing, transport health or source identity. Application orchestration
/// updates media-related fields before the room stores and rebroadcasts this
/// payload.
///
/// Fields are optional so callers can send partial updates. Use
/// [`Self::snapshot_complete`] when serializing a full room snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(optional_fields))]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    /// Whether the participant is currently considered active speaker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_talking: Option<bool>,
    /// Whether the participant is highlighted by room layout policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_featured: Option<bool>,
    /// Whether the participant currently has an active camera publication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_camera_on: Option<bool>,
    /// Whether the participant currently has an active screen-share
    /// publication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_screen_sharing_on: Option<bool>,
    /// Whether the participant muted their own microphone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_self_muted: Option<bool>,
    /// Whether the participant opted out of receiving call audio.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_deaf: Option<bool>,
    /// Whether the participant is raising their hand in the call UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_raising_hand: Option<bool>,
}

impl UserInfo {
    /// Return a complete snapshot with every presence field set to `false`.
    ///
    /// This is useful for initial empty room projections where absence would be
    /// ambiguous for the browser contract.
    #[must_use]
    pub fn snapshot_defaults() -> Self {
        Self::default().snapshot_complete()
    }

    /// Fill missing presence fields with `false` for snapshot emission.
    ///
    /// Partial updates keep `None` to mean "unchanged". Full room snapshots use
    /// this method so receivers can render every visible participant state
    /// without merging against stale local data.
    #[must_use]
    pub fn snapshot_complete(self) -> Self {
        Self {
            is_talking: Some(self.is_talking.unwrap_or(false)),
            is_featured: Some(self.is_featured.unwrap_or(false)),
            is_camera_on: Some(self.is_camera_on.unwrap_or(false)),
            is_screen_sharing_on: Some(self.is_screen_sharing_on.unwrap_or(false)),
            is_self_muted: Some(self.is_self_muted.unwrap_or(false)),
            is_deaf: Some(self.is_deaf.unwrap_or(false)),
            is_raising_hand: Some(self.is_raising_hand.unwrap_or(false)),
        }
    }

    /// Merge a partial presence update into the current stored value.
    ///
    /// `None` means "unchanged", matching the wire contract for incremental
    /// user-info updates.
    pub fn apply_partial_update(&mut self, update: &Self) {
        if let Some(is_talking) = update.is_talking {
            self.is_talking = Some(is_talking);
        }
        if let Some(is_featured) = update.is_featured {
            self.is_featured = Some(is_featured);
        }
        if let Some(is_camera_on) = update.is_camera_on {
            self.is_camera_on = Some(is_camera_on);
        }
        if let Some(is_screen_sharing_on) = update.is_screen_sharing_on {
            self.is_screen_sharing_on = Some(is_screen_sharing_on);
        }
        if let Some(is_self_muted) = update.is_self_muted {
            self.is_self_muted = Some(is_self_muted);
        }
        if let Some(is_deaf) = update.is_deaf {
            self.is_deaf = Some(is_deaf);
        }
        if let Some(is_raising_hand) = update.is_raising_hand {
            self.is_raising_hand = Some(is_raising_hand);
        }
    }

    /// Return this presence payload with the room-layout featured flag applied.
    #[must_use]
    pub fn with_featured(mut self, is_featured: Option<bool>) -> Self {
        self.is_featured = is_featured;
        self
    }
}

/// Full peer entry sent when a client needs the current room membership view.
///
/// The serialized `sessionId` field is the Odoo-facing user identity. Runtime
/// connection ids are intentionally absent because reconnection and replacement
/// are server-local concerns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct PeerSnapshot {
    /// Odoo-facing participant identity for this peer.
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    /// Complete participant state projected from room presence, layout and
    /// media activity.
    #[serde(default)]
    pub info: UserInfo,
}

/// Receiver intent for which streams to download from one peer.
///
/// This is business intent from a client, not a transport subscription object.
/// The room translates it into routing policy, source selection and transport
/// effects. Missing fields mean the current receiver preference for that stream
/// or layout should be left unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(optional_fields))]
pub struct DownloadStates {
    /// Desired audio receive state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    /// Desired camera receive state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera: Option<bool>,
    /// Desired screen-share receive state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub screen: Option<bool>,
    /// Layout intent that guides camera source priority for this receiver.
    #[serde(rename = "cameraLayout", skip_serializing_if = "Option::is_none")]
    pub camera_layout: Option<VideoLayoutIntent>,
    /// Layout intent that guides screen-share source priority for this
    /// receiver.
    #[serde(rename = "screenLayout", skip_serializing_if = "Option::is_none")]
    pub screen_layout: Option<VideoLayoutIntent>,
}

impl DownloadStates {
    /// Iterate over explicit stream toggles in this update.
    ///
    /// Layout preferences are intentionally not yielded because they do not map
    /// one-to-one to enabling or disabling a media stream.
    pub fn iter(&self) -> impl Iterator<Item = (StreamType, bool)> + '_ {
        [
            self.audio.map(|v| (StreamType::Audio, v)),
            self.camera.map(|v| (StreamType::Camera, v)),
            self.screen.map(|v| (StreamType::Screen, v)),
        ]
        .into_iter()
        .flatten()
    }
}

/// Receiver-side layout role for a video stream.
///
/// The room uses this business hint to prioritize selected video layers under
/// bandwidth pressure. It does not name an RTP encoding, simulcast RID or
/// concrete packet gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum VideoLayoutIntent {
    /// Main speaker or call focus.
    Featured,
    /// User-pinned stream that should be protected more strongly than ordinary
    /// thumbnails.
    Pinned,
    /// Thumbnail that is currently visible in the client layout.
    VisibleThumbnail,
    /// Stream hidden by the client layout.
    Hidden,
    /// Stream outside the currently visible layout range.
    /// currently the same as hidden, but the meaning can allow more granular
    /// control in the future.
    Overflow,
}

/// Business stream category exposed to Odoo clients.
///
/// This is intentionally smaller than the internal source model. The source
/// model may contain encodings, RTP metadata and transport-local media ids,
/// while `StreamType` only says which user-facing stream a caller means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub enum StreamType {
    /// Microphone audio.
    #[serde(rename = "audio")]
    Audio,
    /// Camera video.
    #[serde(rename = "camera")]
    Camera,
    /// Screen-share video.
    #[serde(rename = "screen")]
    Screen,
}

/// Recording modes requested by a user.
///
/// Missing fields mean the caller did not request that mode. The room combines
/// these options with feature flags, current recording state and
/// [`UserPermissions`] before mutating recording state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(optional_fields))]
pub struct RecordingOptions {
    /// Request audio capture for a new recording.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    /// Request transcription for a new or already active recording.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    /// Request video capture for a new recording.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
}

/// WebSocket close code vocabulary shared by server and browser protocol code.
///
/// Standard codes keep their RFC meaning. Custom codes mirror the legacy
/// Odoo SFU websocket close vocabulary used by browser clients and
/// low-cardinality telemetry. The custom values stay in the `4100` subrange
/// used by the legacy SFU instead of the Odoo bus websocket close-code range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum WebSocketCloseCode {
    /// Normal websocket closure.
    Clean = 1000,
    /// The peer is leaving.
    Leaving = 1001,
    /// The peer sent a malformed or invalid protocol message.
    ProtocolError = 1002,
    /// The server hit an internal error while handling the socket.
    Error = 1011,

    /// Authentication failed.
    AuthFailed = 4106,
    /// The client did not authenticate before the server timeout.
    AuthTimeout = 4107,
    /// The runtime intentionally removed this client from the room.
    Kicked = 4108,
    /// Admission failed because the room cannot accept another user.
    RoomFull = 4109,
}

impl WebSocketCloseCode {
    /// Decode a raw websocket close code if it belongs to the shared vocabulary.
    ///
    /// Unknown codes return `None` so the caller can keep foreign websocket
    /// close reasons out of business telemetry labels and protocol state
    /// machines.
    #[must_use]
    pub const fn from_u16(value: u16) -> Option<Self> {
        match value {
            1000 => Some(Self::Clean),
            1001 => Some(Self::Leaving),
            1002 => Some(Self::ProtocolError),
            1011 => Some(Self::Error),
            4106 => Some(Self::AuthFailed),
            4107 => Some(Self::AuthTimeout),
            4108 => Some(Self::Kicked),
            4109 => Some(Self::RoomFull),
            _ => None,
        }
    }
}

impl From<WebSocketCloseCode> for u16 {
    fn from(value: WebSocketCloseCode) -> Self {
        match value {
            WebSocketCloseCode::Clean => 1000,
            WebSocketCloseCode::Leaving => 1001,
            WebSocketCloseCode::ProtocolError => 1002,
            WebSocketCloseCode::Error => 1011,
            WebSocketCloseCode::AuthFailed => 4106,
            WebSocketCloseCode::AuthTimeout => 4107,
            WebSocketCloseCode::Kicked => 4108,
            WebSocketCloseCode::RoomFull => 4109,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UserId;

    #[test]
    fn user_id_normalization_keeps_numeric_runtime_identity_canonical() {
        assert_eq!(
            UserId::String("42".to_owned()).normalized_for_runtime(),
            UserId::Integer(42)
        );
        assert_eq!(
            UserId::Integer(42).normalized_for_runtime(),
            UserId::Integer(42)
        );
    }

    #[test]
    fn user_id_normalization_preserves_arbitrary_string_ids() {
        assert_eq!(
            UserId::String("guest-42".to_owned()).normalized_for_runtime(),
            UserId::String("guest-42".to_owned())
        );
    }
}
