use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_rfc::webrtc::MediaKind;
use o_sfu_router::MediaStream as RouterRtpParameters;
use thiserror::Error;

use crate::{
    ConnectionId, RoomInstanceId,
    runtime::{UserId, source_model::UploadLayerPolicyRole},
};

/// Room-scoped transport-adapter user identity.
///
/// A `UserId` alone is not unique across the server: the same id can appear
/// in different rooms simultaneously. This composite key allows one user
/// to be uniquely identified by the owning room instance, media worker,
/// signaling connection, and user id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransportSessionKey {
    room_instance: RoomInstanceId,
    media_worker: usize,
    connection: ConnectionId,
    user: Arc<UserId>,
}

impl TransportSessionKey {
    #[must_use]
    pub fn new(
        room_instance_id: RoomInstanceId,
        media_worker_id: usize,
        connection_id: ConnectionId,
        user_id: UserId,
    ) -> Self {
        Self {
            room_instance: room_instance_id,
            media_worker: media_worker_id,
            connection: connection_id,
            user: Arc::new(user_id),
        }
    }

    #[must_use]
    pub const fn room_instance_id(&self) -> RoomInstanceId {
        self.room_instance
    }

    #[must_use]
    pub fn media_worker_id(&self) -> usize {
        self.media_worker
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        self.user.as_ref()
    }
}

pub type TransportResult<T> = Result<T, TransportAdapterError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TransportAdapterError {
    #[error("transport unavailable")]
    TransportUnavailable,
    #[error("invalid transport input")]
    InvalidInput,
    #[error("unsupported transport feature")]
    UnsupportedFeature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportSessionHealth {
    Connected,
    Disconnected,
}

/// Transport facts materialized while applying one negotiated SDP answer.
///
/// Producer RTP parameters are answer-derived because the browser owns the
/// final SSRC and RID acceptance details. Returning them with the accepted
/// answer lets room state commit staged publishes from the same projection
/// pass instead of issuing a second transport lookup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedSessionAnswer {
    negotiated_producer_parameters: BTreeMap<TransportMediaId, RouterRtpParameters>,
}

impl AppliedSessionAnswer {
    #[must_use]
    pub fn from_negotiated_producers(
        negotiated_producer_parameters: impl IntoIterator<
            Item = (TransportMediaId, RouterRtpParameters),
        >,
    ) -> Self {
        Self {
            negotiated_producer_parameters: negotiated_producer_parameters.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn negotiated_producer_parameters(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&RouterRtpParameters> {
        self.negotiated_producer_parameters.get(&transport_media_id)
    }
}

/// Point-in-time bitrate measurement aggregated across one or more transport users.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportBitrateSnapshot {
    pub total: u64,
    pub per_media: Vec<(TransportMediaId, u64)>,
}

/// Latest receiver-side bandwidth estimates keyed by transport user.
///
/// These values are produced by the WebRTC egress BWE path and consumed by
/// room-owned media policy. They are cold-path control-plane facts, not packet
/// loop routing state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReceiverBandwidthSnapshot {
    pub per_session: Vec<(TransportSessionKey, u64)>,
}

/// Opaque identifier for a media line allocated by the media transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct TransportMediaId(u64);

impl TransportMediaId {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// Transport-observed active speaker keyed by the producing media source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSpeakerSource {
    transport_media_id: TransportMediaId,
    observed_at: Instant,
}

impl ActiveSpeakerSource {
    #[must_use]
    pub const fn new(transport_media_id: TransportMediaId, observed_at: Instant) -> Self {
        Self {
            transport_media_id,
            observed_at,
        }
    }

    #[must_use]
    pub const fn transport_media_id(self) -> TransportMediaId {
        self.transport_media_id
    }

    #[must_use]
    pub const fn observed_at(self) -> Instant {
        self.observed_at
    }
}

/// Diagnostic state for the transport-owned active-speaker policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSpeakerActivityState {
    Active,
    Idle,
    Blocked,
    RecentlyExpired,
}

/// Reason attached to one transport-owned active-speaker diagnostic state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSpeakerActivityReason {
    Vad,
    AudioLevel,
    AudioLevelWarmup,
    VadFalse,
    LowNoise,
    BelowSpeechThreshold,
    MissingAudioMetadata,
    Expired,
    NoMetadata,
}

/// Read-only explanation for one audio source's active-speaker policy state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSpeakerSourceDiagnostic {
    transport_media_id: TransportMediaId,
    state: ActiveSpeakerActivityState,
    reason: ActiveSpeakerActivityReason,
    last_audio_level_dbov: Option<i8>,
    confidence_observations: u8,
    hold_remaining: Option<Duration>,
}

impl ActiveSpeakerSourceDiagnostic {
    #[must_use]
    pub const fn new(
        transport_media_id: TransportMediaId,
        state: ActiveSpeakerActivityState,
        reason: ActiveSpeakerActivityReason,
        last_audio_level_dbov: Option<i8>,
        confidence_observations: u8,
        hold_remaining: Option<Duration>,
    ) -> Self {
        Self {
            transport_media_id,
            state,
            reason,
            last_audio_level_dbov,
            confidence_observations,
            hold_remaining,
        }
    }

    #[must_use]
    pub const fn transport_media_id(self) -> TransportMediaId {
        self.transport_media_id
    }

    #[must_use]
    pub const fn state(self) -> ActiveSpeakerActivityState {
        self.state
    }

    #[must_use]
    pub const fn reason(self) -> ActiveSpeakerActivityReason {
        self.reason
    }

    #[must_use]
    pub const fn last_audio_level_dbov(self) -> Option<i8> {
        self.last_audio_level_dbov
    }

    #[must_use]
    pub const fn confidence_observations(self) -> u8 {
        self.confidence_observations
    }

    #[must_use]
    pub const fn hold_remaining(self) -> Option<Duration> {
        self.hold_remaining
    }
}

/// Transport-owned packet gate applied to one published source.
///
/// Room policy decides in source-domain terms. The transport boundary receives
/// only the packet-facing projection that the worker can apply without knowing
/// room layout, source identity, or future relay placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePacketGate {
    Open,
    Rid(String),
    OperatingPoint(SourcePacketOperatingPoint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerPacketGateUpdate {
    consumer_session_key: TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    packet_gate: SourcePacketGate,
}

impl ConsumerPacketGateUpdate {
    #[must_use]
    pub fn new(
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Self {
        Self {
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            packet_gate,
        }
    }

    #[must_use]
    pub fn consumer_session_key(&self) -> &TransportSessionKey {
        &self.consumer_session_key
    }

    #[must_use]
    pub const fn consumer_transport_media_id(&self) -> TransportMediaId {
        self.consumer_transport_media_id
    }

    #[must_use]
    pub fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    #[must_use]
    pub const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    #[must_use]
    pub fn packet_gate(&self) -> &SourcePacketGate {
        &self.packet_gate
    }
}

/// Packet-facing layered operating point selected for one source route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePacketOperatingPoint {
    rid: Option<String>,
    max_temporal_layer_id: u8,
}

impl SourcePacketOperatingPoint {
    #[must_use]
    pub fn new(rid: Option<String>, max_temporal_layer_id: u8) -> Self {
        Self {
            rid,
            max_temporal_layer_id,
        }
    }

    #[must_use]
    pub fn rid(&self) -> Option<&str> {
        self.rid.as_deref()
    }

    #[must_use]
    pub const fn max_temporal_layer_id(&self) -> u8 {
        self.max_temporal_layer_id
    }
}

/// Transitional server-authored SDP offer returned by the transport boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOffer {
    sdp: String,
    upload_slots: Vec<SessionUploadSlot>,
}

impl SessionOffer {
    #[must_use]
    pub fn new(sdp: String) -> Self {
        Self {
            sdp,
            upload_slots: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_upload_slots(mut self, upload_slots: Vec<SessionUploadSlot>) -> Self {
        self.upload_slots = upload_slots;
        self
    }

    #[must_use]
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn into_sdp(self) -> String {
        self.sdp
    }

    #[must_use]
    pub fn into_parts(self) -> (String, Vec<SessionUploadSlot>) {
        (self.sdp, self.upload_slots)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionUploadSlot {
    pub mid: String,
    pub kind: MediaKind,
    pub codecs: Vec<String>,
    pub simulcast_encodings: Vec<SessionUploadEncoding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionUploadEncoding {
    pub rid: String,
    pub max_bitrate: Option<u64>,
    pub resolution_scale: Option<u16>,
    pub max_framerate: Option<u16>,
    pub policy_role: Option<UploadLayerPolicyRole>,
}
