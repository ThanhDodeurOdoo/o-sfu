//! media transport boundary values shared by room state, server code and RTC workers

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_rfc::webrtc::MediaKind;
use o_sfu_router::rtp::{MediaCapabilities, MediaStream as RouterRtpParameters};
use thiserror::Error;

use crate::{Bitrate, ConnectionId, MediaWorkerId, RoomInstanceId, engine::UserId};

/// room-scoped media-transport user identity
///
/// a `UserId` alone is not unique across the server
/// the same id can appear in different rooms simultaneously
/// this composite key includes room instance, media worker, signaling
/// connection and user id
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransportSessionKey {
    room_instance: RoomInstanceId,
    media_worker: MediaWorkerId,
    connection: ConnectionId,
    user: Arc<UserId>,
}

impl TransportSessionKey {
    #[must_use]
    pub fn new(
        room_instance_id: RoomInstanceId,
        media_worker_id: MediaWorkerId,
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
    pub const fn media_worker_id(&self) -> MediaWorkerId {
        self.media_worker
    }

    #[must_use]
    pub const fn connection_id(&self) -> ConnectionId {
        self.connection
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

/// producer-side transport activity state
///
/// this is transport execution policy, not room membership
/// the room remains
/// responsible for deciding whether a source should be considered published or
/// visible to participants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerActivity {
    /// RTP from this producer should be forwarded when routes allow it
    Active,
    /// RTP from this producer should not be forwarded until reactivated
    Inactive,
}

impl ProducerActivity {
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// consumer-side transport activity state
///
/// a consumer can be inactive while the room still owns the subscription
/// that distinction lets source policy pause delivery without deleting the
/// negotiated transport route
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerActivity {
    /// RTP may be delivered when this consumer's packet gate also allows it
    Active,
    /// RTP delivery to this consumer is paused
    Inactive,
}

impl ConsumerActivity {
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Transport facts materialized while applying one negotiated SDP answer.
///
/// Producer RTP parameters are answer-derived because the browser owns the
/// final RTP capability, SSRC and RID acceptance details. Returning them with
/// the accepted answer lets room state commit negotiated session and producer
/// facts from the same projection pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedSessionAnswer {
    client_capabilities: Option<MediaCapabilities>,
    negotiated_producers: BTreeMap<TransportMediaId, AppliedProducer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProducer {
    rtp_parameters: RouterRtpParameters,
    upload_encodings: Vec<SessionUploadEncoding>,
}

impl AppliedProducer {
    #[must_use]
    pub fn new(
        rtp_parameters: RouterRtpParameters,
        upload_encodings: Vec<SessionUploadEncoding>,
    ) -> Self {
        Self {
            rtp_parameters,
            upload_encodings,
        }
    }

    #[must_use]
    pub const fn rtp_parameters(&self) -> &RouterRtpParameters {
        &self.rtp_parameters
    }

    #[must_use]
    pub fn upload_encodings(&self) -> &[SessionUploadEncoding] {
        &self.upload_encodings
    }
}

impl AppliedSessionAnswer {
    #[must_use]
    pub fn from_negotiated_producers(
        negotiated_producer_parameters: impl IntoIterator<
            Item = (TransportMediaId, RouterRtpParameters),
        >,
    ) -> Self {
        Self {
            client_capabilities: None,
            negotiated_producers: negotiated_producer_parameters
                .into_iter()
                .map(|(transport_media_id, rtp_parameters)| {
                    (
                        transport_media_id,
                        AppliedProducer::new(rtp_parameters, Vec::new()),
                    )
                })
                .collect(),
        }
    }

    #[must_use]
    pub fn from_negotiated_producer_details(
        negotiated_producers: impl IntoIterator<Item = (TransportMediaId, AppliedProducer)>,
    ) -> Self {
        Self {
            client_capabilities: None,
            negotiated_producers: negotiated_producers.into_iter().collect(),
        }
    }

    pub(crate) fn with_client_capabilities(
        mut self,
        capabilities: Option<MediaCapabilities>,
    ) -> Self {
        self.client_capabilities = capabilities;
        self
    }

    pub(crate) const fn client_capabilities(&self) -> Option<&MediaCapabilities> {
        self.client_capabilities.as_ref()
    }

    #[must_use]
    pub fn negotiated_producer_parameters(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&RouterRtpParameters> {
        self.negotiated_producers
            .get(&transport_media_id)
            .map(AppliedProducer::rtp_parameters)
    }

    #[must_use]
    pub fn negotiated_producer_upload_encodings(
        &self,
        transport_media_id: TransportMediaId,
    ) -> &[SessionUploadEncoding] {
        self.negotiated_producers
            .get(&transport_media_id)
            .map_or(&[], AppliedProducer::upload_encodings)
    }
}

/// Point-in-time bitrate measurement aggregated across one or more transport users.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportBitrateSnapshot {
    pub total: Bitrate,
    pub per_media: Vec<(TransportMediaId, Bitrate)>,
}

/// Latest receiver-side bandwidth estimates keyed by transport user.
///
/// These values are produced by the WebRTC egress BWE path and consumed by
/// room media policy. They are cold-path control-plane facts, not packet
/// loop routing state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReceiverBandwidthSnapshot {
    pub per_session: Vec<(TransportSessionKey, Bitrate)>,
}

/// Latest sampled transport-quality facts keyed by transport user.
///
/// These values come from str0m stats events and are intended for diagnostics.
/// Prometheus receives only aggregate counters and histograms so user identity
/// never becomes a metrics label.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportQualitySnapshot {
    pub per_session: Vec<(TransportSessionKey, TransportQualitySample)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportQualitySample {
    pub latest_bwe_bps: Option<u64>,
    pub rtt_ms: Option<u64>,
    pub ingress_loss_ppm: Option<u64>,
    pub egress_loss_ppm: Option<u64>,
    pub egress_jitter_rtp_timestamp_units: Option<u64>,
    pub sample_count: u64,
}

/// Latest packet-path activity for producer media.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportSourceActivitySnapshot {
    pub per_media: Vec<TransportSourceActivity>,
}

/// Recent packet and decoder-refresh age for one producer media id.
///
/// The source-level fields answer whether any packets are still arriving for a
/// publication. RID entries answer whether a selected simulcast layer is still
/// producing packets and decoder refreshes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportSourceActivity {
    transport_media_id: TransportMediaId,
    last_packet_age: Duration,
    last_keyframe_age: Option<Duration>,
    rids: Vec<TransportRidActivity>,
}

impl TransportSourceActivity {
    #[must_use]
    pub fn new(
        transport_media_id: TransportMediaId,
        last_packet_age: Duration,
        last_keyframe_age: Option<Duration>,
        rids: Vec<TransportRidActivity>,
    ) -> Self {
        Self {
            transport_media_id,
            last_packet_age,
            last_keyframe_age,
            rids,
        }
    }

    #[must_use]
    pub const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    #[must_use]
    pub const fn last_packet_age(&self) -> Duration {
        self.last_packet_age
    }

    #[must_use]
    pub const fn last_keyframe_age(&self) -> Option<Duration> {
        self.last_keyframe_age
    }

    #[must_use]
    pub fn rids(&self) -> &[TransportRidActivity] {
        &self.rids
    }
}

/// Recent packet and decoder-refresh age for one producer RID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportRidActivity {
    rid: String,
    last_packet_age: Duration,
    last_keyframe_age: Option<Duration>,
}

impl TransportRidActivity {
    #[must_use]
    pub fn new(
        rid: String,
        last_packet_age: Duration,
        last_keyframe_age: Option<Duration>,
    ) -> Self {
        Self {
            rid,
            last_packet_age,
            last_keyframe_age,
        }
    }

    #[must_use]
    pub fn rid(&self) -> &str {
        &self.rid
    }

    #[must_use]
    pub const fn last_packet_age(&self) -> Duration {
        self.last_packet_age
    }

    #[must_use]
    pub const fn last_keyframe_age(&self) -> Option<Duration> {
        self.last_keyframe_age
    }
}

/// Transport-observed pressure used by room-local placement policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportPlacementPressureSnapshot {
    pub egress_bitrate: Bitrate,
    pub packet_loop_lag_ms: u64,
    pub command_backlog_depth: usize,
    pub relay_mailbox_depth: usize,
    pub worker_pressure_score: u8,
}

impl TransportPlacementPressureSnapshot {
    #[must_use]
    pub fn merged_with(self, other: Self) -> Self {
        Self {
            egress_bitrate: self.egress_bitrate.saturating_add(other.egress_bitrate),
            packet_loop_lag_ms: self.packet_loop_lag_ms.max(other.packet_loop_lag_ms),
            command_backlog_depth: self.command_backlog_depth.max(other.command_backlog_depth),
            relay_mailbox_depth: self.relay_mailbox_depth.max(other.relay_mailbox_depth),
            worker_pressure_score: self.worker_pressure_score.max(other.worker_pressure_score),
        }
    }
}

/// Transport-observed pressure for one local media worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportWorkerPressureSnapshot {
    pub media_worker_id: MediaWorkerId,
    pub pressure: TransportPlacementPressureSnapshot,
}

impl TransportWorkerPressureSnapshot {
    #[must_use]
    pub const fn new(
        media_worker_id: MediaWorkerId,
        pressure: TransportPlacementPressureSnapshot,
    ) -> Self {
        Self {
            media_worker_id,
            pressure,
        }
    }
}

/// Opaque identifier for a media line allocated by the media transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct TransportMediaId(u64);

impl TransportMediaId {
    #[must_use]
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
    last_audio_level_dbov: Option<i8>,
}

impl ActiveSpeakerSource {
    #[must_use]
    pub const fn new(transport_media_id: TransportMediaId, observed_at: Instant) -> Self {
        Self {
            transport_media_id,
            observed_at,
            last_audio_level_dbov: None,
        }
    }

    #[must_use]
    pub const fn with_audio_level(
        transport_media_id: TransportMediaId,
        observed_at: Instant,
        last_audio_level_dbov: Option<i8>,
    ) -> Self {
        Self {
            transport_media_id,
            observed_at,
            last_audio_level_dbov,
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

    #[must_use]
    pub const fn last_audio_level_dbov(self) -> Option<i8> {
        self.last_audio_level_dbov
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

/// Packet gate applied by transport to one published source.
///
/// Room policy decides in source-domain terms. The transport boundary receives
/// only the packet-facing projection that the worker can apply without knowing
/// room layout, source identity or relay placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePacketGate {
    Open,
    Rid(String),
    OperatingPoint(SourcePacketOperatingPoint),
}

/// Relay route mutation applied by the media transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportRelayRouteEffect {
    pub source: TransportSourceKey,
    pub target_media_worker_id: MediaWorkerId,
    pub action: TransportRelayRouteAction,
}

/// relay-route transport activity state
///
/// inactive relay routes keep their target registration but stop source-worker
/// fanout to that target
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayRouteActivity {
    Active,
    Inactive,
}

impl RelayRouteActivity {
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportRelayRouteAction {
    Install,
    Release,
    SetActivity(RelayRouteActivity),
}

/// producer-side source identity owned by the transport boundary
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransportSourceKey {
    source_session_key: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
}

impl TransportSourceKey {
    #[must_use]
    pub fn new(
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            source_session_key,
            source_transport_media_id,
        }
    }

    #[must_use]
    pub fn session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    #[must_use]
    pub const fn transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    #[must_use]
    pub const fn room_instance_id(&self) -> RoomInstanceId {
        self.source_session_key.room_instance_id()
    }
}

/// consumer-to-source route identity owned by the transport boundary
///
/// carrying these fields together keeps room code from passing source and
/// receiver ids as adjacent positional arguments
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportConsumerRoute {
    consumer_session_key: TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source: TransportSourceKey,
}

impl TransportConsumerRoute {
    #[must_use]
    pub fn new(
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source: TransportSourceKey,
    ) -> Self {
        Self {
            consumer_session_key,
            consumer_transport_media_id,
            source,
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
        self.source.session_key()
    }

    #[must_use]
    pub const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source.transport_media_id()
    }

    #[must_use]
    pub fn source(&self) -> &TransportSourceKey {
        &self.source
    }

    #[must_use]
    pub fn is_single_room(&self) -> bool {
        self.consumer_session_key.room_instance_id() == self.source.room_instance_id()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerPacketGateUpdate {
    route: TransportConsumerRoute,
    packet_gate: SourcePacketGate,
}

impl ConsumerPacketGateUpdate {
    #[must_use]
    pub fn new(route: TransportConsumerRoute, packet_gate: SourcePacketGate) -> Self {
        Self { route, packet_gate }
    }

    #[must_use]
    pub fn route(&self) -> &TransportConsumerRoute {
        &self.route
    }

    #[must_use]
    pub fn packet_gate(&self) -> &SourcePacketGate {
        &self.packet_gate
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverBweTargetUpdate {
    session_key: TransportSessionKey,
    target: Bitrate,
}

impl ReceiverBweTargetUpdate {
    #[must_use]
    pub fn new(session_key: TransportSessionKey, target: Bitrate) -> Self {
        Self {
            session_key,
            target,
        }
    }

    #[must_use]
    pub fn session_key(&self) -> &TransportSessionKey {
        &self.session_key
    }

    #[must_use]
    pub const fn target(&self) -> Bitrate {
        self.target
    }

    pub const fn set_target(&mut self, target: Bitrate) {
        self.target = target;
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
    pub max_bitrate: Option<Bitrate>,
    pub resolution_scale: Option<u16>,
    pub max_framerate: Option<u16>,
}
