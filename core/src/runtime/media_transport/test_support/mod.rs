use std::{collections::BTreeSet, sync::Arc, time::Instant};

mod fake_transport;

#[cfg(any(test, feature = "testing-transport"))]
pub use fake_transport::{FakeMediaTransport, FakeMediaTransportEvent};
#[cfg(any(test, feature = "testing-transport"))]
use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
#[cfg(any(test, feature = "testing-transport"))]
use str0m::media::Mid;

#[cfg(any(test, feature = "testing-transport"))]
use super::shard_set::RtcTransportShardSet;
use super::{MediaTransportBackend, runtime_adapter::MediaTransport};
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::RoomInstanceId;
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::rtc_engine::test_support::DebugRouteEntry;
#[cfg(any(test, feature = "testing-transport"))]
use crate::transport::SourcePolicyUpdateSubscription;
#[cfg(any(test, feature = "testing-transport"))]
use crate::transport::TransportSessionHealth;
#[cfg(any(test, feature = "testing-transport"))]
use crate::transport::{
    ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
    ReceiverBandwidthSnapshot, TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};
#[cfg(any(test, feature = "testing-transport"))]
use crate::transport::{
    ConsumerActivity, MediaPort, NegotiationPort, ObservabilityPort, ProducerActivity,
    SessionOffer, SessionPort, SourcePacketGate, SourcePolicyPort, TransportAdapterError,
};

#[cfg(any(test, feature = "testing-transport"))]
impl NegotiationPort for FakeMediaTransport {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        Self::create_initial_session_offer(self, session_key).await
    }

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        Self::create_session_renegotiation_offer(self, session_key).await
    }

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        Self::apply_session_answer(self, session_key, answer_sdp).await
    }

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        Self::project_answered_client_rtp_capabilities(answer_sdp, offered_router_capabilities)
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl SessionPort for FakeMediaTransport {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        Self::close_session(self, session_key).await
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl MediaPort for FakeMediaTransport {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        Self::remove_media(self, session_key, transport_media_id).await
    }

    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        Self::publish_media(self, session_key, media_kind, rtp_parameters).await
    }

    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        Self::consume_media(
            self,
            consumer_session_key,
            media_kind,
            source_session_key,
            source_media_id,
            consumer_rtp_parameters,
        )
        .await
    }

    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        Self::set_producer_active(self, session_key, transport_media_id, activity.is_active()).await
    }

    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        activity: ConsumerActivity,
    ) -> Result<(), TransportAdapterError> {
        Self::set_consumer_active(
            self,
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            activity.is_active(),
        )
        .await
    }

    async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        Self::set_consumer_packet_gate(
            self,
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            packet_gate,
        )
        .await
    }

    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        Self::request_consumer_keyframe(
            self,
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
        )
        .await
    }

    async fn transport_media_mid(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
    ) -> Option<String> {
        None
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl ObservabilityPort for FakeMediaTransport {
    fn transport_bitrate_snapshot(
        &self,
        _session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        TransportBitrateSnapshot::default()
    }

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        Self::receiver_bandwidth_snapshot(self, session_keys)
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        Self::active_speaker_source_snapshot(self).await
    }

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        Self::active_speaker_diagnostic_snapshot(self).await
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        None
    }

    async fn expired_active_speaker_room_instance_ids(
        &self,
        _now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        BTreeSet::new()
    }

    fn session_transport_health(
        &self,
        _session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        None
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl SourcePolicyPort for FakeMediaTransport {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.source_policy_signal().subscribe()
    }
}

impl MediaTransport {
    #[cfg(any(test, feature = "testing-transport"))]
    #[allow(
        dead_code,
        reason = "the fake transport remains available only for deterministic test and feature-gated development workflows"
    )]
    #[must_use]
    pub fn fake_for_testing() -> Self {
        Self::from_fake_transport(Arc::new(FakeMediaTransport::default()))
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[allow(
        dead_code,
        reason = "targeted tests still need to inject a preconfigured fake media transport instance"
    )]
    #[must_use]
    pub fn from_fake_transport(transport: Arc<FakeMediaTransport>) -> Self {
        Self {
            backend: MediaTransportBackend::Fake(transport),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn as_fake_transport(&self) -> Option<&Arc<FakeMediaTransport>> {
        match &self.backend {
            MediaTransportBackend::Rtc(_) => None,
            MediaTransportBackend::Fake(transport) => Some(transport),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        match &self.backend {
            MediaTransportBackend::Rtc(transport) => {
                transport
                    .shards()
                    .shard_for_user(session_key)
                    .media()
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
            MediaTransportBackend::Fake(transport) => {
                transport
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
        }
    }

    #[cfg(test)]
    pub(super) fn as_rtc_shard_set(&self) -> Option<&Arc<RtcTransportShardSet>> {
        match &self.backend {
            MediaTransportBackend::Rtc(transport) => Some(transport.shards()),
            MediaTransportBackend::Fake(_) => None,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let MediaTransportBackend::Rtc(adapter) = &self.backend {
            adapter
                .shards()
                .shard_for_user(session_key)
                .debug_set_session_transport_health(session_key, health);
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[allow(
        dead_code,
        reason = "test-only route inspection stays available for targeted RTC engine assertions even when one edit removes its current call sites"
    )]
    pub async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        match self {
            Self {
                backend: MediaTransportBackend::Fake(_),
            } => None,
            Self {
                backend: MediaTransportBackend::Rtc(adapter),
            } => {
                adapter
                    .shards()
                    .debug_route_entry(source_session_key, source_mid)
                    .await
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        match self {
            Self {
                backend: MediaTransportBackend::Fake(_),
            } => None,
            Self {
                backend: MediaTransportBackend::Rtc(adapter),
            } => {
                adapter
                    .shards()
                    .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
                    .await
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[allow(
        dead_code,
        reason = "test-only route inspection stays available for targeted RTC engine assertions even when one edit removes its current call sites"
    )]
    pub async fn debug_route_entry_by_media_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        match self {
            Self {
                backend: MediaTransportBackend::Fake(_),
            } => None,
            Self {
                backend: MediaTransportBackend::Rtc(adapter),
            } => {
                adapter
                    .shards()
                    .debug_route_entry_by_media_id(source_transport_media_id)
                    .await
            }
        }
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl RtcTransportShardSet {
    pub(super) async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.shard_for_user(source_session_key)
            .debug_route_entry(source_session_key, source_mid)
            .await
    }

    pub(super) async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        for shard in self.all_shards() {
            if let Some(entry) = shard
                .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
                .await
            {
                return Some(entry);
            }
        }
        None
    }

    pub(super) async fn debug_route_entry_by_media_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        for shard in self.all_shards() {
            if let Some(entry) = shard
                .debug_route_entry_by_media_id(source_transport_media_id)
                .await
            {
                return Some(entry);
            }
        }
        None
    }
}
