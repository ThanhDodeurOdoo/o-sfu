use std::{collections::BTreeSet, sync::Arc, time::Instant};

#[cfg(any(test, feature = "testing-transport"))]
use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
#[cfg(test)]
use str0m::media::Mid;

pub(crate) use super::fake::FakeWebRtcAdapter;
#[cfg(test)]
pub(crate) use super::fake::FakeWebRtcEvent;
#[cfg(test)]
use super::shard_set::RtcTransportAdapterShardSet;
#[cfg(any(test, feature = "testing-transport"))]
use super::types::{
    ActiveSpeakerSource, TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};
#[cfg(any(test, feature = "testing-transport"))]
use super::types::{SessionOffer, SourcePacketGate, TransportAdapterError};
use super::{
    ports::{MediaPort, NegotiationPort, ObservabilityPort, SessionPort, SourcePolicyPort},
    runtime_adapter::RuntimeTransportAdapter,
};
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::ChannelInstanceId;
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::rtc_adapter::TransportSessionHealth;
#[cfg(test)]
use crate::runtime::rtc_adapter::test_support::DebugRouteEntry;
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::transport_adapter::SourcePolicyUpdateSubscription;

#[cfg(any(test, feature = "testing-transport"))]
impl NegotiationPort for FakeWebRtcAdapter {
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
    ) -> Result<(), TransportAdapterError> {
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
impl SessionPort for FakeWebRtcAdapter {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        Self::close_session(self, session_key).await
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl MediaPort for FakeWebRtcAdapter {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        Self::remove_media(self, session_key, transport_media_id).await
    }

    async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        Self::negotiated_producer_parameters(self, session_key, transport_media_id).await
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
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        Self::set_producer_active(self, session_key, transport_media_id, active).await
    }

    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        Self::set_consumer_active(
            self,
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            active,
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

    async fn set_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        Self::set_source_packet_gate(
            self,
            source_session_key,
            source_transport_media_id,
            packet_gate,
        )
        .await
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl ObservabilityPort for FakeWebRtcAdapter {
    fn transport_bitrate_snapshot(
        &self,
        _session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        TransportBitrateSnapshot::default()
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        Self::active_speaker_source_snapshot(self).await
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        None
    }

    async fn expired_active_speaker_channel_instance_ids(
        &self,
        _now: Instant,
    ) -> BTreeSet<ChannelInstanceId> {
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
impl SourcePolicyPort for FakeWebRtcAdapter {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.source_policy_signal().subscribe()
    }
}

impl RuntimeTransportAdapter {
    #[cfg(any(test, feature = "testing-transport"))]
    #[allow(
        dead_code,
        reason = "the fake transport remains available only for deterministic test and feature-gated development workflows"
    )]
    #[must_use]
    pub(crate) fn fake_for_testing() -> Self {
        Self::Test(Arc::new(FakeWebRtcAdapter::default()))
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[allow(
        dead_code,
        reason = "targeted tests still need to inject a preconfigured fake transport adapter instance"
    )]
    #[must_use]
    pub(crate) fn from_fake_adapter(adapter: Arc<FakeWebRtcAdapter>) -> Self {
        Self::Test(adapter)
    }

    #[cfg(test)]
    pub(crate) fn as_fake_adapter(&self) -> Option<&Arc<FakeWebRtcAdapter>> {
        match self {
            Self::Rtc(_) => None,
            Self::Test(adapter) => Some(adapter),
        }
    }

    #[cfg(test)]
    pub(crate) fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let Self::Rtc(adapter) = self {
            adapter
                .shard_for_session(session_key)
                .debug_set_session_transport_health(session_key, health);
        }
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "test-only route inspection stays available for targeted RTC adapter assertions even when one edit removes its current call sites"
    )]
    pub(crate) async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Test(_) => None,
            Self::Rtc(adapter) => {
                adapter
                    .debug_route_entry(source_session_key, source_mid)
                    .await
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Test(_) => None,
            Self::Rtc(adapter) => {
                adapter
                    .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
                    .await
            }
        }
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "test-only route inspection stays available for targeted RTC adapter assertions even when one edit removes its current call sites"
    )]
    pub(crate) async fn debug_route_entry_by_media_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Test(_) => None,
            Self::Rtc(adapter) => {
                adapter
                    .debug_route_entry_by_media_id(source_transport_media_id)
                    .await
            }
        }
    }
}

#[cfg(test)]
impl RtcTransportAdapterShardSet {
    pub(super) async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.shard_for_session(source_session_key)
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
