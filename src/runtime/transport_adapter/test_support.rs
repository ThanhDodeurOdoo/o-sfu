use std::sync::Arc;

use super::facade::RuntimeTransportAdapter;
pub(crate) use super::fake::FakeWebRtcAdapter;
#[cfg(test)]
pub(crate) use super::fake::FakeWebRtcEvent;
#[cfg(test)]
use super::shard_set::RtcTransportAdapterShardSet;
use super::types::{ActiveSpeakerSource, SessionOffer, SourcePacketGate, TransportBitrateSnapshot};
#[cfg(any(test, feature = "testing-transport"))]
use super::types::{TransportAdapterError, TransportMediaId, TransportSessionKey};
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::rtc_adapter::TransportSessionHealth;
#[cfg(test)]
use crate::runtime::rtc_adapter::test_support::DebugRouteEntry;
#[cfg(test)]
use crate::runtime::transport_bootstrap::SessionTransportBootstrap;
use o_sfu_router::{MediaCapabilities, MediaKind, RtpParameters as RouterRtpParameters};
#[cfg(test)]
use str0m::media::Mid;

#[cfg(any(test, feature = "testing-transport"))]
#[derive(Debug)]
pub(crate) enum TestTransportBackend {
    Fake(Arc<FakeWebRtcAdapter>),
}

#[cfg(any(test, feature = "testing-transport"))]
impl TestTransportBackend {
    fn from_fake_adapter(adapter: Arc<FakeWebRtcAdapter>) -> Arc<Self> {
        Arc::new(Self::Fake(adapter))
    }

    fn as_fake_adapter(&self) -> &Arc<FakeWebRtcAdapter> {
        match self {
            Self::Fake(adapter) => adapter,
        }
    }

    pub(crate) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.as_fake_adapter()
            .create_initial_session_offer(session_key)
            .await
    }

    pub(crate) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.as_fake_adapter()
            .create_session_renegotiation_offer(session_key)
            .await
    }

    pub(crate) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        self.as_fake_adapter()
            .apply_session_answer(session_key, answer_sdp)
            .await
    }

    pub(crate) fn negotiated_client_rtp_capabilities(
        answer_sdp: &str,
        offered_router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        FakeWebRtcAdapter::project_answered_client_rtp_capabilities(
            answer_sdp,
            offered_router_capabilities,
        )
    }

    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.as_fake_adapter().close_session(session_key).await
    }

    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.as_fake_adapter()
            .remove_media(session_key, transport_media_id)
            .await
    }

    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.as_fake_adapter()
            .negotiated_producer_parameters(session_key, transport_media_id)
            .await
    }

    pub(crate) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.as_fake_adapter()
            .publish_media(session_key, media_kind, rtp_parameters)
            .await
    }

    pub(crate) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.as_fake_adapter()
            .consume_media(
                consumer_session_key,
                media_kind,
                source_session_key,
                consumer_rtp_parameters,
            )
            .await
    }

    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.as_fake_adapter()
            .set_producer_active(session_key, transport_media_id, active)
            .await
    }

    pub(crate) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.as_fake_adapter()
            .set_consumer_active(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                active,
            )
            .await
    }

    pub(crate) fn transport_media_mid(
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
    ) -> Option<String> {
        None
    }

    pub(crate) async fn set_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError> {
        self.as_fake_adapter()
            .set_source_packet_gate(source_session_key, source_transport_media_id, packet_gate)
            .await
    }

    pub(crate) fn transport_bitrate_snapshot(
        _session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        TransportBitrateSnapshot::default()
    }

    pub(crate) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.as_fake_adapter()
            .active_speaker_source_snapshot()
            .await
    }

    pub(crate) fn session_transport_health(
        _session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        None
    }

    #[cfg(test)]
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<SessionTransportBootstrap, TransportAdapterError> {
        self.as_fake_adapter()
            .transport_bootstrap_payload(session_key, router_capabilities)
            .await
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
        Self::Test(TestTransportBackend::from_fake_adapter(Arc::new(
            FakeWebRtcAdapter::default(),
        )))
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[allow(
        dead_code,
        reason = "targeted tests still need to inject a preconfigured fake transport adapter instance"
    )]
    #[must_use]
    pub(crate) fn from_fake_adapter(adapter: Arc<FakeWebRtcAdapter>) -> Self {
        Self::Test(TestTransportBackend::from_fake_adapter(adapter))
    }

    #[cfg(test)]
    pub(crate) fn as_fake_adapter(&self) -> Option<&Arc<FakeWebRtcAdapter>> {
        match self {
            Self::Rtc(_) => None,
            Self::Test(adapter) => Some(adapter.as_fake_adapter()),
        }
    }

    /// Build session transport bootstrap state for transport tests and benchmarks.
    #[cfg(test)]
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<SessionTransportBootstrap, TransportAdapterError> {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Test(adapter) => {
                adapter
                    .transport_bootstrap_payload(session_key, router_capabilities)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .transport_bootstrap_payload(session_key, router_capabilities)
                    .await
            }
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
