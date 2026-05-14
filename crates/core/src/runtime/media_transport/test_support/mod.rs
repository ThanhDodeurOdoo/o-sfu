use std::sync::Arc;

mod fake_transport;

#[cfg(any(test, feature = "testing-transport"))]
pub use fake_transport::{FakeMediaTransport, FakeMediaTransportEvent};
#[cfg(test)]
use o_sfu_router::MediaStream as RouterRtpParameters;
#[cfg(any(test, feature = "testing-transport"))]
use str0m::media::Mid;

use super::MediaTransport;
#[cfg(any(test, feature = "testing-transport"))]
use super::shard_set::RtcTransportShardSet;
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::rtc_engine::test_support::DebugRouteEntry;
#[cfg(test)]
use crate::transport::TransportAdapterError;
#[cfg(test)]
use crate::transport::TransportMediaId;
#[cfg(any(test, feature = "testing-transport"))]
use crate::transport::TransportSessionHealth;
#[cfg(any(test, feature = "testing-transport"))]
use crate::transport::TransportSessionKey;

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
        Self::Fake(transport)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn as_fake_transport(&self) -> Option<&Arc<FakeMediaTransport>> {
        match self {
            Self::Rtc(_) => None,
            Self::Fake(transport) => Some(transport),
        }
    }

    #[cfg(test)]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards()
                    .shard_for_user(session_key)
                    .media()
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
            Self::Fake(transport) => {
                transport
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
        }
    }

    #[cfg(test)]
    pub(super) fn as_rtc_shard_set(&self) -> Option<&Arc<RtcTransportShardSet>> {
        match self {
            Self::Rtc(transport) => Some(transport.shards()),
            Self::Fake(_) => None,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let Self::Rtc(adapter) = self {
            adapter
                .shards()
                .shard_for_user(session_key)
                .debug_set_session_transport_health(session_key, health);
        }
    }

    #[cfg(test)]
    pub async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        match self {
            Self::Fake(_) => None,
            Self::Rtc(adapter) => {
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
            Self::Fake(_) => None,
            Self::Rtc(adapter) => {
                adapter
                    .shards()
                    .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
                    .await
            }
        }
    }

    #[cfg(test)]
    pub async fn debug_route_entry_by_media_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        match self {
            Self::Fake(_) => None,
            Self::Rtc(adapter) => {
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
    #[cfg(test)]
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

    #[cfg(test)]
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
