use std::sync::Arc;

use super::facade::RuntimeTransportAdapter;
pub(crate) use super::fake::FakeWebRtcAdapter;
#[cfg(test)]
pub(crate) use super::fake::FakeWebRtcEvent;
#[cfg(test)]
use super::shard_set::RtcTransportAdapterShardSet;
#[cfg(test)]
use super::types::{TransportMediaId, TransportSessionKey};
#[cfg(test)]
use crate::runtime::rtc_adapter::TransportSessionHealth;
#[cfg(test)]
use crate::runtime::rtc_adapter::test_support::DebugRouteEntry;
#[cfg(test)]
use str0m::media::Mid;

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
