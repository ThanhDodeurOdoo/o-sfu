use std::sync::Arc;

use super::facade::RuntimeTransportAdapter;
pub(crate) use super::fake::FakeWebRtcAdapter;
#[cfg(test)]
pub(crate) use super::fake::FakeWebRtcEvent;
#[cfg(test)]
use super::types::{TransportAdapterError, TransportMediaId, TransportSessionKey};
#[cfg(test)]
use crate::runtime::rtc_adapter::{TransportSessionHealth, test_support::DebugRouteEntry};
#[cfg(test)]
use crate::runtime::transport_bootstrap::SessionTransportBootstrap;
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
        Self::Fake(Arc::new(FakeWebRtcAdapter::default()))
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[allow(
        dead_code,
        reason = "targeted tests still need to inject a preconfigured fake transport adapter instance"
    )]
    #[must_use]
    pub(crate) fn from_fake_adapter(adapter: Arc<FakeWebRtcAdapter>) -> Self {
        Self::Fake(adapter)
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
            Self::Fake(adapter) => {
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
            Self::Fake(_) => None,
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
            Self::Fake(_) => None,
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
            Self::Fake(_) => None,
            Self::Rtc(adapter) => {
                adapter
                    .debug_route_entry_by_media_id(source_transport_media_id)
                    .await
            }
        }
    }
}
