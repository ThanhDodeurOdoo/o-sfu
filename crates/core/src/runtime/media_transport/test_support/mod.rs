use std::sync::Arc;

mod fake_transport;

#[cfg(any(test, feature = "testing-transport"))]
pub use fake_transport::{FakeMediaTransport, FakeMediaTransportEvent};
#[cfg(test)]
use o_sfu_router::MediaStream as RouterRtpParameters;
#[cfg(any(test, feature = "testing-transport"))]
use str0m::media::Mid;

#[cfg(any(test, feature = "testing-transport"))]
use super::worker_manager::RtcWorkerManager;
use super::{Backend, MediaTransport};
#[cfg(test)]
use super::{TransportAdapterError, TransportMediaId};
#[cfg(any(test, feature = "testing-transport"))]
use super::{TransportSessionHealth, TransportSessionKey};
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::rtc_engine::test_support::DebugRouteEntry;

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
            backend: Backend::Fake(transport),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn as_fake_transport(&self) -> Option<&Arc<FakeMediaTransport>> {
        match &self.backend {
            Backend::Rtc(_) => None,
            Backend::Fake(transport) => Some(transport),
        }
    }

    #[cfg(test)]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        match &self.backend {
            Backend::Rtc(transport) => {
                transport
                    .worker_manager()
                    .worker_for_user(session_key)
                    .ok_or(TransportAdapterError::TransportUnavailable)?
                    .media()
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
            Backend::Fake(transport) => {
                transport
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
        }
    }

    #[cfg(test)]
    pub(super) fn as_rtc_worker_manager(&self) -> Option<&Arc<RtcWorkerManager>> {
        match &self.backend {
            Backend::Rtc(transport) => Some(transport.worker_manager()),
            Backend::Fake(_) => None,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let Backend::Rtc(adapter) = &self.backend
            && let Some(worker) = adapter.worker_manager().worker_for_user(session_key)
        {
            worker.debug_set_session_transport_health(session_key, health);
        }
    }

    #[cfg(test)]
    pub async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        match &self.backend {
            Backend::Fake(_) => None,
            Backend::Rtc(adapter) => {
                adapter
                    .worker_manager()
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
        match &self.backend {
            Backend::Fake(_) => None,
            Backend::Rtc(adapter) => {
                adapter
                    .worker_manager()
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
        match &self.backend {
            Backend::Fake(_) => None,
            Backend::Rtc(adapter) => {
                adapter
                    .worker_manager()
                    .debug_route_entry_by_media_id(source_transport_media_id)
                    .await
            }
        }
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl RtcWorkerManager {
    #[cfg(test)]
    pub(super) async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.worker_for_user(source_session_key)?
            .debug_route_entry(source_session_key, source_mid)
            .await
    }

    pub(super) async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        for worker in self.all_workers() {
            if let Some(entry) = worker
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
        for worker in self.all_workers() {
            if let Some(entry) = worker
                .debug_route_entry_by_media_id(source_transport_media_id)
                .await
            {
                return Some(entry);
            }
        }
        None
    }
}
