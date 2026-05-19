#[cfg(test)]
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::Mid;

#[cfg(test)]
use super::TransportAdapterError;
use super::{
    MediaTransport, TransportMediaId, TransportSessionHealth, TransportSessionKey,
    worker_manager::RtcWorkerManager,
};
use crate::runtime::rtc_engine::test_support::DebugRouteEntry;

impl MediaTransport {
    #[cfg(test)]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.worker_manager()
            .worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .media()
            .negotiated_producer_parameters(session_key, transport_media_id)
            .await
    }

    #[cfg(test)]
    pub(super) fn as_rtc_worker_manager(&self) -> &Arc<RtcWorkerManager> {
        self.worker_manager()
    }

    /// Overrides a real RTC session health snapshot in test builds.
    ///
    /// This is a route-test hook for failure injection and is not a production
    /// control-plane operation.
    pub fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let Some(worker) = self.worker_manager().worker_for_user(session_key) {
            worker.debug_set_session_transport_health(session_key, health);
        }
    }

    pub async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.worker_manager()
            .debug_route_entry(source_session_key, source_mid)
            .await
    }

    /// Inspects a real RTC route by consumer mid in test builds.
    ///
    /// This is exposed for integration assertions that need to prove routing
    /// state without exposing worker internals to production callers.
    pub async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.worker_manager()
            .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
            .await
    }

    pub async fn debug_route_entry_by_media_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        self.worker_manager()
            .debug_route_entry_by_media_id(source_transport_media_id)
            .await
    }

    pub async fn debug_observe_audio_activity(
        &self,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) {
        self.worker_manager()
            .debug_observe_audio_activity(transport_media_id, now)
            .await;
    }
}

impl RtcWorkerManager {
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

    async fn debug_observe_audio_activity(
        &self,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) {
        for worker in self.all_workers() {
            worker
                .debug_observe_audio_activity(transport_media_id, Some(true), Some(-20), now)
                .await;
        }
    }
}
