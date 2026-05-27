use std::time::Instant;
#[cfg(test)]
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

#[cfg(test)]
use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::Mid;

#[cfg(test)]
use super::TransportAdapterError;
use super::{MediaTransport, TransportMediaId, TransportSessionHealth, TransportSessionKey};
#[cfg(test)]
use super::{MediaTransportBuilder, MediaTransportConfig, MediaTransportDeps};
#[cfg(test)]
use crate::runtime::rtc_engine::RtcWorker;
use crate::runtime::rtc_engine::test_support::DebugRouteEntry;
#[cfg(test)]
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    VideoBitrateLimits,
    runtime::{
        diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

#[derive(Debug, Clone, Copy)]
pub struct MediaTransportTestApi<'a> {
    transport: &'a MediaTransport,
}

impl MediaTransport {
    #[must_use]
    pub fn test_api(&self) -> MediaTransportTestApi<'_> {
        MediaTransportTestApi { transport: self }
    }
}

impl MediaTransportTestApi<'_> {
    #[cfg(test)]
    pub(crate) async fn negotiated_producer_parameters(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.transport
            .worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .negotiated_producer_parameters(session_key, transport_media_id)
            .await
    }

    #[cfg(test)]
    pub(super) fn worker_for_user(
        self,
        session_key: &TransportSessionKey,
    ) -> Option<Arc<RtcWorker>> {
        self.transport.worker_for_user(session_key)
    }

    /// Overrides a real RTC session health snapshot in test builds.
    ///
    /// This is a route-test hook for failure injection and is not a production
    /// control-plane operation.
    pub fn set_session_transport_health(
        self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let Some(worker) = self.transport.worker_for_user(session_key) {
            worker.debug_set_session_transport_health(session_key, health);
        }
    }

    pub async fn route_entry(
        self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.transport
            .worker_for_user(source_session_key)?
            .debug_route_entry(source_session_key, source_mid)
            .await
    }

    /// Inspects a real RTC route by consumer mid in test builds.
    ///
    /// This is exposed for integration assertions that need to prove routing
    /// state without exposing worker internals to production callers.
    pub async fn route_entry_by_consumer_mid(
        self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        for worker in self.transport.all_workers() {
            if let Some(entry) = worker
                .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
                .await
            {
                return Some(entry);
            }
        }
        None
    }

    pub async fn route_entry_by_media_id(
        self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        for worker in self.transport.all_workers() {
            if let Some(entry) = worker
                .debug_route_entry_by_media_id(source_transport_media_id)
                .await
            {
                return Some(entry);
            }
        }
        None
    }

    pub async fn observe_audio_activity(self, transport_media_id: TransportMediaId, now: Instant) {
        self.observe_audio_activity_with_level(transport_media_id, -20, now)
            .await;
    }

    pub async fn observe_audio_activity_with_level(
        self,
        transport_media_id: TransportMediaId,
        audio_level_dbov: i8,
        now: Instant,
    ) {
        for worker in self.transport.all_workers() {
            worker
                .debug_observe_audio_activity(
                    transport_media_id,
                    Some(true),
                    Some(audio_level_dbov),
                    now,
                )
                .await;
        }
    }
}

#[cfg(test)]
pub(crate) fn test_media_transport_builder(rtc_port_range: RtcPortRange) -> MediaTransportBuilder {
    MediaTransport::builder()
        .transport_config(MediaTransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(
                Bitrate::from_mbps(8),
                Bitrate::from_mbps(10),
            ),
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_port_range,
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: CodecPreferences::default(),
            media_quality_interval: None,
        })
        .deps(MediaTransportDeps {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
        })
}
