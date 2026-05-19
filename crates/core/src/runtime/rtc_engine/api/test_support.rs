#[cfg(test)]
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
#[cfg(test)]
use std::sync::Arc;
use std::time::Instant;

use str0m::media::Mid;
use tokio::sync::oneshot;

use super::{
    super::{
        state::TransportSessionHealth,
        test_support::{DebugRouteEntry, DebugRtcWorkerCommand},
    },
    facade::RtcWorker,
};
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::media_transport::TransportMediaId;
use crate::runtime::{media_transport::TransportSessionKey, metrics};
#[cfg(test)]
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    runtime::{
        diagnostics::DiagnosticsStore,
        media_transport::{MediaTransportConfig, MediaTransportDeps, SourcePolicySignal},
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

impl RtcWorker {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn test_builder() -> RtcWorkerTestBuilder {
        RtcWorkerTestBuilder::default()
    }

    pub fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return;
        };
        let Ok(mut snapshot_state) = worker_handle.snapshot_state.lock() else {
            return;
        };
        let previous = snapshot_state.set_transport_health(session_key, health);
        self.metrics.record_transport_health_transition(
            previous.map(metrics::transport_health_state),
            Some(metrics::transport_health_state(health)),
        );
    }

    async fn request_debug_worker<T, F>(&self, build_command: F) -> Option<T>
    where
        F: FnOnce(oneshot::Sender<T>) -> DebugRtcWorkerCommand,
    {
        let worker_handle = self.ensure_packet_loop_started().ok()?;
        worker_handle.debug_handle.request(build_command).await
    }

    #[cfg(test)]
    pub async fn debug_resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::ResolveMid {
            transport_media_id,
            response,
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_remote_addr_owner(
        &self,
        source_addr: SocketAddr,
    ) -> Option<TransportSessionKey> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RemoteAddrOwner {
            source_addr,
            response,
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_has_any_remote_addr_session(&self) -> bool {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::HasAnyRemoteAddrSession {
            response,
        })
        .await
        .unwrap_or(false)
    }

    #[cfg(test)]
    pub async fn debug_remember_remote_addr(
        &self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) {
        let _ = self
            .request_debug_worker(|response| DebugRtcWorkerCommand::RememberRemoteAddr {
                source_addr,
                session_key: session_key.clone(),
                response,
            })
            .await;
    }

    #[cfg(test)]
    pub async fn debug_session_stream_rx_ssrc(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<u32> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::SessionStreamRxSsrc {
            session_key: session_key.clone(),
            mid,
            response,
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_session_stream_tx_ssrc(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<u32> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::SessionStreamTxSsrc {
            session_key: session_key.clone(),
            mid,
            response,
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_session_max_bitrate_in(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<Bitrate> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::SessionMaxBitrateIn {
            session_key: session_key.clone(),
            response,
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_session_max_bitrate_out(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<Bitrate> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::SessionMaxBitrateOut {
            session_key: session_key.clone(),
            response,
        })
        .await
        .flatten()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RouteEntry {
            source_session_key: source_session_key.clone(),
            source_mid,
            response,
        })
        .await
        .flatten()
    }

    pub async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RouteEntryByConsumerMid {
            consumer_session_key: consumer_session_key.clone(),
            consumer_mid,
            response,
        })
        .await
        .flatten()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_route_entry_by_media_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RouteEntryByMediaId {
            source_transport_media_id,
            response,
        })
        .await
        .flatten()
    }

    #[cfg(test)]
    pub async fn debug_record_incoming_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
        now: Instant,
    ) {
        let _ = self
            .request_debug_worker(|response| DebugRtcWorkerCommand::RecordIncomingMedia {
                session_key: session_key.clone(),
                transport_media_id,
                payload_bytes,
                now,
                response,
            })
            .await;
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn debug_observe_audio_activity(
        &self,
        transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) {
        let _ = self
            .request_debug_worker(|response| DebugRtcWorkerCommand::ObserveAudioActivity {
                transport_media_id,
                voice_activity,
                audio_level_dbov,
                now,
                response,
            })
            .await;
    }

    #[cfg(test)]
    pub async fn debug_relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RelayTargetCount {
            source_transport_media_id,
            response,
        })
        .await
        .unwrap_or(0)
    }

    #[cfg(test)]
    pub async fn debug_active_relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::ActiveRelayTargetCount {
            source_transport_media_id,
            response,
        })
        .await
        .unwrap_or(0)
    }
}

#[cfg(test)]
pub(crate) struct RtcWorkerTestBuilder {
    max_bitrate_in: Bitrate,
    max_bitrate_out: Bitrate,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
}

#[cfg(test)]
impl RtcWorkerTestBuilder {
    #[must_use]
    pub(crate) fn bitrate_limits(
        mut self,
        max_bitrate_in: Bitrate,
        max_bitrate_out: Bitrate,
    ) -> Self {
        self.max_bitrate_in = max_bitrate_in;
        self.max_bitrate_out = max_bitrate_out;
        self
    }

    #[must_use]
    pub(crate) fn port_range(mut self, rtc_port_range: RtcPortRange) -> Self {
        self.rtc_port_range = rtc_port_range;
        self
    }

    #[must_use]
    pub(crate) fn codec_flags(mut self, codec_flags: MediaCodecFlags) -> Self {
        self.codec_flags = codec_flags;
        self
    }

    #[must_use]
    pub(crate) fn codec_policy(
        mut self,
        codec_flags: MediaCodecFlags,
        codec_preferences: CodecPreferences,
    ) -> Self {
        self.codec_flags = codec_flags;
        self.codec_preferences = codec_preferences;
        self
    }

    #[must_use]
    pub(crate) fn build(self) -> RtcWorker {
        RtcWorker::new(
            &MediaTransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                bitrate_limits: SessionBitrateLimits::new(
                    self.max_bitrate_in,
                    self.max_bitrate_out,
                ),
                video_bitrate_limits: crate::VideoBitrateLimits::default(),
                rtc_port_range: self.rtc_port_range,
                codec_flags: self.codec_flags,
                codec_preferences: self.codec_preferences,
            },
            &MediaTransportDeps {
                diagnostics: Arc::new(DiagnosticsStore::default()),
                packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
                metrics: Arc::new(RuntimeMetrics::default()),
            },
            Arc::new(SourcePolicySignal::default()),
            0,
            0,
        )
    }
}

#[cfg(test)]
impl Default for RtcWorkerTestBuilder {
    fn default() -> Self {
        Self {
            max_bitrate_in: Bitrate::from_mbps(8),
            max_bitrate_out: Bitrate::from_mbps(10),
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: CodecPreferences::default(),
        }
    }
}

#[cfg(test)]
impl Default for RtcWorker {
    fn default() -> Self {
        Self::test_builder()
            .port_range(RtcPortRange::new(40_000, 49_999))
            .build()
    }
}
