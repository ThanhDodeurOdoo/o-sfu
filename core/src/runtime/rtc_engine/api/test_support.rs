use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Instant,
};

use str0m::media::Mid;
use tokio::sync::oneshot;

use super::{
    super::{
        state::TransportSessionHealth,
        test_support::{DebugRouteEntry, DebugRtcWorkerCommand},
    },
    facade::RtcTransportShard,
};
use crate::{
    MediaCodecFlags, RtcPortRange,
    runtime::{
        diagnostics::DiagnosticsStore,
        media_transport::{
            MediaTransportDeps, RtcTransportConfig, SessionBitrateLimits, SourcePolicySignal,
            TransportMediaId, TransportSessionKey,
        },
        metrics::{self, RuntimeMetrics},
        packet_sink_registry::RoomPacketSinkRegistry as MediaTap,
    },
};

impl RtcTransportShard {
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

    pub async fn debug_resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::ResolveMid {
            transport_media_id,
            response,
        })
        .await
        .flatten()
    }

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

    pub async fn debug_has_any_remote_addr_session(&self) -> bool {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::HasAnyRemoteAddrSession {
            response,
        })
        .await
        .unwrap_or(false)
    }

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

    pub async fn debug_session_max_bitrate_in(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<u64> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::SessionMaxBitrateIn {
            session_key: session_key.clone(),
            response,
        })
        .await
        .flatten()
    }

    pub async fn debug_session_max_bitrate_out(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<u64> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::SessionMaxBitrateOut {
            session_key: session_key.clone(),
            response,
        })
        .await
        .flatten()
    }

    pub async fn debug_remote_source_owner(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<TransportSessionKey> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RemoteSourceOwner {
            source_transport_media_id,
            response,
        })
        .await
        .flatten()
    }

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

    pub fn debug_relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.relay_registry
            .target_count_for_source(source_transport_media_id)
    }

    pub fn debug_active_relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.relay_registry
            .active_target_count_for_source(source_transport_media_id)
    }
}

impl Default for RtcTransportShard {
    fn default() -> Self {
        Self::new(
            &RtcTransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                bitrate_limits: SessionBitrateLimits::new(8_000_000, 10_000_000),
                video_bitrate_limits: crate::VideoBitrateLimits::default(),
                rtc_port_range: RtcPortRange::new(40_000, 49_999),
                codec_flags: MediaCodecFlags::default(),
                codec_preferences: crate::CodecPreferences::default(),
            },
            &MediaTransportDeps {
                diagnostics: Arc::new(DiagnosticsStore::default()),
                packet_sink_registry: Arc::new(MediaTap::default()),
                metrics: Arc::new(RuntimeMetrics::default()),
            },
            Arc::new(SourcePolicySignal::default()),
            0,
        )
    }
}
