use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Instant,
};

use crate::{
    config::{MediaCodecFlags, RtcPortRange},
    runtime::{
        metrics::RuntimeMetrics,
        recording::MediaTap,
        transport_adapter::{
            RtcTransportAdapterConfig, TransportAdapterError, TransportConnectDirection,
            TransportMediaId, TransportSessionKey,
        },
    },
};
use str0m::media::Mid;
use tokio::sync::oneshot;

use super::super::{
    commands::{DebugRouteEntry, DebugRtcCommand, RtcWorkerCommand},
    state::{TransportLifecycleState, TransportSessionHealth, TransportStateKey},
};
use super::facade::RtcTransportAdapter;

impl RtcTransportAdapter {
    pub(super) fn mark_bootstrap_sent(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let Ok(mut states) = self.transport_states.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        for direction in [
            TransportConnectDirection::Upload,
            TransportConnectDirection::Download,
        ] {
            states.insert(
                TransportStateKey {
                    session_key: session_key.clone(),
                    direction,
                },
                TransportLifecycleState::BootstrapSent,
            );
        }
        Ok(())
    }

    pub(super) fn ensure_connect_transition(
        &self,
        session_key: &TransportSessionKey,
        direction: TransportConnectDirection,
    ) -> Result<(), TransportAdapterError> {
        let key = TransportStateKey {
            session_key: session_key.clone(),
            direction,
        };
        let Ok(states) = self.transport_states.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        match states.get(&key) {
            Some(TransportLifecycleState::BootstrapSent) => Ok(()),
            Some(TransportLifecycleState::Connected) => Err(TransportAdapterError::InvalidInput),
            None => Err(TransportAdapterError::TransportUnavailable),
        }
    }

    pub(super) fn mark_connected(
        &self,
        session_key: &TransportSessionKey,
        direction: TransportConnectDirection,
    ) -> Result<(), TransportAdapterError> {
        let key = TransportStateKey {
            session_key: session_key.clone(),
            direction,
        };
        let Ok(mut states) = self.transport_states.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let Some(state) = states.get_mut(&key) else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        *state = TransportLifecycleState::Connected;
        Ok(())
    }

    pub(crate) fn debug_set_session_transport_health(
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
        self.metrics
            .record_transport_health_transition(previous, Some(health));
    }

    async fn request_debug_worker<T, F>(&self, build_command: F) -> Option<T>
    where
        F: FnOnce(oneshot::Sender<T>) -> DebugRtcCommand,
    {
        let worker_handle = self.ensure_packet_loop_started().ok()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(build_command(response_tx)))
            .await
            .ok()?;
        response_rx.await.ok()
    }

    pub(crate) async fn debug_resolve_mid(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<Mid> {
        self.request_debug_worker(|response| DebugRtcCommand::ResolveMid {
            transport_media_id,
            response,
        })
        .await
        .flatten()
    }

    pub(crate) async fn debug_remote_addr_owner(
        &self,
        source_addr: SocketAddr,
    ) -> Option<TransportSessionKey> {
        self.request_debug_worker(|response| DebugRtcCommand::RemoteAddrOwner {
            source_addr,
            response,
        })
        .await
        .flatten()
    }

    pub(crate) async fn debug_has_any_remote_addr_session(&self) -> bool {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return false;
        };
        let (response_tx, response_rx) = oneshot::channel();
        if worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(
                DebugRtcCommand::HasAnyRemoteAddrSession {
                    response: response_tx,
                },
            ))
            .await
            .is_err()
        {
            return false;
        }
        response_rx.await.unwrap_or(false)
    }

    pub(crate) async fn debug_remember_remote_addr(
        &self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) {
        let _ = self
            .request_debug_worker(|response| DebugRtcCommand::RememberRemoteAddr {
                source_addr,
                session_key: session_key.clone(),
                response,
            })
            .await;
    }

    pub(crate) async fn debug_session_stream_rx_ssrc(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<u32> {
        self.request_debug_worker(|response| DebugRtcCommand::SessionStreamRxSsrc {
            session_key: session_key.clone(),
            mid,
            response,
        })
        .await
        .flatten()
    }

    pub(crate) async fn debug_session_stream_tx_ssrc(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<u32> {
        self.request_debug_worker(|response| DebugRtcCommand::SessionStreamTxSsrc {
            session_key: session_key.clone(),
            mid,
            response,
        })
        .await
        .flatten()
    }

    pub(crate) async fn debug_remote_source_owner(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<TransportSessionKey> {
        self.request_debug_worker(|response| DebugRtcCommand::RemoteSourceOwner {
            source_transport_media_id,
            response,
        })
        .await
        .flatten()
    }

    pub(crate) async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.request_debug_worker(|response| DebugRtcCommand::RouteEntry {
            source_session_key: source_session_key.clone(),
            source_mid,
            response,
        })
        .await
        .flatten()
    }

    pub(crate) async fn debug_route_entry_by_consumer_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.request_debug_worker(|response| DebugRtcCommand::RouteEntryByConsumerMid {
            consumer_session_key: consumer_session_key.clone(),
            consumer_mid,
            response,
        })
        .await
        .flatten()
    }

    pub(crate) async fn debug_route_entry_by_media_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        self.request_debug_worker(|response| DebugRtcCommand::RouteEntryByMediaId {
            source_transport_media_id,
            response,
        })
        .await
        .flatten()
    }

    pub(crate) async fn debug_record_incoming_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
        now: Instant,
    ) {
        let _ = self
            .request_debug_worker(|response| DebugRtcCommand::RecordIncomingMedia {
                session_key: session_key.clone(),
                transport_media_id,
                payload_bytes,
                now,
                response,
            })
            .await;
    }

    pub(crate) async fn debug_observe_audio_activity(
        &self,
        transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) {
        let _ = self
            .request_debug_worker(|response| DebugRtcCommand::ObserveAudioActivity {
                transport_media_id,
                voice_activity,
                audio_level_dbov,
                now,
                response,
            })
            .await;
    }

    pub(crate) fn debug_activate_relay_route(
        &self,
        source_transport_media_id: TransportMediaId,
        target: &Self,
    ) -> Result<(), TransportAdapterError> {
        self.activate_relay_route(source_transport_media_id, target)
    }

    pub(crate) fn debug_deactivate_relay_route(
        &self,
        source_transport_media_id: TransportMediaId,
        target: &Self,
    ) {
        self.deactivate_relay_route(source_transport_media_id, target);
    }

    pub(crate) fn debug_relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.relay_registry
            .target_count_for_source(source_transport_media_id)
    }

    pub(crate) fn debug_active_relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.relay_registry
            .active_target_count_for_source(source_transport_media_id)
    }
}

impl Default for RtcTransportAdapter {
    fn default() -> Self {
        Self::new(&RtcTransportAdapterConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(40_000, 49_999),
            MediaCodecFlags::default(),
            Arc::new(MediaTap::default()),
            Arc::new(RuntimeMetrics::default()),
        ))
    }
}
