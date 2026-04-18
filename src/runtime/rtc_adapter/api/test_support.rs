use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    config::{MediaCodecFlags, RtcPortRange},
    runtime::{
        metrics::RuntimeMetrics,
        recording::MediaTap,
        transport_adapter::{
            RtcTransportAdapterConfig, SessionOffer, SourcePacketGate, TransportAdapterError,
            TransportConnectDirection, TransportConnectRequest, TransportMediaId,
            TransportSessionKey,
        },
    },
};
use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::{MediaKind, Mid};
use tokio::sync::oneshot;
use tracing::debug;

use super::super::{
    commands::{
        RemoteSourceControl,
        debug::{DebugRouteEntry, DebugRtcWorkerCommand},
    },
    state::{
        TransportSessionHealth,
        test_support::{TransportLifecycleState, TransportStateKey},
    },
    validation,
};
use super::facade::{RtcTransportAdapter, RtcTransportMediaFacade, RtcTransportSessionFacade};

pub(super) type TransportLifecycleMirror =
    Arc<Mutex<BTreeMap<TransportStateKey, TransportLifecycleState>>>;

pub(super) fn new_transport_lifecycle_mirror() -> TransportLifecycleMirror {
    Arc::new(Mutex::new(BTreeMap::new()))
}

pub(super) fn mark_bootstrap_sent(
    adapter: &RtcTransportAdapter,
    session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    let Ok(mut states) = adapter.transport_states.lock() else {
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

pub(super) fn clear_session_transport_states(
    adapter: &RtcTransportAdapter,
    session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    let Ok(mut transport_states) = adapter.transport_states.lock() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    transport_states.retain(|key, _| key.session_key != *session_key);
    Ok(())
}

fn ensure_connect_transition(
    adapter: &RtcTransportAdapter,
    session_key: &TransportSessionKey,
    direction: TransportConnectDirection,
) -> Result<(), TransportAdapterError> {
    let key = TransportStateKey {
        session_key: session_key.clone(),
        direction,
    };
    let Ok(states) = adapter.transport_states.lock() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    match states.get(&key) {
        Some(TransportLifecycleState::BootstrapSent) => Ok(()),
        Some(TransportLifecycleState::Connected) => Err(TransportAdapterError::InvalidInput),
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}

fn mark_connected(
    adapter: &RtcTransportAdapter,
    session_key: &TransportSessionKey,
    direction: TransportConnectDirection,
) -> Result<(), TransportAdapterError> {
    let key = TransportStateKey {
        session_key: session_key.clone(),
        direction,
    };
    let Ok(mut states) = adapter.transport_states.lock() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let Some(state) = states.get_mut(&key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    *state = TransportLifecycleState::Connected;
    Ok(())
}

impl RtcTransportAdapter {
    pub(crate) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.negotiation()
            .create_initial_session_offer(session_key)
            .await
    }

    pub(crate) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.negotiation()
            .create_session_renegotiation_offer(session_key)
            .await
    }

    pub(crate) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        self.negotiation()
            .apply_session_answer(session_key, answer_sdp)
            .await
    }

    pub(crate) async fn connect_transport(
        &self,
        session_key: &TransportSessionKey,
        request: TransportConnectRequest<'_>,
    ) -> Result<(), TransportAdapterError> {
        if let Some(sdp_offer) = request.sdp_offer() {
            validation::validate_sdp_offer(sdp_offer)?;
        }
        let parsed_dtls_parameters = validation::parse_dtls_parameters(request.dtls_parameters())?;
        let remote_ice_credentials =
            validation::parse_remote_ice_credentials(request.ice_parameters())?;
        ensure_connect_transition(self, session_key, request.direction())?;
        debug!(
            direction = ?request.direction(),
            session_id = ?session_key.session_id(),
            media_worker_id = session_key.media_worker_id(),
            "validated DTLS parameters and transport lifecycle state before rtc transport connect"
        );
        self.request_worker(|response| {
            super::super::commands::RtcWorkerCommand::ConnectTransport {
                session_key: session_key.clone(),
                direction: request.direction(),
                parsed_dtls_parameters,
                remote_ice_credentials,
                response,
            }
        })
        .await?;
        mark_connected(self, session_key, request.direction())?;
        Ok(())
    }

    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.sessions().close_session(session_key).await
    }

    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .remove_media(session_key, transport_media_id)
            .await
    }

    #[allow(
        dead_code,
        reason = "protocol publish commit wiring is landing incrementally and this lookup is already exercised by negotiation tests"
    )]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.media()
            .negotiated_producer_parameters(session_key, transport_media_id)
            .await
    }

    pub(crate) async fn add_recv_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.media()
            .add_recv_media(session_key, media_kind, rtp_parameters)
            .await
    }

    pub(crate) async fn add_send_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.media()
            .add_send_media(
                consumer_session_key,
                media_kind,
                source_session_key,
                source_transport_media_id,
                remote_source_control,
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
        self.media()
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
        self.media()
            .set_consumer_active(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                active,
            )
            .await
    }

    #[allow(
        dead_code,
        reason = "Phase 6 introduces the server-owned source gate before the channel/runtime policy caller lands, so this adapter entry point is intentionally staged"
    )]
    pub(super) async fn set_route_control_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<super::super::route_control::PacketLayerGate>,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_route_control_source_packet_gate(
                source_session_key,
                source_transport_media_id,
                packet_gate,
            )
            .await
    }

    pub(crate) async fn set_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_source_packet_gate(source_session_key, source_transport_media_id, packet_gate)
            .await
    }

    pub(crate) fn activate_relay_route(
        &self,
        source_transport_media_id: TransportMediaId,
        target: &Self,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .activate_relay_route(source_transport_media_id, target)
    }

    pub(crate) fn deactivate_relay_route(
        &self,
        source_transport_media_id: TransportMediaId,
        target: &Self,
    ) {
        self.media()
            .deactivate_relay_route(source_transport_media_id, target);
    }

    pub(crate) fn set_relay_route_active(
        &self,
        source_transport_media_id: TransportMediaId,
        target: &Self,
        active: bool,
    ) {
        self.media()
            .set_relay_route_active(source_transport_media_id, target, active);
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
        F: FnOnce(oneshot::Sender<T>) -> DebugRtcWorkerCommand,
    {
        let worker_handle = self.ensure_packet_loop_started().ok()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .debug_tx
            .send(build_command(response_tx))
            .await
            .ok()?;
        response_rx.await.ok()
    }

    pub(crate) async fn debug_resolve_mid(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<Mid> {
        self.request_debug_worker(|response| DebugRtcWorkerCommand::ResolveMid {
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
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RemoteAddrOwner {
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
            .debug_tx
            .send(DebugRtcWorkerCommand::HasAnyRemoteAddrSession {
                response: response_tx,
            })
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
            .request_debug_worker(|response| DebugRtcWorkerCommand::RememberRemoteAddr {
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
        self.request_debug_worker(|response| DebugRtcWorkerCommand::SessionStreamRxSsrc {
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
        self.request_debug_worker(|response| DebugRtcWorkerCommand::SessionStreamTxSsrc {
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
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RemoteSourceOwner {
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
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RouteEntry {
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
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RouteEntryByConsumerMid {
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
        self.request_debug_worker(|response| DebugRtcWorkerCommand::RouteEntryByMediaId {
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
            .request_debug_worker(|response| DebugRtcWorkerCommand::RecordIncomingMedia {
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
            .request_debug_worker(|response| DebugRtcWorkerCommand::ObserveAudioActivity {
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

impl RtcTransportSessionFacade<'_> {
    pub(crate) async fn close_session(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let _ = self.close_session_with_outcome(session_key).await?;
        Ok(())
    }
}

impl RtcTransportMediaFacade<'_> {
    pub(crate) async fn remove_media(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let _ = self
            .remove_media_with_outcome(session_key, transport_media_id)
            .await?;
        Ok(())
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
