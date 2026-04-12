//! Runtime transport adapter facade for the `rtc` WebRTC backend.

use std::{
    collections::BTreeMap,
    fmt,
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

#[cfg(any(test, feature = "internal-benchmarks"))]
use std::net::SocketAddr;

use crate::config::RtcPortRange;
use crate::runtime::recording::MediaTap;
use crate::runtime::transport_adapter::{
    SessionOffer, TransportAdapterError, TransportBitrateSnapshot, TransportConnectDirection,
    TransportMediaId, TransportSessionKey,
};
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    webrtc::{DtlsParameters, IceParameters},
};
use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::MediaKind;
#[cfg(test)]
use str0m::media::Mid;
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

#[cfg(test)]
use super::commands::{DebugRouteEntry, DebugRtcCommand};
use super::{
    commands::{CloseSessionOutcome, RtcWorkerCommand},
    packet_loop,
    state::{RtcSnapshotState, TransportLifecycleState, TransportStateKey},
    validation,
};

#[derive(Debug, Clone)]
pub(crate) struct RtcWorkerHandle {
    command_tx: mpsc::Sender<RtcWorkerCommand>,
    pub(crate) snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    shutdown_token: CancellationToken,
}

pub(crate) struct RtcTransportAdapter {
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    media_tap: Arc<MediaTap>,
    worker_handle: Mutex<Option<RtcWorkerHandle>>,
    transport_states: Arc<Mutex<BTreeMap<TransportStateKey, TransportLifecycleState>>>,
    pub(crate) packet_loop_started: Arc<AtomicBool>,
}

impl RtcTransportAdapter {
    pub(crate) fn new(
        public_ip: IpAddr,
        rtc_port_range: RtcPortRange,
        media_tap: Arc<MediaTap>,
    ) -> Self {
        Self {
            public_ip,
            rtc_port_range,
            media_tap,
            worker_handle: Mutex::new(None),
            transport_states: Arc::new(Mutex::new(BTreeMap::new())),
            packet_loop_started: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) async fn transport_bootstrap_payload(
        &self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        let payload = self
            .request_worker(|response| RtcWorkerCommand::BuildBootstrap {
                session_key: session_key.clone(),
                router_capabilities: router_capabilities.clone(),
                response,
            })
            .await?;
        validation::validate_bootstrap_payload(&payload)?;
        self.mark_bootstrap_sent(session_key)?;
        Ok(payload)
    }

    pub(crate) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::CreateInitialSessionOffer {
            session_key: session_key.clone(),
            response,
        })
        .await
    }

    pub(crate) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.request_worker(
            |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                session_key: session_key.clone(),
                response,
            },
        )
        .await
    }

    pub(crate) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::ApplySessionAnswer {
            session_key: session_key.clone(),
            answer_sdp: answer_sdp.to_owned(),
            response,
        })
        .await
    }

    pub(crate) async fn connect_transport(
        &self,
        session_key: &TransportSessionKey,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        ice_parameters: Option<&IceParameters>,
        sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        if let Some(sdp_offer) = sdp_offer {
            validation::validate_sdp_offer(sdp_offer)?;
        }
        let parsed_dtls_parameters = validation::parse_dtls_parameters(dtls_parameters)?;
        let remote_ice_credentials = validation::parse_remote_ice_credentials(ice_parameters)?;
        self.ensure_connect_transition(session_key, direction)?;
        debug!(
            ?direction,
            session_id = ?session_key.session_id(),
            channel_runtime_id = session_key.channel_runtime_id(),
            "validated DTLS parameters and transport lifecycle state before rtc transport connect"
        );
        self.request_worker(|response| RtcWorkerCommand::ConnectTransport {
            session_key: session_key.clone(),
            direction,
            parsed_dtls_parameters,
            remote_ice_credentials,
            response,
        })
        .await?;
        self.mark_connected(session_key, direction)?;
        Ok(())
    }

    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        {
            let Ok(mut transport_states) = self.transport_states.lock() else {
                return Err(TransportAdapterError::TransportUnavailable);
            };
            transport_states.retain(|key, _| key.session_key != *session_key);
        }
        let Some(worker_handle) = self.worker_handle()? else {
            return Ok(());
        };
        let close_outcome = self
            .send_worker_command(&worker_handle, |response| RtcWorkerCommand::CloseSession {
                session_key: session_key.clone(),
                response,
            })
            .await?;
        if close_outcome == CloseSessionOutcome::WorkerDrained {
            worker_handle.shutdown_token.cancel();
            if let Ok(mut worker_slot) = self.worker_handle.lock() {
                *worker_slot = None;
            }
            self.packet_loop_started.store(false, Ordering::Release);
        }
        Ok(())
    }

    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::RemoveMedia {
            session_key: session_key.clone(),
            transport_media_id,
            response,
        })
        .await
    }

    /// Declare a receive-only media line on the proudcer's `Rtc` instance.
    ///
    /// str0m need an explicit media declaration to accept incoming RTP for
    /// the given media kind. `Mid` values are server-assigned random identifiers
    /// for the direct-API path (no SDP offer/answer exchange). Returns the
    /// opaque `TransportMediaId` wrapping the allocated `Mid`.
    pub(crate) async fn add_recv_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::AddRecvMedia {
            session_key: session_key.clone(),
            media_kind,
            rtp_parameters: rtp_parameters.clone(),
            response,
        })
        .await
    }

    /// Declare a send-only media line on the consumer's `Rtc` instance and register
    /// a media route from the source producer session/mid to this new destination
    ///
    /// Returns the allocated `Mid` for the new send-only media line.
    pub(crate) async fn add_send_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::AddSendMedia {
            consumer_session_key: consumer_session_key.clone(),
            media_kind,
            source_session_key: source_session_key.clone(),
            source_transport_media_id,
            consumer_rtp_parameters: consumer_rtp_parameters.clone(),
            response,
        })
        .await
    }

    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::SetProducerActive {
            session_key: session_key.clone(),
            transport_media_id,
            active,
            response,
        })
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
        self.request_worker(|response| RtcWorkerCommand::SetConsumerActive {
            consumer_session_key: consumer_session_key.clone(),
            consumer_transport_media_id,
            source_session_key: source_session_key.clone(),
            source_transport_media_id,
            active,
            response,
        })
        .await
    }

    pub(crate) fn worker_handle(&self) -> Result<Option<RtcWorkerHandle>, TransportAdapterError> {
        let Ok(worker_handle) = self.worker_handle.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        Ok(worker_handle.clone())
    }

    fn ensure_packet_loop_started(&self) -> Result<RtcWorkerHandle, TransportAdapterError> {
        if let Some(worker_handle) = self.worker_handle()? {
            return Ok(worker_handle);
        }
        if self.packet_loop_started.swap(true, Ordering::AcqRel) {
            return self
                .worker_handle()?
                .ok_or(TransportAdapterError::TransportUnavailable);
        }
        let Ok(current_runtime) = Handle::try_current() else {
            self.packet_loop_started.store(false, Ordering::Release);
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let (command_tx, command_rx) = mpsc::channel(64);
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let shutdown_token = CancellationToken::new();
        let worker_handle = RtcWorkerHandle {
            command_tx,
            snapshot_state: Arc::clone(&snapshot_state),
            shutdown_token: shutdown_token.clone(),
        };
        {
            let Ok(mut worker_slot) = self.worker_handle.lock() else {
                self.packet_loop_started.store(false, Ordering::Release);
                return Err(TransportAdapterError::TransportUnavailable);
            };
            *worker_slot = Some(worker_handle.clone());
        }
        current_runtime.spawn(packet_loop::run_packet_loop(
            self.public_ip,
            self.rtc_port_range,
            snapshot_state,
            Arc::clone(&self.media_tap),
            command_rx,
            shutdown_token,
        ));
        Ok(worker_handle)
    }

    async fn request_worker<T, F>(&self, build_command: F) -> Result<T, TransportAdapterError>
    where
        F: FnOnce(oneshot::Sender<Result<T, TransportAdapterError>>) -> RtcWorkerCommand,
    {
        let worker_handle = self.ensure_packet_loop_started()?;
        self.send_worker_command(&worker_handle, build_command)
            .await
    }

    async fn send_worker_command<T, F>(
        &self,
        worker_handle: &RtcWorkerHandle,
        build_command: F,
    ) -> Result<T, TransportAdapterError>
    where
        F: FnOnce(oneshot::Sender<Result<T, TransportAdapterError>>) -> RtcWorkerCommand,
    {
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(build_command(response_tx))
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }

    fn mark_bootstrap_sent(
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

    fn ensure_connect_transition(
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

    fn mark_connected(
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

    pub(crate) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return TransportBitrateSnapshot::default();
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return TransportBitrateSnapshot::default();
        };
        snapshot_state.transport_bitrate_snapshot_at(session_keys, Instant::now())
    }
}

#[cfg(feature = "internal-benchmarks")]
impl RtcTransportAdapter {
    pub(crate) async fn benchmark_register_remote_addr(
        &self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::RememberRemoteAddr {
            source_addr,
            session_key: session_key.clone(),
            response,
        })
        .await
    }

    pub(crate) fn benchmark_cached_remote_addr_lookup(&self, source_addr: SocketAddr) -> bool {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return false;
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return false;
        };
        snapshot_state
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr)
            .is_some_and(|session_key| snapshot_state.live_sessions.contains(session_key))
    }

    pub(crate) fn benchmark_linear_remote_addr_lookup(&self, source_addr: SocketAddr) -> bool {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return false;
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return false;
        };
        snapshot_state
            .remote_addr_demux
            .session_entries()
            .any(|(session_key, session_addrs)| {
                snapshot_state.live_sessions.contains(session_key)
                    && session_addrs.contains(&source_addr)
            })
    }
}

#[cfg(test)]
impl RtcTransportAdapter {
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
}

impl fmt::Debug for RtcTransportAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RtcTransportAdapter")
            .field("public_ip", &self.public_ip)
            .field("rtc_port_range", &self.rtc_port_range)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl Default for RtcTransportAdapter {
    fn default() -> Self {
        use std::net::Ipv4Addr;

        Self::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(40_000, 49_999),
            Arc::new(MediaTap::default()),
        )
    }
}
