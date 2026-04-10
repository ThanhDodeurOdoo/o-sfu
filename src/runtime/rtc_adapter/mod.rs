//! Runtime transport adapter for the `rtc` WebRTC backend.
//!
//! Internal modules:
//! - `state`: pure state types and session scheduling
//! - `media_registry`: media handle tracking and mid registry
//! - `demux`: IP hash-indexed demux and media route entries
//! - `bootstrap`: socket binding, session RTC state initialization, transport payload construction
//! - `packet_loop`: async UDP packet loop, session output pumping, incoming packet routing
//! - `validation`: DTLS/SDP/ICE parameter validation and diagnostic mapping
//! - `dtls`: DTLS parameter parsing (RFC 8122, RFC 4572)
//! - `ice`: ICE candidate parsing (RFC 8839, RFC 8445)
//! - `sdp`: SDP offer parsing (RFC 8866)
//! - `parse_diagnostic`: shared parse diagnostic infrastructure

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

use super::{
    transport_adapter::{
        TransportAdapterError, TransportBitrateSnapshot, TransportConnectDirection,
        TransportSessionKey,
    },
    transport_bootstrap,
};
use crate::config::RtcPortRange;
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    webrtc::{DtlsParameters, IceParameters},
};
use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::{MediaKind, Mid, Rid};
use str0m::rtp::Ssrc;
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::transport_adapter::TransportMediaId;

mod bootstrap;
mod demux;
mod dtls;
mod ice;
mod media_registry;
mod packet_loop;
mod parse_diagnostic;
mod sdp;
mod state;
mod validation;

#[cfg(test)]
mod tests;

// Re-exports from extracted submodules for sibling module access
use demux::{MediaRouteDestination, MediaRouteEntry};
use media_registry::RegisteredMediaHandle;
use state::{
    ParsedRemoteIceCredentials, RtcBootstrapState, RtcSessionState, RtcSnapshotState,
    SessionTransportIds, SharedRtcSocket, TransportLifecycleState, TransportStateKey,
};

#[derive(Debug, Clone)]
struct RtcWorkerHandle {
    command_tx: mpsc::Sender<RtcWorkerCommand>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    shutdown_token: CancellationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseSessionOutcome {
    SessionClosed,
    WorkerDrained,
}

enum RtcWorkerCommand {
    BuildBootstrap {
        session_key: TransportSessionKey,
        router_capabilities: o_sfu_router::RtpCapabilities,
        response: oneshot::Sender<Result<CurrentTransportBootstrapPayload, TransportAdapterError>>,
    },
    ConnectTransport {
        session_key: TransportSessionKey,
        direction: TransportConnectDirection,
        parsed_dtls_parameters: dtls::ParsedDtlsParameters,
        remote_ice_credentials: Option<ParsedRemoteIceCredentials>,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
    },
    CloseSession {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Result<CloseSessionOutcome, TransportAdapterError>>,
    },
    RemoveMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
    },
    AddRecvMedia {
        session_key: TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: RouterRtpParameters,
        response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
    },
    AddSendMedia {
        consumer_session_key: TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        consumer_rtp_parameters: RouterRtpParameters,
        response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
    },
    SetProducerActive {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
    },
    SetConsumerActive {
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
    },
    #[cfg(test)]
    Debug(DebugRtcCommand),
    #[cfg(feature = "internal-benchmarks")]
    RememberRemoteAddr {
        source_addr: SocketAddr,
        session_key: TransportSessionKey,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
    },
}

#[cfg(test)]
enum DebugRtcCommand {
    ResolveMid {
        transport_media_id: TransportMediaId,
        response: oneshot::Sender<Option<Mid>>,
    },
    RemoteAddrOwner {
        source_addr: SocketAddr,
        response: oneshot::Sender<Option<TransportSessionKey>>,
    },
    HasAnyRemoteAddrSession {
        response: oneshot::Sender<bool>,
    },
    RememberRemoteAddr {
        source_addr: SocketAddr,
        session_key: TransportSessionKey,
        response: oneshot::Sender<()>,
    },
    SessionStreamRxSsrc {
        session_key: TransportSessionKey,
        mid: Mid,
        response: oneshot::Sender<Option<u32>>,
    },
    SessionStreamTxSsrc {
        session_key: TransportSessionKey,
        mid: Mid,
        response: oneshot::Sender<Option<u32>>,
    },
    RouteEntry {
        source_session_key: TransportSessionKey,
        source_mid: Mid,
        response: oneshot::Sender<Option<DebugRouteEntry>>,
    },
    RecordIncomingMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
        now: Instant,
        response: oneshot::Sender<()>,
    },
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DebugRouteDestination {
    dest_session: TransportSessionKey,
    dest_mid: Mid,
    active: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DebugRouteEntry {
    source_active: bool,
    destinations: Vec<DebugRouteDestination>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runtime transport adapter for the phase-7 `rtc` backend.
///
/// The adapter performs real ICE-lite bootstrap work with `str0m` while
/// transport connect, packet-loop driving, and media forwarding remain staged
/// behind the same boundary for later steps.
pub(crate) struct RtcTransportAdapter {
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    worker_handle: Mutex<Option<RtcWorkerHandle>>,
    transport_states: Arc<Mutex<BTreeMap<TransportStateKey, TransportLifecycleState>>>,
    packet_loop_started: Arc<AtomicBool>,
}

impl RtcTransportAdapter {
    pub(super) fn new(public_ip: IpAddr, rtc_port_range: RtcPortRange) -> Self {
        Self {
            public_ip,
            rtc_port_range,
            worker_handle: Mutex::new(None),
            transport_states: Arc::new(Mutex::new(BTreeMap::new())),
            packet_loop_started: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) async fn transport_bootstrap_payload(
        &self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        let worker_handle = self.ensure_packet_loop_started()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::BuildBootstrap {
                session_key: session_key.clone(),
                router_capabilities: router_capabilities.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        let payload = response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)??;
        validation::validate_bootstrap_payload(&payload)?;
        self.mark_bootstrap_sent(session_key)?;
        Ok(payload)
    }

    pub(super) async fn connect_transport(
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
        let worker_handle = self.ensure_packet_loop_started()?;
        debug!(
            ?direction,
            session_id = ?session_key.session_id(),
            channel_runtime_id = session_key.channel_runtime_id(),
            "validated DTLS parameters and transport lifecycle state before rtc transport connect"
        );
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::ConnectTransport {
                session_key: session_key.clone(),
                direction,
                parsed_dtls_parameters,
                remote_ice_credentials,
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)??;
        self.mark_connected(session_key, direction)?;
        Ok(())
    }

    pub(super) async fn close_session(
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
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::CloseSession {
                session_key: session_key.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        let close_outcome = response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)??;
        if close_outcome == CloseSessionOutcome::WorkerDrained {
            worker_handle.shutdown_token.cancel();
            if let Ok(mut worker_slot) = self.worker_handle.lock() {
                *worker_slot = None;
            }
            self.packet_loop_started.store(false, Ordering::Release);
        }
        Ok(())
    }

    pub(super) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let worker_handle = self.ensure_packet_loop_started()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::RemoveMedia {
                session_key: session_key.clone(),
                transport_media_id,
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }

    /// Declare a receive-only media line on the producer's `Rtc` instance.
    ///
    /// str0m needs an explicit media declaration to accept incoming RTP for
    /// the given media kind. `Mid` values are server-assigned random identifiers
    /// for the direct-API path (no SDP offer/answer exchange). Returns the
    /// opaque `TransportMediaId` wrapping the allocated `Mid`.
    pub(super) async fn add_recv_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let worker_handle = self.ensure_packet_loop_started()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::AddRecvMedia {
                session_key: session_key.clone(),
                media_kind,
                rtp_parameters: rtp_parameters.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }

    /// Declare a send-only media line on the consumer's `Rtc` instance and register
    /// a media route from the source producer session/mid to this new destination.
    ///
    /// Returns the allocated `Mid` for the new send-only media line.
    pub(super) async fn add_send_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let worker_handle = self.ensure_packet_loop_started()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::AddSendMedia {
                consumer_session_key: consumer_session_key.clone(),
                media_kind,
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                consumer_rtp_parameters: consumer_rtp_parameters.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }

    pub(super) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        let worker_handle = self.ensure_packet_loop_started()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::SetProducerActive {
                session_key: session_key.clone(),
                transport_media_id,
                active,
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }

    pub(super) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        let worker_handle = self.ensure_packet_loop_started()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::SetConsumerActive {
                consumer_session_key: consumer_session_key.clone(),
                consumer_transport_media_id,
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                active,
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }
}

// ---------------------------------------------------------------------------
// Private orchestration
// ---------------------------------------------------------------------------

impl RtcTransportAdapter {
    fn worker_handle(&self) -> Result<Option<RtcWorkerHandle>, TransportAdapterError> {
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
            command_rx,
            shutdown_token,
        ));
        Ok(worker_handle)
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

    pub(super) fn transport_bitrate_snapshot(
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
    pub(super) async fn benchmark_register_remote_addr(
        &self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let worker_handle = self.ensure_packet_loop_started()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::RememberRemoteAddr {
                source_addr,
                session_key: session_key.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }

    pub(super) fn benchmark_cached_remote_addr_lookup(&self, source_addr: SocketAddr) -> bool {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return false;
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return false;
        };
        snapshot_state
            .session_key_for_remote_addr(source_addr)
            .is_some_and(|session_key| snapshot_state.live_sessions.contains(session_key))
    }

    pub(super) fn benchmark_linear_remote_addr_lookup(&self, source_addr: SocketAddr) -> bool {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return false;
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return false;
        };
        snapshot_state
            .remote_addrs_by_session
            .iter()
            .any(|(session_key, session_addrs)| {
                snapshot_state.live_sessions.contains(session_key)
                    && session_addrs.contains(&source_addr)
            })
    }
}

#[cfg(test)]
impl RtcTransportAdapter {
    async fn debug_resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        let worker_handle = self.ensure_packet_loop_started().ok()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(DebugRtcCommand::ResolveMid {
                transport_media_id,
                response: response_tx,
            }))
            .await
            .ok()?;
        response_rx.await.ok().flatten()
    }

    async fn debug_remote_addr_owner(
        &self,
        source_addr: SocketAddr,
    ) -> Option<TransportSessionKey> {
        let worker_handle = self.ensure_packet_loop_started().ok()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(DebugRtcCommand::RemoteAddrOwner {
                source_addr,
                response: response_tx,
            }))
            .await
            .ok()?;
        response_rx.await.ok().flatten()
    }

    async fn debug_has_any_remote_addr_session(&self) -> bool {
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

    async fn debug_remember_remote_addr(
        &self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) {
        let Ok(worker_handle) = self.ensure_packet_loop_started() else {
            return;
        };
        let (response_tx, response_rx) = oneshot::channel();
        if worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(
                DebugRtcCommand::RememberRemoteAddr {
                    source_addr,
                    session_key: session_key.clone(),
                    response: response_tx,
                },
            ))
            .await
            .is_err()
        {
            return;
        }
        let _ = response_rx.await;
    }

    async fn debug_session_stream_rx_ssrc(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<u32> {
        let worker_handle = self.ensure_packet_loop_started().ok()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(
                DebugRtcCommand::SessionStreamRxSsrc {
                    session_key: session_key.clone(),
                    mid,
                    response: response_tx,
                },
            ))
            .await
            .ok()?;
        response_rx.await.ok().flatten()
    }

    async fn debug_session_stream_tx_ssrc(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> Option<u32> {
        let worker_handle = self.ensure_packet_loop_started().ok()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(
                DebugRtcCommand::SessionStreamTxSsrc {
                    session_key: session_key.clone(),
                    mid,
                    response: response_tx,
                },
            ))
            .await
            .ok()?;
        response_rx.await.ok().flatten()
    }

    async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        let worker_handle = self.ensure_packet_loop_started().ok()?;
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(DebugRtcCommand::RouteEntry {
                source_session_key: source_session_key.clone(),
                source_mid,
                response: response_tx,
            }))
            .await
            .ok()?;
        response_rx.await.ok().flatten()
    }

    async fn debug_record_incoming_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
        now: Instant,
    ) {
        let Ok(worker_handle) = self.ensure_packet_loop_started() else {
            return;
        };
        let (response_tx, response_rx) = oneshot::channel();
        if worker_handle
            .command_tx
            .send(RtcWorkerCommand::Debug(
                DebugRtcCommand::RecordIncomingMedia {
                    session_key: session_key.clone(),
                    transport_media_id,
                    payload_bytes,
                    now,
                    response: response_tx,
                },
            ))
            .await
            .is_err()
        {
            return;
        }
        let _ = response_rx.await;
    }
}

fn handle_worker_command(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    command: RtcWorkerCommand,
) {
    match command {
        #[cfg(test)]
        RtcWorkerCommand::Debug(command) => handle_debug_command(state, snapshot_state, command),
        #[cfg(feature = "internal-benchmarks")]
        RtcWorkerCommand::RememberRemoteAddr {
            source_addr,
            session_key,
            response,
        } => {
            respond_remember_remote_addr(
                state,
                snapshot_state,
                source_addr,
                &session_key,
                response,
            );
        }
        command => {
            handle_core_worker_command(state, snapshot_state, public_ip, rtc_port_range, command);
        }
    }
}

fn handle_core_worker_command(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::BuildBootstrap {
            session_key,
            router_capabilities,
            response,
        } => respond_build_bootstrap(
            state,
            snapshot_state,
            public_ip,
            rtc_port_range,
            &session_key,
            &router_capabilities,
            response,
        ),
        RtcWorkerCommand::ConnectTransport {
            session_key,
            direction,
            parsed_dtls_parameters,
            remote_ice_credentials,
            response,
        } => respond_connect_transport(
            state,
            &session_key,
            direction,
            &parsed_dtls_parameters,
            remote_ice_credentials.as_ref(),
            response,
        ),
        RtcWorkerCommand::CloseSession {
            session_key,
            response,
        } => respond_close_session(state, snapshot_state, &session_key, response),
        RtcWorkerCommand::RemoveMedia {
            session_key,
            transport_media_id,
            response,
        } => respond_remove_media(state, &session_key, transport_media_id, response),
        RtcWorkerCommand::AddRecvMedia {
            session_key,
            media_kind,
            rtp_parameters,
            response,
        } => respond_add_recv_media(state, &session_key, media_kind, &rtp_parameters, response),
        RtcWorkerCommand::AddSendMedia {
            consumer_session_key,
            media_kind,
            source_session_key,
            source_transport_media_id,
            consumer_rtp_parameters,
            response,
        } => respond_add_send_media(
            state,
            &consumer_session_key,
            media_kind,
            &source_session_key,
            source_transport_media_id,
            &consumer_rtp_parameters,
            response,
        ),
        RtcWorkerCommand::SetProducerActive {
            session_key,
            transport_media_id,
            active,
            response,
        } => respond_set_producer_active(state, &session_key, transport_media_id, active, response),
        RtcWorkerCommand::SetConsumerActive {
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            active,
            response,
        } => respond_set_consumer_active(
            state,
            &consumer_session_key,
            consumer_transport_media_id,
            &source_session_key,
            source_transport_media_id,
            active,
            response,
        ),
        #[cfg(test)]
        RtcWorkerCommand::Debug(_command) => {}
        #[cfg(feature = "internal-benchmarks")]
        RtcWorkerCommand::RememberRemoteAddr { .. } => {}
    }
}

fn respond_build_bootstrap(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    session_key: &TransportSessionKey,
    router_capabilities: &o_sfu_router::RtpCapabilities,
    response: oneshot::Sender<Result<CurrentTransportBootstrapPayload, TransportAdapterError>>,
) {
    let _ = response.send(worker_build_bootstrap_payload(
        state,
        snapshot_state,
        public_ip,
        rtc_port_range,
        session_key,
        router_capabilities,
    ));
}

fn respond_connect_transport(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    direction: TransportConnectDirection,
    parsed_dtls_parameters: &dtls::ParsedDtlsParameters,
    remote_ice_credentials: Option<&ParsedRemoteIceCredentials>,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let result = worker_ensure_transport_connect_compatibility(
        state,
        session_key,
        parsed_dtls_parameters,
        remote_ice_credentials,
    )
    .and_then(|()| {
        worker_apply_transport_connect(
            state,
            session_key,
            direction,
            parsed_dtls_parameters,
            remote_ice_credentials,
        )
    });
    let _ = response.send(result);
}

fn respond_close_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Result<CloseSessionOutcome, TransportAdapterError>>,
) {
    let close_outcome = worker_close_session(state, snapshot_state, session_key);
    let _ = response.send(Ok(close_outcome));
}

fn respond_remove_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_remove_media(state, session_key, transport_media_id));
}

fn respond_add_recv_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
) {
    let _ = response.send(worker_add_recv_media(
        state,
        session_key,
        media_kind,
        rtp_parameters,
    ));
}

fn respond_add_send_media(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    media_kind: MediaKind,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    consumer_rtp_parameters: &RouterRtpParameters,
    response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
) {
    let _ = response.send(worker_add_send_media(
        state,
        consumer_session_key,
        media_kind,
        source_session_key,
        source_transport_media_id,
        consumer_rtp_parameters,
    ));
}

fn respond_set_producer_active(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_producer_active(
        state,
        session_key,
        transport_media_id,
        active,
    ));
}

fn respond_set_consumer_active(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_consumer_active(
        state,
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
        active,
    ));
}

#[cfg(feature = "internal-benchmarks")]
fn respond_remember_remote_addr(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let result = if state.sessions.contains_key(session_key) {
        state.remember_remote_addr(source_addr, session_key);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            snapshot.remember_remote_addr(source_addr, session_key);
        }
        Ok(())
    } else {
        Err(TransportAdapterError::TransportUnavailable)
    };
    let _ = response.send(result);
}

fn worker_build_bootstrap_payload(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    session_key: &TransportSessionKey,
    router_capabilities: &o_sfu_router::RtpCapabilities,
) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
    let candidate_addr = if let Some(shared_socket) = state.shared_socket.as_ref() {
        shared_socket.candidate_addr
    } else {
        let shared_socket = bootstrap::bind_shared_rtc_socket(public_ip, rtc_port_range)?;
        let candidate_addr = shared_socket.candidate_addr;
        state.shared_socket = Some(shared_socket);
        candidate_addr
    };
    bootstrap::ensure_session_rtc_state(&mut state.sessions, session_key, candidate_addr)?;
    state.mark_session_dirty(session_key);
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.add_session(session_key);
    }
    let Some(session_state) = state.sessions.get(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    Ok(transport_bootstrap::transport_bootstrap_payload(
        router_capabilities,
        bootstrap::build_transport_bootstrap(
            session_state.transport_ids.download.as_str(),
            candidate_addr,
            &session_state.local_ice_credentials,
            &session_state.local_dtls_fingerprint,
        ),
        bootstrap::build_transport_bootstrap(
            session_state.transport_ids.upload.as_str(),
            candidate_addr,
            &session_state.local_ice_credentials,
            &session_state.local_dtls_fingerprint,
        ),
    ))
}

fn worker_ensure_transport_connect_compatibility(
    state: &RtcBootstrapState,
    session_key: &TransportSessionKey,
    parsed_dtls_parameters: &dtls::ParsedDtlsParameters,
    remote_ice_credentials: Option<&ParsedRemoteIceCredentials>,
) -> Result<(), TransportAdapterError> {
    let Some(session_state) = state.sessions.get(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let Some(primary_fingerprint) = parsed_dtls_parameters.fingerprints().first() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    let fingerprint_literal = format!(
        "{} {}",
        primary_fingerprint.algorithm(),
        primary_fingerprint.value()
    );
    validation::ensure_remote_fingerprint_compatibility(session_state, &fingerprint_literal)?;
    validation::ensure_remote_ice_credentials_compatibility(session_state, remote_ice_credentials)?;
    Ok(())
}

fn worker_apply_transport_connect(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    direction: TransportConnectDirection,
    parsed_dtls_parameters: &dtls::ParsedDtlsParameters,
    remote_ice_credentials: Option<&ParsedRemoteIceCredentials>,
) -> Result<(), TransportAdapterError> {
    let Some(session_state) = state.sessions.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let Some(primary_fingerprint) = parsed_dtls_parameters.fingerprints().first() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    let fingerprint_literal = format!(
        "{} {}",
        primary_fingerprint.algorithm(),
        primary_fingerprint.value()
    );
    let remote_fingerprint = validation::parse_remote_fingerprint(primary_fingerprint)?;
    let should_start_dtls = !session_state.dtls_started;
    {
        let mut direct_api = session_state.rtc.direct_api();
        if let Some(remote_ice_credentials) = remote_ice_credentials
            && session_state.remote_ice_credentials.is_none()
        {
            direct_api.set_remote_ice_credentials(remote_ice_credentials.as_ice_creds());
            session_state.remote_ice_credentials = Some(remote_ice_credentials.clone());
        }
        if session_state.remote_dtls_fingerprint.is_none() {
            direct_api.set_remote_fingerprint(remote_fingerprint);
            session_state.remote_dtls_fingerprint = Some(fingerprint_literal);
        }
        if should_start_dtls {
            let local_active_role =
                validation::local_dtls_active_role(parsed_dtls_parameters.role());
            direct_api
                .start_dtls(local_active_role)
                .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
            session_state.dtls_started = true;
            debug!(
                ?direction,
                session_id = ?session_key.session_id(),
                channel_runtime_id = session_key.channel_runtime_id(),
                local_active_role,
                "started rtc DTLS handshake after transport connect"
            );
        } else {
            debug!(
                ?direction,
                session_id = ?session_key.session_id(),
                channel_runtime_id = session_key.channel_runtime_id(),
                "rtc DTLS handshake already started for session"
            );
        }
    }
    state.mark_session_dirty(session_key);
    Ok(())
}

fn worker_close_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
) -> CloseSessionOutcome {
    state.sessions.remove(session_key);
    state.clear_session_schedule(session_key);
    state.forget_session_remote_addrs(session_key);
    state
        .mid_registry
        .retain(|_id, handle| handle.session_key() != session_key);
    state
        .recv_media_ids
        .retain(|(source_session, _), _| source_session != session_key);
    state
        .media_route_index
        .retain(|(source_session, _), _| source_session != session_key);
    state.media_route_index.retain(|_source, entry| {
        entry
            .destinations
            .retain(|destination| destination.dest_session != *session_key);
        !entry.destinations.is_empty()
    });
    if state.sessions.is_empty() {
        state.shared_socket = None;
    }
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.remove_session(session_key);
    }
    if state.sessions.is_empty() {
        CloseSessionOutcome::WorkerDrained
    } else {
        CloseSessionOutcome::SessionClosed
    }
}

fn worker_remove_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    let Some(handle) = state.remove_media_handle(transport_media_id) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if handle.session_key() != session_key {
        return Err(TransportAdapterError::InvalidInput);
    }
    match handle {
        RegisteredMediaHandle::Producer { session_key, mid } => {
            let should_remove_media = !state.session_has_mid(&session_key, mid);
            let should_remove_stream_type = !state.session_has_producer_mid(&session_key, mid);
            if let Some(session_state) = state.sessions.get_mut(&session_key) {
                remove_mid_once(&mut session_state.recv_mids, mid);
                if should_remove_media {
                    session_state.rtc.direct_api().remove_media(mid);
                }
            }
            if should_remove_stream_type {
                state.recv_media_ids.remove(&(session_key.clone(), mid));
                state.media_route_index.remove(&(session_key.clone(), mid));
            }
            state.mark_session_dirty(&session_key);
        }
        RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            source_session_key,
            source_mid,
        } => {
            let should_remove_media = !state.session_has_mid(&session_key, mid);
            if let Some(session_state) = state.sessions.get_mut(&session_key) {
                remove_mid_once(&mut session_state.send_mids, mid);
                if should_remove_media {
                    session_state.rtc.direct_api().remove_media(mid);
                }
            }
            if let Some(route_entry) = state
                .media_route_index
                .get_mut(&(source_session_key.clone(), source_mid))
            {
                if let Some(position) = route_entry.destinations.iter().position(|destination| {
                    destination.dest_session == session_key && destination.dest_mid == mid
                }) {
                    route_entry.destinations.remove(position);
                }
                if route_entry.destinations.is_empty() {
                    state
                        .media_route_index
                        .remove(&(source_session_key, source_mid));
                }
            }
            state.mark_session_dirty(&session_key);
        }
    }
    Ok(())
}

fn worker_add_recv_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> Result<TransportMediaId, TransportAdapterError> {
    let Some(session_state) = state.sessions.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = transport_mid(rtp_parameters).unwrap_or_default();
    let has_media = session_state.rtc.media(mid).is_some();
    {
        let mut api = session_state.rtc.direct_api();
        if !has_media {
            api.declare_media(mid, media_kind);
        }
        if let Some((ssrc, rid)) = primary_encoding_identity(rtp_parameters) {
            api.expect_stream_rx(ssrc, None, mid, rid);
        }
    }
    session_state.recv_mids.push(mid);
    state.mark_session_dirty(session_key);
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid,
    });
    state
        .recv_media_ids
        .insert((session_key.clone(), mid), transport_media_id);
    debug!(
        session_id = ?session_key.session_id(),
        channel_runtime_id = session_key.channel_runtime_id(),
        ?transport_media_id,
        ?media_kind,
        "declared recv-only media on rtc session for incoming producer RTP"
    );
    Ok(transport_media_id)
}

fn worker_add_send_media(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    media_kind: MediaKind,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    consumer_rtp_parameters: &RouterRtpParameters,
) -> Result<TransportMediaId, TransportAdapterError> {
    let source_mid = state
        .resolve_mid(source_transport_media_id)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let Some(session_state) = state.sessions.get_mut(consumer_session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = transport_mid(consumer_rtp_parameters).unwrap_or_default();
    let has_media = session_state.rtc.media(mid).is_some();
    {
        let mut api = session_state.rtc.direct_api();
        if !has_media {
            api.declare_media(mid, media_kind);
        }
        let (ssrc, rid) = primary_encoding_identity(consumer_rtp_parameters)
            .unwrap_or_else(|| (api.new_ssrc(), None));
        api.declare_stream_tx(ssrc, None, mid, rid);
    }
    session_state.send_mids.push(mid);
    state.mark_session_dirty(consumer_session_key);
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: consumer_session_key.clone(),
        mid,
        source_session_key: source_session_key.clone(),
        source_mid,
    });
    state
        .media_route_index
        .entry((source_session_key.clone(), source_mid))
        .or_insert_with(|| MediaRouteEntry {
            source_active: true,
            destinations: Vec::new(),
        })
        .destinations
        .push(MediaRouteDestination {
            dest_session: consumer_session_key.clone(),
            dest_mid: mid,
            active: true,
        });
    debug!(
        consumer_session_id = ?consumer_session_key.session_id(),
        consumer_channel_runtime_id = consumer_session_key.channel_runtime_id(),
        source_session_id = ?source_session_key.session_id(),
        source_channel_runtime_id = source_session_key.channel_runtime_id(),
        ?source_mid,
        ?transport_media_id,
        ?media_kind,
        "declared send-only media and registered media route for consumer"
    );
    Ok(transport_media_id)
}

fn worker_set_producer_active(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    let source_mid = state
        .resolve_mid(transport_media_id)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let route_entry = state
        .media_route_index
        .get_mut(&(session_key.clone(), source_mid))
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    route_entry.source_active = active;
    Ok(())
}

fn worker_set_consumer_active(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    let source_mid = state
        .resolve_mid(source_transport_media_id)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let consumer_mid = state
        .resolve_mid(consumer_transport_media_id)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let route_entry = state
        .media_route_index
        .get_mut(&(source_session_key.clone(), source_mid))
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let destination = route_entry
        .destinations
        .iter_mut()
        .find(|destination| {
            destination.dest_session == *consumer_session_key
                && destination.dest_mid == consumer_mid
        })
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    destination.active = active;
    Ok(())
}

#[cfg(test)]
fn handle_debug_command(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    command: DebugRtcCommand,
) {
    match command {
        DebugRtcCommand::ResolveMid {
            transport_media_id,
            response,
        } => respond_debug_resolve_mid(state, transport_media_id, response),
        DebugRtcCommand::RemoteAddrOwner {
            source_addr,
            response,
        } => respond_debug_remote_addr_owner(snapshot_state, source_addr, response),
        DebugRtcCommand::HasAnyRemoteAddrSession { response } => {
            respond_debug_has_any_remote_addr_session(snapshot_state, response);
        }
        DebugRtcCommand::RememberRemoteAddr {
            source_addr,
            session_key,
            response,
        } => respond_debug_remember_remote_addr(
            state,
            snapshot_state,
            source_addr,
            &session_key,
            response,
        ),
        DebugRtcCommand::SessionStreamRxSsrc {
            session_key,
            mid,
            response,
        } => respond_debug_session_stream_rx_ssrc(state, &session_key, mid, response),
        DebugRtcCommand::SessionStreamTxSsrc {
            session_key,
            mid,
            response,
        } => respond_debug_session_stream_tx_ssrc(state, &session_key, mid, response),
        DebugRtcCommand::RouteEntry {
            source_session_key,
            source_mid,
            response,
        } => respond_debug_route_entry(state, &source_session_key, source_mid, response),
        DebugRtcCommand::RecordIncomingMedia {
            session_key,
            transport_media_id,
            payload_bytes,
            now,
            response,
        } => respond_debug_record_incoming_media(
            snapshot_state,
            &session_key,
            transport_media_id,
            payload_bytes,
            now,
            response,
        ),
    }
}

#[cfg(test)]
fn respond_debug_resolve_mid(
    state: &RtcBootstrapState,
    transport_media_id: TransportMediaId,
    response: oneshot::Sender<Option<Mid>>,
) {
    let _ = response.send(state.resolve_mid(transport_media_id));
}

#[cfg(test)]
fn respond_debug_remote_addr_owner(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    response: oneshot::Sender<Option<TransportSessionKey>>,
) {
    let value = snapshot_state
        .lock()
        .ok()
        .and_then(|snapshot| snapshot.session_key_for_remote_addr(source_addr).cloned());
    let _ = response.send(value);
}

#[cfg(test)]
fn respond_debug_has_any_remote_addr_session(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    response: oneshot::Sender<bool>,
) {
    let value = snapshot_state
        .lock()
        .ok()
        .is_some_and(|snapshot| !snapshot.remote_addrs_by_session.is_empty());
    let _ = response.send(value);
}

#[cfg(test)]
fn respond_debug_remember_remote_addr(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<()>,
) {
    state.remember_remote_addr(source_addr, session_key);
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.remember_remote_addr(source_addr, session_key);
    }
    let _ = response.send(());
}

#[cfg(test)]
fn respond_debug_session_stream_rx_ssrc(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    mid: Mid,
    response: oneshot::Sender<Option<u32>>,
) {
    let value = state
        .sessions
        .get_mut(session_key)
        .and_then(|session_state| {
            let mut direct_api = session_state.rtc.direct_api();
            direct_api
                .stream_rx_by_mid(mid, None)
                .map(|stream_rx| *stream_rx.ssrc())
        });
    let _ = response.send(value);
}

#[cfg(test)]
fn respond_debug_session_stream_tx_ssrc(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    mid: Mid,
    response: oneshot::Sender<Option<u32>>,
) {
    let value = state
        .sessions
        .get_mut(session_key)
        .and_then(|session_state| {
            let mut direct_api = session_state.rtc.direct_api();
            direct_api
                .stream_tx_by_mid(mid, None)
                .map(|stream_tx| *stream_tx.ssrc())
        });
    let _ = response.send(value);
}

#[cfg(test)]
fn respond_debug_route_entry(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_mid: Mid,
    response: oneshot::Sender<Option<DebugRouteEntry>>,
) {
    let value = state
        .media_route_index
        .get(&(source_session_key.clone(), source_mid))
        .map(|entry| DebugRouteEntry {
            source_active: entry.source_active,
            destinations: entry
                .destinations
                .iter()
                .map(|destination| DebugRouteDestination {
                    dest_session: destination.dest_session.clone(),
                    dest_mid: destination.dest_mid,
                    active: destination.active,
                })
                .collect(),
        });
    let _ = response.send(value);
}

#[cfg(test)]
fn respond_debug_record_incoming_media(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    payload_bytes: usize,
    now: Instant,
    response: oneshot::Sender<()>,
) {
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.record_incoming_media(session_key, transport_media_id, now, payload_bytes);
    }
    let _ = response.send(());
}

// ---------------------------------------------------------------------------
// Trait impls
// ---------------------------------------------------------------------------

impl fmt::Debug for RtcTransportAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RtcTransportAdapter")
            .field("public_ip", &self.public_ip)
            .field("rtc_port_range", &self.rtc_port_range)
            .finish_non_exhaustive()
    }
}

fn transport_mid(rtp_parameters: &RouterRtpParameters) -> Option<Mid> {
    rtp_parameters.mid().map(Into::into)
}

fn primary_encoding_identity(rtp_parameters: &RouterRtpParameters) -> Option<(Ssrc, Option<Rid>)> {
    let encoding = rtp_parameters
        .encodings()
        .find(|encoding| encoding.ssrc().is_some() || encoding.rid().is_some())?;
    let ssrc = encoding.ssrc().map(Ssrc::from)?;
    let rid = encoding.rid().map(Into::into);
    Some((ssrc, rid))
}

fn remove_mid_once(mids: &mut Vec<Mid>, mid: Mid) {
    if let Some(position) = mids.iter().position(|current_mid| *current_mid == mid) {
        mids.remove(position);
    }
}

#[cfg(test)]
impl Default for RtcTransportAdapter {
    fn default() -> Self {
        use std::net::Ipv4Addr;
        Self::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(40_000, 49_999),
        )
    }
}
