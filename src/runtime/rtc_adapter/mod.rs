//! Runtime transport adapter for the `rtc` WebRTC backend.
//!
//! Internal modules:
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
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use super::{
    transport_adapter::{TransportAdapterError, TransportConnectDirection},
    transport_bootstrap,
};
use crate::config::RtcPortRange;
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload, shared::SessionId, webrtc::DtlsParameters,
};
use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::config::Fingerprint;
use str0m::media::{MediaKind, Mid, Rid};
use str0m::rtp::Ssrc;
use str0m::{IceCreds, Rtc};
use tokio::{net::UdpSocket, runtime::Handle};
use tracing::debug;

use super::transport_adapter::TransportMediaId;

mod bootstrap;
mod dtls;
mod ice;
mod packet_loop;
mod parse_diagnostic;
mod sdp;
mod validation;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Internal types shared across submodules
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportLifecycleState {
    BootstrapSent,
    Connected,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TransportStateKey {
    session_id: SessionId,
    direction: TransportConnectDirection,
}

pub(super) struct SharedRtcSocket {
    pub(super) socket: Arc<UdpSocket>,
    pub(super) candidate_addr: SocketAddr,
}

pub(super) struct RtcSessionState {
    pub(super) rtc: Rtc,
    pub(super) local_ice_credentials: IceCreds,
    pub(super) local_dtls_fingerprint: Fingerprint,
    pub(super) transport_ids: SessionTransportIds,
    pub(super) remote_dtls_fingerprint: Option<String>,
    pub(super) dtls_started: bool,
    pub(super) recv_mids: Vec<Mid>,
    pub(super) send_mids: Vec<Mid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionTransportIds {
    pub(super) upload: String,
    pub(super) download: String,
}

/// A single forwarding destination within the media route index.
#[derive(Debug, Clone)]
pub(super) struct MediaRouteDestination {
    pub(super) dest_session: SessionId,
    pub(super) dest_mid: Mid,
}

/// Media route source key: `(producer session, producer mid)`.
pub(super) type MediaRouteKey = (SessionId, Mid);

#[derive(Default)]
pub(super) struct RtcBootstrapState {
    pub(super) shared_socket: Option<SharedRtcSocket>,
    pub(super) sessions: BTreeMap<SessionId, RtcSessionState>,
    pub(super) media_route_index: BTreeMap<MediaRouteKey, Vec<MediaRouteDestination>>,
    mid_registry: BTreeMap<u64, Mid>,
    next_media_id: u64,
}

impl RtcBootstrapState {
    fn register_mid(&mut self, mid: Mid) -> TransportMediaId {
        let id = self.next_media_id;
        self.next_media_id = self.next_media_id.saturating_add(1);
        self.mid_registry.insert(id, mid);
        TransportMediaId::new(id)
    }

    fn resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.mid_registry.get(&transport_media_id.as_u64()).copied()
    }
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
    bootstrap_state: Arc<Mutex<RtcBootstrapState>>,
    transport_states: Arc<Mutex<BTreeMap<TransportStateKey, TransportLifecycleState>>>,
    packet_loop_started: Arc<AtomicBool>,
}

impl RtcTransportAdapter {
    pub(super) fn new(public_ip: IpAddr, rtc_port_range: RtcPortRange) -> Self {
        Self {
            public_ip,
            rtc_port_range,
            bootstrap_state: Arc::new(Mutex::new(RtcBootstrapState::default())),
            transport_states: Arc::new(Mutex::new(BTreeMap::new())),
            packet_loop_started: Arc::new(AtomicBool::new(false)),
        }
    }

    #[allow(
        clippy::unused_async,
        reason = "the transport-adapter boundary stays async so packet-loop work can be added without changing runtime call sites"
    )]
    pub(super) async fn transport_bootstrap_payload(
        &self,
        session_id: &SessionId,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        let payload = self.build_bootstrap_payload(session_id, router_capabilities)?;
        self.ensure_packet_loop_started()?;
        validation::validate_bootstrap_payload(&payload)?;
        self.mark_bootstrap_sent(session_id)?;
        Ok(payload)
    }

    #[allow(
        clippy::unused_async,
        reason = "the transport-adapter boundary stays async while runtime call sites and sibling adapters keep the same signature"
    )]
    pub(super) async fn connect_transport(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        if let Some(sdp_offer) = sdp_offer {
            validation::validate_sdp_offer(sdp_offer)?;
        }
        let parsed_dtls_parameters = validation::parse_dtls_parameters(dtls_parameters)?;
        self.ensure_connect_transition(session_id, direction)?;
        debug!(
            ?direction,
            session_id = ?session_id,
            "validated DTLS parameters and transport lifecycle state before rtc transport connect"
        );
        self.apply_transport_connect(session_id, direction, &parsed_dtls_parameters)?;
        self.mark_connected(session_id, direction)?;
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "the transport-adapter boundary stays async so packet-loop work can be added without changing runtime call sites"
    )]
    pub(super) async fn close_session(
        &self,
        session_id: &SessionId,
    ) -> Result<(), TransportAdapterError> {
        let Ok(mut transport_states) = self.transport_states.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        transport_states.retain(|key, _| key.session_id != *session_id);
        drop(transport_states);

        let Ok(mut bootstrap_state) = self.bootstrap_state.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        if let Some(session_state) = bootstrap_state.sessions.remove(session_id) {
            let stale_mids: Vec<_> = session_state
                .recv_mids
                .iter()
                .chain(session_state.send_mids.iter())
                .copied()
                .collect();
            bootstrap_state
                .mid_registry
                .retain(|_id, mid| !stale_mids.contains(mid));
        }
        // Remove all routes where this session is the source.
        bootstrap_state
            .media_route_index
            .retain(|(source_session, _), _| source_session != session_id);
        // Remove destination entries where this session is the consumer.
        for destinations in bootstrap_state.media_route_index.values_mut() {
            destinations.retain(|dest| dest.dest_session != *session_id);
        }
        if bootstrap_state.sessions.is_empty() {
            bootstrap_state.shared_socket = None;
        }
        Ok(())
    }

    /// Declare a receive-only media line on the producer's `Rtc` instance.
    ///
    /// str0m needs an explicit media declaration to accept incoming RTP for
    /// the given media kind. `Mid` values are server-assigned random identifiers
    /// for the direct-API path (no SDP offer/answer exchange). Returns the
    /// opaque `TransportMediaId` wrapping the allocated `Mid`.
    #[allow(
        clippy::unused_async,
        reason = "the transport-adapter boundary stays async while runtime call sites and sibling adapters keep the same signature"
    )]
    pub(super) async fn add_recv_media(
        &self,
        session_id: &SessionId,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let Ok(mut bootstrap_state) = self.bootstrap_state.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let Some(session_state) = bootstrap_state.sessions.get_mut(session_id) else {
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
        let transport_media_id = bootstrap_state.register_mid(mid);
        debug!(
            session_id = ?session_id,
            ?transport_media_id,
            ?media_kind,
            "declared recv-only media on rtc session for incoming producer RTP"
        );
        Ok(transport_media_id)
    }

    /// Declare a send-only media line on the consumer's `Rtc` instance and register
    /// a media route from the source producer session/mid to this new destination.
    ///
    /// Returns the allocated `Mid` for the new send-only media line.
    #[allow(
        clippy::unused_async,
        reason = "the transport-adapter boundary stays async while runtime call sites and sibling adapters keep the same signature"
    )]
    pub(super) async fn add_send_media(
        &self,
        consumer_session_id: &SessionId,
        media_kind: MediaKind,
        source_session_id: &SessionId,
        source_transport_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let Ok(mut bootstrap_state) = self.bootstrap_state.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let source_mid = bootstrap_state
            .resolve_mid(source_transport_media_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let Some(session_state) = bootstrap_state.sessions.get_mut(consumer_session_id) else {
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
        let transport_media_id = bootstrap_state.register_mid(mid);
        bootstrap_state
            .media_route_index
            .entry((source_session_id.clone(), source_mid))
            .or_default()
            .push(MediaRouteDestination {
                dest_session: consumer_session_id.clone(),
                dest_mid: mid,
            });
        debug!(
            consumer_session_id = ?consumer_session_id,
            source_session_id = ?source_session_id,
            ?source_mid,
            ?transport_media_id,
            ?media_kind,
            "declared send-only media and registered media route for consumer"
        );
        Ok(transport_media_id)
    }
}

// ---------------------------------------------------------------------------
// Private orchestration
// ---------------------------------------------------------------------------

impl RtcTransportAdapter {
    fn build_bootstrap_payload(
        &self,
        session_id: &SessionId,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        let Ok(mut bootstrap_state) = self.bootstrap_state.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let candidate_addr = if let Some(shared_socket) = bootstrap_state.shared_socket.as_ref() {
            shared_socket.candidate_addr
        } else {
            let shared_socket =
                bootstrap::bind_shared_rtc_socket(self.public_ip, self.rtc_port_range)?;
            let candidate_addr = shared_socket.candidate_addr;
            bootstrap_state.shared_socket = Some(shared_socket);
            candidate_addr
        };
        bootstrap::ensure_session_rtc_state(
            &mut bootstrap_state.sessions,
            session_id,
            candidate_addr,
        )?;
        let Some(session_state) = bootstrap_state.sessions.get(session_id) else {
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

    fn ensure_packet_loop_started(&self) -> Result<(), TransportAdapterError> {
        if self.packet_loop_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let Ok(current_runtime) = Handle::try_current() else {
            self.packet_loop_started.store(false, Ordering::Release);
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let bootstrap_state = Arc::clone(&self.bootstrap_state);
        current_runtime.spawn(async move {
            packet_loop::run_packet_loop(bootstrap_state).await;
        });
        Ok(())
    }

    fn apply_transport_connect(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
        parsed_dtls_parameters: &dtls::ParsedDtlsParameters,
    ) -> Result<(), TransportAdapterError> {
        let Ok(mut bootstrap_state) = self.bootstrap_state.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let Some(session_state) = bootstrap_state.sessions.get_mut(session_id) else {
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
        validation::ensure_remote_fingerprint_compatibility(session_state, &fingerprint_literal)?;
        let should_start_dtls = !session_state.dtls_started;
        {
            let mut direct_api = session_state.rtc.direct_api();
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
                    session_id = ?session_id,
                    local_active_role,
                    "started rtc DTLS handshake after transport connect"
                );
            } else {
                debug!(
                    ?direction,
                    session_id = ?session_id,
                    "rtc DTLS handshake already started for session"
                );
            }
        }
        Ok(())
    }

    fn mark_bootstrap_sent(&self, session_id: &SessionId) -> Result<(), TransportAdapterError> {
        let Ok(mut states) = self.transport_states.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        for direction in [
            TransportConnectDirection::Upload,
            TransportConnectDirection::Download,
        ] {
            states.insert(
                TransportStateKey {
                    session_id: session_id.clone(),
                    direction,
                },
                TransportLifecycleState::BootstrapSent,
            );
        }
        Ok(())
    }

    fn ensure_connect_transition(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
    ) -> Result<(), TransportAdapterError> {
        let key = TransportStateKey {
            session_id: session_id.clone(),
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
        session_id: &SessionId,
        direction: TransportConnectDirection,
    ) -> Result<(), TransportAdapterError> {
        let key = TransportStateKey {
            session_id: session_id.clone(),
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
