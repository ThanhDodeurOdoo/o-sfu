use o_sfu_router::{RtpCapabilities, RtpParameters as RouterRtpParameters};
use str0m::media::MediaKind;
#[cfg(test)]
use str0m::media::Mid;
use tokio::sync::oneshot;

#[cfg(any(test, feature = "internal-benchmarks"))]
use std::net::SocketAddr;
#[cfg(test)]
use std::time::Instant;

use crate::runtime::transport_adapter::{
    SessionOffer, TransportAdapterError, TransportConnectDirection, TransportMediaId,
    TransportSessionKey,
};
use crate::signaling::current_protocol::CurrentTransportBootstrapPayload;

use super::{dtls, state::ParsedRemoteIceCredentials};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CloseSessionOutcome {
    SessionClosed,
    WorkerDrained,
}

pub(super) enum RtcWorkerCommand {
    BuildBootstrap {
        session_key: TransportSessionKey,
        router_capabilities: RtpCapabilities,
        response: oneshot::Sender<Result<CurrentTransportBootstrapPayload, TransportAdapterError>>,
    },
    CreateInitialSessionOffer {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Result<SessionOffer, TransportAdapterError>>,
    },
    CreateSessionRenegotiationOffer {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Result<SessionOffer, TransportAdapterError>>,
    },
    ApplySessionAnswer {
        session_key: TransportSessionKey,
        answer_sdp: String,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
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
pub(super) enum DebugRtcCommand {
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
pub(crate) struct DebugRouteDestination {
    pub(crate) dest_session: TransportSessionKey,
    pub(crate) dest_mid: Mid,
    pub(crate) active: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DebugRouteEntry {
    pub(crate) source_active: bool,
    pub(crate) destinations: Vec<DebugRouteDestination>,
}
