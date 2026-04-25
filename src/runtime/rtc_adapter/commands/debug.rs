use std::{net::SocketAddr, time::Instant};

use str0m::media::Mid;
use tokio::sync::oneshot;

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

pub(crate) enum DebugRtcWorkerCommand {
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
    SessionMaxBitrateIn {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Option<u64>>,
    },
    SessionMaxBitrateOut {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Option<u64>>,
    },
    RemoteSourceOwner {
        source_transport_media_id: TransportMediaId,
        response: oneshot::Sender<Option<TransportSessionKey>>,
    },
    RouteEntry {
        source_session_key: TransportSessionKey,
        source_mid: Mid,
        response: oneshot::Sender<Option<DebugRouteEntry>>,
    },
    RouteEntryByConsumerMid {
        consumer_session_key: TransportSessionKey,
        consumer_mid: Mid,
        response: oneshot::Sender<Option<DebugRouteEntry>>,
    },
    RouteEntryByMediaId {
        source_transport_media_id: TransportMediaId,
        response: oneshot::Sender<Option<DebugRouteEntry>>,
    },
    RecordIncomingMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
        now: Instant,
        response: oneshot::Sender<()>,
    },
    ObserveAudioActivity {
        transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
        response: oneshot::Sender<()>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DebugRouteDestination {
    pub(crate) dest_session: TransportSessionKey,
    pub(crate) dest_transport_media_id: TransportMediaId,
    pub(crate) dest_mid: Mid,
    pub(crate) active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DebugRouteEntry {
    pub(crate) source_transport_media_id: TransportMediaId,
    pub(crate) source_active: bool,
    pub(crate) effective_packet_gate: DebugPacketGate,
    pub(crate) destinations: Vec<DebugRouteDestination>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DebugPacketGate {
    Open,
    Block,
    Rid(String),
    OperatingPoint {
        rid: Option<String>,
        max_temporal_layer_id: u8,
    },
}
