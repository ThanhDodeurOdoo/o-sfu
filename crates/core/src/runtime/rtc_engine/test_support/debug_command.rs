#[cfg(test)]
use std::{net::SocketAddr, time::Instant};

use str0m::media::Mid;
use tokio::sync::oneshot;

#[cfg(test)]
use crate::Bitrate;
use crate::runtime::media_transport::{TransportMediaId, TransportSessionKey};

pub(in crate::runtime::rtc_engine) enum DebugRtcWorkerCommand {
    #[cfg(test)]
    ResolveMid {
        transport_media_id: TransportMediaId,
        response: oneshot::Sender<Option<Mid>>,
    },
    #[cfg(test)]
    RemoteAddrOwner {
        source_addr: SocketAddr,
        response: oneshot::Sender<Option<TransportSessionKey>>,
    },
    #[cfg(test)]
    HasAnyRemoteAddrSession { response: oneshot::Sender<bool> },
    #[cfg(test)]
    RememberRemoteAddr {
        source_addr: SocketAddr,
        session_key: TransportSessionKey,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    SessionStreamRxSsrc {
        session_key: TransportSessionKey,
        mid: Mid,
        response: oneshot::Sender<Option<u32>>,
    },
    #[cfg(test)]
    SessionStreamTxSsrc {
        session_key: TransportSessionKey,
        mid: Mid,
        response: oneshot::Sender<Option<u32>>,
    },
    #[cfg(test)]
    SessionMaxBitrateIn {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Option<Bitrate>>,
    },
    #[cfg(test)]
    SessionMaxBitrateOut {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Option<Bitrate>>,
    },
    #[cfg(test)]
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
    #[cfg(test)]
    RouteEntryByMediaId {
        source_transport_media_id: TransportMediaId,
        response: oneshot::Sender<Option<DebugRouteEntry>>,
    },
    #[cfg(test)]
    RecordIncomingMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
        now: Instant,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    ObserveAudioActivity {
        transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    RelayTargetCount {
        source_transport_media_id: TransportMediaId,
        response: oneshot::Sender<usize>,
    },
    #[cfg(test)]
    ActiveRelayTargetCount {
        source_transport_media_id: TransportMediaId,
        response: oneshot::Sender<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugRouteDestination {
    pub dest_session: TransportSessionKey,
    pub dest_transport_media_id: TransportMediaId,
    pub dest_mid: Mid,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugRouteEntry {
    pub source_transport_media_id: TransportMediaId,
    pub source_active: bool,
    pub active_destination_count: usize,
    pub effective_packet_gate: DebugPacketGate,
    pub destinations: Vec<DebugRouteDestination>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DebugPacketGate {
    Open,
    Block,
    Rid(String),
    OperatingPoint {
        rid: Option<String>,
        max_temporal_layer_id: u8,
    },
}
