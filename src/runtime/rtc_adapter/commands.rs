#[cfg(test)]
pub(crate) mod debug;

#[cfg(any(test, feature = "internal-benchmarks"))]
use o_sfu_router::RtpCapabilities;
use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::{KeyframeRequestKind, MediaKind, Rid};
use tokio::sync::{mpsc, oneshot};

#[cfg(feature = "internal-benchmarks")]
use std::net::SocketAddr;

use crate::runtime::transport_adapter::{
    ActiveSpeakerSource, SessionOffer, TransportAdapterError, TransportMediaId, TransportSessionKey,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::runtime::transport_bootstrap::SessionTransportBootstrap;
#[cfg(test)]
use crate::runtime::transport_connect::TransportConnectDirection;

use super::relay_registry::RelayTargetId;
use super::route_control::PacketLayerGate;
#[cfg(test)]
use super::{dtls, state::ParsedRemoteIceCredentials};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseSessionState {
    SessionClosed,
    WorkerDrained,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelayCleanup {
    source_session_key: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
}

impl RelayCleanup {
    pub(super) fn new(
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            source_session_key,
            source_transport_media_id,
        }
    }

    pub(crate) fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    pub(crate) const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CloseSessionOutcome {
    state: CloseSessionState,
    relay_cleanup: Vec<RelayCleanup>,
}

impl CloseSessionOutcome {
    pub(super) fn new(state: CloseSessionState, relay_cleanup: Vec<RelayCleanup>) -> Self {
        Self {
            state,
            relay_cleanup,
        }
    }

    pub(crate) const fn state(&self) -> CloseSessionState {
        self.state
    }

    pub(crate) fn relay_cleanup(&self) -> &[RelayCleanup] {
        &self.relay_cleanup
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoveMediaOutcome {
    relay_cleanup: Option<RelayCleanup>,
}

impl RemoveMediaOutcome {
    pub(super) const fn without_relay_cleanup() -> Self {
        Self {
            relay_cleanup: None,
        }
    }

    pub(super) fn with_relay_cleanup(
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            relay_cleanup: Some(RelayCleanup::new(
                source_session_key,
                source_transport_media_id,
            )),
        }
    }

    pub(crate) fn relay_cleanup(&self) -> Option<&RelayCleanup> {
        self.relay_cleanup.as_ref()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RemoteSourceControl {
    tx: mpsc::Sender<RtcWorkerCommand>,
    target_id: RelayTargetId,
}

impl RemoteSourceControl {
    pub(super) fn new(tx: mpsc::Sender<RtcWorkerCommand>, target_id: RelayTargetId) -> Self {
        Self { tx, target_id }
    }

    pub(crate) fn request_keyframe(
        &self,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    ) {
        let _ = self.tx.try_send(RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id,
            target_id: self.target_id,
            rid,
            kind,
        });
    }

    pub(crate) fn set_route_active(
        &self,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) {
        let _ = self
            .tx
            .try_send(RtcWorkerCommand::SetRemoteSourceRouteActive {
                source_session_key,
                source_transport_media_id,
                target_id: self.target_id,
                active,
            });
    }

    pub(super) fn set_packet_gate(
        &self,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        let _ = self
            .tx
            .try_send(RtcWorkerCommand::SetRemoteSourcePacketGate {
                source_session_key,
                source_transport_media_id,
                target_id: self.target_id,
                packet_gate,
            });
    }
}

pub(super) enum RtcWorkerCommand {
    #[cfg(any(test, feature = "internal-benchmarks"))]
    BuildBootstrap {
        session_key: TransportSessionKey,
        router_capabilities: RtpCapabilities,
        response: oneshot::Sender<Result<SessionTransportBootstrap, TransportAdapterError>>,
    },
    CreateInitialSessionOffer {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Result<SessionOffer, TransportAdapterError>>,
    },
    CreateSessionRenegotiationOffer {
        session_key: TransportSessionKey,
        response: oneshot::Sender<Result<SessionOffer, TransportAdapterError>>,
    },
    ActiveSpeakerSourceSnapshot {
        response: oneshot::Sender<Result<Vec<ActiveSpeakerSource>, TransportAdapterError>>,
    },
    ApplySessionAnswer {
        session_key: TransportSessionKey,
        answer_sdp: String,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
    },
    #[cfg(test)]
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
        response: oneshot::Sender<Result<RemoveMediaOutcome, TransportAdapterError>>,
    },
    ResolveNegotiatedProducerParameters {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        response: oneshot::Sender<Result<RouterRtpParameters, TransportAdapterError>>,
    },
    ResolveMediaMid {
        transport_media_id: TransportMediaId,
        response: oneshot::Sender<Result<Option<String>, TransportAdapterError>>,
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
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: RouterRtpParameters,
        response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
    },
    RequestRemoteKeyframe {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    },
    SetRemoteSourceRouteActive {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
    },
    SetRemoteSourcePacketGate {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    },
    #[allow(
        dead_code,
        reason = "Phase 6 stages the source-owned layer policy command ahead of the lasting runtime caller so tests can verify the worker boundary first"
    )]
    SetSourcePacketGate {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
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
    #[cfg(feature = "internal-benchmarks")]
    RememberRemoteAddr {
        source_addr: SocketAddr,
        session_key: TransportSessionKey,
        response: oneshot::Sender<Result<(), TransportAdapterError>>,
    },
}
