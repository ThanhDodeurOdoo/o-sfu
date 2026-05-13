use std::{collections::BTreeSet, time::Instant};

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{KeyframeRequestKind, MediaKind, Rid};
use tokio::sync::{mpsc, oneshot};

use super::{
    relay_registry::{RelayTargetId, RelayTargetTransport},
    route_control::PacketLayerGate,
};
use crate::runtime::{
    RoomInstanceId,
    media_transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, SessionOffer,
        TransportMediaId, TransportResult, TransportSessionKey,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseSessionState {
    SessionClosed,
    WorkerDrained,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseSessionOutcome {
    state: CloseSessionState,
}

impl CloseSessionOutcome {
    pub(super) const fn new(state: CloseSessionState) -> Self {
        Self { state }
    }

    pub const fn state(&self) -> CloseSessionState {
        self.state
    }
}

#[derive(Debug, Clone)]
pub struct RemoteSourceControl {
    tx: mpsc::Sender<RtcWorkerCommand>,
    target_id: RelayTargetId,
}

impl RemoteSourceControl {
    pub(super) fn new(tx: mpsc::Sender<RtcWorkerCommand>, target_id: RelayTargetId) -> Self {
        Self { tx, target_id }
    }

    pub fn request_keyframe(
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

pub type RtcWorkerResponse<T> = oneshot::Sender<TransportResult<T>>;

#[derive(Debug, Clone)]
pub struct ConsumerPacketGateCommand {
    consumer_session_key: TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    packet_gate: PacketLayerGate,
}

impl ConsumerPacketGateCommand {
    pub fn new(
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) -> Self {
        Self {
            consumer_session_key,
            consumer_transport_media_id,
            packet_gate,
        }
    }

    pub fn into_parts(self) -> (TransportSessionKey, TransportMediaId, PacketLayerGate) {
        (
            self.consumer_session_key,
            self.consumer_transport_media_id,
            self.packet_gate,
        )
    }
}

pub(super) enum RtcWorkerCommand {
    CreateInitialSessionOffer {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<SessionOffer>,
    },
    CreateSessionRenegotiationOffer {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<SessionOffer>,
    },
    ActiveSpeakerSourceSnapshot {
        response: RtcWorkerResponse<Vec<ActiveSpeakerSource>>,
    },
    ActiveSpeakerDiagnosticSnapshot {
        response: RtcWorkerResponse<Vec<ActiveSpeakerSourceDiagnostic>>,
    },
    NextActiveSpeakerDeadline {
        response: RtcWorkerResponse<Option<Instant>>,
    },
    ExpiredActiveSpeakerRoomInstanceIds {
        now: Instant,
        response: RtcWorkerResponse<BTreeSet<RoomInstanceId>>,
    },
    ApplySessionAnswer {
        session_key: TransportSessionKey,
        answer_sdp: String,
        response: RtcWorkerResponse<AppliedSessionAnswer>,
    },
    CloseSession {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<CloseSessionOutcome>,
    },
    RemoveMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<()>,
    },
    #[cfg(test)]
    ResolveNegotiatedProducerParameters {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<RouterRtpParameters>,
    },
    ResolveMediaMid {
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<Option<String>>,
    },
    AddRecvMedia {
        session_key: TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: RouterRtpParameters,
        response: RtcWorkerResponse<TransportMediaId>,
    },
    AddSendMedia {
        consumer_session_key: TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: RouterRtpParameters,
        response: RtcWorkerResponse<TransportMediaId>,
    },
    AddRelayTarget {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        target: RelayTargetTransport,
        response: RtcWorkerResponse<()>,
    },
    RemoveRelayTarget {
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        response: RtcWorkerResponse<()>,
    },
    SetRelayTargetActive {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
        response: RtcWorkerResponse<()>,
    },
    RequestRemoteKeyframe {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    },
    SetRemoteSourcePacketGate {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    },
    SetProducerActive {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
        response: RtcWorkerResponse<()>,
    },
    SetConsumerActive {
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
        response: RtcWorkerResponse<()>,
    },
    SetConsumerPacketGate {
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
        response: RtcWorkerResponse<()>,
    },
    SetConsumerPacketGateBatch {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        updates: Vec<ConsumerPacketGateCommand>,
        response: RtcWorkerResponse<Vec<TransportResult<()>>>,
    },
    RequestConsumerKeyframe {
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<()>,
    },
}
