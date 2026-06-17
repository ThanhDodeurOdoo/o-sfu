//! mailbox command contract for the RTC worker engine
//!
//! `MediaTransport` production paths and cfg-gated worker harnesses translate
//! transport intent into these values before the packet-loop task dispatches
//! them while it owns mutable rtc state
//! request commands carry a oneshot response
//! fire-and-forget route controls are best-effort because they may target a
//! worker that has already torn down the corresponding relay or session

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{KeyframeRequestKind, MediaKind, Rid};
use tokio::sync::{mpsc, oneshot};

use super::{
    relay_registry::{RelayPacketMailbox, RelayTargetId},
    route_control::{PacketLayerGate, PacketOperatingPointGate},
};
use crate::engine::{
    RoomInstanceId,
    media_transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
        ConsumerPacketGateUpdate, ReceiverBweTargetUpdate, SessionOffer, SourcePacketGate,
        TransportConsumerRoute, TransportMediaId, TransportResult, TransportSessionKey,
        TransportSourceActivitySnapshot, TransportSourceKey,
    },
    metrics::{RtcMetricsRecorder, RtcRemoteControlDropKind, RtcRemotePacketGateConvergence},
};

/// result class returned by a close-session command
///
/// close requests can remove only one session or leave the worker without live
/// sessions
/// the worker lifecycle uses this distinction to decide whether the lazy handle
/// must be cleared after the command completes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseSessionState {
    /// the requested session is no longer present while the worker can stay live
    SessionClosed,
    /// the requested session was the last live session on the worker
    WorkerIdle,
}

/// command handle used by remote consumers to push control back to a source worker
///
/// a route that consumes media from another worker keeps this handle beside the
/// remote-source registration
/// later keyframe or layer-gate requests can then reach the worker that owns
/// the producer without exposing the source worker internals
///
/// sends are deliberately best-effort
/// stale remote routes, closed workers and full mailboxes are normal during
/// teardown or topology churn
#[derive(Debug, Clone)]
pub struct RemoteSourceControl {
    tx: mpsc::Sender<RtcWorkerCommand>,
    target_id: RelayTargetId,
    metrics: Arc<RtcMetricsRecorder>,
}

impl RemoteSourceControl {
    /// creates a source-control handle for one relay target on a worker mailbox
    #[cfg(test)]
    pub(super) fn new(tx: mpsc::Sender<RtcWorkerCommand>, target_id: RelayTargetId) -> Self {
        Self::with_metrics(tx, target_id, Arc::new(RtcMetricsRecorder::default()))
    }

    pub(super) fn with_metrics(
        tx: mpsc::Sender<RtcWorkerCommand>,
        target_id: RelayTargetId,
        metrics: Arc<RtcMetricsRecorder>,
    ) -> Self {
        Self {
            tx,
            target_id,
            metrics,
        }
    }

    /// asks the source worker to request a keyframe for a remote consumer
    ///
    /// this never waits for the source worker
    /// if the command cannot be queued, the caller has no stronger recovery
    /// action than later media or control traffic triggering another request
    pub(super) fn request_kf(
        &self,
        source: &TransportSourceKey,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    ) -> bool {
        self.send_command(
            RtcWorkerCommand::MediaControl(RtcMediaControlCommand::Apply {
                request: RouteControlRequest::RequestRemoteKeyframe {
                    source: source.clone(),
                    target_id: self.target_id,
                    rid,
                    kind,
                },
                response: None,
            }),
            RtcRemoteControlDropKind::Keyframe,
        )
    }

    /// publishes the effective remote-source packet gate to the source worker
    pub(super) fn set_pkt_gate(
        &self,
        source: &TransportSourceKey,
        packet_gate: PacketLayerGate,
    ) -> bool {
        self.send_command(
            RtcWorkerCommand::MediaControl(RtcMediaControlCommand::Apply {
                request: RouteControlRequest::SetRemoteSourcePacketGate {
                    source: source.clone(),
                    target_id: self.target_id,
                    packet_gate,
                },
                response: None,
            }),
            RtcRemoteControlDropKind::PacketGate,
        )
    }

    pub(super) fn record_pkt_gate_retry(&self) {
        self.metrics
            .record_rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Retry);
    }

    pub(super) fn record_pkt_gate_flushed(&self) {
        self.metrics
            .record_rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Flushed);
    }

    fn send_command(&self, command: RtcWorkerCommand, drop_kind: RtcRemoteControlDropKind) -> bool {
        match self.tx.try_send(command) {
            Ok(()) => true,
            Err(_error) => {
                self.metrics.record_rtc_remote_control_drop(drop_kind);
                false
            }
        }
    }
}

/// response channel used by request commands that complete on the packet loop
///
/// dropping the receiver cancels the API wait but does not cancel the worker
/// mutation that is already being handled
pub type RtcWorkerResponse<T> = oneshot::Sender<TransportResult<T>>;

/// one consumer packet-gate update inside a source-scoped batch
///
/// batches keep dense-room layer changes as one mailbox command while still
/// returning one result per consumer update
#[derive(Debug, Clone)]
pub struct ConsumerPacketGateCommand {
    consumer_key: TransportSessionKey,
    consumer_media: TransportMediaId,
    packet_gate: PacketLayerGate,
}

impl ConsumerPacketGateCommand {
    pub fn from_update(update: &ConsumerPacketGateUpdate) -> Self {
        Self::new(
            update.route().consumer_session_key().clone(),
            update.route().consumer_transport_media_id(),
            packet_layer_gate(update.packet_gate()),
        )
    }

    /// builds one consumer update for a source-scoped packet-gate batch
    pub fn new(
        consumer_key: TransportSessionKey,
        consumer_media: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) -> Self {
        Self {
            consumer_key,
            consumer_media,
            packet_gate,
        }
    }

    /// splits the batch entry for worker-side validation and route mutation
    pub fn into_parts(self) -> (TransportSessionKey, TransportMediaId, PacketLayerGate) {
        (self.consumer_key, self.consumer_media, self.packet_gate)
    }
}

pub enum RtcMediaControlCommand {
    Apply {
        request: RouteControlRequest,
        response: Option<RtcWorkerResponse<()>>,
    },
    SetConsumerPacketGateBatch {
        source: TransportSourceKey,
        updates: Vec<ConsumerPacketGateCommand>,
        response: RtcWorkerResponse<Vec<TransportResult<()>>>,
    },
}

pub enum RouteControlRequest {
    SetProducerActive {
        source: TransportSourceKey,
        active: bool,
    },
    SetConsumerActive {
        route: TransportConsumerRoute,
        active: bool,
    },
    SetConsumerPacketGate {
        route: TransportConsumerRoute,
        packet_gate: PacketLayerGate,
    },
    RequestConsumerKeyframe {
        route: TransportConsumerRoute,
    },
    AddRelayTarget {
        source: TransportSourceKey,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    },
    RemoveRelayTarget {
        src_media: TransportMediaId,
        target_id: RelayTargetId,
    },
    SetRelayTargetActive {
        source: TransportSourceKey,
        target_id: RelayTargetId,
        active: bool,
    },
    RequestRemoteKeyframe {
        source: TransportSourceKey,
        target_id: RelayTargetId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    },
    SetRemoteSourcePacketGate {
        source: TransportSourceKey,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    },
}

impl RouteControlRequest {
    pub fn set_consumer_packet_gate(
        route: TransportConsumerRoute,
        packet_gate: &SourcePacketGate,
    ) -> Self {
        Self::SetConsumerPacketGate {
            route,
            packet_gate: packet_layer_gate(packet_gate),
        }
    }
}

fn packet_layer_gate(packet_gate: &SourcePacketGate) -> PacketLayerGate {
    match packet_gate {
        SourcePacketGate::Open => PacketLayerGate::Open,
        SourcePacketGate::Rid(rid) => PacketLayerGate::Rid(rid.as_str().into()),
        SourcePacketGate::OperatingPoint(operating_point) => {
            PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(
                operating_point.rid().map(Into::into),
                operating_point.max_temporal_layer_id(),
            ))
        }
    }
}

/// production command handled by the rtc packet-loop task
///
/// variants are grouped by ownership boundary: negotiation mutates str0m SDP
/// state, media commands mutate producer or consumer registrations, relay
/// commands mutate cross-worker fanout and observability commands read
/// worker-local snapshots
pub enum RtcWorkerCommand {
    /// bootstrap a session before any media registration exists
    ///
    /// this may bind the shared UDP socket, allocate the worker-local `Rtc`,
    /// register session bitrate tracking and stage the initial offer that probes
    /// browser capabilities
    /// it fails if an offer is already pending or the session already moved
    /// past bootstrap
    CreateInitialSessionOffer {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<SessionOffer>,
    },
    /// drain a staged follow-up offer after media topology changed
    ///
    /// media add and remove commands stage the SDP work before this command
    /// runs
    /// this command hands the staged offer to the worker API and preserves the
    /// one-outstanding-offer rule owned by the worker
    CreateSessionRenegotiationOffer {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<SessionOffer>,
    },
    /// read active-speaker sources from worker-local route-control state
    ///
    /// the result is a cold-path observation for room policy
    /// it does not mutate route state or packet-loop scheduling
    ActiveSpeakerSourceSnapshot {
        response: RtcWorkerResponse<Vec<ActiveSpeakerSource>>,
    },
    /// read detailed active-speaker diagnostics for operators and tests
    ///
    /// diagnostics expose the worker's route-control view rather than room
    /// policy state, so callers can inspect what the packet loop will actually
    /// use for source activity decisions
    ActiveSpeakerDiagnosticSnapshot {
        response: RtcWorkerResponse<Vec<ActiveSpeakerSourceDiagnostic>>,
    },
    /// read source packet activity from worker-local packet-loop state
    ///
    /// this is a cold-path diagnostics view. querying through the worker
    /// mailbox avoids mirroring every packet into a shared snapshot table.
    SourceActivitySnapshot {
        transport_media_ids: Vec<TransportMediaId>,
        response: RtcWorkerResponse<TransportSourceActivitySnapshot>,
    },
    /// read the next active-speaker expiry deadline owned by this worker
    ///
    /// schedulers use this to sleep until packet-loop observations need a
    /// room-level refresh instead of polling every live worker
    NextActiveSpeakerDeadline {
        response: RtcWorkerResponse<Option<Instant>>,
    },
    /// collect room ids whose active-speaker observations expired by `now`
    ///
    /// the command keeps expiry calculation beside the worker-local observation
    /// state and returns only the rooms that need an external wakeup
    ExpiredActiveSpeakerRoomInstanceIds {
        now: Instant,
        response: RtcWorkerResponse<BTreeSet<RoomInstanceId>>,
    },
    /// update receiver-side desired send bitrate for BWE probing
    ///
    /// the worker caps every target at its outgoing bitrate ceiling and dedupes
    /// unchanged values before touching str0m
    SetReceiverBweTargetBatch {
        updates: Vec<ReceiverBweTargetUpdate>,
        response: RtcWorkerResponse<Vec<TransportResult<()>>>,
    },
    /// accept the answer for the current pending local offer
    ///
    /// this commits str0m SDP state, marks the session dirty, refreshes
    /// negotiated producer parameters, registers remote candidate recovery hints
    /// and returns the producer details that became usable after the answer
    ApplySessionAnswer {
        session_key: TransportSessionKey,
        answer_sdp: String,
        response: RtcWorkerResponse<AppliedSessionAnswer>,
    },
    /// remove a session and report whether the worker has no live sessions
    ///
    /// cleanup removes rtc state, media handles, route destinations, demux
    /// indexes, bitrate counters and snapshot entries owned by the session
    /// `WorkerIdle` tells the worker lifecycle that only the idle packet loop
    /// and its reusable shared socket remain
    CloseSession {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<CloseSessionState>,
    },
    /// remove one producer or consumer media registration owned by a session
    ///
    /// producer removal drops incoming bitrate tracking and the source route
    /// consumer removal drops local rewrite state and the destination route
    /// negotiated media removal may stage the next SDP offer before the handle
    /// leaves the public registry
    RemoveMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<()>,
    },
    /// resolve negotiated producer parameters for adapter tests
    ///
    /// this test-only command reads the answer-derived producer state after
    /// negotiation so adapter tests can assert the transport boundary without
    /// reaching into private registries
    #[cfg(test)]
    ResolveNegotiatedProducerParameters {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<RouterRtpParameters>,
    },
    /// resolve the negotiated MID for one transport media id when it is known
    ///
    /// this is a best-effort lookup for worker API code that relates public
    /// transport ids back to browser-visible SDP identity
    /// it returns `None` before negotiation commits or after media removal
    ResolveMediaMid {
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<Option<String>>,
    },
    /// register one browser upload as worker-local producer media
    ///
    /// before the initial answer this can declare receive state directly in
    /// str0m
    /// after negotiation it stages a recv-only m-section plus pending
    /// receive identities, then registers bitrate counters and the producer
    /// media handle
    AddRecvMedia {
        session_key: TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: RouterRtpParameters,
        response: RtcWorkerResponse<TransportMediaId>,
    },
    /// register one browser download as consumer media for a source
    ///
    /// the worker validates local or remote source ownership, stages or declares
    /// send-only media, registers the consumer handle and creates the packet-loop
    /// route destination
    /// remote sources install rollback-protected control so failed consumer
    /// setup does not leave stale relay state behind
    AddSendMedia {
        consumer_key: TransportSessionKey,
        media_kind: MediaKind,
        source: TransportSourceKey,
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: RouterRtpParameters,
        active: bool,
        response: RtcWorkerResponse<TransportMediaId>,
    },
    MediaControl(RtcMediaControlCommand),
}

impl RtcWorkerCommand {
    pub fn media_control(request: RouteControlRequest, response: RtcWorkerResponse<()>) -> Self {
        Self::MediaControl(RtcMediaControlCommand::Apply {
            request,
            response: Some(response),
        })
    }
}
