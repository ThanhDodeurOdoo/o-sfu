//! mailbox command contract for the worker-owned rtc engine
//!
//! public facades translate transport API calls into these values before the
//! packet-loop task dispatches them while it owns mutable rtc state
//! request commands carry a oneshot response
//! fire-and-forget route controls are intentionally best-effort because they
//! may target a worker that has already torn down the corresponding relay or
//! session

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

/// result class returned by a close-session command
///
/// close requests can remove only one session or drain the whole worker
/// the facade uses this distinction to decide whether the lazy worker handle
/// must be cleared after the command completes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseSessionState {
    /// the requested session is no longer present while the worker can stay live
    SessionClosed,
    /// the requested session was the last worker-owned session
    WorkerDrained,
}

/// close-session response shared with the transport facade
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseSessionOutcome {
    state: CloseSessionState,
}

impl CloseSessionOutcome {
    /// builds a close outcome from the worker-local session cleanup result
    pub(super) const fn new(state: CloseSessionState) -> Self {
        Self { state }
    }

    /// returns the lifecycle state that the facade must apply after close
    pub const fn state(&self) -> CloseSessionState {
        self.state
    }
}

/// command handle used by remote consumers to push control back to a source worker
///
/// a route that consumes media from another worker keeps this handle beside the
/// remote-source registration
/// later keyframe or layer-gate requests can then reach the worker that owns
/// the producer without exposing its full facade
///
/// sends are deliberately best-effort
/// stale remote routes, closed workers and full mailboxes are normal during
/// teardown or topology churn
#[derive(Debug, Clone)]
pub struct RemoteSourceControl {
    tx: mpsc::Sender<RtcWorkerCommand>,
    target_id: RelayTargetId,
}

impl RemoteSourceControl {
    /// creates a source-control handle for one relay target on a worker mailbox
    pub(super) fn new(tx: mpsc::Sender<RtcWorkerCommand>, target_id: RelayTargetId) -> Self {
        Self { tx, target_id }
    }

    /// asks the source worker to request a keyframe for a remote consumer
    ///
    /// this never waits for the source worker
    /// if the command cannot be queued, the caller has no stronger recovery
    /// action than future media or control traffic triggering another request
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

    /// publishes the effective remote-source packet gate to the source worker
    ///
    /// the command is best-effort for the same reason as keyframe requests
    /// route-control state is eventually refreshed by later route mutations
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

/// response channel used by request commands that complete on the packet loop
///
/// dropping the receiver cancels the facade wait but does not cancel the worker
/// mutation that is already being handled
pub type RtcWorkerResponse<T> = oneshot::Sender<TransportResult<T>>;

/// one consumer packet-gate update inside a source-scoped batch
///
/// batches keep dense-room layer changes as one mailbox command while still
/// returning one result per consumer update
#[derive(Debug, Clone)]
pub struct ConsumerPacketGateCommand {
    consumer_session_key: TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    packet_gate: PacketLayerGate,
}

impl ConsumerPacketGateCommand {
    /// builds one consumer update for a source-scoped packet-gate batch
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

    /// splits the batch entry for worker-side validation and route mutation
    pub fn into_parts(self) -> (TransportSessionKey, TransportMediaId, PacketLayerGate) {
        (
            self.consumer_session_key,
            self.consumer_transport_media_id,
            self.packet_gate,
        )
    }
}

/// production command handled by the rtc packet-loop task
///
/// variants are grouped by ownership boundary: negotiation mutates str0m SDP
/// state, media commands mutate producer or consumer registrations, relay
/// commands mutate cross-worker fanout and observability commands read
/// worker-local snapshots
pub(super) enum RtcWorkerCommand {
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
    /// this command hands the staged offer to the facade and preserves the
    /// one-outstanding-offer rule owned by the worker
    CreateSessionRenegotiationOffer {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<SessionOffer>,
    },
    /// read active-speaker sources from worker-local route-control state
    ///
    /// the result is a cold-path observation for room orchestration
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
    /// read the next active-speaker expiry deadline owned by this worker
    ///
    /// schedulers use this to sleep until packet-loop observations need a
    /// room-level refresh instead of polling every live worker
    NextActiveSpeakerDeadline {
        response: RtcWorkerResponse<Option<Instant>>,
    },
    /// collect room ids whose active-speaker observations expired by `now`
    ///
    /// the command keeps expiry calculation beside the worker-owned observation
    /// state and returns only the rooms that need an external wakeup
    ExpiredActiveSpeakerRoomInstanceIds {
        now: Instant,
        response: RtcWorkerResponse<BTreeSet<RoomInstanceId>>,
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
    /// remove a session and report whether the worker can be shut down
    ///
    /// cleanup removes rtc state, media handles, route destinations, demux
    /// indexes, bitrate counters and snapshot entries owned by the session
    /// `WorkerDrained` tells the facade to clear the lazy worker handle
    CloseSession {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<CloseSessionOutcome>,
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
    /// this is a best-effort lookup for facade code that needs to relate public
    /// transport ids back to browser-visible SDP identity
    /// it returns `None` before negotiation commits or after media removal
    ResolveMediaMid {
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<Option<String>>,
    },
    /// register one browser upload as worker-owned producer media
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
        consumer_session_key: TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: RouterRtpParameters,
        response: RtcWorkerResponse<TransportMediaId>,
    },
    /// attach a relay target for a source media stream
    ///
    /// the source worker validates producer ownership and records the target in
    /// packet-loop relay state
    /// later route planning can then include the target when the source-wide
    /// gate permits forwarding
    AddRelayTarget {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        target: RelayTargetTransport,
        response: RtcWorkerResponse<()>,
    },
    /// detach a relay target from a source media stream
    ///
    /// this removes the target from relay fanout for the source and answers
    /// success even when the target was already gone, which makes teardown
    /// idempotent for callers
    RemoveRelayTarget {
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        response: RtcWorkerResponse<()>,
    },
    /// toggle whether one relay target receives packets for a source media stream
    ///
    /// activity is separate from target registration
    /// inactive targets keep their identity and transport handle but stop
    /// receiving packets and remote keyframe requests for the source
    SetRelayTargetActive {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
        response: RtcWorkerResponse<()>,
    },
    /// request a keyframe from a remote source worker for one relay target
    ///
    /// this is sent by `RemoteSourceControl` without a response channel
    /// the source worker first checks that the relay target is still active,
    /// then applies normal RID selection and keyframe throttling
    RequestRemoteKeyframe {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    },
    /// update the source-worker packet gate derived from remote consumers
    ///
    /// this is the cross-worker layer-selection feedback path
    /// the source worker stores the target gate in relay route-control so remote
    /// demand influences which producer layers leave the source worker
    SetRemoteSourcePacketGate {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    },
    /// toggle source-wide fanout for one producer media id
    ///
    /// this preserves the producer media handle and its consumer routes while
    /// room policy pauses or resumes forwarding from the source
    /// it does not renegotiate SDP
    SetProducerActive {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
        response: RtcWorkerResponse<()>,
    },
    /// toggle one consumer destination without changing other routes
    ///
    /// the worker revalidates the consumer handle and source route, mutates only
    /// that destination and refreshes the aggregate source packet gate when the
    /// effective route changed
    SetConsumerActive {
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
        response: RtcWorkerResponse<()>,
    },
    /// replace one consumer destination layer gate
    ///
    /// selected-RID gates are checked against packet-path liveness before they
    /// become effective
    /// the route still remembers pending strict gates so a browser can switch
    /// layers once the target RID becomes decodable
    SetConsumerPacketGate {
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
        response: RtcWorkerResponse<()>,
    },
    /// replace several consumer layer gates for one source media id
    ///
    /// batching keeps dense-room layer updates in one worker turn and one
    /// source-gate refresh
    /// the outer result reports command handling while the inner results
    /// preserve per-consumer validation errors
    SetConsumerPacketGateBatch {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        updates: Vec<ConsumerPacketGateCommand>,
        response: RtcWorkerResponse<Vec<TransportResult<()>>>,
    },
    /// request a keyframe for a local consumer route
    ///
    /// the worker revalidates consumer and source ownership, maps the consumer
    /// route gate back to a producer RID when needed and either asks the local
    /// producer or forwards the request through remote-source control
    RequestConsumerKeyframe {
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<()>,
    },
}
