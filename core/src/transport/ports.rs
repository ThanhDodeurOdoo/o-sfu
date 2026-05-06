//! Concern-oriented transport ports used by room and signaling code.
//!
//! A port is a narrow trait that exposes one transport concern such as SDP
//! negotiation, session teardown, media wiring, observability or source-policy
//! wakeups. Callers depend on the smallest port they need, which keeps RTC
//! backend details below the core boundary and keeps deterministic test
//! transports substitutable.
//!
//! A caller that creates an offer, applies an answer and projects negotiated RTP
//! capabilities knows it is performing negotiation. It does not know whether the
//! backend is backed by str0m, a fake transport or a future transport
//! implementation.

use std::{collections::BTreeSet, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};

use crate::{
    RoomInstanceId,
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
        ConsumerPacketGateUpdate, ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate,
        SourcePolicyUpdateSubscription, TransportAdapterError, TransportBitrateSnapshot,
        TransportMediaId, TransportSessionHealth, TransportSessionKey,
    },
};

/// Full transport backend contract required by the media-core facade.
///
/// `SfuCore` needs negotiation, media mutation, session cleanup and read-only
/// transport observability against the same backend. Code that only needs one
/// concern should keep depending on the narrower port trait instead.
pub trait TransportFacade:
    Clone + MediaPort + NegotiationPort + ObservabilityPort + SessionPort + Send + Sync
{
}

impl<T> TransportFacade for T where
    T: Clone + MediaPort + NegotiationPort + ObservabilityPort + SessionPort + Send + Sync
{
}

/// Producer-side transport activity state.
///
/// This is transport execution policy, not room membership. The room remains
/// responsible for deciding whether a source should be considered published or
/// visible to participants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerActivity {
    /// RTP from this producer should be forwarded when routes allow it.
    Active,
    /// RTP from this producer should not be forwarded until reactivated.
    Inactive,
}

impl ProducerActivity {
    /// Converts a boolean activity flag into the explicit transport state.
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    /// Returns whether this state allows producer forwarding.
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Consumer-side transport activity state.
///
/// A consumer can be inactive even while the room still owns the subscription.
/// That distinction lets source policy pause delivery without deleting the
/// negotiated transport route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerActivity {
    /// RTP may be delivered to this consumer when its packet gate also allows
    /// the packet.
    Active,
    /// RTP delivery to this consumer is paused.
    Inactive,
}

impl ConsumerActivity {
    /// Converts a boolean activity flag into the explicit transport state.
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    /// Returns whether this state allows consumer delivery.
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Handles the SDP negotiation lifecycle for a transport session.
///
/// The room owns participant intent. The transport owns the session-local RTC
/// offer and answer state needed to realize that intent. Implementations return
/// [`TransportAdapterError`] when the session is unknown, SDP input is invalid
/// or the backend cannot advance its RTC state.
#[allow(
    async_fn_in_trait,
    reason = "transport ports are intentionally static-dispatch crate-boundary contracts and are not exposed as dyn trait objects"
)]
pub trait NegotiationPort {
    /// Creates the first SDP offer for a transport session.
    ///
    /// The session must already be assigned to a transport worker by its
    /// [`TransportSessionKey`]. The returned offer is transport-owned state that
    /// callers should send to the browser unchanged.
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError>;

    /// Creates a new SDP offer after transport media state changed.
    ///
    /// Implementations reject sessions that cannot renegotiate in their current
    /// backend state. Callers should treat the returned offer as replacing any
    /// older pending transport offer for the same session.
    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError>;

    /// Applies a browser SDP answer to a pending transport offer.
    ///
    /// The answer can reveal transport-derived producer facts such as mapped
    /// RTP parameters. Those facts are returned so room code can commit staged
    /// media using values observed by the transport.
    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError>;

    /// Projects the client's negotiated RTP capabilities from an SDP answer.
    ///
    /// This method does not mutate transport state. It validates the answer
    /// against the router capabilities offered to the browser and returns the
    /// capability set room policy may use for future consumer creation.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the answer cannot be parsed or
    /// cannot be projected onto the offered capabilities.
    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError>;
}

/// Handles transport-session teardown.
///
/// This port is separate from media mutation so cleanup paths can depend only
/// on the ability to close a session and release its transport resources.
#[allow(
    async_fn_in_trait,
    reason = "transport ports are intentionally static-dispatch crate-boundary contracts and are not exposed as dyn trait objects"
)]
pub trait SessionPort {
    /// Closes all backend state owned by a transport session.
    ///
    /// Implementations should release producer, consumer and relay state tied
    /// to the session. The operation is idempotent only if the backend documents
    /// that behavior through its returned [`TransportAdapterError`].
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError>;
}

/// Handles transport media state for established transport sessions.
///
/// This port turns room-owned media intent into transport-owned producer and
/// consumer handles. A transport media id is never a room source id. It is a
/// backend-local realization that room code stores only so it can address later
/// transport mutations.
#[allow(
    async_fn_in_trait,
    reason = "transport ports are intentionally static-dispatch crate-boundary contracts and are not exposed as dyn trait objects"
)]
pub trait MediaPort {
    /// Removes one producer or consumer handle from a transport session.
    ///
    /// Backends return [`TransportAdapterError`] when the session or media id is
    /// unknown or when the underlying transport cannot release the resource.
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError>;

    /// Declares a new producer on a transport session.
    ///
    /// `rtp_parameters` must come from router media state accepted by the core.
    /// The returned [`TransportMediaId`] addresses the backend-local producer
    /// for later route, activity and cleanup operations.
    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError>;

    /// Declares a new consumer route from a source session to a consumer
    /// session.
    ///
    /// The source and consumer sessions must belong to the same room instance.
    /// Cross-worker routing is an implementation detail hidden behind this
    /// method. Callers receive only the consumer-side transport media id.
    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError>;

    /// Updates whether a producer may forward packets.
    ///
    /// This is a transport-level switch. Removing the room source or changing
    /// participant permissions remains the responsibility of room state.
    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError>;

    /// Updates whether a consumer route may receive packets.
    ///
    /// The source and consumer ids identify the same route from both sides so
    /// cross-worker backends can address the correct forwarding state.
    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        activity: ConsumerActivity,
    ) -> Result<(), TransportAdapterError>;

    /// Applies source-policy packet gating to one consumer route.
    ///
    /// Packet gates are transport execution policy derived from room-owned
    /// source selection. Implementations should reject cross-room routes and
    /// unknown transport media ids with [`TransportAdapterError`].
    async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError>;

    /// Applies packet gates for multiple routes and preserves input order in
    /// the returned results.
    ///
    /// The default implementation is deliberately sequential. Backends with
    /// shard-local batching can override it while keeping the same result
    /// contract for room policy.
    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let mut results = Vec::with_capacity(updates.len());
        for update in updates {
            results.push(
                self.set_consumer_packet_gate(
                    update.consumer_session_key(),
                    update.consumer_transport_media_id(),
                    update.source_session_key(),
                    update.source_transport_media_id(),
                    update.packet_gate().clone(),
                )
                .await,
            );
        }
        results
    }

    /// Requests a keyframe for one consumer route.
    ///
    /// This is a best-effort transport command used after route changes or
    /// visible layer changes. Failure means the backend could not address the
    /// requested route at the time of the call.
    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError>;

    /// Returns the negotiated MID for a transport media handle when known.
    ///
    /// `None` means the media id is unknown, no MID has been negotiated yet or
    /// the backend has already removed the handle.
    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String>;
}

/// Exposes read-only transport snapshots for diagnostics and runtime decisions.
///
/// Observability methods return transport-observed facts. They can race with
/// packet processing and session cleanup, so callers must not treat them as
/// room membership authority.
#[allow(
    async_fn_in_trait,
    reason = "transport ports are intentionally static-dispatch crate-boundary contracts and are not exposed as dyn trait objects"
)]
pub trait ObservabilityPort {
    /// Returns the latest bitrate estimates for the requested sessions.
    ///
    /// Missing sessions are omitted from the snapshot. Estimates are suitable
    /// for diagnostics and policy input, not for accounting.
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot;

    /// Returns receiver-side bandwidth estimates for the requested sessions.
    ///
    /// Room policy may use these estimates to choose source operating points.
    /// They are best-effort observations from the transport backend.
    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot;

    /// Returns recent active-speaker sources observed by transport workers.
    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource>;

    /// Returns diagnostic active-speaker state for operator-facing output.
    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic>;

    /// Returns the next known deadline for active-speaker expiry work.
    ///
    /// Runtimes use this to schedule source-policy wakeups without polling all
    /// rooms on a fixed cadence.
    async fn next_active_speaker_deadline(&self) -> Option<Instant>;

    /// Returns rooms whose transport-observed active-speaker state expired by
    /// `now`.
    ///
    /// The returned ids identify room instances that should resync room-owned
    /// source policy after transport observations changed.
    async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId>;

    /// Returns the latest known transport health for one session.
    ///
    /// Health is connectivity evidence only. It should not be used as the
    /// source of truth for whether a participant belongs to a room.
    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth>;
}

/// Exposes wakeups for transport-observed source-policy changes.
///
/// A source-policy subscription lets room tasks sleep until transport
/// observations make source policy dirty, such as active-speaker expiry or
/// route health changes. The returned subscription carries signals only. Room
/// state remains the authority for the policy itself.
pub trait SourcePolicyPort {
    /// Subscribes to source-policy invalidation signals emitted by transport
    /// workers.
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription;
}
