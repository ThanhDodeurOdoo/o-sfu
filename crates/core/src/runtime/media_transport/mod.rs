//! Runtime media transport boundary used by core orchestration.
//!
//! This module contains the service surface that room, server plus [`crate::SfuCore`]
//! code use when they need media work to happen outside the pure router. It is
//! intentionally named after the capability it provides: callers ask the media transport to create SDP
//! offers, apply answers, publish or consume media, close sessions, read
//! diagnostics snapshots plus subscribe to source-policy wakeups.
//!
//! The boundary has three responsibilities:
//!
//! - expose the opaque [`MediaTransport`] handle plus the narrow construction
//!   inputs needed by the server runtime
//! - expose transport operations directly on [`MediaTransport`] so higher
//!   layers express intent without knowing about RTC workers, Str0m state, UDP
//!   sockets or worker-local relay routing
//!
//! Code above this module should depend on [`MediaTransport`]. Code below this
//! module, especially the RTC engine, may deal with worker-local state machines
//! and packet-loop details. Keeping that split explicit prevents room and
//! signaling orchestration from growing knowledge of the concrete WebRTC
//! implementation.

mod builder;
mod config;
mod operation;
mod source_policy;
#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;
mod types;
mod worker_manager;

use std::{collections::BTreeSet, sync::Arc, time::Instant};

pub use builder::{MediaTransportBuildError, MediaTransportBuilder};
pub use config::{MediaTransportConfig, MediaTransportDeps};
use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
use operation::TransportControlOperation;
pub use source_policy::{
    SourcePolicyDirtyState, SourcePolicySignal, SourcePolicyUpdateSubscription,
};
use tracing::warn;
pub use types::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
    ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerActivity,
    ConsumerPacketGateUpdate, ProducerActivity, ReceiverBandwidthSnapshot, RelayRouteActivity,
    SessionOffer, SessionUploadEncoding, SessionUploadSlot, SourcePacketGate,
    SourcePacketOperatingPoint, TransportAdapterError, TransportBitrateSnapshot,
    TransportConsumerRoute, TransportMediaId, TransportPlacementPressureSnapshot,
    TransportRelayRouteAction, TransportRelayRouteEffect, TransportResult, TransportSessionHealth,
    TransportSessionKey, TransportWorkerPressureSnapshot,
};
use worker_manager::RtcWorkerManager;

use crate::runtime::RoomInstanceId;

/// Opaque runtime media transport handle.
///
/// `MediaTransport` is the type server orchestration and `SfuCore` should hold.
/// It hides the production RTC worker topology. Callers express intent through
/// inherent methods and must not branch on concrete worker internals.
///
/// The handle also centralizes warning logs for failed transport effects. Inner
/// backends return typed errors, while this boundary adds stable diagnostic
/// context such as session keys, media ids and SDP lengths.
#[derive(Debug, Clone)]
pub struct MediaTransport {
    worker_manager: Arc<RtcWorkerManager>,
}

impl MediaTransport {
    /// Creates the first SDP offer for a transport session.
    ///
    /// The session must already be assigned to a transport worker by its
    /// [`TransportSessionKey`]. The returned offer is transport-owned state that
    /// callers should send to the browser unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed or
    /// the active backend cannot create the offer.
    pub async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.worker_manager
            .create_initial_session_offer(session_key)
            .await
            .inspect_err(|error| {
                warn!(
                    ?session_key,
                    ?error,
                    "media transport failed to create initial user offer"
                );
            })
    }

    /// Creates a new SDP offer after transport media state changed.
    ///
    /// Backends reject sessions that cannot renegotiate in their current state.
    /// Callers should treat the returned offer as replacing any older pending
    /// transport offer for the same session.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed or
    /// the active backend cannot create a renegotiation offer.
    pub async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.worker_manager
            .create_session_renegotiation_offer(session_key)
            .await
            .inspect_err(|error| {
                warn!(
                    ?session_key,
                    ?error,
                    "media transport failed to create renegotiation offer"
                );
            })
    }

    /// Applies a browser SDP answer to a pending transport offer.
    ///
    /// The answer can reveal transport-derived producer facts such as mapped
    /// RTP parameters. Those facts are returned so room code can commit staged
    /// media using values observed by the transport.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed,
    /// the answer is invalid or the active backend cannot apply it.
    pub async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        self.worker_manager
            .apply_session_answer(session_key, answer_sdp)
            .await
            .inspect_err(|error| {
                warn!(
                    ?session_key,
                    answer_len = answer_sdp.len(),
                    ?error,
                    "media transport failed to apply user answer"
                );
            })
    }

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
    pub fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        RtcWorkerManager::negotiated_client_rtp_capabilities(
            answer_sdp,
            offered_router_capabilities,
        )
        .inspect_err(|error| {
            warn!(
                answer_len = answer_sdp.len(),
                ?error,
                "media transport failed to derive client RTP capabilities from answer SDP"
            );
        })
    }
    /// Closes all backend state owned by a transport session.
    ///
    /// Backends should release producer, consumer and relay state tied to the
    /// session. The operation is idempotent only when the active backend
    /// documents that behavior through its returned [`TransportAdapterError`].
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed or
    /// the active backend cannot release its resources.
    pub async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.worker_manager
            .close_session(session_key)
            .await
            .inspect_err(|error| {
                warn!(?session_key, ?error, "media transport failed to close user");
            })
    }
    /// Removes one producer or consumer handle from a transport session.
    ///
    /// Returns [`TransportAdapterError`] when the session or media id is unknown
    /// or when the underlying transport cannot release the resource.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session or media id is unknown
    /// or when the active backend cannot release the resource.
    pub async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.worker_manager
            .remove_media(session_key, transport_media_id)
            .await
            .inspect_err(|error| {
                warn!(
                    ?session_key,
                    ?transport_media_id,
                    ?error,
                    "media transport failed to remove media"
                );
            })
    }

    /// Declares a new producer on a transport session.
    ///
    /// `rtp_parameters` must come from router media state accepted by the core.
    /// The returned [`TransportMediaId`] addresses the backend-local producer
    /// for later route, activity and cleanup operations.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed,
    /// the RTP parameters are invalid or the active backend cannot create the
    /// producer.
    pub async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.worker_manager
            .publish_media(session_key, media_kind, rtp_parameters)
            .await
            .inspect_err(|error| {
                warn!(
                    ?session_key,
                    ?media_kind,
                    mid = rtp_parameters.mid(),
                    ?error,
                    "media transport failed to declare producer media"
                );
            })
    }

    /// Declares a new consumer route from a source session to a consumer session.
    ///
    /// The source and consumer sessions must belong to the same room instance.
    /// Cross-worker routing is an implementation detail hidden behind this
    /// method. Callers receive only the consumer-side transport media id.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when either session cannot be
    /// addressed, the source media id is unknown or the active backend cannot
    /// create the consumer.
    pub async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.worker_manager
            .consume_media(
                consumer_session_key,
                media_kind,
                source_session_key,
                source_media_id,
                consumer_rtp_parameters,
            )
            .await
            .inspect_err(|error| {
                warn!(
                    ?consumer_session_key,
                    ?source_session_key,
                    ?source_media_id,
                    ?media_kind,
                    mid = consumer_rtp_parameters.mid(),
                    ?error,
                    "media transport failed to declare consumer media"
                );
            })
    }

    /// Applies a room-owned relay route mutation.
    ///
    /// The transport may install, release or gate the packet-loop relay target.
    /// Room state remains the lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the referenced route cannot be
    /// addressed or the active backend cannot apply the relay mutation.
    pub async fn apply_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        self.worker_manager
            .execute_control_operation(TransportControlOperation::RelayRouteEffect(effect.clone()))
            .await
            .inspect_err(|error| {
                warn!(
                source_session_key = ?effect.source_session_key,
                source_transport_media_id = ?effect.source_transport_media_id,
                target_media_worker_id = effect.target_media_worker_id,
                action = ?effect.action,
                ?error,
                "media transport failed to apply relay route effect"
                );
            })
    }

    /// Updates whether a producer may forward packets.
    ///
    /// This is a transport-level switch. Removing the room source or changing
    /// participant permissions remains the responsibility of room state.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session or media id is unknown
    /// or the active backend cannot update producer activity.
    pub async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        self.worker_manager
            .execute_control_operation(TransportControlOperation::SetProducerActivity {
                session_key: session_key.clone(),
                transport_media_id,
                activity,
            })
            .await
            .inspect_err(|error| {
                warn!(
                    ?session_key,
                    ?transport_media_id,
                    active = activity.is_active(),
                    ?error,
                    "media transport failed to update producer activity"
                );
            })
    }

    /// Updates whether a consumer route may receive packets.
    ///
    /// The route identifies the same path from both sides so cross-worker
    /// backends can address the correct forwarding state.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when either session or media id is
    /// unknown or the active backend cannot update consumer activity.
    pub async fn set_consumer_active(
        &self,
        route: &TransportConsumerRoute,
        activity: ConsumerActivity,
    ) -> Result<(), TransportAdapterError> {
        self.worker_manager
            .execute_control_operation(TransportControlOperation::SetConsumerActivity {
                route: route.clone(),
                activity,
            })
            .await
            .inspect_err(|error| {
                warn!(
                    ?route,
                    active = activity.is_active(),
                    ?error,
                    "media transport failed to update consumer activity"
                );
            })
    }

    /// Applies source-policy packet gating to one consumer route.
    ///
    /// Packet gates are transport execution policy derived from room-owned
    /// source selection. Invalid routes or unknown transport media ids return
    /// [`TransportAdapterError`].
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when either session or media id is
    /// unknown, the route is invalid or the active backend cannot apply the
    /// packet gate.
    pub async fn set_consumer_packet_gate(
        &self,
        route: &TransportConsumerRoute,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        self.worker_manager
            .execute_control_operation(TransportControlOperation::SetConsumerPacketGate {
                route: route.clone(),
                packet_gate: packet_gate.clone(),
            })
            .await
            .inspect_err(|error| {
                warn!(
                    ?route,
                    ?packet_gate,
                    ?error,
                    "media transport failed to update consumer packet gate"
                );
            })
    }

    /// Applies packet gates for multiple routes and preserves input order in
    /// the returned results.
    pub async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let results = self
            .worker_manager
            .execute_consumer_packet_gate_batch(updates)
            .await;
        for (update, result) in updates.iter().zip(results.iter()) {
            if let Err(error) = result {
                warn!(
                    ?error,
                    route = ?update.route(),
                    packet_gate = ?update.packet_gate(),
                    "media transport failed to update a batched consumer packet gate"
                );
            }
        }
        results
    }

    /// Requests a keyframe for one consumer route.
    ///
    /// This is a best-effort transport command used after route changes or
    /// visible layer changes. Failure means the backend could not address the
    /// requested route at the time of the call.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when either session or media id is
    /// unknown or the active backend cannot request a keyframe.
    pub async fn request_consumer_keyframe(
        &self,
        route: &TransportConsumerRoute,
    ) -> Result<(), TransportAdapterError> {
        self.worker_manager
            .execute_control_operation(TransportControlOperation::RequestConsumerKeyframe {
                route: route.clone(),
            })
            .await
            .inspect_err(|error| {
                warn!(
                    ?route,
                    ?error,
                    "media transport failed to request a consumer keyframe refresh"
                );
            })
    }

    /// Returns the negotiated MID for a transport media handle when known.
    ///
    /// `None` means the media id is unknown, no MID has been negotiated yet or
    /// the backend has already removed the handle.
    pub async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        self.worker_manager
            .transport_media_mid(session_key, transport_media_id)
            .await
    }
    /// Returns the latest bitrate estimates for the requested sessions.
    ///
    /// Missing sessions are omitted from the snapshot. Estimates are suitable
    /// for diagnostics and policy input, not for accounting.
    #[must_use]
    pub fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        self.worker_manager.transport_bitrate_snapshot(session_keys)
    }

    /// Returns receiver-side bandwidth estimates for the requested sessions.
    ///
    /// Room policy may use these estimates to choose source operating points.
    /// They are best-effort observations from the transport backend.
    #[must_use]
    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        self.worker_manager
            .receiver_bandwidth_snapshot(session_keys)
    }

    /// Returns transport-worker pressure for the workers that own the sessions.
    ///
    /// Room placement uses this as a best-effort load signal. Missing or drained
    /// workers return no pressure because room membership remains authoritative.
    #[must_use]
    pub fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        self.worker_manager
            .placement_pressure_snapshot(session_keys)
    }

    /// Returns transport-worker pressure for every worker the backend can rank.
    ///
    /// Placement uses this as best-effort process-local load input. Missing
    /// workers are treated by callers as idle because transport snapshots can
    /// race with worker startup and teardown.
    #[must_use]
    pub fn worker_pressure_snapshots(&self) -> Vec<TransportWorkerPressureSnapshot> {
        self.worker_manager.worker_pressure_snapshots()
    }

    /// Returns recent active-speaker sources observed by transport workers.
    pub async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.worker_manager.active_speaker_source_snapshot().await
    }

    /// Returns diagnostic active-speaker state for operator-facing output.
    pub async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.worker_manager
            .active_speaker_diagnostic_snapshot()
            .await
    }

    /// Returns the next known deadline for active-speaker expiry work.
    ///
    /// Runtimes use this to schedule source-policy wakeups without polling all
    /// rooms on a fixed cadence.
    pub async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        self.worker_manager.next_active_speaker_deadline().await
    }

    /// Returns rooms whose transport-observed active-speaker state expired by
    /// `now`.
    ///
    /// The returned ids identify room instances that should resync room-owned
    /// source policy after transport observations changed.
    pub async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.worker_manager
            .expired_active_speaker_room_instance_ids(now)
            .await
    }

    /// Returns the latest known transport health for one session.
    ///
    /// Health is connectivity evidence only. It should not be used as the
    /// source of truth for whether a participant belongs to a room.
    #[must_use]
    pub fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.worker_manager.session_transport_health(session_key)
    }
    /// Subscribes to source-policy invalidation signals emitted by transport
    /// workers.
    #[must_use]
    pub fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.worker_manager.source_policy_subscription()
    }
}

#[cfg(test)]
mod tests;
