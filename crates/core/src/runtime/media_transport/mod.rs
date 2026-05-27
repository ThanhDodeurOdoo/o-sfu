//! Runtime media transport boundary used by core orchestration.
//!
//! This module contains the service surface that room, server plus [`crate::prelude::SfuCore`]
//! code use when they need media work to happen outside the pure router:
//! callers ask the media transport to create SDP
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
mod policy_invalidation;
#[cfg(any(test, feature = "testing-transport", feature = "internal-benchmarks"))]
pub mod test_support;
mod types;
mod workers;

use std::{collections::BTreeSet, sync::Arc, time::Instant};

pub use builder::{MediaTransportBuildError, MediaTransportBuilder};
pub use config::{MediaTransportConfig, MediaTransportDeps};
use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
pub use policy_invalidation::{
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
    TransportQualitySample, TransportQualitySnapshot, TransportRelayRouteAction,
    TransportRelayRouteEffect, TransportResult, TransportSessionHealth, TransportSessionKey,
    TransportSourceKey, TransportWorkerPressureSnapshot,
};

use self::workers::signaling_to_str0m_media_kind;
use crate::runtime::{
    RoomInstanceId,
    rtc_engine::{RtcSendMediaSource, RtcWorker},
};

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
    /// RTC workers indexed by canonical runtime media-worker ids.
    workers: Arc<[Arc<RtcWorker>]>,
    /// Shared wakeup signal used by every worker to notify room-level source
    /// policy tasks about transport-observed changes without polling every room.
    source_policy_signal: Arc<SourcePolicySignal>,
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
        async {
            self.require_worker_for_user(session_key)?
                .create_initial_session_offer(session_key)
                .await
        }
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
        async {
            self.require_worker_for_user(session_key)?
                .create_session_renegotiation_offer(session_key)
                .await
        }
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
        async {
            self.require_worker_for_user(session_key)?
                .apply_session_answer(session_key, answer_sdp)
                .await
        }
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
        offered_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        Self::project_client_rtp_capabilities(answer_sdp, offered_capabilities).inspect_err(
            |error| {
                warn!(
                    answer_len = answer_sdp.len(),
                    ?error,
                    "media transport failed to derive client RTP capabilities from answer SDP"
                );
            },
        )
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
        async {
            self.require_worker_for_user(session_key)?
                .close_session(session_key)
                .await
                .map(|_| ())
        }
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
        async {
            self.require_worker_for_user(session_key)?
                .remove_media(session_key, transport_media_id)
                .await
        }
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
        async {
            self.require_worker_for_user(session_key)?
                .add_recv_media(
                    session_key,
                    signaling_to_str0m_media_kind(media_kind),
                    rtp_parameters,
                )
                .await
        }
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
    /// method. The initial activity controls whether the packet loop can
    /// forward packets before later room policy updates arrive.
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
        initial_activity: ConsumerActivity,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        async {
            Self::ensure_same_room(consumer_session_key, source_session_key)?;
            let relay_route =
                self.relay_registration_workers(consumer_session_key, source_session_key)?;
            let remote_source_control = relay_route
                .as_ref()
                .map(|(source_worker, consumer_worker)| {
                    source_worker.remote_source_control(consumer_worker.as_ref())
                })
                .transpose()?;
            self.require_worker_for_user(consumer_session_key)?
                .add_send_media(
                    consumer_session_key,
                    signaling_to_str0m_media_kind(media_kind),
                    RtcSendMediaSource {
                        source: TransportSourceKey::new(
                            source_session_key.clone(),
                            source_media_id,
                        ),
                        remote_source_control,
                    },
                    consumer_rtp_parameters,
                    initial_activity.is_active(),
                )
                .await
        }
        .await
        .inspect_err(|error| {
            warn!(
                ?consumer_session_key,
                ?source_session_key,
                ?source_media_id,
                ?media_kind,
                mid = consumer_rtp_parameters.mid(),
                initial_active = initial_activity.is_active(),
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
        self.execute_relay_route_effect(effect)
            .await
            .inspect_err(|error| {
                warn!(
                source = ?effect.source,
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
        source: &TransportSourceKey,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        async {
            self.require_worker_for_user(source.session_key())?
                .set_producer_active(source, activity.is_active())
                .await
        }
        .await
        .inspect_err(|error| {
            warn!(
                ?source,
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
        async {
            self.require_consumer_worker_for_route(route)?
                .set_consumer_active(route, activity.is_active())
                .await
        }
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
        async {
            self.require_consumer_worker_for_route(route)?
                .set_consumer_packet_gate(route, packet_gate.clone())
                .await
        }
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
        let results = self.execute_consumer_packet_gate_batch(updates).await;
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
        async {
            self.require_consumer_worker_for_route(route)?
                .request_consumer_keyframe(route)
                .await
        }
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
        self.transport_media_mid_from_worker(session_key, transport_media_id)
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
        self.transport_bitrate_snapshot_from_workers(session_keys)
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
        self.receiver_bandwidth_snapshot_from_workers(session_keys)
    }

    /// Returns sampled transport-quality observations for the requested sessions.
    #[must_use]
    pub fn transport_quality_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        self.transport_quality_snapshot_from_workers(session_keys)
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
        self.placement_pressure_snapshot_from_workers(session_keys)
    }

    /// Returns transport-worker pressure for every worker the backend can rank.
    ///
    /// Placement uses this as best-effort process-local load input. Missing
    /// workers are treated by callers as idle because transport snapshots can
    /// race with worker startup and teardown.
    #[must_use]
    pub fn worker_pressure_snapshots(&self) -> Vec<TransportWorkerPressureSnapshot> {
        self.worker_pressure_snapshots_from_workers()
    }

    /// Returns recent active-speaker sources observed by transport workers.
    pub async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.active_speaker_source_snapshot_from_workers().await
    }

    /// Returns diagnostic active-speaker state for operator-facing output.
    pub async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.active_speaker_diagnostic_snapshot_from_workers().await
    }

    /// Returns the next known deadline for active-speaker expiry work.
    ///
    /// Runtimes use this to schedule source-policy wakeups without polling all
    /// rooms on a fixed cadence.
    pub async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        self.next_active_speaker_deadline_from_workers().await
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
        self.expired_active_speaker_room_instance_ids_from_workers(now)
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
        self.session_transport_health_from_worker(session_key)
    }
    /// Subscribes to source-policy invalidation signals emitted by transport
    /// workers.
    #[must_use]
    pub fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.source_policy_subscription_from_workers()
    }
}

#[cfg(test)]
mod tests;
