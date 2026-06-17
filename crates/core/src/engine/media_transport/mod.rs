//! Runtime media transport boundary used by core.
//!
//! This module contains the service surface that room, server and
//! [`crate::prelude::SfuCore`] code use when media work must happen outside the
//! pure router. Callers ask the media transport to create SDP offers, apply
//! answers, publish or consume media, close sessions, read diagnostics
//! snapshots and subscribe to source-policy wakeups.
//!
//! The boundary exposes the opaque [`MediaTransport`] handle, narrow
//! construction inputs for the server runtime and transport operations that let
//! higher layers express intent without knowing about RTC workers, Str0m state,
//! UDP sockets or worker-local relay routing
//!
//! Code above this module should depend on [`MediaTransport`]. Code below this
//! module, especially the RTC engine, may deal with worker-local state machines
//! and packet-loop details. Keeping that split explicit prevents room and
//! signaling code from growing knowledge of the concrete WebRTC
//! implementation.

mod builder;
mod config;
mod policy_invalidation;
mod rtc;
#[cfg(any(test, feature = "testing-transport", feature = "internal-benchmarks"))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;
mod types;
mod workers;

use std::{collections::BTreeSet, sync::Arc, time::Instant};

pub use builder::{MediaTransportBuildError, MediaTransportBuilder};
pub use config::{MediaTransportConfig, MediaTransportDeps};
use o_sfu_router::{MediaKind, MediaStream as RouterRtpParameters};
pub use policy_invalidation::{
    SourcePolicyDirtyState, SourcePolicySignal, SourcePolicyUpdateSubscription,
};
#[cfg(feature = "internal-benchmarks")]
pub mod benchmark_support {
    pub use super::rtc::benchmark_support::*;
}
#[cfg(any(test, feature = "fuzzing"))]
pub mod fuzz_support {
    pub use super::rtc::{
        client_rtp_capabilities_from_answer, fuzz_support::route_packet_loop_ingress_demux,
    };
}
#[cfg(any(test, feature = "testing-transport"))]
pub use rtc::ForwardedPacket;
use rtc::{RouteControlRequest, RtcWorker, RtcWorkerCommand, RtcWorkerResponse};
use tracing::{debug, warn};
pub use types::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
    ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerActivity,
    ConsumerPacketGateUpdate, ProducerActivity, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate,
    RelayRouteActivity, SessionOffer, SessionUploadEncoding, SessionUploadSlot, SourcePacketGate,
    SourcePacketOperatingPoint, TransportAdapterError, TransportBitrateSnapshot,
    TransportConsumerRoute, TransportMediaId, TransportPlacementPressureSnapshot,
    TransportQualitySample, TransportQualitySnapshot, TransportRelayRouteAction,
    TransportRelayRouteEffect, TransportResult, TransportRidActivity, TransportSessionHealth,
    TransportSessionKey, TransportSourceActivity, TransportSourceActivitySnapshot,
    TransportSourceKey, TransportWorkerPressureSnapshot,
};

use self::workers::signaling_to_str0m_media_kind;
use crate::engine::RoomInstanceId;

/// Opaque runtime media transport handle.
///
/// [`MediaTransport`] is the handle server code and [`crate::prelude::SfuCore`]
/// should hold.
/// It hides the production RTC worker topology. Callers express intent through
/// inherent methods and must not branch on concrete worker internals.
///
/// The handle also centralizes warning logs for failed transport effects. Inner
/// backends return typed errors, while this boundary adds stable diagnostic
/// context such as session keys, media ids and SDP lengths.
#[derive(Debug, Clone)]
pub struct MediaTransport {
    workers: Arc<[Arc<RtcWorker>]>,
    /// Shared wakeup signal used by every worker to notify room-level source
    /// policy tasks about transport-observed changes without polling every room.
    source_policy_signal: Arc<SourcePolicySignal>,
}

#[derive(Debug, Clone, Copy)]
enum TransportCommandOp {
    CreateInitialSessionOffer,
    CreateSessionRenegotiationOffer,
    ApplySessionAnswer,
    CloseSession,
    RemoveMedia,
    PublishMedia,
    ConsumeMedia,
    SetProducerActive,
    SetConsumerActive,
    SetConsumerPacketGate,
    RequestConsumerKeyframe,
}

fn warn_session_command_failed(
    session_key: &TransportSessionKey,
    op: TransportCommandOp,
    error: TransportAdapterError,
) {
    warn!(
        ?session_key,
        ?op,
        ?error,
        "media transport worker command failed"
    );
}

impl MediaTransport {
    async fn request_session_command<T>(
        &self,
        session_key: &TransportSessionKey,
        build: impl FnOnce(RtcWorkerResponse<T>) -> RtcWorkerCommand,
        log_error: impl FnOnce(TransportAdapterError),
    ) -> Result<T, TransportAdapterError> {
        let result = match self.require_worker_for_user(session_key) {
            Ok(worker) => worker.request_worker(build).await,
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            log_error(*error);
        }
        result
    }

    async fn request_consumer_route_command<T>(
        &self,
        route: &TransportConsumerRoute,
        build: impl FnOnce(RtcWorkerResponse<T>) -> RtcWorkerCommand,
        log_error: impl FnOnce(TransportAdapterError),
    ) -> Result<T, TransportAdapterError> {
        let result = match self.require_consumer_worker_for_route(route) {
            Ok(worker) => worker.request_worker(build).await,
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            log_error(*error);
        }
        result
    }

    /// Creates the first SDP offer for a transport session.
    ///
    /// The session must already be assigned to a transport worker by its
    /// [`TransportSessionKey`]. The returned offer is transport state that
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
        self.request_session_command(
            session_key,
            |response| RtcWorkerCommand::CreateInitialSessionOffer {
                session_key: session_key.clone(),
                response,
            },
            |error| {
                warn_session_command_failed(
                    session_key,
                    TransportCommandOp::CreateInitialSessionOffer,
                    error,
                );
            },
        )
        .await
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
        self.request_session_command(
            session_key,
            |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                session_key: session_key.clone(),
                response,
            },
            |error| {
                warn_session_command_failed(
                    session_key,
                    TransportCommandOp::CreateSessionRenegotiationOffer,
                    error,
                );
            },
        )
        .await
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
        self.request_session_command(
            session_key,
            |response| RtcWorkerCommand::ApplySessionAnswer {
                session_key: session_key.clone(),
                answer_sdp: answer_sdp.to_owned(),
                response,
            },
            |error| {
                warn!(
                    ?session_key,
                    op = ?TransportCommandOp::ApplySessionAnswer,
                    answer_len = answer_sdp.len(),
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
    }

    /// Closes all backend state for a transport session.
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
        let result = async {
            let worker = self.require_worker_for_user(session_key)?;
            let Some(handle) = worker.worker_handle()? else {
                return Ok(());
            };
            worker
                .send_worker_command(&handle, |response| RtcWorkerCommand::CloseSession {
                    session_key: session_key.clone(),
                    response,
                })
                .await
                .map(|_| ())
        }
        .await;
        if let Err(error) = &result {
            warn_session_command_failed(session_key, TransportCommandOp::CloseSession, *error);
        }
        result
    }
    /// Removes one producer or consumer handle from a transport session.
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
        self.request_session_command(
            session_key,
            |response| RtcWorkerCommand::RemoveMedia {
                session_key: session_key.clone(),
                transport_media_id,
                response,
            },
            |error| {
                warn!(
                    ?session_key,
                    op = ?TransportCommandOp::RemoveMedia,
                    ?transport_media_id,
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
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
        self.request_session_command(
            session_key,
            |response| RtcWorkerCommand::AddRecvMedia {
                session_key: session_key.clone(),
                media_kind: signaling_to_str0m_media_kind(media_kind),
                rtp_parameters: rtp_parameters.clone(),
                response,
            },
            |error| {
                warn!(
                    ?session_key,
                    op = ?TransportCommandOp::PublishMedia,
                    ?media_kind,
                    mid = rtp_parameters.mid(),
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
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
        let result = async {
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
                .request_worker(|response| RtcWorkerCommand::AddSendMedia {
                    consumer_key: consumer_session_key.clone(),
                    media_kind: signaling_to_str0m_media_kind(media_kind),
                    source: TransportSourceKey::new(source_session_key.clone(), source_media_id),
                    remote_source_control,
                    consumer_rtp_parameters: consumer_rtp_parameters.clone(),
                    active: initial_activity.is_active(),
                    response,
                })
                .await
        }
        .await;
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?source_session_key,
                ?source_media_id,
                op = ?TransportCommandOp::ConsumeMedia,
                ?media_kind,
                mid = consumer_rtp_parameters.mid(),
                initial_active = initial_activity.is_active(),
                ?error,
                "media transport worker command failed"
            );
        }
        result
    }

    /// Applies a room relay route mutation.
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
                target_media_worker_id = effect.target_media_worker_id.as_usize(),
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
        self.request_session_command(
            source.session_key(),
            |response| {
                RtcWorkerCommand::media_control(
                    RouteControlRequest::SetProducerActive {
                        source: source.clone(),
                        active: activity.is_active(),
                    },
                    response,
                )
            },
            |error| {
                warn!(
                    ?source,
                    op = ?TransportCommandOp::SetProducerActive,
                    active = activity.is_active(),
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
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
        self.request_consumer_route_command(
            route,
            |response| {
                RtcWorkerCommand::media_control(
                    RouteControlRequest::SetConsumerActive {
                        route: route.clone(),
                        active: activity.is_active(),
                    },
                    response,
                )
            },
            |error| {
                warn!(
                    ?route,
                    op = ?TransportCommandOp::SetConsumerActive,
                    active = activity.is_active(),
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
    }

    /// Applies source-policy packet gating to one consumer route.
    ///
    /// Packet gates are transport execution policy derived from room
    /// source selection.
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
        self.request_consumer_route_command(
            route,
            |response| {
                RtcWorkerCommand::media_control(
                    RouteControlRequest::set_consumer_packet_gate(route.clone(), &packet_gate),
                    response,
                )
            },
            |error| {
                warn!(
                    ?route,
                    op = ?TransportCommandOp::SetConsumerPacketGate,
                    ?packet_gate,
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
    }

    /// Applies packet gates for multiple routes and preserves input order in
    /// the returned results.
    pub async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let results = self.apply_consumer_pkt_gate_batch(updates).await;
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

    /// Applies receiver-side desired BWE targets and preserves input order in
    /// the returned results.
    pub async fn set_receiver_bwe_targets(
        &self,
        updates: &[ReceiverBweTargetUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let results = self.execute_receiver_bwe_target_batch(updates).await;
        for (update, result) in updates.iter().zip(results.iter()) {
            match result {
                Ok(()) => {}
                Err(TransportAdapterError::InvalidInput) => {
                    debug!(
                        session_key = ?update.session_key(),
                        target = update.target().as_bps(),
                        "media transport skipped a stale receiver BWE target"
                    );
                }
                Err(error) => {
                    warn!(
                        ?error,
                        session_key = ?update.session_key(),
                        target = update.target().as_bps(),
                        "media transport failed to update a receiver BWE target"
                    );
                }
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
        self.request_consumer_route_command(
            route,
            |response| {
                RtcWorkerCommand::media_control(
                    RouteControlRequest::RequestConsumerKeyframe {
                        route: route.clone(),
                    },
                    response,
                )
            },
            |error| {
                warn!(
                    ?route,
                    op = ?TransportCommandOp::RequestConsumerKeyframe,
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
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

    /// Returns packet activity for producer sources.
    ///
    /// This is a diagnostics-only view. Missing sources may be inactive,
    /// removed or not yet observed on the packet path.
    pub(crate) async fn source_activity_snapshot(
        &self,
        sources: &[TransportSourceKey],
    ) -> TransportSourceActivitySnapshot {
        self.source_activity_snapshot_from_workers(sources).await
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
    /// The returned ids identify room instances that should resync room
    /// source policy after transport observations changed.
    pub async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.expired_active_speaker_rooms_from_workers(now).await
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
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
