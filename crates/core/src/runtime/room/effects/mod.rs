//! Post-lock effect plans for `Room` transitions.
//!
//! `RoomState` owns pure room mutation and validation under lock. This
//! module contain the transport calls, diagnostics writes and fanout that must run
//! after that lock is released.
//!
//! 1. read or mutate room state under lock
//! 2. build a typed plan for the side effects of that transition
//! 3. execute transport work after unlock
//! 4. commit only the post-transport state that is still valid
//!
//! The important invariant is that each transition makes its ordering and failure
//! handling explicit where room state meets transport state.

use tracing::warn;

use crate::{UnpublishOutcome, runtime::UserId};

mod batch;
mod source_policy;
mod transport;
pub(super) use batch::{MediaCountDelta, RoomEffectBatch, RoomEffectContext, TransportUserCleanup};
use o_sfu_telemetry::schema::event as telemetry_event;
pub(super) use source_policy::SourcePolicyEffectPlan;
pub(in crate::runtime::room) use transport::{
    ConsumerCreationContinuation, PublishReservationContinuation, RoomTransportEffect,
};

use super::{
    Room, RoomMediaCounts,
    state::{
        ConsumerBootstrapOrigin, ConsumerRouteTransportRef, ConsumerRouteUpdate,
        PendingConsumerBootstrap, PendingConsumerBootstrapTarget, PlannedConsumerBootstrap,
        PlannedSubscriptionChange, PreparedConsumerBootstrap, RelayRouteEffect,
    },
};
use crate::runtime::{
    ConnectionId,
    diagnostics::DiagnosticsEventData,
    media_transport::{ConsumerActivity, MediaTransport, TransportMediaId},
    source_model::UserStreamId,
};

#[derive(Debug)]
struct SubscriptionTransportOp {
    route: ConsumerRouteTransportRef,
    stream_id: UserStreamId,
    media_kind: o_sfu_router::MediaKind,
    active: bool,
    diagnostics: DiagnosticsEventData,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SubscriptionEffectContext<'a> {
    pub(super) user_id: &'a UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) target_user_id: &'a UserId,
    pub(super) media_counts_before: RoomMediaCounts,
    pub(super) media_counts_after: RoomMediaCounts,
    pub(super) origin: ConsumerBootstrapOrigin,
}

#[derive(Debug, Default)]
pub(super) struct SubscriptionEffectPlan {
    media_count_delta: Option<MediaCountDelta>,
    relay_ops: Vec<RelayRouteEffect>,
    transport_ops: Vec<SubscriptionTransportOp>,
    bootstrap_ops: Vec<ConsumerBootstrapOp>,
}

#[derive(Debug)]
struct ConsumerBootstrapOp {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending_bootstrap: PendingConsumerBootstrap,
    relay_effects: Vec<RelayRouteEffect>,
    origin: ConsumerBootstrapOrigin,
}

impl SubscriptionEffectPlan {
    /// Persists the accepted route toggles as transport work plus the matching
    /// diagnostics payloads. The diagnostics are shaped here so the caller can
    /// finish all subscription-side side effects from one post-lock executor
    pub(super) fn from_route_updates(
        room: &Room,
        user_id: &UserId,
        connection_id: ConnectionId,
        _target_user_id: &UserId,
        route_updates: Vec<ConsumerRouteUpdate>,
    ) -> Self {
        let transport_ops = route_updates
            .into_iter()
            .map(|route_update| {
                let route = route_update.route().clone();
                let diagnostics = DiagnosticsEventData::for_user(
                    room.uuid(),
                    user_id,
                    telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
                )
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(room.media_worker_id())
                .with_transport_media_id(route.consumer_media().as_u64())
                .insert_field("active", route_update.active())
                .insert_field(
                    "producer_user_id",
                    serde_json::to_value(route.source_user_id()).unwrap_or(serde_json::Value::Null),
                )
                .insert_field("source_transport_media_id", route.source_media().as_u64())
                .insert_field("stream_id", route_update.stream_id().to_string());
                SubscriptionTransportOp {
                    route,
                    stream_id: route_update.stream_id().clone(),
                    media_kind: route_update.media_kind(),
                    active: route_update.active(),
                    diagnostics,
                }
            })
            .collect();
        Self {
            media_count_delta: None,
            relay_ops: Vec::new(),
            transport_ops,
            bootstrap_ops: Vec::new(),
        }
    }

    pub(super) fn from_planned_change(
        room: &Room,
        context: SubscriptionEffectContext<'_>,
        planned_change: PlannedSubscriptionChange,
    ) -> Self {
        let (route_updates, planned_bootstraps, relay_effects) = planned_change.into_parts();
        let mut effect_plan = Self::from_route_updates(
            room,
            context.user_id,
            context.connection_id,
            context.target_user_id,
            route_updates,
        );
        effect_plan.media_count_delta = Some(MediaCountDelta::new(
            context.media_counts_before,
            context.media_counts_after,
        ));
        effect_plan.relay_ops = relay_effects;
        effect_plan.bootstrap_ops = planned_bootstraps
            .into_iter()
            .map(|planned_bootstrap| ConsumerBootstrapOp::new(planned_bootstrap, context.origin))
            .collect();
        effect_plan
    }

    pub(super) fn from_planned_bootstraps(
        media_counts_before: RoomMediaCounts,
        media_counts_after: RoomMediaCounts,
        planned_bootstraps: Vec<PlannedConsumerBootstrap>,
        origin: ConsumerBootstrapOrigin,
    ) -> Self {
        Self {
            media_count_delta: Some(MediaCountDelta::new(
                media_counts_before,
                media_counts_after,
            )),
            relay_ops: Vec::new(),
            transport_ops: Vec::new(),
            bootstrap_ops: planned_bootstraps
                .into_iter()
                .map(|planned_bootstrap| ConsumerBootstrapOp::new(planned_bootstrap, origin))
                .collect(),
        }
    }

    /// Applies the subscription decision to the media transport.
    ///
    /// A resumed video route needs more than an `active=true` flip. After a
    /// long pause the receiver may need a fresh decodable frame before it can
    /// render again, so successful video resumes trigger an immediate keyframe
    /// request on the underlying consumer route.
    pub(super) async fn execute(self, room: &Room, media_port: &MediaTransport) {
        RoomEffectBatch::new()
            .with_optional_media_count_delta(self.media_count_delta)
            .with_relay_effects(self.relay_ops)
            .execute(room, RoomEffectContext::runtime(media_port))
            .await;
        for transport_op in self.transport_ops {
            let route = &transport_op.route;
            let transport_route = room.transport_consumer_route(route);
            // Transport activity is best-effort here because the room intent has
            // already been committed, so the failure path is to surface it to
            // operators instead of trying to rebuild the previous room state.
            if (RoomTransportEffect::ConsumerActivity {
                route: transport_route.clone(),
                activity: ConsumerActivity::from_active(transport_op.active),
            })
            .execute_unit(media_port)
            .await
            .is_err()
            {
                warn!(
                    ?route,
                    stream_id = %transport_op.stream_id,
                    active = transport_op.active,
                    "media transport failed to update consumer route activity"
                );
            } else if transport_op.active
                && transport_op.media_kind == o_sfu_router::MediaKind::Video
                && (RoomTransportEffect::KeyframeRequest {
                    route: transport_route,
                })
                .execute_unit(media_port)
                .await
                .is_err()
            {
                warn!(
                    ?route,
                    stream_id = %transport_op.stream_id,
                    "media transport failed to request a consumer keyframe refresh"
                );
            }
            room.diagnostics.record(transport_op.diagnostics);
        }
        for bootstrap_op in self.bootstrap_ops {
            bootstrap_op.execute(room, media_port).await;
        }
    }
}

impl ConsumerBootstrapOp {
    fn new(planned_bootstrap: PlannedConsumerBootstrap, origin: ConsumerBootstrapOrigin) -> Self {
        let (target, prepared, pending_bootstrap, relay_effects) = planned_bootstrap.into_parts();
        Self {
            target,
            prepared,
            pending_bootstrap,
            relay_effects,
            origin,
        }
    }

    async fn execute(self, room: &Room, media_port: &MediaTransport) {
        let Self {
            target,
            prepared,
            pending_bootstrap,
            relay_effects,
            origin,
        } = self;
        let execution = RoomEffectBatch::new()
            .with_relay_effects(relay_effects)
            .execute(room, RoomEffectContext::runtime(media_port))
            .await;
        if !execution.relay_effects_applied() {
            room.release_pending_consumer_bootstrap(&target, media_port)
                .await;
            return;
        }
        let Some((consumer_transport_media_id, consumer_mid)) =
            Self::declare_consumer_transport_media(&target, &prepared, origin, room, media_port)
                .await
        else {
            return;
        };
        let (media_counts_before, outbound, media_counts_after) = {
            let mut state = room.state.write().await;
            let media_counts_before = state.media_counts();
            let outbound = state.commit_consumer_bootstrap(
                &target,
                pending_bootstrap,
                consumer_transport_media_id,
                consumer_mid,
            );
            let media_counts_after = state.media_counts();
            drop(state);
            (media_counts_before, outbound, media_counts_after)
        };
        let media_count_delta = MediaCountDelta::new(media_counts_before, media_counts_after);
        Self::finish(
            room,
            media_port,
            &target,
            origin,
            consumer_transport_media_id,
            media_count_delta,
            outbound,
        )
        .await;
    }

    /// Declare the transport-side consumer media before committing the room
    /// bootstrap.
    ///
    /// Keeping this outside the state commit makes transport failure handling
    /// explicit: the room can release the pending bootstrap instead of
    /// committing room state that points at missing transport media
    async fn declare_consumer_transport_media(
        target: &PendingConsumerBootstrapTarget,
        prepared: &PreparedConsumerBootstrap,
        origin: ConsumerBootstrapOrigin,
        room: &Room,
        media_port: &MediaTransport,
    ) -> Option<(TransportMediaId, Option<String>)> {
        let effect = RoomTransportEffect::ConsumerCreation {
            continuation: ConsumerCreationContinuation {
                user: target.consumer_user_id().clone(),
                connection: target.consumer_connection_id(),
                stream: target.stream_id().clone(),
            },
            consumer_session_key: room
                .transport_user_key(target.consumer_user_id(), target.consumer_connection_id()),
            media_kind: target.media_kind(),
            producer_session_key: room
                .transport_user_key(target.producer_user_id(), target.producer_connection_id()),
            source_transport_media_id: target.transport_media_id(),
            consumer_rtp_parameters: prepared.consumer_rtp_parameters().clone(),
        };
        match effect.execute_consumer_creation(media_port).await {
            Ok(result) => Some(result),
            Err(error) => {
                room.release_pending_consumer_bootstrap(target, media_port)
                    .await;
                warn!(
                    consumer_user_id = ?target.consumer_user_id(),
                    consumer_connection_id = ?target.consumer_connection_id(),
                    producer_user_id = ?target.producer_user_id(),
                    producer_connection_id = ?target.producer_connection_id(),
                    source_transport_media_id = ?target.transport_media_id(),
                    error = ?error,
                    consumer_mid = prepared.consumer_rtp_parameters().mid(),
                    ?origin,
                    "media transport rejected consume media declaration"
                );
                None
            }
        }
    }

    /// Finalize a prepared consumer bootstrap once the transport media exists.
    ///
    /// This step intentionally does not request a keyframe. Fresh subscribers
    /// need that refresh only after the receiver has applied the
    /// relevant SDP answer, which is handled by the later user-negotiation
    /// callbacks.
    async fn finish(
        room: &Room,
        media_port: &MediaTransport,
        target: &PendingConsumerBootstrapTarget,
        origin: ConsumerBootstrapOrigin,
        consumer_transport_media_id: TransportMediaId,
        media_count_delta: MediaCountDelta,
        outbound: Option<(
            super::outbound::OutboundSender,
            super::RemoteTrackBootstrap,
            bool,
        )>,
    ) {
        let Some((sender, bootstrap, consumer_active)) = outbound else {
            RoomEffectBatch::new()
                .with_media_count_delta_value(media_count_delta)
                .execute(room, RoomEffectContext::runtime(media_port))
                .await;
            room.release_pending_consumer_bootstrap(target, media_port)
                .await;
            room
                .cleanup_transport_media_with_retry(
                    target.consumer_user_id(),
                    target.consumer_connection_id(),
                    consumer_transport_media_id,
                    media_port,
                    "media transport failed to remove consumer transport media after bootstrap state commit failed",
                )
                .await;
            return;
        };
        let mut batch = RoomEffectBatch::new().with_media_count_delta_value(media_count_delta);
        if !consumer_active {
            batch = batch.with_initial_consumer_pause(
                room,
                target,
                consumer_transport_media_id,
                origin,
            );
        }
        batch
            .record_diagnostics(
                DiagnosticsEventData::for_user(
                    room.uuid(),
                    target.consumer_user_id(),
                    telemetry_event::SUBSCRIBE_SUCCEEDED,
                )
                .with_connection_id(target.consumer_connection_id().as_u64())
                .with_media_worker_id(room.media_worker_id())
                .with_transport_media_id(consumer_transport_media_id.as_u64())
                .insert_field(
                    "producer_user_id",
                    serde_json::to_value(target.producer_user_id())
                        .unwrap_or(serde_json::Value::Null),
                )
                .insert_field(
                    "source_transport_media_id",
                    target.transport_media_id().as_u64(),
                )
                .insert_field("stream_id", target.stream_id().to_string())
                .insert_field("origin", format!("{origin:?}").to_lowercase()),
            )
            .send_outbound_request(sender, bootstrap.into_room_event_request())
            .execute(room, RoomEffectContext::runtime(media_port))
            .await;
    }
}

#[derive(Debug)]
pub(super) struct UnpublishEffectPlan {
    user: UserId,
    connection: ConnectionId,
    stream: UserStreamId,
}

impl UnpublishEffectPlan {
    pub(super) fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        stream_id: UserStreamId,
    ) -> Self {
        Self {
            user: user_id,
            connection: connection_id,
            stream: stream_id,
        }
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        media_port: &MediaTransport,
    ) -> UnpublishOutcome {
        let (media_counts_before, outcome, media_counts_after) = {
            let mut state = room.state.write().await;
            let media_counts_before = state.media_counts();
            let outcome = state.unpublish_track(&self.user, self.connection, &self.stream);
            let media_counts_after = state.media_counts();
            drop(state);
            (media_counts_before, outcome, media_counts_after)
        };
        let Some(outcome) = outcome else {
            return UnpublishOutcome::MissingPublication;
        };
        let execution = RoomEffectBatch::new()
            .with_media_count_delta(media_counts_before, media_counts_after)
            .with_relay_effects(outcome.relay_effects().iter().cloned())
            .with_transport_removals(outcome.transport_removals().iter().cloned())
            .execute(room, RoomEffectContext::runtime(media_port))
            .await;
        outcome.emit(&self.user, &self.stream);
        UnpublishOutcome::Unpublished {
            cleanup: execution.cleanup(),
        }
    }
}
