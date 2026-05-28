//! Post-lock effect plans for `Room` transitions.
//!
//! `RoomState` owns pure room mutation and validation under lock. This
//! module contains the transport calls, diagnostics writes and fanout that must run
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

use crate::{UnpublishOutcome, engine::UserId};

mod batch;
pub(super) use batch::{MediaCountDelta, RoomEffectBatch, RoomEffectContext, TransportUserCleanup};
use o_sfu_telemetry::schema::event as telemetry_event;

use super::{
    Room, RoomMediaCounts, SourcePolicyEvent,
    cleanup::TransportCleanupOperation,
    media_graph::{
        ConsumerBootstrapOrigin, ConsumerRouteTransportRef, ConsumerRouteUpdate,
        PendingConsumerBootstrap, PendingConsumerBootstrapTarget, PlannedConsumerBootstrap,
        PlannedSubscriptionChange, PreparedConsumerBootstrap, RelayRouteEffect,
    },
};
use crate::engine::{
    ConnectionId,
    diagnostics::DiagnosticsEventData,
    media_transport::{ConsumerActivity, MediaTransport, TransportMediaId},
    source_model::UserStreamId,
};

#[derive(Debug)]
struct SubscriptionRouteActivityOp {
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
    pub(super) media_counts_before: RoomMediaCounts,
    pub(super) media_counts_after: RoomMediaCounts,
    pub(super) origin: ConsumerBootstrapOrigin,
}

#[derive(Debug, Default)]
pub(super) struct SubscriptionEffectPlan {
    media_count_delta: Option<MediaCountDelta>,
    relay_ops: Vec<RelayRouteEffect>,
    route_activity_ops: Vec<SubscriptionRouteActivityOp>,
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
        route_updates: Vec<ConsumerRouteUpdate>,
    ) -> Self {
        let media_worker_id = room
            .transport_user_key(user_id, connection_id)
            .media_worker_id();
        let route_activity_ops = route_updates
            .into_iter()
            .map(|route_update| {
                let ConsumerRouteUpdate {
                    route,
                    stream_id,
                    media_kind,
                    active,
                } = route_update;
                let diagnostics = DiagnosticsEventData::for_user(
                    room.uuid(),
                    user_id,
                    telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
                )
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(media_worker_id)
                .with_transport_media_id(route.consumer_media().as_u64())
                .insert_field("active", active)
                .insert_field(
                    "producer_user_id",
                    serde_json::to_value(route.source_user_id()).unwrap_or(serde_json::Value::Null),
                )
                .insert_field("source_transport_media_id", route.source_media().as_u64())
                .insert_field("stream_id", stream_id.to_string());
                SubscriptionRouteActivityOp {
                    route,
                    stream_id,
                    media_kind,
                    active,
                    diagnostics,
                }
            })
            .collect();
        Self {
            media_count_delta: None,
            relay_ops: Vec::new(),
            route_activity_ops,
            bootstrap_ops: Vec::new(),
        }
    }

    pub(super) fn from_planned_change(
        room: &Room,
        context: SubscriptionEffectContext<'_>,
        planned_change: PlannedSubscriptionChange,
    ) -> Self {
        let (route_updates, planned_bootstraps, relay_effects) = planned_change.into_parts();
        let mut effect_plan =
            Self::from_route_updates(room, context.user_id, context.connection_id, route_updates);
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
            route_activity_ops: Vec::new(),
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
        for route_activity in self.route_activity_ops {
            let route = &route_activity.route;
            let transport_route = room.transport_consumer_route(route);
            // Transport activity is best-effort here because the room intent has
            // already been committed, so the failure path is to surface it to
            // operators instead of trying to rebuild the previous room state.
            if media_port
                .set_consumer_active(
                    &transport_route,
                    ConsumerActivity::from_active(route_activity.active),
                )
                .await
                .is_err()
            {
                warn!(
                    ?route,
                    stream_id = %route_activity.stream_id,
                    active = route_activity.active,
                    "media transport failed to update consumer route activity"
                );
            } else if route_activity.active
                && route_activity.media_kind == o_sfu_router::MediaKind::Video
                && media_port
                    .request_consumer_keyframe(&transport_route)
                    .await
                    .is_err()
            {
                warn!(
                    ?route,
                    stream_id = %route_activity.stream_id,
                    "media transport failed to request a consumer keyframe refresh"
                );
            }
            room.diagnostics.record(route_activity.diagnostics);
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
        let initial_activity = ConsumerActivity::from_active(pending_bootstrap.consumer_active());
        let Some((consumer_transport_media_id, consumer_mid)) =
            Self::declare_consumer_transport_media(
                &target,
                &prepared,
                initial_activity,
                origin,
                room,
                media_port,
            )
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
        initial_activity: ConsumerActivity,
        origin: ConsumerBootstrapOrigin,
        room: &Room,
        media_port: &MediaTransport,
    ) -> Option<(TransportMediaId, Option<String>)> {
        let consumer_session_key =
            room.transport_user_key(target.consumer_user_id(), target.consumer_connection_id());
        let producer_session_key =
            room.transport_user_key(target.producer_user_id(), target.producer_connection_id());
        match media_port
            .consume_media(
                &consumer_session_key,
                target.media_kind(),
                &producer_session_key,
                target.transport_media_id(),
                &prepared.consumer_rtp_parameters,
                initial_activity,
            )
            .await
        {
            Ok(consumer_transport_media_id) => {
                let mid = media_port
                    .transport_media_mid(&consumer_session_key, consumer_transport_media_id)
                    .await;
                Some((consumer_transport_media_id, mid))
            }
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
                    consumer_mid = prepared.consumer_rtp_parameters.mid(),
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
        outbound: Option<(super::outbound::OutboundSender, super::RemoteTrackBootstrap)>,
    ) {
        let Some((sender, bootstrap)) = outbound else {
            RoomEffectBatch::new()
                .with_media_count_delta_value(media_count_delta)
                .execute(room, RoomEffectContext::runtime(media_port))
                .await;
            room.release_pending_consumer_bootstrap(target, media_port)
                .await;
            let cleanup = [TransportCleanupOperation::RemoveMedia {
                session_key: room
                    .transport_user_key(target.consumer_user_id(), target.consumer_connection_id()),
                connection_id: target.consumer_connection_id(),
                transport_media_id: consumer_transport_media_id,
            }];
            room.execute_transport_cleanup_operations(media_port, &cleanup)
                .await;
            return;
        };
        RoomEffectBatch::new()
            .with_media_count_delta_value(media_count_delta)
            .record_diagnostics(
                DiagnosticsEventData::for_user(
                    room.uuid(),
                    target.consumer_user_id(),
                    telemetry_event::SUBSCRIBE_SUCCEEDED,
                )
                .with_connection_id(target.consumer_connection_id().as_u64())
                .with_media_worker_id(
                    room.transport_user_key(
                        target.consumer_user_id(),
                        target.consumer_connection_id(),
                    )
                    .media_worker_id(),
                )
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
        room.handle_source_policy_event(SourcePolicyEvent::RouteGraphChanged, Some(media_port))
            .await;
        room.reconcile_spillover_routers().await;
        UnpublishOutcome::Unpublished {
            cleanup: execution.cleanup(),
        }
    }
}
