//! Post-lock effect plans for `Room` transitions.
//!
//! `RoomState` owns pure room mutation and validation under lock. This
//! module owns the transport calls, diagnostics writes and fanout that must run
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

use crate::runtime::{StreamType, UserId};

mod source_policy;
pub(super) use source_policy::SourcePolicyEffectPlan;

use super::{
    Room, RoomMediaCounts,
    state::{
        ConsumerBootstrapOrigin, ConsumerRouteUpdate, PendingConsumerBootstrap,
        PendingConsumerBootstrapTarget, PlannedConsumerBootstrap, PlannedSubscriptionChange,
        PreparedConsumerBootstrap, TransportMediaRemoval,
    },
};
use crate::runtime::{
    ConnectionId,
    diagnostics::DiagnosticsEventData,
    telemetry::schema::event as telemetry_event,
    transport_adapter::{MediaPort, TransportMediaId},
};

#[derive(Debug, Clone, Copy)]
pub(super) struct MediaCountDelta {
    before: RoomMediaCounts,
    after: RoomMediaCounts,
}

impl MediaCountDelta {
    fn new(before: RoomMediaCounts, after: RoomMediaCounts) -> Self {
        Self { before, after }
    }

    fn record(self, room: &Room) {
        room.record_media_count_delta(self.before, self.after);
    }
}

#[derive(Debug)]
struct SubscriptionTransportOp {
    consumer_user_id: UserId,
    consumer_connection_id: ConnectionId,
    consumer_media: TransportMediaId,
    producer_user_id: UserId,
    producer_connection_id: ConnectionId,
    source_media: TransportMediaId,
    stream_type: StreamType,
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
    transport_ops: Vec<SubscriptionTransportOp>,
    bootstrap_ops: Vec<ConsumerBootstrapOp>,
}

#[derive(Debug)]
struct ConsumerBootstrapOp {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending_bootstrap: PendingConsumerBootstrap,
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
            .map(|route_update| SubscriptionTransportOp {
                consumer_user_id: user_id.clone(),
                consumer_connection_id: route_update.consumer_connection_id(),
                consumer_media: route_update.consumer_media(),
                producer_user_id: route_update.producer_user_id().clone(),
                producer_connection_id: route_update.source_connection_id(),
                source_media: route_update.source_media(),
                stream_type: route_update.stream_type(),
                active: route_update.active(),
                diagnostics: DiagnosticsEventData::for_user(
                    room.uuid(),
                    user_id,
                    telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
                )
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(room.media_worker_id())
                .with_transport_media_id(route_update.consumer_media().as_u64())
                .insert_field("active", route_update.active())
                .insert_field(
                    "producer_user_id",
                    serde_json::to_value(route_update.producer_user_id())
                        .unwrap_or(serde_json::Value::Null),
                )
                .insert_field(
                    "source_transport_media_id",
                    route_update.source_media().as_u64(),
                )
                .insert_field(
                    "stream_type",
                    format!("{:?}", route_update.stream_type()).to_lowercase(),
                ),
            })
            .collect();
        Self {
            media_count_delta: None,
            transport_ops,
            bootstrap_ops: Vec::new(),
        }
    }

    pub(super) fn from_planned_change(
        room: &Room,
        context: SubscriptionEffectContext<'_>,
        planned_change: PlannedSubscriptionChange,
    ) -> Self {
        let (route_updates, planned_bootstraps) = planned_change.into_parts();
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
            transport_ops: Vec::new(),
            bootstrap_ops: planned_bootstraps
                .into_iter()
                .map(|planned_bootstrap| ConsumerBootstrapOp::new(planned_bootstrap, origin))
                .collect(),
        }
    }

    /// Applies the subscription decision to the transport adapter.
    ///
    /// A resumed video route needs more than an `active=true` flip. After a
    /// long pause the receiver may need a fresh decodable frame before it can
    /// render again, so successful camera and screen resumes trigger an
    /// immediate keyframe request on the underlying consumer route.
    pub(super) async fn execute(self, room: &Room, media_port: &impl MediaPort) {
        if let Some(media_count_delta) = self.media_count_delta {
            media_count_delta.record(room);
        }
        for transport_op in self.transport_ops {
            // Transport activity is best-effort here because the room intent has
            // already been committed, so the failure path is to surface it to
            // operators instead of trying to rebuild the previous room state.
            if media_port
                .set_consumer_active(
                    &room.transport_user_key(
                        &transport_op.consumer_user_id,
                        transport_op.consumer_connection_id,
                    ),
                    transport_op.consumer_media,
                    &room.transport_user_key(
                        &transport_op.producer_user_id,
                        transport_op.producer_connection_id,
                    ),
                    transport_op.source_media,
                    transport_op.active,
                )
                .await
                .is_err()
            {
                warn!(
                    user_id = ?transport_op.consumer_user_id,
                    target_user_id = ?transport_op.producer_user_id,
                    stream_type = ?transport_op.stream_type,
                    active = transport_op.active,
                    "transport adapter failed to update consumer route activity"
                );
            } else if transport_op.active
                && matches!(
                    transport_op.stream_type,
                    StreamType::Camera | StreamType::Screen
                )
                && media_port
                    .request_consumer_keyframe(
                        &room.transport_user_key(
                            &transport_op.consumer_user_id,
                            transport_op.consumer_connection_id,
                        ),
                        transport_op.consumer_media,
                        &room.transport_user_key(
                            &transport_op.producer_user_id,
                            transport_op.producer_connection_id,
                        ),
                        transport_op.source_media,
                    )
                    .await
                    .is_err()
            {
                warn!(
                    user_id = ?transport_op.consumer_user_id,
                    target_user_id = ?transport_op.producer_user_id,
                    stream_type = ?transport_op.stream_type,
                    "transport adapter failed to request a consumer keyframe refresh"
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
        let (target, prepared, pending_bootstrap) = planned_bootstrap.into_parts();
        Self {
            target,
            prepared,
            pending_bootstrap,
            origin,
        }
    }

    async fn execute(self, room: &Room, media_port: &impl MediaPort) {
        let Self {
            target,
            prepared,
            pending_bootstrap,
            origin,
        } = self;
        let Some((consumer_transport_media_id, consumer_mid)) =
            Self::declare_consumer_transport_media(&target, &prepared, origin, room, media_port)
                .await
        else {
            return;
        };
        let (media_counts_before, outbound, media_counts_after) = {
            let mut state = room.state.write().await;
            let media_counts_before = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let outbound = state.commit_consumer_bootstrap(
                &target,
                pending_bootstrap,
                consumer_transport_media_id,
                consumer_mid,
            );
            let media_counts_after = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
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
        media_port: &impl MediaPort,
    ) -> Option<(TransportMediaId, Option<String>)> {
        let consumer_session_key =
            room.transport_user_key(target.consumer_user_id(), target.consumer_connection_id());
        let consumer_transport_media_id = match media_port
            .consume_media(
                &consumer_session_key,
                target.media_kind(),
                &room
                    .transport_user_key(target.producer_user_id(), target.producer_connection_id()),
                target.transport_media_id(),
                prepared.consumer_rtp_parameters(),
            )
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(error) => {
                room.release_pending_consumer_bootstrap(target).await;
                warn!(
                    consumer_user_id = ?target.consumer_user_id(),
                    consumer_connection_id = ?target.consumer_connection_id(),
                    producer_user_id = ?target.producer_user_id(),
                    producer_connection_id = ?target.producer_connection_id(),
                    source_transport_media_id = ?target.transport_media_id(),
                    error = ?error,
                    consumer_mid = prepared.consumer_rtp_parameters().mid(),
                    ?origin,
                    "transport adapter rejected consume media declaration"
                );
                return None;
            }
        };
        let consumer_mid = media_port
            .transport_media_mid(&consumer_session_key, consumer_transport_media_id)
            .await;
        Some((consumer_transport_media_id, consumer_mid))
    }

    /// Finalize a prepared consumer bootstrap once the transport media exists.
    ///
    /// This step intentionally does not request a keyframe. Fresh subscribers
    /// need that refresh only after the receiver has applied the
    /// relevant SDP answer, which is handled by the later user-negotiation
    /// callbacks.
    async fn finish(
        room: &Room,
        media_port: &impl MediaPort,
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
            media_count_delta.record(room);
            room
                .cleanup_transport_media(
                    target.consumer_user_id(),
                    target.consumer_connection_id(),
                    consumer_transport_media_id,
                    media_port,
                    "transport adapter failed to remove consumer transport media after bootstrap state commit failed",
                )
                .await;
            return;
        };
        media_count_delta.record(room);
        room.apply_initial_consumer_pause_state(
            target,
            consumer_transport_media_id,
            consumer_active,
            media_port,
            origin,
        )
        .await;
        room.diagnostics.record(
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
                serde_json::to_value(target.producer_user_id()).unwrap_or(serde_json::Value::Null),
            )
            .insert_field(
                "source_transport_media_id",
                target.transport_media_id().as_u64(),
            )
            .insert_field(
                "stream_type",
                format!("{:?}", target.stream_type()).to_lowercase(),
            )
            .insert_field("origin", format!("{origin:?}").to_lowercase()),
        );
        let _ = sender.send(super::UserOutbound::Request(Box::new(
            bootstrap.into_room_event_request(),
        )));
    }
}

#[derive(Debug)]
pub(super) struct UnpublishEffectPlan {
    user_id: UserId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    transport_removals: Vec<TransportMediaRemoval>,
}

impl UnpublishEffectPlan {
    pub(super) fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        transport_removals: Vec<TransportMediaRemoval>,
    ) -> Self {
        Self {
            user_id,
            connection_id,
            stream_type,
            transport_removals,
        }
    }

    pub(super) async fn execute(self, room: &Room, media_port: &impl MediaPort) -> bool {
        // Explicit unpublish tears down transport state first so a later state
        // commit failure cannot leave routable media alive for a track the room
        // already considers removed.
        if !room
            .cleanup_transport_removals_strict(media_port, &self.transport_removals)
            .await
        {
            return false;
        }
        let (media_counts_before, outcome, media_counts_after) = {
            let mut state = room.state.write().await;
            let media_counts_before = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let outcome =
                state.unpublish_track(&self.user_id, self.connection_id, self.stream_type);
            let media_counts_after = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            (media_counts_before, outcome, media_counts_after)
        };
        let Some(outcome) = outcome else {
            warn!(
                user_id = ?self.user_id,
                connection_id = ?self.connection_id,
                stream_type = ?self.stream_type,
                "transport cleanup succeeded but room state commit failed"
            );
            return false;
        };
        MediaCountDelta::new(media_counts_before, media_counts_after).record(room);
        outcome.emit(&self.user_id, self.stream_type);
        true
    }
}
