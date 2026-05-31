//! subscription transition choreography
//!
//! this module turns committed room-state decisions into transport and outbound
//! work for receiver intent and consumer bootstrap flows
//!
//! room graph and topology commits stay synchronous room-state work
//! relay changes, transport consume calls and outbound requests run only after
//! the room-state lock is released

use std::collections::BTreeMap;

use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::super::{
    RemoteTrackBootstrap, Room, RoomMediaCounts, RoomUserOperation, SourcePolicyEvent,
    cleanup::TransportCleanupOperation,
    effects::{MediaCountDelta, RoomEffectBatch, RoomEffectContext},
    media_graph::{
        ConsumerBootstrapOrigin, ConsumerRouteTransportRef, ConsumerRouteUpdate,
        PendingConsumerBootstrap, PendingConsumerBootstrapTarget, PlannedConsumerBootstrap,
        PlannedSubscriptionChange, PreparedConsumerBootstrap, RelayRouteEffect,
    },
    outbound::OutboundSender,
};
use crate::{
    SubscriptionUpdateOutcome,
    engine::{
        ConnectionId, UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{ConsumerActivity, MediaTransport, TransportMediaId},
        source_model::{SourceSubscriptionIntent, UserStreamId},
    },
};

#[derive(Debug)]
/// pending transport activity change for one committed consumer route
///
/// this carries the route identity plus the diagnostics payload captured while
/// room state still knew which subscription update accepted the change
struct SubscriptionRouteActivityOp {
    /// transport-facing route that should be paused or resumed
    route: ConsumerRouteTransportRef,
    /// source stream used in diagnostics and keyframe refresh logs
    stream: UserStreamId,
    /// media kind used to request a keyframe after video resume
    kind: o_sfu_router::MediaKind,
    /// receiver-local route activity accepted by room state
    active: bool,
    /// diagnostics record emitted after the transport update attempt
    diagnostics: DiagnosticsEventData,
}

#[derive(Debug, Clone, Copy)]
/// state snapshots and receiver identity needed to build one effect plan
///
/// the context is captured before unlock so later async work never reads live
/// room state to rediscover which transition produced the effects
struct SubscriptionEffectContext<'a> {
    /// receiver whose intent or bootstrap created the work
    user: &'a UserId,
    /// connection that was current while room state accepted the plan
    connection: ConnectionId,
    /// media counts before the accepted room-state mutation
    before: RoomMediaCounts,
    /// media counts after the accepted room-state mutation
    after: RoomMediaCounts,
    /// user-visible cause recorded on successful consumer bootstrap
    origin: ConsumerBootstrapOrigin,
}

#[derive(Debug, Default)]
/// post-lock work produced by one subscription state transition
///
/// every field is a fact captured before transport awaits start
/// transport and relay failures are handled after the original state
/// transaction ends, with cleanup when a bootstrap lease cannot be committed
pub struct SubscriptionEffectPlan {
    /// media gauge delta for the committed room-state mutation
    media_delta: Option<MediaCountDelta>,
    /// relay route mutations accepted by room state
    relay_ops: Vec<RelayRouteEffect>,
    /// transport route activity updates plus matching diagnostics
    route_ops: Vec<SubscriptionRouteActivityOp>,
    /// pending consumer bootstraps that still need transport media
    bootstraps: Vec<PendingConsumerLease>,
}

#[derive(Debug)]
/// single-use lease for a pending consumer bootstrap
///
/// the lease carries the pending graph reservation plus the prepared transport
/// input that must be consumed exactly once by the async effect path
/// if relay setup or transport consume fails, releasing this lease removes
/// pending graph and relay state so the next bootstrap attempt starts cleanly
struct PendingConsumerLease {
    /// immutable identity of the producer and receiver route being bootstrapped
    target: PendingConsumerBootstrapTarget,
    /// transport parameters negotiated from the producer snapshot
    prepared: PreparedConsumerBootstrap,
    /// graph reservation to consume during final room-state commit
    pending: PendingConsumerBootstrap,
    /// relay routes that must exist before the transport consumer is declared
    relays: Vec<RelayRouteEffect>,
    /// bootstrap cause carried into diagnostics after commit
    origin: ConsumerBootstrapOrigin,
}

impl RoomUserOperation<'_> {
    /// boots missing consumer routes for the current connection after negotiation
    ///
    /// callers use this when a receiver has become able to consume after earlier
    /// room state already planned or published sources
    /// returns `false` when the connection is stale or no bootstrap work exists
    ///
    /// the method holds the room-state lock only while building the plan
    pub(crate) async fn bootstrap_consumers(self) -> bool {
        let room = self.room();
        let worker_lookup = room.placement_state.worker_lookup();
        let mut state = room.state.write().await;
        let before = state.media_counts();
        let Some(bootstraps) =
            state.plan_missing_consumers(self.user_id(), self.connection_id(), worker_lookup)
        else {
            return false;
        };
        let after = state.media_counts();
        drop(state);
        let effects = SubscriptionEffectPlan::from_bootstraps(
            before,
            after,
            bootstraps,
            ConsumerBootstrapOrigin::LateJoin,
        );
        effects.execute(room, self.media_transport()).await;
        room.handle_source_policy_event(
            SourcePolicyEvent::RouteGraphChanged,
            Some(self.media_transport()),
        )
        .await;
        true
    }

    /// stores receiver intent and applies the resulting route work
    ///
    /// the caller must pass the current connection id for the receiver
    /// stale connections are rejected before intent is persisted
    ///
    /// route activity, relay changes and missing consumer bootstrap work are
    /// executed after the state lock is released
    pub(crate) async fn update_subscription(
        self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> SubscriptionUpdateOutcome {
        let room = self.room();
        let (effects, source_policy_event) = {
            let worker_lookup = room.placement_state.worker_lookup();
            let mut state = room.state.write().await;
            if state
                .user_for_connection(self.user_id(), self.connection_id())
                .is_none()
            {
                return SubscriptionUpdateOutcome::StaleConnection;
            }
            let before = state.media_counts();
            let change = state.plan_subscription_change(
                self.user_id(),
                self.connection_id(),
                target_user_id,
                intents,
                worker_lookup,
            );
            let source_policy_event = if change.touches_route_graph() {
                SourcePolicyEvent::RouteGraphChanged
            } else {
                SourcePolicyEvent::ReceiverIntentChanged
            };
            let after = state.media_counts();
            drop(state);
            (
                SubscriptionEffectPlan::from_change(
                    room,
                    SubscriptionEffectContext {
                        user: self.user_id(),
                        connection: self.connection_id(),
                        before,
                        after,
                        origin: ConsumerBootstrapOrigin::Subscribe,
                    },
                    change,
                ),
                source_policy_event,
            )
        };
        effects.execute(room, self.media_transport()).await;
        room.handle_source_policy_event(source_policy_event, Some(self.media_transport()))
            .await;
        SubscriptionUpdateOutcome::Applied
    }
}

impl Room {
    /// bootstraps concrete consumer targets captured by another room transition
    ///
    /// publish commit uses this when a new producer should reach existing receivers
    /// the target list is validated again under room state before any transport
    /// work starts, so stale targets become no-ops
    pub(super) async fn bootstrap_consumers(
        &self,
        media_port: &MediaTransport,
        origin: ConsumerBootstrapOrigin,
        targets: Vec<PendingConsumerBootstrapTarget>,
    ) {
        let effects = {
            let worker_lookup = self.placement_state.worker_lookup();
            let mut state = self.state.write().await;
            let before = state.media_counts();
            let bootstraps = state.plan_consumers(targets, worker_lookup);
            let after = state.media_counts();
            drop(state);
            SubscriptionEffectPlan::from_bootstraps(before, after, bootstraps, origin)
        };
        effects.execute(self, media_port).await;
    }

    /// releases a pending consumer-bootstrap reservation after its lease ends
    ///
    /// this mirrors staged-publish ownership on the subscriber side
    /// room state owns the reservation while metrics and relay cleanup happen
    /// after unlock
    async fn release_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
        media_port: &MediaTransport,
    ) {
        let (before, after, relays) = {
            let mut state = self.state.write().await;
            let before = state.media_counts();
            let relays = state.release_bootstrap(target);
            let after = state.media_counts();
            drop(state);
            (before, after, relays)
        };
        RoomEffectBatch::new()
            .with_media_count_delta(before, after)
            .with_relay_effects(relays)
            .execute(self, RoomEffectContext::runtime(media_port))
            .await;
        self.handle_source_policy_event(SourcePolicyEvent::FanoutPressureChanged, Some(media_port))
            .await;
    }
}

impl SubscriptionEffectPlan {
    /// builds route activity effects from route updates accepted by room state
    ///
    /// diagnostics are shaped here so callers can finish all subscription-side
    /// side effects from one post-lock executor
    pub fn from_route_updates(
        room: &Room,
        user_id: &UserId,
        connection_id: ConnectionId,
        updates: Vec<ConsumerRouteUpdate>,
    ) -> Self {
        let media_worker_id = room
            .transport_user_key(user_id, connection_id)
            .media_worker_id();
        let route_ops = updates
            .into_iter()
            .map(|route_update| {
                let ConsumerRouteUpdate {
                    route,
                    stream,
                    kind,
                    active,
                } = route_update;
                let diagnostics = DiagnosticsEventData::for_user(
                    room.uuid(),
                    user_id,
                    telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
                )
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(media_worker_id.as_usize())
                .with_transport_media_id(route.consumer_media().as_u64())
                .insert_field("active", active)
                .insert_field(
                    "producer_user_id",
                    serde_json::to_value(route.source_user_id()).unwrap_or(serde_json::Value::Null),
                )
                .insert_field("source_transport_media_id", route.source_media().as_u64())
                .insert_field("stream_id", stream.to_string());
                SubscriptionRouteActivityOp {
                    route,
                    stream,
                    kind,
                    active,
                    diagnostics,
                }
            })
            .collect();
        Self {
            media_delta: None,
            relay_ops: Vec::new(),
            route_ops,
            bootstraps: Vec::new(),
        }
    }

    /// builds the full post-lock plan for a receiver intent change
    ///
    /// the plan may contain route toggles, relay changes and new consumer
    /// bootstraps because changing receiver intent can affect existing and
    /// missing routes at the same time
    fn from_change(
        room: &Room,
        context: SubscriptionEffectContext<'_>,
        change: PlannedSubscriptionChange,
    ) -> Self {
        let (updates, bootstraps, relays) = change.into_parts();
        let mut effect_plan =
            Self::from_route_updates(room, context.user, context.connection, updates);
        effect_plan.media_delta = Some(MediaCountDelta::new(context.before, context.after));
        effect_plan.relay_ops = relays;
        effect_plan.bootstraps = bootstraps
            .into_iter()
            .map(|bootstrap| PendingConsumerLease::new(bootstrap, context.origin))
            .collect();
        effect_plan
    }

    /// builds the post-lock plan for bootstrap work without route toggles
    ///
    /// late-join and publish paths use this when room state has already chosen
    /// concrete consumer targets and only needs transport-side setup
    fn from_bootstraps(
        before: RoomMediaCounts,
        after: RoomMediaCounts,
        bootstraps: Vec<PlannedConsumerBootstrap>,
        origin: ConsumerBootstrapOrigin,
    ) -> Self {
        Self {
            media_delta: Some(MediaCountDelta::new(before, after)),
            relay_ops: Vec::new(),
            route_ops: Vec::new(),
            bootstraps: bootstraps
                .into_iter()
                .map(|it| PendingConsumerLease::new(it, origin))
                .collect(),
        }
    }

    /// executes captured subscription effects after the room lock is gone
    ///
    /// resumed video routes request a keyframe after the activity update because
    /// the receiver may need a fresh decodable frame after a long pause
    /// pending consumer leases are consumed after shared batch effects so relay
    /// routes exist before transport media is declared
    pub async fn execute(self, room: &Room, media_port: &MediaTransport) {
        RoomEffectBatch::new()
            .with_optional_media_count_delta(self.media_delta)
            .with_relay_effects(self.relay_ops)
            .execute(room, RoomEffectContext::runtime(media_port))
            .await;
        for op in self.route_ops {
            let route = &op.route;
            let transport_route = room.transport_consumer_route(route);
            if media_port
                .set_consumer_active(&transport_route, ConsumerActivity::from_active(op.active))
                .await
                .is_err()
            {
                warn!(
                    ?route,
                    stream_id = %op.stream,
                    active = op.active,
                    "media transport failed to update consumer route activity"
                );
            } else if op.active
                && op.kind == o_sfu_router::MediaKind::Video
                && media_port
                    .request_consumer_keyframe(&transport_route)
                    .await
                    .is_err()
            {
                warn!(
                    ?route,
                    stream_id = %op.stream,
                    "media transport failed to request a consumer keyframe refresh"
                );
            }
            room.diagnostics.record(op.diagnostics);
        }
        for lease in self.bootstraps {
            lease.execute(room, media_port).await;
        }
    }
}

impl PendingConsumerLease {
    fn new(bootstrap: PlannedConsumerBootstrap, origin: ConsumerBootstrapOrigin) -> Self {
        let (target, prepared, pending, relays) = bootstrap.into_parts();
        Self {
            target,
            prepared,
            pending,
            relays,
            origin,
        }
    }

    /// runs relay setup, transport consume and final room commit for one lease
    ///
    /// the room-state reservation is consumed only after relay and transport work
    /// succeeds
    /// every failure path releases pending graph and relay state so retry sees
    /// the source through normal planning
    async fn execute(self, room: &Room, media_port: &MediaTransport) {
        let Self {
            target,
            prepared,
            pending,
            relays,
            origin,
        } = self;
        let execution = RoomEffectBatch::new()
            .with_relay_effects(relays)
            .execute(room, RoomEffectContext::runtime(media_port))
            .await;
        if !execution.relay_effects_applied() {
            room.release_bootstrap(&target, media_port).await;
            return;
        }
        let activity = ConsumerActivity::from_active(pending.consumer_active());
        let Some((consumer_media, mid)) =
            Self::declare_transport_media(&target, &prepared, activity, origin, room, media_port)
                .await
        else {
            return;
        };
        let (before, outbound, after) = {
            let mut state = room.state.write().await;
            let before = state.media_counts();
            let outbound = state.commit_bootstrap(&target, pending, consumer_media, mid);
            let after = state.media_counts();
            drop(state);
            (before, outbound, after)
        };
        let delta = MediaCountDelta::new(before, after);
        Self::finish(
            room,
            media_port,
            &target,
            origin,
            consumer_media,
            delta,
            outbound,
        )
        .await;
    }

    /// declares transport media for a pending consumer before graph commit
    ///
    /// transport rejection releases pending bootstrap because no committed graph
    /// route may point at missing adapter media
    async fn declare_transport_media(
        target: &PendingConsumerBootstrapTarget,
        prepared: &PreparedConsumerBootstrap,
        activity: ConsumerActivity,
        origin: ConsumerBootstrapOrigin,
        room: &Room,
        media_port: &MediaTransport,
    ) -> Option<(TransportMediaId, Option<String>)> {
        let consumer_session =
            room.transport_user_key(target.consumer_user_id(), target.consumer_connection_id());
        let producer_session =
            room.transport_user_key(target.producer_user_id(), target.producer_connection_id());
        match media_port
            .consume_media(
                &consumer_session,
                target.media_kind(),
                &producer_session,
                target.transport_media_id(),
                &prepared.rtp,
                activity,
            )
            .await
        {
            Ok(consumer_media) => {
                let mid = media_port
                    .transport_media_mid(&consumer_session, consumer_media)
                    .await;
                Some((consumer_media, mid))
            }
            Err(error) => {
                room.release_bootstrap(target, media_port).await;
                warn!(
                    consumer_user_id = ?target.consumer_user_id(),
                    consumer_connection_id = ?target.consumer_connection_id(),
                    producer_user_id = ?target.producer_user_id(),
                    producer_connection_id = ?target.producer_connection_id(),
                    source_transport_media_id = ?target.transport_media_id(),
                    error = ?error,
                    consumer_mid = prepared.rtp.mid(),
                    ?origin,
                    "media transport rejected consume media declaration"
                );
                None
            }
        }
    }

    /// emits a committed consumer bootstrap or cleans up rejected transport media
    ///
    /// the caller passes the graph commit outcome rather than letting this method
    /// query room state again
    /// rejection means the transport media has no graph owner, so cleanup removes it
    ///
    /// fresh subscribers get keyframe refresh through later negotiation callbacks
    /// after the receiver applies the relevant SDP answer
    async fn finish(
        room: &Room,
        media_port: &MediaTransport,
        target: &PendingConsumerBootstrapTarget,
        origin: ConsumerBootstrapOrigin,
        consumer_media: TransportMediaId,
        delta: MediaCountDelta,
        outbound: Option<(OutboundSender, RemoteTrackBootstrap)>,
    ) {
        let Some((sender, bootstrap)) = outbound else {
            RoomEffectBatch::new()
                .with_media_count_delta_value(delta)
                .execute(room, RoomEffectContext::runtime(media_port))
                .await;
            room.release_bootstrap(target, media_port).await;
            let cleanup = [TransportCleanupOperation::RemoveMedia {
                session_key: room
                    .transport_user_key(target.consumer_user_id(), target.consumer_connection_id()),
                connection_id: target.consumer_connection_id(),
                transport_media_id: consumer_media,
            }];
            room.execute_transport_cleanup_operations(media_port, &cleanup)
                .await;
            return;
        };
        RoomEffectBatch::new()
            .with_media_count_delta_value(delta)
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
                    .media_worker_id()
                    .as_usize(),
                )
                .with_transport_media_id(consumer_media.as_u64())
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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "transition tests fail loudly when fixed room setup is invalid"
    )]

    use std::{collections::BTreeMap, sync::Arc};

    use o_sfu_router::test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
    };

    use super::super::super::{
        Room, RoomConfig, RoomManager, UserOutboundSender, media_graph::ConsumerRouteState,
    };
    use crate::{
        PublishStageOutcome, SessionNegotiationOutcome, SubscriptionUpdateOutcome,
        engine::{
            ConnectionId, TestSourceKind, UserId, UserPermissions,
            media_transport::{
                AppliedSessionAnswer, MediaTransport, TransportMediaId,
                test_support::{test_media_transport_builder, test_rtc_port_range},
            },
            metrics::RuntimeMetrics,
            source_model::{
                SourceSubscriptionIntent, UserStreamId,
                test_support::{source_publish_intent_for_source, stream_id_for_source},
            },
        },
    };

    fn media_transport() -> MediaTransport {
        let rtc_port_range = test_rtc_port_range(4).expect("test ports should be available");
        test_media_transport_builder(rtc_port_range)
            .worker_count(4)
            .build()
            .expect("test media transport config should be valid")
    }

    fn test_sender() -> UserOutboundSender {
        UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default())).0
    }

    fn pause_scalable_video_intents() -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
        BTreeMap::from([(
            stream_id_for_source(TestSourceKind::ScalableVideo),
            SourceSubscriptionIntent::new(Some(false), None),
        )])
    }

    async fn join_negotiated_user(
        room: &Arc<Room>,
        media_transport: &MediaTransport,
        user_id: &UserId,
        create_transport_session: bool,
    ) -> ConnectionId {
        let connection_id = room
            .test_api()
            .lifecycle()
            .join_user(
                user_id.clone(),
                None,
                UserPermissions::default(),
                test_sender(),
            )
            .await
            .expect("test user should join");
        if create_transport_session {
            let session_key = room.transport_user_key(user_id, connection_id);
            media_transport
                .create_initial_session_offer(&session_key)
                .await
                .expect("test session should create an initial offer");
        }
        assert_eq!(
            room.apply_session_negotiated(
                user_id,
                connection_id,
                sample_client_rtp_capabilities(),
                media_transport,
            )
            .await,
            SessionNegotiationOutcome::Applied
        );
        connection_id
    }

    async fn setup_subscription_room(
        create_subscriber_transport_session: bool,
    ) -> (
        Arc<Room>,
        MediaTransport,
        UserId,
        ConnectionId,
        UserId,
        ConnectionId,
    ) {
        let manager = RoomManager::for_test();
        let room = manager
            .serve_room(
                "issuer-transition-subscription",
                "room",
                &RoomConfig::default(),
                None,
            )
            .await;
        let media_transport = media_transport();
        let publisher_id = UserId::Integer(1);
        let subscriber_id = UserId::Integer(2);
        let publisher_connection_id =
            join_negotiated_user(&room, &media_transport, &publisher_id, true).await;
        let subscriber_connection_id = join_negotiated_user(
            &room,
            &media_transport,
            &subscriber_id,
            create_subscriber_transport_session,
        )
        .await;
        (
            room,
            media_transport,
            publisher_id,
            publisher_connection_id,
            subscriber_id,
            subscriber_connection_id,
        )
    }

    async fn publish_scalable_video(
        room: &Room,
        media_transport: &MediaTransport,
        publisher_id: &UserId,
        publisher_connection_id: ConnectionId,
    ) -> TransportMediaId {
        assert_eq!(
            room.user_operation(publisher_id, publisher_connection_id, media_transport)
                .stage_negotiated_publish(&source_publish_intent_for_source(
                    TestSourceKind::ScalableVideo,
                ))
                .await
                .expect("stage publish should not fail"),
            PublishStageOutcome::Staged
        );
        let transport_media_id = room
            .staged_media_id(
                publisher_id,
                publisher_connection_id,
                TestSourceKind::ScalableVideo,
            )
            .await
            .expect("test publish should be staged");
        let committed = room
            .user_operation(publisher_id, publisher_connection_id, media_transport)
            .commit_staged_publishes(&AppliedSessionAnswer::from_negotiated_producers([(
                transport_media_id,
                sample_simulcast_video_rtp_parameters(None),
            )]))
            .await;
        assert_eq!(
            committed,
            vec![stream_id_for_source(TestSourceKind::ScalableVideo)]
        );
        transport_media_id
    }

    #[tokio::test]
    async fn stored_receiver_intent_applies_to_future_consumer_bootstrap() {
        let (
            room,
            media_transport,
            publisher_id,
            publisher_connection_id,
            subscriber_id,
            subscriber_connection_id,
        ) = setup_subscription_room(true).await;
        let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);

        assert_eq!(
            room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
                .update_subscription(&publisher_id, &pause_scalable_video_intents())
                .await,
            SubscriptionUpdateOutcome::Applied
        );
        publish_scalable_video(
            &room,
            &media_transport,
            &publisher_id,
            publisher_connection_id,
        )
        .await;

        assert_eq!(room.test_api().inspect().consumer_count().await, 1);
        assert_eq!(
            room.state
                .read()
                .await
                .consumer_route_state(&subscriber_id, &publisher_id, &stream_id),
            Some(ConsumerRouteState::Inactive)
        );
    }

    #[tokio::test]
    async fn transport_consume_failure_releases_pending_bootstrap_for_retry() {
        let (
            room,
            media_transport,
            publisher_id,
            publisher_connection_id,
            subscriber_id,
            subscriber_connection_id,
        ) = setup_subscription_room(false).await;
        let source_media_id = publish_scalable_video(
            &room,
            &media_transport,
            &publisher_id,
            publisher_connection_id,
        )
        .await;

        assert_eq!(room.test_api().inspect().consumer_count().await, 0);
        let subscriber_session_key =
            room.transport_user_key(&subscriber_id, subscriber_connection_id);
        media_transport
            .create_initial_session_offer(&subscriber_session_key)
            .await
            .expect("retry session should create an initial offer");

        assert!(
            room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
                .bootstrap_consumers()
                .await
        );
        assert_eq!(room.test_api().inspect().consumer_count().await, 1);
        assert!(
            media_transport
                .test_api()
                .route_entry_by_media_id(source_media_id)
                .await
                .is_some_and(|entry| !entry.destinations.is_empty())
        );
    }

    #[tokio::test]
    async fn stale_receiver_subscription_update_is_rejected() {
        let (room, media_transport, publisher_id, _, subscriber_id, stale_connection_id) =
            setup_subscription_room(true).await;
        let _current_connection_id =
            join_negotiated_user(&room, &media_transport, &subscriber_id, true).await;

        assert_eq!(
            room.user_operation(&subscriber_id, stale_connection_id, &media_transport)
                .update_subscription(&publisher_id, &pause_scalable_video_intents())
                .await,
            SubscriptionUpdateOutcome::StaleConnection
        );
    }

    #[tokio::test]
    async fn committed_consumer_reaches_graph_topology_and_transport() {
        let (
            room,
            media_transport,
            publisher_id,
            publisher_connection_id,
            subscriber_id,
            _subscriber_connection_id,
        ) = setup_subscription_room(true).await;
        let source_media_id = publish_scalable_video(
            &room,
            &media_transport,
            &publisher_id,
            publisher_connection_id,
        )
        .await;

        assert_eq!(room.test_api().inspect().consumer_count().await, 1);
        assert_eq!(
            room.state.read().await.consumer_route_state(
                &subscriber_id,
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
            ),
            Some(ConsumerRouteState::Active)
        );
        assert!(
            media_transport
                .test_api()
                .route_entry_by_media_id(source_media_id)
                .await
                .is_some_and(|entry| !entry.destinations.is_empty())
        );
    }
}
