//! post-lock effect batches for committed room transitions
//!
//! transition modules mutate [`RoomState`] under its lock, convert the accepted
//! outcome into one [`RoomEffects`] value through `effects::batch::build_*`,
//! then drop the guard before calling [`RoomEffects::execute`]
//!
//! keeping the raw batch fields private makes this file the only place where
//! effect ordering, diagnostics context and transport cleanup policy can be
//! assembled

use o_sfu_router::MediaKind;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::consumer_setup::ConsumerSetupEffect;
use crate::{
    TransportEffectOutcome,
    engine::{
        ConnectionId, MediaWorkerId, UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{
            ConsumerActivity, MediaTransport, ProducerActivity, TransportConsumerRoute,
            TransportMediaId, TransportRelayRouteAction, TransportRelayRouteEffect,
            TransportSourceKey,
        },
        room::{
            Room, RoomMediaCounts, SourcePolicyEvent, TrackBindingUpdate, UserOutbound,
            cleanup::TransportCleanupOperation,
            media_graph::{
                ConsumerRouteUpdate, ConsumerSetupOrigin, PendingConsumerSetup,
                ResolvedRelayRouteEffect,
            },
            outbound::OutboundSender,
            routing::CommittedRoutingReceipt,
            state::{
                DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, LifecycleEffects,
                RoomState,
            },
        },
        source_model::UserStreamId,
    },
};

/// transport side-effect policy for a room effect batch
///
/// production uses [`RoomEffectContext::runtime`] so relay routes, cleanup,
/// producer activity, route activity, consumer setup and source policy reach
/// the media transport
///
/// state-only tests use [`RoomEffectContext::state_only`] to disable routed
/// transport effects while optionally keeping producer activity and source
/// policy active without closing transport resources
#[derive(Debug, Clone, Copy)]
pub(in crate::engine::room) struct RoomEffectContext<'a> {
    producer_activity_and_source_policy: Option<&'a MediaTransport>,
    routed_transport_effects: Option<&'a MediaTransport>,
}

impl<'a> RoomEffectContext<'a> {
    /// enables every effect family against one media transport boundary
    pub const fn runtime(media_transport: &'a MediaTransport) -> Self {
        Self {
            producer_activity_and_source_policy: Some(media_transport),
            routed_transport_effects: Some(media_transport),
        }
    }

    /// disables routed transport effects while keeping selected observers active
    #[cfg(any(test, feature = "testing-transport"))]
    pub const fn state_only(media_transport: Option<&'a MediaTransport>) -> Self {
        Self {
            producer_activity_and_source_policy: media_transport,
            routed_transport_effects: None,
        }
    }
}

/// diagnostics identity captured before the state guard is released
#[derive(Debug, Clone, Copy)]
pub(in crate::engine::room) struct RoomDiagnosticsContext<'a> {
    user: &'a UserId,
    connection: ConnectionId,
    worker: MediaWorkerId,
}

impl<'a> RoomDiagnosticsContext<'a> {
    pub const fn new(user: &'a UserId, connection: ConnectionId, worker: MediaWorkerId) -> Self {
        Self {
            user,
            connection,
            worker,
        }
    }

    pub fn event_data(self, room: &Room, event: &'static str) -> DiagnosticsEventData {
        DiagnosticsEventData::for_user(room.uuid(), self.user, event)
            .with_connection_id(self.connection.as_u64())
            .with_media_worker_id(self.worker.as_usize())
    }
}

/// counter delta captured under the same [`RoomState`] lock as the transition
///
/// callers pass counts into the batch instead of re-reading room state after an
/// await point
#[derive(Debug, Clone, Copy)]
pub(in crate::engine::room) struct RoomGaugeDelta {
    users: Option<UserCountDelta>,
    media: MediaCountDelta,
}

impl RoomGaugeDelta {
    pub const fn membership(
        users_before: usize,
        users_after: usize,
        media_before: RoomMediaCounts,
        media_after: RoomMediaCounts,
    ) -> Self {
        Self {
            users: Some(UserCountDelta {
                before: users_before,
                after: users_after,
            }),
            media: MediaCountDelta::new(media_before, media_after),
        }
    }

    pub const fn media(before: RoomMediaCounts, after: RoomMediaCounts) -> Self {
        Self {
            users: None,
            media: MediaCountDelta::new(before, after),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct MediaCountDelta {
    before: RoomMediaCounts,
    after: RoomMediaCounts,
}

impl MediaCountDelta {
    pub(super) const fn new(before: RoomMediaCounts, after: RoomMediaCounts) -> Self {
        Self { before, after }
    }

    pub(super) fn record(self, room: &Room) {
        let before_publications = i64::try_from(self.before.publications).unwrap_or(i64::MAX);
        let after_publications = i64::try_from(self.after.publications).unwrap_or(i64::MAX);
        room.metrics
            .add_active_publications(after_publications.saturating_sub(before_publications));

        let before_subscriptions = i64::try_from(self.before.subscriptions).unwrap_or(i64::MAX);
        let after_subscriptions = i64::try_from(self.after.subscriptions).unwrap_or(i64::MAX);
        room.metrics
            .add_active_subscriptions(after_subscriptions.saturating_sub(before_subscriptions));
    }
}

/// closed effect batch produced after a room-state transition commits
///
/// only `build_*` functions may construct it so transition modules cannot mix
/// arbitrary effect families after committing room state
#[derive(Debug, Default)]
#[must_use = "room effect batches must be executed after the state transition commits"]
pub(in crate::engine::room) struct RoomEffects {
    users: Option<UserCountDelta>,
    media: Vec<MediaCountDelta>,
    relays: Vec<ResolvedRelayRouteEffect>,
    cleanup: RoomCleanup,
    producers: Vec<ProducerActivityEffect>,
    source_policy: Option<SourcePolicyEvent>,
    lifecycle: Vec<LifecycleEffects>,
    routes: Vec<ConsumerRouteActivity>,
    consumer_setups: Vec<ConsumerSetupEffect>,
    track_bindings: Vec<TrackBindingFanout>,
    diagnostics: Vec<DiagnosticsEffect>,
}

#[derive(Debug, Clone, Copy)]
struct UserCountDelta {
    before: usize,
    after: usize,
}

#[derive(Debug)]
struct ConsumerRouteActivity {
    route: TransportConsumerRoute,
    stream: UserStreamId,
    kind: MediaKind,
    active: bool,
    diagnostics: DiagnosticsEventData,
}

#[derive(Debug, Default)]
struct RoomCleanup {
    before_close: Vec<TransportCleanupOperation>,
    close_users: Vec<TransportCleanupOperation>,
}

#[derive(Debug)]
struct ProducerActivityEffect {
    source: TransportSourceKey,
    active: bool,
    stream: UserStreamId,
    diagnostics: DiagnosticsEventData,
}

#[derive(Debug)]
struct TrackBindingFanout {
    recipients: Vec<OutboundSender>,
    update: TrackBindingUpdate,
}

#[derive(Debug)]
enum DiagnosticsEffect {
    RegisterUser(UserId),
    Record(DiagnosticsEventData),
    ForgetUser(UserId),
}

/// transport outcomes exposed by room APIs after a batch executes
#[derive(Debug, Clone, Copy)]
pub(in crate::engine::room) struct RoomEffectOutcome {
    cleanup: TransportEffectOutcome,
    producer_activity: TransportEffectOutcome,
}

impl RoomEffectOutcome {
    pub const fn cleanup(self) -> TransportEffectOutcome {
        self.cleanup
    }

    pub const fn producer_activity(self) -> TransportEffectOutcome {
        self.producer_activity
    }
}

/// accepted publication activity data that must leave the state lock together
pub(in crate::engine::room) struct PublicationActivityEffect<'a> {
    pub diagnostics: RoomDiagnosticsContext<'a>,
    pub source: TransportSourceKey,
    pub media: TransportMediaId,
    pub stream: &'a UserStreamId,
    pub active: bool,
    pub recipients: Vec<OutboundSender>,
    pub track_update: TrackBindingUpdate,
}

/// accepted subscription change data that must leave the state lock together
pub(in crate::engine::room) struct SubscriptionChangeEffect<'a> {
    pub counts: RoomGaugeDelta,
    pub diagnostics: RoomDiagnosticsContext<'a>,
    pub route_updates: Vec<ConsumerRouteUpdate>,
    pub setups: Vec<PendingConsumerSetup>,
    pub relays: Vec<ResolvedRelayRouteEffect>,
}

pub(in crate::engine::room) fn build_join(
    room: &Room,
    counts: RoomGaugeDelta,
    outcome: JoinUserOutcome,
) -> (RoomEffects, CommittedRoutingReceipt) {
    let JoinUserOutcome {
        effects,
        user_id,
        routing_receipt,
        transport_cleanup,
        relay_effects,
    } = outcome;
    let diagnostics = RoomDiagnosticsContext::new(
        &user_id,
        routing_receipt.connection_id,
        routing_receipt.transport_session_key.media_worker_id(),
    )
    .event_data(room, telemetry_event::USER_JOINED);
    (
        RoomEffects::new()
            .with_gauge_delta(counts)
            .with_relay_effects(relay_effects)
            .with_pre_close_transport_cleanup(transport_cleanup)
            .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
            .with_lifecycle_effects(effects)
            .register_diagnostics_user(user_id)
            .record_diagnostics(diagnostics),
        routing_receipt,
    )
}

/// stale closes may still carry transport cleanup even when room state no
/// longer has a live user session
pub(in crate::engine::room) fn build_connection_close(
    room: &Room,
    counts: RoomGaugeDelta,
    state_outcome: Option<LeaveUserOutcome>,
    user_id: UserId,
    connection_id: ConnectionId,
    transport_close: Option<TransportCleanupOperation>,
) -> RoomEffects {
    let media_worker_id = transport_close
        .as_ref()
        .map(|operation| operation.session_key().media_worker_id());
    let mut batch = RoomEffects::new().with_gauge_delta(counts);
    if let Some(transport_close) = transport_close {
        batch = batch.with_transport_user_close_cleanup(transport_close);
    }
    if let Some(outcome) = state_outcome {
        let mut diagnostics =
            DiagnosticsEventData::for_user(room.uuid(), &user_id, telemetry_event::USER_CLOSED)
                .with_connection_id(connection_id.as_u64());
        if let Some(media_worker_id) = media_worker_id {
            diagnostics = diagnostics.with_media_worker_id(media_worker_id.as_usize());
        }
        batch = batch
            .with_relay_effects(outcome.relay_effects)
            .with_pre_close_transport_cleanup(outcome.transport_cleanup)
            .with_lifecycle_effects(outcome.effects)
            .record_diagnostics(diagnostics)
            .forget_diagnostics_user(user_id)
            .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged);
    }
    batch
}

/// callers pass staged publish cleanup separately because it lives outside
/// [`RoomState`]
pub(in crate::engine::room) fn build_disconnect(
    room: &Room,
    counts: RoomGaugeDelta,
    outcome: DisconnectUsersOutcome,
    staged_cleanup: Vec<TransportCleanupOperation>,
) -> RoomEffects {
    let mut batch = RoomEffects::new()
        .with_gauge_delta(counts)
        .with_relay_effects(outcome.relay_effects)
        .with_pre_close_transport_cleanup(staged_cleanup)
        .with_pre_close_transport_cleanup(outcome.transport_cleanup)
        .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
        .with_lifecycle_effects(outcome.effects);
    for user in outcome.disconnected_users {
        let media_worker_id = user.close_operation.session_key().media_worker_id();
        batch = batch
            .with_transport_user_close_cleanup(user.close_operation)
            .record_diagnostics(
                RoomDiagnosticsContext::new(&user.user_id, user.connection_id, media_worker_id)
                    .event_data(room, telemetry_event::USER_DISCONNECTED),
            )
            .forget_diagnostics_user(user.user_id);
    }
    batch
}

pub(in crate::engine::room) fn build_publish_commit(
    room: &Room,
    publish_counts: RoomGaugeDelta,
    setup_counts: RoomGaugeDelta,
    setups: Vec<PendingConsumerSetup>,
    diagnostics: RoomDiagnosticsContext<'_>,
    media: TransportMediaId,
) -> RoomEffects {
    let diagnostics = diagnostics
        .event_data(room, telemetry_event::PUBLISH_COMMITTED)
        .with_transport_media_id(media.as_u64());
    RoomEffects::new()
        .with_gauge_delta(publish_counts)
        .with_gauge_delta(setup_counts)
        .with_consumer_setups(setups, ConsumerSetupOrigin::Publish)
        .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
        .record_diagnostics(diagnostics)
}

pub(in crate::engine::room) fn build_publication_activity(
    room: &Room,
    effect: PublicationActivityEffect<'_>,
) -> RoomEffects {
    let diagnostics = effect
        .diagnostics
        .event_data(room, telemetry_event::PUBLICATION_ACTIVITY_CHANGED)
        .with_transport_media_id(effect.media.as_u64())
        .insert_field("active", effect.active)
        .insert_field("stream_id", effect.stream.to_string());
    RoomEffects::new()
        .with_producer_activity(
            effect.source,
            effect.active,
            effect.stream.clone(),
            diagnostics,
        )
        .with_track_binding_update(effect.recipients, effect.track_update)
        .with_source_policy_event(SourcePolicyEvent::FanoutPressureChanged)
}

pub(in crate::engine::room) fn build_unpublish(
    counts: RoomGaugeDelta,
    relays: Vec<ResolvedRelayRouteEffect>,
    cleanup: Vec<TransportCleanupOperation>,
    recipients: Vec<OutboundSender>,
    track_update: TrackBindingUpdate,
) -> RoomEffects {
    RoomEffects::new()
        .with_gauge_delta(counts)
        .with_relay_effects(relays)
        .with_pre_close_transport_cleanup(cleanup)
        .with_track_binding_update(recipients, track_update)
        .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
}

pub(in crate::engine::room) fn build_late_join(
    counts: RoomGaugeDelta,
    setups: Vec<PendingConsumerSetup>,
) -> RoomEffects {
    RoomEffects::new()
        .with_gauge_delta(counts)
        .with_consumer_setups(setups, ConsumerSetupOrigin::LateJoin)
        .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
}

pub(in crate::engine::room) fn build_subscription_change(
    state: &RoomState,
    room: &Room,
    effect: SubscriptionChangeEffect<'_>,
) -> RoomEffects {
    let source_policy_event = if effect.route_updates.is_empty()
        && effect.setups.is_empty()
        && effect.relays.is_empty()
    {
        SourcePolicyEvent::ReceiverIntentChanged
    } else {
        SourcePolicyEvent::RouteGraphChanged
    };
    RoomEffects::new()
        .with_gauge_delta(effect.counts)
        .with_relay_effects(effect.relays)
        .with_route_updates(state, room, effect.diagnostics, effect.route_updates)
        .with_consumer_setups(effect.setups, ConsumerSetupOrigin::Subscribe)
        .with_source_policy_event(source_policy_event)
}

impl RoomEffects {
    fn new() -> Self {
        Self::default()
    }

    fn with_gauge_delta(mut self, delta: RoomGaugeDelta) -> Self {
        if let Some(users) = delta.users {
            self.users = Some(users);
        }
        self.media.push(delta.media);
        self
    }

    fn with_relay_effects(
        mut self,
        effects: impl IntoIterator<Item = ResolvedRelayRouteEffect>,
    ) -> Self {
        self.relays.extend(effects);
        self
    }

    fn with_pre_close_transport_cleanup(
        mut self,
        operations: impl IntoIterator<Item = TransportCleanupOperation>,
    ) -> Self {
        self.cleanup.before_close.extend(operations);
        self
    }

    fn with_transport_user_close_cleanup(mut self, operation: TransportCleanupOperation) -> Self {
        debug_assert!(matches!(
            &operation,
            TransportCleanupOperation::CloseUser { .. }
        ));
        self.cleanup.close_users.push(operation);
        self
    }

    fn with_producer_activity(
        mut self,
        source: TransportSourceKey,
        active: bool,
        stream: UserStreamId,
        diagnostics: DiagnosticsEventData,
    ) -> Self {
        self.producers.push(ProducerActivityEffect {
            source,
            active,
            stream,
            diagnostics,
        });
        self
    }

    fn with_track_binding_update(
        mut self,
        recipients: Vec<OutboundSender>,
        update: TrackBindingUpdate,
    ) -> Self {
        self.track_bindings
            .push(TrackBindingFanout { recipients, update });
        self
    }

    fn with_source_policy_event(mut self, event: SourcePolicyEvent) -> Self {
        self.source_policy = Some(event);
        self
    }

    fn with_lifecycle_effects(mut self, effects: LifecycleEffects) -> Self {
        self.lifecycle.push(effects);
        self
    }

    fn with_route_updates(
        mut self,
        state: &RoomState,
        room: &Room,
        context: RoomDiagnosticsContext<'_>,
        updates: Vec<ConsumerRouteUpdate>,
    ) -> Self {
        self.routes.extend(updates.into_iter().map(|update| {
            let ConsumerRouteUpdate {
                route,
                stream,
                kind,
                active,
            } = update;
            let transport_route = state.transport_consumer_route(&route);
            let diagnostics = context
                .event_data(room, telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED)
                .with_transport_media_id(route.consumer_media().as_u64())
                .insert_field("active", active)
                .insert_field(
                    "producer_user_id",
                    serde_json::to_value(route.source_user_id()).unwrap_or(serde_json::Value::Null),
                )
                .insert_field("source_transport_media_id", route.source_media().as_u64())
                .insert_field("stream_id", stream.to_string());
            ConsumerRouteActivity {
                route: transport_route,
                stream,
                kind,
                active,
                diagnostics,
            }
        }));
        self
    }

    fn with_consumer_setups(
        mut self,
        setups: Vec<PendingConsumerSetup>,
        origin: ConsumerSetupOrigin,
    ) -> Self {
        self.consumer_setups.extend(
            setups
                .into_iter()
                .map(|setup| ConsumerSetupEffect::new(setup, origin)),
        );
        self
    }

    fn register_diagnostics_user(mut self, user_id: UserId) -> Self {
        self.diagnostics
            .push(DiagnosticsEffect::RegisterUser(user_id));
        self
    }

    fn record_diagnostics(mut self, diagnostics: DiagnosticsEventData) -> Self {
        self.diagnostics
            .push(DiagnosticsEffect::Record(diagnostics));
        self
    }

    fn forget_diagnostics_user(mut self, user_id: UserId) -> Self {
        self.diagnostics
            .push(DiagnosticsEffect::ForgetUser(user_id));
        self
    }

    /// runs deferred effects after the caller releases the [`RoomState`] guard
    ///
    /// execution preserves the room-wide side-effect order: metrics, relay
    /// routes, cleanup, transport activity, consumer setup, fanout,
    /// source-policy wakeups, lifecycle effects then diagnostics
    pub async fn execute(self, room: &Room, context: RoomEffectContext<'_>) -> RoomEffectOutcome {
        Self::record_gauge_deltas(room, self.users, &self.media);
        if let Some(media_transport) = context.routed_transport_effects {
            execute_relay_route_effects(room, media_transport, &self.relays).await;
        }
        let cleanup = self.cleanup.execute(room, context).await;
        let producer_activity =
            Self::execute_producer_activity(room, context, self.producers).await;
        Self::execute_route_activity(room, context, self.routes).await;
        let mut diagnostics = self.diagnostics;
        Self::execute_consumer_setups(room, context, self.consumer_setups, &mut diagnostics).await;
        Self::emit_track_binding_updates(self.track_bindings);
        if let Some(event) = self.source_policy {
            room.handle_source_policy_event(event, context.producer_activity_and_source_policy)
                .await;
        }
        Self::emit_lifecycle_effects(self.lifecycle);
        Self::record_diagnostics_effects(room, diagnostics);
        RoomEffectOutcome {
            cleanup,
            producer_activity,
        }
    }

    fn record_gauge_deltas(
        room: &Room,
        user_count_delta: Option<UserCountDelta>,
        media_deltas: &[MediaCountDelta],
    ) {
        if let Some(delta) = user_count_delta {
            let before = i64::try_from(delta.before).unwrap_or(i64::MAX);
            let after = i64::try_from(delta.after).unwrap_or(i64::MAX);
            room.metrics.add_active_users(after.saturating_sub(before));
        }
        for delta in media_deltas {
            (*delta).record(room);
        }
    }

    async fn execute_route_activity(
        room: &Room,
        context: RoomEffectContext<'_>,
        routes: Vec<ConsumerRouteActivity>,
    ) {
        let Some(media_transport) = context.routed_transport_effects else {
            return;
        };
        for op in routes {
            let route = &op.route;
            if media_transport
                .set_consumer_active(route, ConsumerActivity::from_active(op.active))
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
                && op.kind == MediaKind::Video
                && media_transport
                    .request_consumer_keyframe(route)
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
    }

    async fn execute_producer_activity(
        room: &Room,
        context: RoomEffectContext<'_>,
        producers: Vec<ProducerActivityEffect>,
    ) -> TransportEffectOutcome {
        let mut outcome = TransportEffectOutcome::Applied;
        for op in producers {
            if let Some(media_transport) = context.producer_activity_and_source_policy
                && media_transport
                    .set_producer_active(&op.source, ProducerActivity::from_active(op.active))
                    .await
                    .is_err()
            {
                outcome = TransportEffectOutcome::Failed;
                warn!(
                    source = ?op.source,
                    stream_id = %op.stream,
                    active = op.active,
                    "media transport failed to update producer route activity"
                );
            }
            room.diagnostics.record(op.diagnostics);
        }
        outcome
    }

    async fn execute_consumer_setups(
        room: &Room,
        context: RoomEffectContext<'_>,
        setups: Vec<ConsumerSetupEffect>,
        diagnostics: &mut Vec<DiagnosticsEffect>,
    ) {
        let Some(media_transport) = context.routed_transport_effects else {
            return;
        };
        for setup in setups {
            if let Some(diagnostic) = setup.execute(room, media_transport).await {
                diagnostics.push(DiagnosticsEffect::Record(diagnostic));
            }
        }
    }

    fn emit_lifecycle_effects(lifecycle: Vec<LifecycleEffects>) {
        for effects in lifecycle {
            for close_request in effects.close_requests {
                let _ = close_request
                    .sender
                    .send(UserOutbound::Close(close_request.reason));
            }
            for fanout in effects.fanouts {
                fanout.emit();
            }
        }
    }

    fn emit_track_binding_updates(fanouts: Vec<TrackBindingFanout>) {
        for fanout in fanouts {
            for recipient in fanout.recipients {
                let _ = recipient.send(UserOutbound::TrackBindingUpdate(fanout.update.clone()));
            }
        }
    }

    fn record_diagnostics_effects(room: &Room, diagnostics: Vec<DiagnosticsEffect>) {
        for effect in diagnostics {
            match effect {
                DiagnosticsEffect::RegisterUser(user_id) => {
                    room.diagnostics.register_user(room.uuid(), &user_id);
                }
                DiagnosticsEffect::Record(diagnostics) => {
                    room.diagnostics.record(diagnostics);
                }
                DiagnosticsEffect::ForgetUser(user_id) => {
                    room.diagnostics.forget_user(room.uuid(), &user_id);
                }
            }
        }
    }
}

impl RoomCleanup {
    async fn execute(self, room: &Room, context: RoomEffectContext<'_>) -> TransportEffectOutcome {
        let Some(media_transport) = context.routed_transport_effects else {
            return TransportEffectOutcome::Applied;
        };
        let before_close =
            Self::execute_operations(room, media_transport, &self.before_close).await;
        let close_users = Self::execute_operations(room, media_transport, &self.close_users).await;
        if before_close == TransportEffectOutcome::Failed
            || close_users == TransportEffectOutcome::Failed
        {
            TransportEffectOutcome::Failed
        } else {
            TransportEffectOutcome::Applied
        }
    }

    async fn execute_operations(
        room: &Room,
        media_transport: &MediaTransport,
        operations: &[TransportCleanupOperation],
    ) -> TransportEffectOutcome {
        if operations.is_empty() {
            return TransportEffectOutcome::Applied;
        }
        room.execute_transport_cleanup_operations(media_transport, operations)
            .await
    }
}

pub(super) async fn execute_relay_route_effects(
    room: &Room,
    media_transport: &MediaTransport,
    effects: &[ResolvedRelayRouteEffect],
) -> bool {
    let mut applied = true;
    for effect in effects {
        if effect.action == TransportRelayRouteAction::Release {
            let operation = [TransportCleanupOperation::ReleaseRelayRoute {
                source_session_key: effect.source_session_key.clone(),
                route: effect.route.clone(),
            }];
            if room
                .execute_transport_cleanup_operations(media_transport, &operation)
                .await
                == TransportEffectOutcome::Failed
            {
                applied = false;
            }
            continue;
        }
        let transport_effect = TransportRelayRouteEffect {
            source: TransportSourceKey::new(
                effect.source_session_key.clone(),
                effect.route.source_media,
            ),
            target_media_worker_id: effect.route.target_worker,
            action: effect.action,
        };
        if let Err(error) = media_transport
            .apply_relay_route_effect(&transport_effect)
            .await
        {
            applied = false;
            warn!(
                ?effect,
                ?error,
                "media transport failed to apply relay route effect"
            );
        }
    }
    applied
}
