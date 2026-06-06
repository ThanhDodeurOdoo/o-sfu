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
            TransportRelayRouteAction, TransportRelayRouteEffect, TransportSourceKey,
        },
        room::{
            Room, RoomMediaCounts, SourcePolicyEvent, TrackBindingUpdate, UserOutbound,
            cleanup::TransportCleanupOperation,
            media_graph::{
                ConsumerRouteUpdate, ConsumerSetupOrigin, PendingConsumerSetup,
                ResolvedRelayRouteEffect,
            },
            outbound::OutboundSender,
            state::{LifecycleEffects, RoomState},
        },
        source_model::UserStreamId,
    },
};

#[derive(Debug, Clone, Copy)]
pub struct RoomEffectContext<'a> {
    media: Option<&'a MediaTransport>,
    cleanup: Option<&'a MediaTransport>,
}

impl<'a> RoomEffectContext<'a> {
    pub const fn runtime(media_transport: &'a MediaTransport) -> Self {
        Self {
            media: Some(media_transport),
            cleanup: Some(media_transport),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub const fn state_only(media_transport: Option<&'a MediaTransport>) -> Self {
        Self {
            media: media_transport,
            cleanup: None,
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

#[derive(Debug, Default)]
pub struct RoomCommit {
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

#[derive(Debug, Clone, Copy)]
pub struct RoomCommitExecution {
    cleanup: TransportEffectOutcome,
    producer_activity: TransportEffectOutcome,
}

impl RoomCommitExecution {
    pub const fn cleanup(self) -> TransportEffectOutcome {
        self.cleanup
    }

    pub const fn producer_activity(self) -> TransportEffectOutcome {
        self.producer_activity
    }
}

impl RoomCommit {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_user_count_delta(mut self, before: usize, after: usize) -> Self {
        self.users = Some(UserCountDelta { before, after });
        self
    }

    pub fn with_media_count_delta(
        mut self,
        before: RoomMediaCounts,
        after: RoomMediaCounts,
    ) -> Self {
        self.media.push(MediaCountDelta::new(before, after));
        self
    }

    pub fn with_relay_effects(
        mut self,
        effects: impl IntoIterator<Item = ResolvedRelayRouteEffect>,
    ) -> Self {
        self.relays.extend(effects);
        self
    }

    pub fn with_pre_close_transport_cleanup(
        mut self,
        operations: impl IntoIterator<Item = TransportCleanupOperation>,
    ) -> Self {
        self.cleanup.before_close.extend(operations);
        self
    }

    pub fn with_transport_user_close_cleanup(
        mut self,
        operation: TransportCleanupOperation,
    ) -> Self {
        debug_assert!(matches!(
            &operation,
            TransportCleanupOperation::CloseUser { .. }
        ));
        self.cleanup.close_users.push(operation);
        self
    }

    pub fn with_producer_activity(
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

    pub fn with_track_binding_update(
        mut self,
        recipients: Vec<OutboundSender>,
        update: TrackBindingUpdate,
    ) -> Self {
        self.track_bindings
            .push(TrackBindingFanout { recipients, update });
        self
    }

    pub fn with_source_policy_event(mut self, event: SourcePolicyEvent) -> Self {
        self.source_policy = Some(event);
        self
    }

    pub fn with_lifecycle_effects(mut self, effects: LifecycleEffects) -> Self {
        self.lifecycle.push(effects);
        self
    }

    pub fn with_route_updates(
        mut self,
        state: &RoomState,
        room: &Room,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_worker_id: MediaWorkerId,
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

    pub fn with_consumer_setups(
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

    pub fn register_diagnostics_user(mut self, user_id: UserId) -> Self {
        self.diagnostics
            .push(DiagnosticsEffect::RegisterUser(user_id));
        self
    }

    pub fn record_diagnostics(mut self, diagnostics: DiagnosticsEventData) -> Self {
        self.diagnostics
            .push(DiagnosticsEffect::Record(diagnostics));
        self
    }

    pub fn forget_diagnostics_user(mut self, user_id: UserId) -> Self {
        self.diagnostics
            .push(DiagnosticsEffect::ForgetUser(user_id));
        self
    }

    pub async fn execute(self, room: &Room, context: RoomEffectContext<'_>) -> RoomCommitExecution {
        Self::record_gauge_deltas(room, self.users, &self.media);
        if let Some(media_transport) = context.cleanup {
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
            room.handle_source_policy_event(event, context.media).await;
        }
        Self::emit_lifecycle_effects(self.lifecycle);
        Self::record_diagnostics_effects(room, diagnostics);
        RoomCommitExecution {
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
        let Some(media_transport) = context.cleanup else {
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
            if let Some(media_transport) = context.media
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
        let Some(media_transport) = context.cleanup else {
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
        let Some(media_transport) = context.cleanup else {
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
