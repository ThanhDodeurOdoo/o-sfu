//! shared post-lock room commit execution

use o_sfu_router::MediaKind;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use crate::{
    TransportEffectOutcome,
    engine::{
        ConnectionId, UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{
            ConsumerActivity, MediaTransport, ProducerActivity, TransportMediaId,
            TransportRelayRouteAction, TransportRelayRouteEffect, TransportSourceKey,
        },
        room::{
            RemoteTrackBootstrap, Room, RoomMediaCounts, SourcePolicyEvent, TrackBindingUpdate,
            UserOutbound,
            cleanup::TransportCleanupOperation,
            media_graph::{
                ConsumerBootstrapOrigin, ConsumerRouteTransportRef, ConsumerRouteUpdate,
                PendingConsumerBootstrapTarget, PlannedConsumerBootstrap,
                PreparedConsumerBootstrap, RelayRouteEffect, TransportMediaRemoval,
            },
            outbound::OutboundSender,
            state::LifecycleEffects,
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
struct MediaCountDelta {
    before: RoomMediaCounts,
    after: RoomMediaCounts,
}

impl MediaCountDelta {
    pub const fn new(before: RoomMediaCounts, after: RoomMediaCounts) -> Self {
        Self { before, after }
    }

    fn record(self, room: &Room) {
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
    relays: Vec<RelayRouteEffect>,
    cleanup: RoomCleanup,
    producers: Vec<ProducerActivityEffect>,
    source_policy: Option<SourcePolicyEvent>,
    lifecycle: Vec<LifecycleEffects>,
    routes: Vec<ConsumerRouteActivity>,
    bootstraps: Vec<ConsumerBootstrapLease>,
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
    route: ConsumerRouteTransportRef,
    stream: UserStreamId,
    kind: MediaKind,
    active: bool,
    diagnostics: DiagnosticsEventData,
}

#[derive(Debug)]
struct ConsumerBootstrapLease {
    bootstrap: PlannedConsumerBootstrap,
    origin: ConsumerBootstrapOrigin,
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
        effects: impl IntoIterator<Item = RelayRouteEffect>,
    ) -> Self {
        self.relays.extend(effects);
        self
    }

    pub fn with_transport_removals(
        mut self,
        room: &Room,
        removals: impl IntoIterator<Item = TransportMediaRemoval>,
    ) -> Self {
        self.cleanup
            .before_close
            .extend(removals.into_iter().map(|removal| {
                let connection_id = removal.connection();
                TransportCleanupOperation::RemoveMedia {
                    session_key: room.transport_user_key(removal.user(), connection_id),
                    connection_id,
                    transport_media_id: removal.transport_media(),
                }
            }));
        self
    }

    pub fn with_transport_user_close(
        mut self,
        room: &Room,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Self {
        self.cleanup
            .close_users
            .push(TransportCleanupOperation::CloseUser {
                session_key: room.transport_user_key(user_id, connection_id),
                connection_id,
            });
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
        room: &Room,
        user_id: &UserId,
        connection_id: ConnectionId,
        updates: Vec<ConsumerRouteUpdate>,
    ) -> Self {
        let media_worker_id = room
            .transport_user_key(user_id, connection_id)
            .media_worker_id();
        self.routes.extend(updates.into_iter().map(|update| {
            let ConsumerRouteUpdate {
                route,
                stream,
                kind,
                active,
            } = update;
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
                route,
                stream,
                kind,
                active,
                diagnostics,
            }
        }));
        self
    }

    pub fn with_bootstraps(
        mut self,
        bootstraps: Vec<PlannedConsumerBootstrap>,
        origin: ConsumerBootstrapOrigin,
    ) -> Self {
        self.bootstraps.extend(
            bootstraps
                .into_iter()
                .map(|bootstrap| ConsumerBootstrapLease { bootstrap, origin }),
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
        Self::execute_bootstraps(room, context, self.bootstraps).await;
        Self::emit_track_binding_updates(self.track_bindings);
        if let Some(event) = self.source_policy {
            room.handle_source_policy_event(event, context.media).await;
        }
        Self::emit_lifecycle_effects(self.lifecycle);
        Self::record_diagnostics_effects(room, self.diagnostics);
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
            let transport_route = room.transport_consumer_route(route);
            if media_transport
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
                && op.kind == MediaKind::Video
                && media_transport
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

    async fn execute_bootstraps(
        room: &Room,
        context: RoomEffectContext<'_>,
        bootstraps: Vec<ConsumerBootstrapLease>,
    ) {
        let Some(media_transport) = context.cleanup else {
            return;
        };
        for lease in bootstraps {
            lease.execute(room, media_transport).await;
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

impl ConsumerBootstrapLease {
    async fn execute(self, room: &Room, media_transport: &MediaTransport) {
        let (target, prepared, pending, relays) = self.bootstrap.into_parts();
        let origin = self.origin;
        if !execute_relay_route_effects(room, media_transport, &relays).await {
            release_consumer_bootstrap(room, &target, media_transport).await;
            return;
        }
        let activity = ConsumerActivity::from_active(pending.consumer_active());
        let Some((consumer_media, mid)) = Self::declare_transport_media(
            &target,
            &prepared,
            activity,
            origin,
            room,
            media_transport,
        )
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
        Self::finish(
            room,
            media_transport,
            &target,
            origin,
            consumer_media,
            MediaCountDelta::new(before, after),
            outbound,
        )
        .await;
    }

    async fn declare_transport_media(
        target: &PendingConsumerBootstrapTarget,
        prepared: &PreparedConsumerBootstrap,
        activity: ConsumerActivity,
        origin: ConsumerBootstrapOrigin,
        room: &Room,
        media_transport: &MediaTransport,
    ) -> Option<(TransportMediaId, Option<String>)> {
        let consumer_session =
            room.transport_user_key(target.consumer_user_id(), target.consumer_connection_id());
        let producer_session =
            room.transport_user_key(target.producer_user_id(), target.producer_connection_id());
        match media_transport
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
                let mid = media_transport
                    .transport_media_mid(&consumer_session, consumer_media)
                    .await;
                Some((consumer_media, mid))
            }
            Err(error) => {
                release_consumer_bootstrap(room, target, media_transport).await;
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

    async fn finish(
        room: &Room,
        media_transport: &MediaTransport,
        target: &PendingConsumerBootstrapTarget,
        origin: ConsumerBootstrapOrigin,
        consumer_media: TransportMediaId,
        delta: MediaCountDelta,
        outbound: Option<(OutboundSender, RemoteTrackBootstrap)>,
    ) {
        let Some((sender, bootstrap)) = outbound else {
            delta.record(room);
            release_consumer_bootstrap(room, target, media_transport).await;
            let cleanup = [TransportCleanupOperation::RemoveMedia {
                session_key: room
                    .transport_user_key(target.consumer_user_id(), target.consumer_connection_id()),
                connection_id: target.consumer_connection_id(),
                transport_media_id: consumer_media,
            }];
            room.execute_transport_cleanup_operations(media_transport, &cleanup)
                .await;
            return;
        };
        delta.record(room);
        room.diagnostics.record(
            DiagnosticsEventData::for_user(
                room.uuid(),
                target.consumer_user_id(),
                telemetry_event::SUBSCRIBE_SUCCEEDED,
            )
            .with_connection_id(target.consumer_connection_id().as_u64())
            .with_media_worker_id(
                room.transport_user_key(target.consumer_user_id(), target.consumer_connection_id())
                    .media_worker_id()
                    .as_usize(),
            )
            .with_transport_media_id(consumer_media.as_u64())
            .insert_field(
                "producer_user_id",
                serde_json::to_value(target.producer_user_id()).unwrap_or(serde_json::Value::Null),
            )
            .insert_field(
                "source_transport_media_id",
                target.transport_media_id().as_u64(),
            )
            .insert_field("stream_id", target.stream_id().to_string())
            .insert_field("origin", origin.as_diagnostic_str()),
        );
        let _ = sender.send(UserOutbound::Request(Box::new(
            bootstrap.into_room_event_request(),
        )));
    }
}

async fn release_consumer_bootstrap(
    room: &Room,
    target: &PendingConsumerBootstrapTarget,
    media_transport: &MediaTransport,
) {
    let (before, after, relays) = {
        let mut state = room.state.write().await;
        let before = state.media_counts();
        let relays = state.release_bootstrap(target);
        let after = state.media_counts();
        drop(state);
        (before, after, relays)
    };
    MediaCountDelta::new(before, after).record(room);
    execute_relay_route_effects(room, media_transport, &relays).await;
    room.handle_source_policy_event(
        SourcePolicyEvent::FanoutPressureChanged,
        Some(media_transport),
    )
    .await;
}

async fn execute_relay_route_effects(
    room: &Room,
    media_transport: &MediaTransport,
    effects: &[RelayRouteEffect],
) -> bool {
    let mut applied = true;
    for effect in effects {
        if effect.action == TransportRelayRouteAction::Release {
            let operation = [TransportCleanupOperation::ReleaseRelayRoute {
                source_session_key: room
                    .transport_user_key(&effect.route.source_user, effect.route.source_connection),
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
                room.transport_user_key(&effect.route.source_user, effect.route.source_connection),
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
