use o_sfu_telemetry::schema::event as telemetry_event;

pub use super::observability::RoomGaugeDelta;
use super::{
    consumer_transport::ConsumerTransportPlan,
    observability::RoomObservabilityPlan,
    output::RoomOutputPlan,
    policy::RoomPolicyPlan,
    transport::{ProducerActivityEffect, RoomTransportPlan},
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    diagnostics::DiagnosticsEventData,
    media_transport::MediaTransport,
    room::{
        Room,
        cleanup::TransportCleanupOperation,
        media_graph::{
            ConsumerReadinessCommit, ConsumerRouteTarget, ConsumerRouteUpdate, ConsumerSetupOrigin,
            MediaTopologyEffects, PendingConsumerSetup, ProducerActivityCommit, PublishCommit,
            ReceiverIntentCommit, UnpublishCommit,
        },
        outbound::MessageFanout,
        routing::CommittedRoutingReceipt,
        state::{DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome},
    },
};

#[derive(Debug, Clone, Copy)]
pub struct RoomEffectContext<'a> {
    media_transport: Option<&'a MediaTransport>,
    route_effects: bool,
}

impl<'a> RoomEffectContext<'a> {
    pub const fn runtime(media_transport: &'a MediaTransport) -> Self {
        Self {
            media_transport: Some(media_transport),
            route_effects: true,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub const fn state_only(media_transport: Option<&'a MediaTransport>) -> Self {
        Self {
            media_transport,
            route_effects: false,
        }
    }

    fn media_transport(self) -> Option<&'a MediaTransport> {
        self.media_transport
    }

    fn route_transport(self) -> Option<&'a MediaTransport> {
        self.route_effects.then_some(self.media_transport).flatten()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RoomDiagnosticsContext<'a> {
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

#[derive(Debug, Default)]
#[must_use = "room effect batches must be executed after the state transition commits"]
pub struct RoomEffects {
    observability: RoomObservabilityPlan,
    transport: RoomTransportPlan,
    consumers: ConsumerTransportPlan,
    output: RoomOutputPlan,
    policy: RoomPolicyPlan,
}

pub fn build_join(
    room: &Room,
    counts: RoomGaugeDelta,
    outcome: JoinUserOutcome,
) -> (RoomEffects, CommittedRoutingReceipt) {
    let JoinUserOutcome {
        effects,
        user_id,
        routing_receipt,
        media_effects,
    } = outcome;
    let diagnostics = RoomDiagnosticsContext::new(
        &user_id,
        routing_receipt.connection_id,
        routing_receipt.transport_session_key.media_worker_id(),
    )
    .event_data(room, telemetry_event::USER_JOINED);
    let mut batch = RoomEffects::default();
    batch.observability.push_gauge(counts);
    batch.extend_media_topology_effects(media_effects);
    batch.policy.route_graph_changed();
    batch.output.push_lifecycle(effects);
    batch.observability.register_user(user_id);
    batch.observability.record(diagnostics);
    (batch, routing_receipt)
}

pub fn build_connection_close(
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
    let mut batch = RoomEffects::default();
    batch.observability.push_gauge(counts);
    if let Some(outcome) = state_outcome {
        let mut diagnostics =
            DiagnosticsEventData::for_user(room.uuid(), &user_id, telemetry_event::USER_CLOSED)
                .with_connection_id(connection_id.as_u64());
        if let Some(media_worker_id) = media_worker_id {
            diagnostics = diagnostics.with_media_worker_id(media_worker_id.as_usize());
        }
        batch.extend_media_topology_effects(outcome.media_effects);
        batch.output.push_lifecycle(outcome.effects);
        batch.observability.record(diagnostics);
        batch.observability.forget_user(user_id);
        batch.policy.route_graph_changed();
    }
    if let Some(transport_close) = transport_close {
        batch.push_transport_user_close_cleanup(transport_close);
    }
    batch
}

pub fn build_disconnect(
    room: &Room,
    counts: RoomGaugeDelta,
    outcome: DisconnectUsersOutcome,
    staged_cleanup: Vec<TransportCleanupOperation>,
) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch.observability.push_gauge(counts);
    batch.transport.extend_cleanup(staged_cleanup);
    batch.extend_media_topology_effects(outcome.media_effects);
    batch.policy.route_graph_changed();
    batch.output.push_lifecycle(outcome.effects);
    for user in outcome.disconnected_users {
        let media_worker_id = user.close_operation.session_key().media_worker_id();
        batch.push_transport_user_close_cleanup(user.close_operation);
        batch.observability.record(
            RoomDiagnosticsContext::new(&user.user_id, user.connection_id, media_worker_id)
                .event_data(room, telemetry_event::USER_DISCONNECTED),
        );
        batch.observability.forget_user(user.user_id);
    }
    batch
}

pub fn build_publish_commit(room: &Room, commit: PublishCommit) -> RoomEffects {
    let diagnostics = RoomDiagnosticsContext::new(&commit.user, commit.connection, commit.worker)
        .event_data(room, telemetry_event::PUBLISH_COMMITTED)
        .with_transport_media_id(commit.media.as_u64());
    let mut batch = RoomEffects::default();
    batch.observability.push_gauge(RoomGaugeDelta::media(
        commit.publish_before,
        commit.publish_after,
    ));
    batch.observability.push_gauge(RoomGaugeDelta::media(
        commit.setup_before,
        commit.setup_after,
    ));
    batch.push_consumer_setups(commit.setups, ConsumerSetupOrigin::Publish);
    batch.policy.route_graph_changed();
    batch.observability.record(diagnostics);
    batch
}

pub fn build_publication_activity(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    commit: ProducerActivityCommit,
) -> RoomEffects {
    let ProducerActivityCommit {
        source,
        media,
        worker,
        stream,
        active,
        recipients,
        update,
    } = commit;
    let diagnostics = RoomDiagnosticsContext::new(user_id, connection_id, worker)
        .event_data(room, telemetry_event::PUBLICATION_ACTIVITY_CHANGED)
        .with_transport_media_id(media.as_u64())
        .insert_field("active", active)
        .insert_field("stream_id", stream.to_string());
    let mut batch = RoomEffects::default();
    batch.transport.push_producer(ProducerActivityEffect::new(
        source,
        active,
        stream,
        diagnostics,
    ));
    batch.output.push_track_binding(recipients, update);
    batch.policy.fanout_pressure_changed();
    batch
}

pub fn build_unpublish(commit: UnpublishCommit) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch
        .observability
        .push_gauge(RoomGaugeDelta::media(commit.before, commit.after));
    batch.extend_media_topology_effects(commit.media_effects);
    batch
        .output
        .push_track_binding(commit.recipients, commit.update);
    batch.policy.route_graph_changed();
    batch
}

pub fn build_consumer_readiness(commit: ConsumerReadinessCommit) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch
        .observability
        .push_gauge(RoomGaugeDelta::media(commit.before, commit.after));
    batch.push_consumer_setups(commit.setups, ConsumerSetupOrigin::Readiness);
    batch.policy.route_graph_changed();
    batch
}

pub fn build_keyframe_refresh(targets: Vec<ConsumerRouteTarget>) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch.consumers.extend_keyframe_refresh(targets);
    batch
}

pub fn build_user_info_update(fanout: MessageFanout) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch.policy.receiver_intent_changed();
    batch.output.push_user_info(fanout);
    batch
}

pub fn build_receiver_intent(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    commit: ReceiverIntentCommit,
) -> RoomEffects {
    let route_graph_changed = !commit.change.updates.is_empty()
        || !commit.change.setups.is_empty()
        || !commit.change.relays.is_empty();
    let diagnostics = RoomDiagnosticsContext::new(user_id, connection_id, commit.media_worker_id);
    let mut batch = RoomEffects::default();
    batch
        .observability
        .push_gauge(RoomGaugeDelta::media(commit.before, commit.after));
    batch.transport.extend_relays(commit.change.relays);
    batch.push_route_updates(room, diagnostics, commit.change.updates);
    batch.push_consumer_setups(commit.change.setups, ConsumerSetupOrigin::Subscribe);
    if route_graph_changed {
        batch.policy.route_graph_changed();
    } else {
        batch.policy.receiver_intent_changed();
    }
    batch
}

impl RoomEffects {
    fn extend_media_topology_effects(&mut self, effects: MediaTopologyEffects) {
        self.transport.extend_topology(effects);
    }

    fn push_transport_user_close_cleanup(&mut self, operation: TransportCleanupOperation) {
        debug_assert!(matches!(
            &operation,
            TransportCleanupOperation::CloseUser { .. }
        ));
        self.transport.push_cleanup(operation);
    }

    fn push_route_updates(
        &mut self,
        room: &Room,
        context: RoomDiagnosticsContext<'_>,
        updates: Vec<ConsumerRouteUpdate>,
    ) {
        for update in updates {
            let ConsumerRouteUpdate { target, active } = update;
            let diagnostics = context
                .event_data(room, telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED)
                .with_transport_media_id(target.consumer_media_id().as_u64())
                .insert_field("active", active)
                .insert_field(
                    "producer_user_id",
                    serde_json::to_value(target.producer_user_id())
                        .unwrap_or(serde_json::Value::Null),
                )
                .insert_field(
                    "source_transport_media_id",
                    target.source_media_id().as_u64(),
                )
                .insert_field("stream_id", target.stream_id().to_string());
            self.consumers.push_activity(target, active, diagnostics);
        }
    }

    fn push_consumer_setups(
        &mut self,
        setups: Vec<PendingConsumerSetup>,
        origin: ConsumerSetupOrigin,
    ) {
        self.consumers.push_setups(setups, origin);
    }

    /// preserves the room-wide side-effect order across transport and policy work
    pub async fn execute(self, room: &Room, context: RoomEffectContext<'_>) {
        let mut observability = self.observability;
        let mut output = self.output;
        let mut policy = self.policy;
        observability.record_gauges(room);
        let transport_diagnostics = self
            .transport
            .execute(room, context.media_transport(), context.route_transport())
            .await;
        observability.extend_records(transport_diagnostics);
        let consumer = self
            .consumers
            .execute(room, context.route_transport())
            .await;
        observability.extend_gauges(consumer.gauges);
        observability.extend_records(consumer.diagnostics);
        policy.extend(consumer.policy);
        observability.record_gauges(room);
        output.emit_before_policy();
        policy.execute(room, context.media_transport()).await;
        output.emit_after_policy();
        observability.record_diagnostics(room);
    }
}
