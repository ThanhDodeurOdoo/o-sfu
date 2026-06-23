use o_sfu_telemetry::schema::event as telemetry_event;

pub use super::observability::RoomGaugeDelta;
use super::{
    observability::RoomObservabilityPlan, output::RoomOutputPlan, transport::RoomTransportPlan,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    diagnostics::DiagnosticsEventData,
    media_transport::MediaTransport,
    room::{
        Room,
        cleanup::TransportCleanupOperation,
        media_graph::{
            CommittedTransportReceipt, ConsumerRouteTarget, ConsumerSetupOrigin,
            MediaTopologyEffects, ProducerActivityCommit, PublishCommit, ReceiverRouteCommit,
            ReceiverRouteWork, UnpublishCommit,
        },
        outbound::MessageFanout,
        source_policy::SourcePolicyWakeups,
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
    output: RoomOutputPlan,
    source_policy: SourcePolicyWakeups,
}

pub fn build_join(
    room: &Room,
    counts: RoomGaugeDelta,
    outcome: JoinUserOutcome,
) -> (RoomEffects, CommittedTransportReceipt) {
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
    batch.source_policy.route_graph_changed();
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
        batch.source_policy.route_graph_changed();
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
) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch.observability.push_gauge(counts);
    batch.extend_media_topology_effects(outcome.media_effects);
    batch.source_policy.route_graph_changed();
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
    batch.push_receiver_work(
        room,
        RoomDiagnosticsContext::new(&commit.user, commit.connection, commit.worker),
        commit.receiver_route_work,
        ConsumerSetupOrigin::Publish,
    );
    batch.source_policy.route_graph_changed();
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
    batch.transport.push_producer(source, active, diagnostics);
    batch.output.push_track_binding(recipients, update);
    batch.source_policy.fanout_pressure_changed();
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
    batch.source_policy.route_graph_changed();
    batch
}

pub fn build_consumer_readiness(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    commit: ReceiverRouteCommit,
) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch
        .observability
        .push_gauge(RoomGaugeDelta::media(commit.before, commit.after));
    batch.push_receiver_work(
        room,
        RoomDiagnosticsContext::new(user_id, connection_id, commit.media_worker_id),
        commit.work,
        ConsumerSetupOrigin::Readiness,
    );
    batch.source_policy.route_graph_changed();
    batch
}

pub fn build_keyframe_refresh(targets: Vec<ConsumerRouteTarget>) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch.transport.push_keyframes(targets);
    batch
}

pub fn build_user_info_update(fanout: MessageFanout) -> RoomEffects {
    let mut batch = RoomEffects::default();
    batch.source_policy.receiver_intent_changed();
    batch.output.push_user_info(fanout);
    batch
}

pub fn build_receiver_intent(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    commit: ReceiverRouteCommit,
) -> RoomEffects {
    let route_graph_changed = commit.work.route_graph_changed();
    let mut batch = RoomEffects::default();
    batch
        .observability
        .push_gauge(RoomGaugeDelta::media(commit.before, commit.after));
    batch.push_receiver_work(
        room,
        RoomDiagnosticsContext::new(user_id, connection_id, commit.media_worker_id),
        commit.work,
        ConsumerSetupOrigin::Subscribe,
    );
    if route_graph_changed {
        batch.source_policy.route_graph_changed();
    } else {
        batch.source_policy.receiver_intent_changed();
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

    fn push_receiver_work(
        &mut self,
        room: &Room,
        context: RoomDiagnosticsContext<'_>,
        work: ReceiverRouteWork,
        origin: ConsumerSetupOrigin,
    ) {
        self.transport.push_receiver_work(work, origin, |activity| {
            let target = activity.target();
            context
                .event_data(room, telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED)
                .with_transport_media_id(target.consumer_media_id().as_u64())
                .insert_field("active", activity.active())
                .insert_field(
                    "producer_user_id",
                    serde_json::to_value(target.producer_user_id())
                        .unwrap_or(serde_json::Value::Null),
                )
                .insert_field(
                    "source_transport_media_id",
                    target.source_media_id().as_u64(),
                )
                .insert_field("stream_id", target.stream_id().to_string())
        });
    }

    /// preserves the room-wide side-effect order across transport and policy work
    pub async fn execute(self, room: &Room, context: RoomEffectContext<'_>) {
        let mut observability = self.observability;
        let mut output = self.output;
        let mut source_policy = self.source_policy;
        observability.record_gauges(room);
        let transport_outcome = self
            .transport
            .execute(room, context.route_transport())
            .await;
        observability.extend_gauges(transport_outcome.gauges);
        observability.extend_records(transport_outcome.diagnostics);
        source_policy.extend(transport_outcome.source_policy);
        observability.record_gauges(room);
        output.emit_before_policy();
        source_policy.execute(room, context.media_transport()).await;
        output.emit_after_policy();
        observability.record_diagnostics(room);
    }
}
