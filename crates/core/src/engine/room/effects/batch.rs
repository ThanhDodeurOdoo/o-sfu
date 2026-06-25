use o_sfu_telemetry::schema::event as telemetry_event;

use super::{
    RoomGaugeDelta, observability::RoomObservabilityPlan, output::RoomOutputPlan,
    transport::RoomTransportPlan,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    diagnostics::DiagnosticsEventData,
    media_transport::MediaTransport,
    room::{
        Room,
        media_graph::{
            ConsumerRouteTarget, ConsumerSetupOrigin, MediaTopologyEffects, ProducerActivityCommit,
            PublishCommit, ReceiverRouteCommit, ReceiverRouteWork, UnpublishCommit,
        },
        outbound::MessageFanout,
        source_policy::SourcePolicyWakeups,
        state::{ConnectionCloseCommit, DisconnectCommit, JoinCommit},
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

pub(in crate::engine::room) enum RoomCommit {
    Join(JoinCommit),
    ConnectionClose(ConnectionCloseCommit),
    Disconnect(DisconnectCommit),
    UserInfo(MessageFanout),
    Publish(PublishCommit),
    PublicationActivity(ProducerActivityCommit),
    Unpublish(UnpublishCommit),
    ReceiverIntent(ReceiverRouteCommit),
    ConsumerReadiness(ReceiverRouteCommit),
}

impl RoomEffects {
    pub(in crate::engine::room) fn from_commit(room: &Room, commit: RoomCommit) -> Self {
        match commit {
            RoomCommit::Join(commit) => Self::from_join(room, commit),
            RoomCommit::ConnectionClose(commit) => Self::from_connection_close(room, commit),
            RoomCommit::Disconnect(commit) => Self::from_disconnect(room, commit),
            RoomCommit::UserInfo(commit) => {
                let mut batch = Self::default();
                batch.source_policy.receiver_intent_changed();
                batch.output.push_user_info(commit);
                batch
            }
            RoomCommit::Publish(commit) => Self::from_publish(room, commit),
            RoomCommit::PublicationActivity(commit) => {
                Self::from_publication_activity(room, commit)
            }
            RoomCommit::Unpublish(commit) => {
                let mut batch = Self::default();
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
            RoomCommit::ReceiverIntent(commit) => {
                let changed = commit.work.route_graph_changed();
                let mut batch =
                    Self::from_receiver_route(room, commit, ConsumerSetupOrigin::Subscribe);
                if changed {
                    batch.source_policy.route_graph_changed();
                } else {
                    batch.source_policy.receiver_intent_changed();
                }
                batch
            }
            RoomCommit::ConsumerReadiness(commit) => {
                let mut batch =
                    Self::from_receiver_route(room, commit, ConsumerSetupOrigin::Readiness);
                batch.source_policy.route_graph_changed();
                batch
            }
        }
    }

    pub(in crate::engine::room) fn keyframe_refresh(targets: Vec<ConsumerRouteTarget>) -> Self {
        let mut batch = Self::default();
        batch.transport.push_keyframes(targets);
        batch
    }

    fn from_join(room: &Room, commit: JoinCommit) -> Self {
        let JoinCommit {
            counts,
            effects,
            receipt,
            media_effects,
        } = commit;
        let user_id = receipt.transport_session_key.user_id().clone();
        let diagnostics = RoomDiagnosticsContext::new(
            &user_id,
            receipt.connection_id,
            receipt.transport_session_key.media_worker_id(),
        )
        .event_data(room, telemetry_event::USER_JOINED);
        let mut batch = Self::default();
        batch.observability.push_gauge(counts);
        batch.extend_media_topology_effects(media_effects);
        batch.source_policy.route_graph_changed();
        batch.output.push_lifecycle(effects);
        batch.observability.register_user(user_id);
        batch.observability.record(diagnostics);
        batch
    }

    fn from_connection_close(room: &Room, commit: ConnectionCloseCommit) -> Self {
        let mut batch = Self::default();
        match commit {
            ConnectionCloseCommit::Current {
                counts,
                user_id,
                connection_id,
                cleanup,
                effects,
                media_effects,
            } => {
                batch.observability.push_gauge(counts);
                let mut diagnostics = DiagnosticsEventData::for_user(
                    room.uuid(),
                    &user_id,
                    telemetry_event::USER_CLOSED,
                )
                .with_connection_id(connection_id.as_u64());
                if let Some(cleanup) = cleanup.as_ref() {
                    diagnostics = diagnostics
                        .with_media_worker_id(cleanup.session_key().media_worker_id().as_usize());
                }
                batch.extend_media_topology_effects(media_effects);
                batch.output.push_lifecycle(effects);
                batch.observability.record(diagnostics);
                batch.observability.forget_user(user_id);
                batch.source_policy.route_graph_changed();
                if let Some(cleanup) = cleanup {
                    batch.transport.push_cleanup(cleanup);
                }
            }
            ConnectionCloseCommit::StalePlacement { counts, cleanup } => {
                batch.observability.push_gauge(counts);
                batch.transport.push_cleanup(cleanup);
            }
        }
        batch
    }

    fn from_disconnect(room: &Room, commit: DisconnectCommit) -> Self {
        let mut batch = Self::default();
        batch.observability.push_gauge(commit.counts);
        batch.extend_media_topology_effects(commit.media_effects);
        batch.source_policy.route_graph_changed();
        batch.output.push_lifecycle(commit.effects);
        for close_operation in commit.close_operations {
            let session = close_operation.session_key();
            batch.observability.record(
                RoomDiagnosticsContext::new(
                    session.user_id(),
                    session.connection_id(),
                    session.media_worker_id(),
                )
                .event_data(room, telemetry_event::USER_DISCONNECTED),
            );
            batch.observability.forget_user(session.user_id().clone());
            batch.transport.push_cleanup(close_operation);
        }
        batch
    }

    fn from_publish(room: &Room, commit: PublishCommit) -> Self {
        let context = RoomDiagnosticsContext::new(&commit.user, commit.connection, commit.worker);
        let diagnostics = context
            .event_data(room, telemetry_event::PUBLISH_COMMITTED)
            .with_transport_media_id(commit.media.as_u64());
        let mut batch = Self::default();
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
            context,
            commit.receiver_route_work,
            ConsumerSetupOrigin::Publish,
        );
        batch.source_policy.route_graph_changed();
        batch.observability.record(diagnostics);
        batch
    }

    fn from_publication_activity(room: &Room, commit: ProducerActivityCommit) -> Self {
        let ProducerActivityCommit {
            source,
            active,
            recipients,
            update,
        } = commit;
        let session = source.session_key();
        let diagnostics = RoomDiagnosticsContext::new(
            &update.user_id,
            session.connection_id(),
            session.media_worker_id(),
        )
        .event_data(room, telemetry_event::PUBLICATION_ACTIVITY_CHANGED)
        .with_transport_media_id(source.transport_media_id().as_u64())
        .insert_field("active", active)
        .insert_field("stream_id", update.stream_id.to_string());
        let mut batch = Self::default();
        batch.transport.push_producer(source, active, diagnostics);
        batch.output.push_track_binding(recipients, update);
        batch.source_policy.fanout_pressure_changed();
        batch
    }

    fn from_receiver_route(
        room: &Room,
        commit: ReceiverRouteCommit,
        origin: ConsumerSetupOrigin,
    ) -> Self {
        let mut batch = Self::default();
        batch.observability.push_gauge(commit.counts);
        batch.push_receiver_work(
            room,
            RoomDiagnosticsContext::new(
                &commit.receiver_user_id,
                commit.receiver_connection_id,
                commit.media_worker_id,
            ),
            commit.work,
            origin,
        );
        batch
    }

    fn extend_media_topology_effects(&mut self, effects: MediaTopologyEffects) {
        self.transport.extend_topology(effects);
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
