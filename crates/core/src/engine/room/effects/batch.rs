use super::{
    RoomGaugeDelta, observability::record_gauges, output::RoomOutputPlan,
    transport::RoomTransportPlan,
};
use crate::engine::{
    media_transport::MediaTransport,
    room::{
        Room,
        media_graph::{
            ConsumerSetupOrigin, ProducerActivityCommit, PublishCommit, ReceiverRouteCommit,
        },
        source_policy::SourcePolicyTurn,
        state::{
            ConnectionCloseCommit, DisconnectCommit, JoinCommit, PresenceCommit, UserJoinedFanout,
        },
    },
};

#[derive(Debug, Clone, Copy)]
pub struct RoomEffectContext<'a> {
    media_transport: Option<&'a MediaTransport>,
    route_effects: bool,
    joined_fanout: UserJoinedFanout,
}

impl<'a> RoomEffectContext<'a> {
    pub const fn runtime(media_transport: &'a MediaTransport) -> Self {
        Self {
            media_transport: Some(media_transport),
            route_effects: true,
            joined_fanout: UserJoinedFanout::Emit,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub const fn state_only(media_transport: Option<&'a MediaTransport>) -> Self {
        Self {
            media_transport,
            route_effects: false,
            joined_fanout: UserJoinedFanout::Suppress,
        }
    }

    pub(in crate::engine::room) const fn user_joined_fanout(self) -> UserJoinedFanout {
        self.joined_fanout
    }

    fn media_transport(self) -> Option<&'a MediaTransport> {
        self.media_transport
    }

    fn route_transport(self) -> Option<&'a MediaTransport> {
        self.route_effects.then_some(self.media_transport).flatten()
    }
}

#[derive(Debug, Default)]
#[must_use = "room effect batches must be executed after the state transition commits"]
pub struct RoomEffects {
    gauges: Vec<RoomGaugeDelta>,
    policy_before_transport: bool,
    transport: RoomTransportPlan,
    output: RoomOutputPlan,
    source_policy: SourcePolicyTurn,
}

impl RoomEffects {
    pub(in crate::engine::room) fn from_join(commit: JoinCommit) -> Self {
        let JoinCommit {
            counts,
            effects,
            transport_plan,
            ..
        } = commit;
        let mut batch = Self::default();
        batch.gauges.push(counts);
        batch.transport.extend(transport_plan);
        batch.source_policy.request();
        batch.output.push_lifecycle(effects);
        batch
    }

    pub(in crate::engine::room) fn from_connection_close(commit: ConnectionCloseCommit) -> Self {
        let mut batch = Self::default();
        match commit {
            ConnectionCloseCommit::Current {
                counts,
                session_teardown,
                effects,
                transport_plan,
                ..
            } => {
                batch.gauges.push(counts);
                batch.transport.extend(transport_plan);
                batch.output.push_lifecycle(effects);
                batch.source_policy.request();
                batch.transport.extend_teardown(session_teardown);
            }
            ConnectionCloseCommit::StalePlacement {
                counts,
                session_teardown,
            } => {
                batch.gauges.push(counts);
                batch.transport.extend_teardown([session_teardown]);
            }
        }
        batch
    }

    pub(in crate::engine::room) fn from_disconnect(commit: DisconnectCommit) -> Self {
        let mut batch = Self::default();
        batch.gauges.push(commit.counts);
        batch.transport.extend(commit.transport_plan);
        batch.source_policy.request();
        batch.output.push_lifecycle(commit.effects);
        batch.transport.extend_teardown(commit.session_teardowns);
        batch
    }

    pub(in crate::engine::room) fn from_presence(commit: PresenceCommit) -> Self {
        let mut batch = Self::default();
        batch.output.push_user_info(commit.fanout);
        batch.source_policy.request();
        batch
    }

    pub(in crate::engine::room) fn from_publish(commit: PublishCommit) -> Self {
        let mut batch = Self::default();
        batch.gauges.push(RoomGaugeDelta::media(
            commit.publish_before,
            commit.publish_after,
        ));
        batch.gauges.push(RoomGaugeDelta::media(
            commit.setup_before,
            commit.setup_after,
        ));
        batch
            .transport
            .push_receiver_work(commit.receiver_route_work, ConsumerSetupOrigin::Publish);
        batch.push_presence_before_policy(commit.presence);
        batch.source_policy.request();
        batch
    }

    pub(in crate::engine::room) fn from_publication_activity(
        commit: ProducerActivityCommit,
    ) -> Self {
        let ProducerActivityCommit {
            source,
            stream_id,
            update,
            remote_activity_effects,
            source_snapshots,
            presence,
        } = commit;
        let mut batch = Self {
            policy_before_transport: update.activity().is_active(),
            ..Self::default()
        };
        batch
            .transport
            .extend_remote_source_activity(remote_activity_effects);
        batch.transport.push_producer(source, stream_id, update);
        batch.output.push_source_snapshots(source_snapshots);
        batch.push_presence_before_policy(presence);
        batch.source_policy.request();
        batch
    }

    pub(in crate::engine::room) fn from_receiver_intent(commit: ReceiverRouteCommit) -> Self {
        let mut batch = Self::from_receiver_route(commit, ConsumerSetupOrigin::Subscribe);
        batch.source_policy.request();
        batch
    }

    pub(in crate::engine::room) fn from_consumer_readiness(commit: ReceiverRouteCommit) -> Self {
        let mut batch = Self::from_receiver_route(commit, ConsumerSetupOrigin::Readiness);
        batch.source_policy.request();
        batch
    }

    fn push_presence_before_policy(&mut self, presence: Option<PresenceCommit>) {
        if let Some(presence) = presence {
            self.output.push_user_info_before_policy(presence.fanout);
            self.source_policy.request();
        }
    }

    fn from_receiver_route(commit: ReceiverRouteCommit, origin: ConsumerSetupOrigin) -> Self {
        let mut batch = Self::default();
        batch.gauges.push(commit.counts);
        batch.transport.push_receiver_work(commit.work, origin);
        batch
    }

    /// preserves the room-wide side-effect order across transport and policy work
    pub async fn execute(self, room: &Room, context: RoomEffectContext<'_>) {
        if self.policy_before_transport {
            let _guard = room.source_policy_turn.lock().await;
            self.execute_inner(room, context, true).await;
        } else {
            self.execute_inner(room, context, false).await;
        }
    }

    pub(in crate::engine::room) async fn execute_with_source_policy_guard(
        self,
        room: &Room,
        context: RoomEffectContext<'_>,
    ) {
        self.execute_inner(room, context, true).await;
    }

    async fn execute_inner(
        self,
        room: &Room,
        context: RoomEffectContext<'_>,
        source_policy_guarded: bool,
    ) {
        let mut gauges = self.gauges;
        let mut output = self.output;
        let mut source_policy = self.source_policy;
        record_gauges(&mut gauges, room);
        if self.policy_before_transport {
            output.emit_user_info_before_policy();
            source_policy
                .execute_guarded(room, context.media_transport(), None)
                .await;
            source_policy = SourcePolicyTurn::default();
        }
        let transport_outcome = self
            .transport
            .execute(room, context.route_transport())
            .await;
        gauges.extend(transport_outcome.gauges);
        source_policy.extend(&transport_outcome.source_policy);
        record_gauges(&mut gauges, room);
        output.emit_before_policy();
        if source_policy_guarded {
            source_policy
                .execute_guarded(room, context.media_transport(), None)
                .await;
        } else {
            source_policy
                .execute(room, context.media_transport(), None)
                .await;
        }
        output.emit_after_policy();
    }
}
