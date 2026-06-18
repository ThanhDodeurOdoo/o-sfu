use crate::engine::{
    media_transport::MediaTransport,
    room::{
        Room,
        source_policy::{SourcePolicyTrigger, SourcePolicyTurn},
    },
};

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct RoomPolicyPlan {
    trigger: Option<SourcePolicyTrigger>,
}

impl RoomPolicyPlan {
    pub(super) fn route_graph_changed(&mut self) {
        self.push(SourcePolicyTrigger::RouteGraph);
    }

    pub(super) fn receiver_intent_changed(&mut self) {
        self.push(SourcePolicyTrigger::PacketSelection);
    }

    pub(super) fn fanout_pressure_changed(&mut self) {
        self.push(SourcePolicyTrigger::FanoutPressure);
    }

    pub(super) fn extend(&mut self, plan: Self) {
        if let Some(trigger) = plan.trigger {
            self.push(trigger);
        }
    }

    pub(super) async fn execute(self, room: &Room, media_transport: Option<&MediaTransport>) {
        if let Some(trigger) = self.trigger {
            SourcePolicyTurn::new(room, trigger, media_transport)
                .run()
                .await;
        }
    }

    fn push(&mut self, trigger: SourcePolicyTrigger) {
        self.trigger = Some(
            self.trigger
                .map_or(trigger, |current| current.merge(trigger)),
        );
    }
}
