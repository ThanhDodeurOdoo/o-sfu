use crate::engine::{
    media_transport::MediaTransport,
    room::{Room, SourcePolicyEvent},
};

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct RoomPolicyPlan {
    event: Option<SourcePolicyEvent>,
}

impl RoomPolicyPlan {
    pub(super) fn route_graph_changed(&mut self) {
        self.push(SourcePolicyEvent::RouteGraphChanged);
    }

    pub(super) fn receiver_intent_changed(&mut self) {
        self.push(SourcePolicyEvent::ReceiverIntentChanged);
    }

    pub(super) fn fanout_pressure_changed(&mut self) {
        self.push(SourcePolicyEvent::FanoutPressureChanged);
    }

    pub(super) fn extend(&mut self, plan: Self) {
        if let Some(event) = plan.event {
            self.push(event);
        }
    }

    pub(super) async fn execute(self, room: &Room, media_transport: Option<&MediaTransport>) {
        if let Some(event) = self.event {
            room.handle_source_policy_event(event, media_transport)
                .await;
        }
    }

    fn push(&mut self, event: SourcePolicyEvent) {
        self.event = Some(
            self.event
                .map_or(event, |current| merge_events(current, event)),
        );
    }
}

fn merge_events(current: SourcePolicyEvent, next: SourcePolicyEvent) -> SourcePolicyEvent {
    use SourcePolicyEvent::{FanoutPressureChanged, ReceiverIntentChanged, RouteGraphChanged};

    match (current, next) {
        (RouteGraphChanged, _)
        | (_, RouteGraphChanged)
        | (ReceiverIntentChanged, FanoutPressureChanged)
        | (FanoutPressureChanged, ReceiverIntentChanged) => RouteGraphChanged,
        (ReceiverIntentChanged, ReceiverIntentChanged) => ReceiverIntentChanged,
        (FanoutPressureChanged, FanoutPressureChanged) => FanoutPressureChanged,
    }
}
