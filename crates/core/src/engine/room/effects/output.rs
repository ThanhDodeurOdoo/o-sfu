use crate::engine::room::{
    TrackBindingUpdate, UserOutbound,
    outbound::{MessageFanout, OutboundSender},
    state::LifecycleEffects,
};

#[derive(Debug, Default)]
pub(super) struct RoomOutputPlan {
    track_bindings: Vec<(Vec<OutboundSender>, TrackBindingUpdate)>,
    user_info: Vec<MessageFanout>,
    lifecycle: Vec<LifecycleEffects>,
}

impl RoomOutputPlan {
    pub(super) fn push_track_binding(
        &mut self,
        recipients: Vec<OutboundSender>,
        update: TrackBindingUpdate,
    ) {
        self.track_bindings.push((recipients, update));
    }

    pub(super) fn push_user_info(&mut self, fanout: MessageFanout) {
        self.user_info.push(fanout);
    }

    pub(super) fn push_lifecycle(&mut self, effects: LifecycleEffects) {
        self.lifecycle.push(effects);
    }

    pub(super) fn emit_before_policy(&mut self) {
        for (recipients, update) in self.track_bindings.drain(..) {
            for recipient in recipients {
                let _ = recipient.send(UserOutbound::TrackBindingUpdate(update.clone()));
            }
        }
    }

    pub(super) fn emit_after_policy(self) {
        for fanout in self.user_info {
            fanout.emit();
        }
        for effects in self.lifecycle {
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
}
