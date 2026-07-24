use crate::engine::room::{
    UserOutbound,
    outbound::{MessageFanout, OutboundSender, RemoteSourceSnapshot},
    state::LifecycleEffects,
};

#[derive(Debug, Default)]
pub(super) struct RoomOutputPlan {
    source_snapshots: Vec<(OutboundSender, RemoteSourceSnapshot)>,
    user_info_before_policy: Vec<MessageFanout>,
    user_info: Vec<MessageFanout>,
    lifecycle: Vec<LifecycleEffects>,
}

impl RoomOutputPlan {
    pub(super) fn push_source_snapshots(
        &mut self,
        snapshots: Vec<(OutboundSender, RemoteSourceSnapshot)>,
    ) {
        self.source_snapshots.extend(snapshots);
    }

    pub(super) fn push_user_info(&mut self, fanout: MessageFanout) {
        self.user_info.push(fanout);
    }

    pub(super) fn push_user_info_before_policy(&mut self, fanout: MessageFanout) {
        self.user_info_before_policy.push(fanout);
    }

    pub(super) fn push_lifecycle(&mut self, effects: LifecycleEffects) {
        self.lifecycle.push(effects);
    }

    pub(super) fn emit_before_policy(&mut self) {
        for (recipient, snapshot) in self.source_snapshots.drain(..) {
            let _ = recipient.send(UserOutbound::RemoteSources(snapshot));
        }
        self.emit_user_info_before_policy();
    }

    pub(super) fn emit_user_info_before_policy(&mut self) {
        for fanout in self.user_info_before_policy.drain(..) {
            fanout.emit();
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
            for (recipient, snapshot) in effects.source_snapshots {
                let _ = recipient.send(UserOutbound::RemoteSources(snapshot));
            }
            for fanout in effects.fanouts {
                fanout.emit();
            }
        }
    }
}
