use tokio::sync::mpsc;

use super::{ChannelEventMessage, SessionOutbound};

pub(super) type OutboundSender = mpsc::UnboundedSender<SessionOutbound>;

#[derive(Debug, Clone)]
pub(super) struct MessageFanout {
    recipients: Vec<OutboundSender>,
    message: ChannelEventMessage,
}

impl MessageFanout {
    pub(super) fn emit(self) {
        for recipient in self.recipients {
            let _ = recipient.send(SessionOutbound::Message(self.message.clone()));
        }
    }
}

pub(super) fn fanout_all(
    recipients: impl IntoIterator<Item = OutboundSender>,
    message: &ChannelEventMessage,
) -> MessageFanout {
    MessageFanout {
        recipients: recipients.into_iter().collect(),
        message: message.clone(),
    }
}

pub(super) fn fanout_all_except(
    recipients: impl IntoIterator<Item = OutboundSender>,
    message: &ChannelEventMessage,
) -> MessageFanout {
    MessageFanout {
        recipients: recipients.into_iter().collect(),
        message: message.clone(),
    }
}
