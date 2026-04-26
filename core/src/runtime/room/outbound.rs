use tokio::sync::mpsc;

use super::{RoomEventMessage, UserOutbound};

pub(super) type OutboundSender = mpsc::UnboundedSender<UserOutbound>;

#[derive(Debug, Clone)]
pub(super) struct MessageFanout {
    recipients: Vec<OutboundSender>,
    message: RoomEventMessage,
}

impl MessageFanout {
    pub(super) fn emit(self) {
        for recipient in self.recipients {
            let _ = recipient.send(UserOutbound::Message(self.message.clone()));
        }
    }
}

pub(super) fn fanout_all(
    recipients: impl IntoIterator<Item = OutboundSender>,
    message: &RoomEventMessage,
) -> MessageFanout {
    MessageFanout {
        recipients: recipients.into_iter().collect(),
        message: message.clone(),
    }
}

pub(super) fn fanout_all_except(
    recipients: impl IntoIterator<Item = OutboundSender>,
    message: &RoomEventMessage,
) -> MessageFanout {
    MessageFanout {
        recipients: recipients.into_iter().collect(),
        message: message.clone(),
    }
}
