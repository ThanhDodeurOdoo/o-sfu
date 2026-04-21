use o_sfu_protocol::shared::SessionId;

use super::super::{Channel, ChannelManager};

mod inspect;
mod lifecycle;
mod media;

pub(crate) use inspect::ChannelTestInspect;
pub(crate) use lifecycle::ChannelTestLifecycle;
pub(crate) use media::{ChannelTestMedia, NegotiatedPublish};

#[derive(Clone, Copy)]
pub(crate) struct ChannelTestApi<'a> {
    channel: &'a Channel,
}

#[derive(Clone, Copy)]
pub(crate) struct ChannelManagerTestApi<'a> {
    manager: &'a ChannelManager,
}

impl Channel {
    #[must_use]
    pub(crate) const fn test_api(&self) -> ChannelTestApi<'_> {
        ChannelTestApi { channel: self }
    }
}

impl ChannelManager {
    #[must_use]
    pub(crate) const fn test_api(&self) -> ChannelManagerTestApi<'_> {
        ChannelManagerTestApi { manager: self }
    }
}

impl<'a> ChannelTestApi<'a> {
    #[must_use]
    pub(crate) const fn lifecycle(self) -> ChannelTestLifecycle<'a> {
        ChannelTestLifecycle {
            channel: self.channel,
        }
    }

    #[must_use]
    pub(crate) const fn media(self) -> ChannelTestMedia<'a> {
        ChannelTestMedia {
            channel: self.channel,
        }
    }

    #[must_use]
    pub(crate) const fn inspect(self) -> ChannelTestInspect<'a> {
        ChannelTestInspect {
            channel: self.channel,
        }
    }
}

impl ChannelManagerTestApi<'_> {
    pub(crate) async fn has_session(self, channel_uuid: &str, session_id: &SessionId) -> bool {
        let Some(channel) = self.manager.get_by_uuid(channel_uuid).await else {
            return false;
        };
        channel.test_api().inspect().has_session(session_id).await
    }
}
