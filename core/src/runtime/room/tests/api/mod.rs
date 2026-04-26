use o_sfu_protocol::shared::UserId;

use super::super::{Room, RoomManager};

mod inspect;
mod lifecycle;
mod media;

pub use inspect::RoomTestInspect;
pub use lifecycle::RoomTestLifecycle;
#[cfg(test)]
pub use media::NegotiatedPublish;
pub use media::RoomTestMedia;

#[derive(Clone, Copy)]
pub struct RoomTestApi<'a> {
    room: &'a Room,
}

#[derive(Clone, Copy)]
pub struct RoomManagerTestApi<'a> {
    manager: &'a RoomManager,
}

impl Room {
    #[must_use]
    pub const fn test_api(&self) -> RoomTestApi<'_> {
        RoomTestApi { room: self }
    }
}

impl RoomManager {
    #[must_use]
    pub const fn test_api(&self) -> RoomManagerTestApi<'_> {
        RoomManagerTestApi { manager: self }
    }
}

impl<'a> RoomTestApi<'a> {
    #[must_use]
    pub const fn lifecycle(self) -> RoomTestLifecycle<'a> {
        RoomTestLifecycle { room: self.room }
    }

    #[must_use]
    pub const fn media(self) -> RoomTestMedia<'a> {
        RoomTestMedia { room: self.room }
    }

    #[must_use]
    pub const fn inspect(self) -> RoomTestInspect<'a> {
        RoomTestInspect { room: self.room }
    }
}

impl RoomManagerTestApi<'_> {
    pub async fn has_session(self, room_id: &str, user_id: &UserId) -> bool {
        let Some(room) = self.manager.get_by_uuid(room_id).await else {
            return false;
        };
        room.test_api().inspect().has_session(user_id).await
    }
}
