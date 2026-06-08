use super::Room;
use crate::engine::{ConnectionId, UserId, media_transport::MediaTransport};

#[derive(Clone, Copy)]
pub(crate) struct RoomUserOperation<'a> {
    pub room: &'a Room,
    pub user_id: &'a UserId,
    pub connection_id: ConnectionId,
    pub media_transport: &'a MediaTransport,
}

impl<'a> RoomUserOperation<'a> {
    pub const fn new(
        room: &'a Room,
        user_id: &'a UserId,
        connection_id: ConnectionId,
        media_transport: &'a MediaTransport,
    ) -> Self {
        Self {
            room,
            user_id,
            connection_id,
            media_transport,
        }
    }
}
