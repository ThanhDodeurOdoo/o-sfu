use super::Room;
use crate::runtime::{
    ConnectionId, UserId,
    media_transport::{MediaTransport, TransportSessionKey},
};

#[derive(Clone, Copy)]
pub(crate) struct RoomUserOperation<'a> {
    room: &'a Room,
    user_id: &'a UserId,
    connection_id: ConnectionId,
    media_transport: &'a MediaTransport,
}

impl<'a> RoomUserOperation<'a> {
    pub(in crate::runtime::room) const fn new(
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

    pub(in crate::runtime::room) const fn room(self) -> &'a Room {
        self.room
    }

    pub(in crate::runtime::room) const fn user_id(self) -> &'a UserId {
        self.user_id
    }

    pub(in crate::runtime::room) const fn connection_id(self) -> ConnectionId {
        self.connection_id
    }

    pub(in crate::runtime::room) const fn media_transport(self) -> &'a MediaTransport {
        self.media_transport
    }

    pub(in crate::runtime::room) fn transport_user_key(self) -> TransportSessionKey {
        self.room
            .transport_user_key(self.user_id, self.connection_id)
    }
}
