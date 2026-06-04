//! user-scoped room operation handle plus domain-specific method groups

mod membership;

use super::Room;
use crate::engine::{
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
    pub(in crate::engine::room) const fn new(
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

    pub(in crate::engine::room) const fn room(self) -> &'a Room {
        self.room
    }

    pub(in crate::engine::room) const fn user_id(self) -> &'a UserId {
        self.user_id
    }

    pub(in crate::engine::room) const fn connection_id(self) -> ConnectionId {
        self.connection_id
    }

    pub(in crate::engine::room) const fn media_transport(self) -> &'a MediaTransport {
        self.media_transport
    }

    pub(in crate::engine::room) async fn transport_user_key(self) -> TransportSessionKey {
        self.room
            .transport_user_key(self.user_id, self.connection_id)
            .await
    }
}
