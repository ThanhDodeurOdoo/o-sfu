//! Room media queries above pure state.
//!
//! Public callers should normally enter through [`crate::prelude::MediaSession`]. This
//! module keeps room-internal media facts that do not belong to publication or
//! subscription transition choreography.
//!
//! The room does not translate product stream labels here. A caller must already
//! have a [`UserStreamId`]. That keeps stream policy at the application edge
//! while this layer focuses on source authority.

use super::{Room, RoomUserOperation};
use crate::engine::{UserId, source_model::UserStreamId};

impl Room {
    pub async fn is_stream_published(&self, user_id: &UserId, stream_id: &UserStreamId) -> bool {
        self.state
            .read()
            .await
            .producer_route_target_for_user(user_id, stream_id)
            .is_some()
    }
}

impl RoomUserOperation<'_> {
    pub(crate) async fn is_stream_published(self, stream_id: &UserStreamId) -> bool {
        self.room()
            .is_stream_published(self.user_id(), stream_id)
            .await
    }
}
