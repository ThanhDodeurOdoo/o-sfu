use super::Room;
#[cfg(any(test, feature = "testing-transport"))]
use super::media_graph::ConsumerRouteState;
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::{UserId, source_model::UserStreamId};

impl Room {
    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn is_stream_published(&self, user_id: &UserId, stream_id: &UserStreamId) -> bool {
        self.state
            .read()
            .await
            .producer_route_target_for_user(user_id, stream_id)
            .is_some()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn consumer_route_state(
        &self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<ConsumerRouteState> {
        self.state
            .read()
            .await
            .consumer_route_state(consumer_user_id, producer_user_id, stream_id)
    }
}
