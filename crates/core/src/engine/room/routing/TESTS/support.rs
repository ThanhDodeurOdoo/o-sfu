#[cfg(test)]
use o_sfu_router::RouterId;

use super::RoomRoutingState;
#[cfg(test)]
use crate::engine::UserId;
#[cfg(test)]
use crate::{
    MediaCodecFlags,
    engine::{RoomInstanceId, room::rtp_capabilities::router_rtp_capabilities},
};

impl RoomRoutingState {
    #[cfg(test)]
    pub fn new(primary_router_id: RouterId) -> Self {
        Self::new_with_runtime(
            RoomInstanceId::from_raw(0),
            primary_router_id,
            None,
            router_rtp_capabilities(MediaCodecFlags::default()),
        )
    }

    #[cfg(test)]
    pub fn user_count(&self) -> u64 {
        u64::try_from(self.sessions.active_connection_by_user.len()).unwrap_or(u64::MAX)
    }

    pub fn router_count(&self) -> usize {
        self.routers.len()
    }

    #[cfg(test)]
    pub fn mapped_session_count_for_router(&self, router_id: RouterId) -> Option<usize> {
        self.routers
            .get(&router_id)
            .map(super::RoomRouterState::mapped_session_count_for_test)
    }

    #[cfg(test)]
    pub fn home_router_id_for_user(&self, user_id: &UserId) -> Option<RouterId> {
        self.sessions
            .active(user_id)
            .map(|session| session.runtime.router)
    }

    #[cfg(test)]
    pub fn remove_router_for_test(&mut self, router_id: RouterId) {
        self.routers.remove(&router_id);
    }

    #[cfg(test)]
    pub fn remove_session_mapping_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.home_router_id_for_user(user_id) else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_session_mapping_for_test(user_id);
    }

    #[cfg(test)]
    pub fn remove_transport_mapping_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.home_router_id_for_user(user_id) else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_transport_mapping_for_test(user_id);
    }
}
