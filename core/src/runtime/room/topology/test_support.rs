#[cfg(test)]
use std::sync::Arc;

use o_sfu_router::RouterId;

#[cfg(test)]
use super::RoomRouterObserverFactory;
use super::RoomTopology;
use crate::runtime::UserId;
#[cfg(test)]
use crate::{
    MediaCodecFlags, RoomShardingPolicy,
    runtime::{
        RoomInstanceId,
        metrics::RuntimeMetrics,
        recording::{MediaSource, MediaTap, RecordingService},
        room::{
            LocalRoomRouterPlacements, LocalRouterRuntimeContext,
            rtp_capabilities::router_rtp_capabilities,
        },
    },
};

impl RoomTopology {
    #[cfg(test)]
    pub(in crate::runtime::room) fn new(primary_router_id: RouterId) -> Self {
        Self::new_with_policy(
            primary_router_id,
            RoomShardingPolicy::strict_single_router(),
            1,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn new_with_policy(
        primary_router_id: RouterId,
        room_sharding_policy: RoomShardingPolicy,
        local_router_count: usize,
    ) -> Self {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        let primary = LocalRouterRuntimeContext {
            router: primary_router_id,
            media_worker: 0,
        };
        let spillover = (1..local_router_count.max(1))
            .map(|offset| LocalRouterRuntimeContext {
                router: RouterId(
                    primary_router_id
                        .0
                        .saturating_add(u64::try_from(offset).unwrap_or(u64::MAX)),
                ),
                media_worker: offset,
            })
            .collect::<Vec<_>>();
        Self::new_with_recording_observer_factory(
            LocalRoomRouterPlacements::new(primary, spillover),
            room_sharding_policy,
            router_rtp_capabilities(MediaCodecFlags::default()),
            &RoomRouterObserverFactory::new(Arc::new(RecordingService::new(
                RoomInstanceId::from_raw(0),
                media_source,
                Arc::new(RuntimeMetrics::default()),
            ))),
        )
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn new_with_bounded_spillover(
        primary_router_id: RouterId,
        local_router_count: usize,
    ) -> Self {
        Self::new_with_policy(
            primary_router_id,
            RoomShardingPolicy::bounded_local_spillover(local_router_count),
            local_router_count,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn new_with_load_spillover(
        primary_router_id: RouterId,
        local_router_count: usize,
        policy: crate::LocalSpilloverPolicy,
    ) -> Self {
        Self::new_with_policy(
            primary_router_id,
            RoomShardingPolicy::load_triggered_local_spillover(local_router_count, policy),
            local_router_count,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn user_count(&self) -> u64 {
        u64::try_from(self.session_home_router.len()).unwrap_or(u64::MAX)
    }

    pub(in crate::runtime::room) fn router_count(&self) -> usize {
        self.routers.len()
    }

    pub(in crate::runtime::room) fn home_router_id_for_user(
        &self,
        user_id: &UserId,
    ) -> Option<RouterId> {
        self.session_home_router.get(user_id).copied()
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn active_load_router_count_for_test(&self) -> usize {
        self.placement_policy.active_router_count_for_test()
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn last_load_pressure_reason_for_test(
        &self,
    ) -> Option<super::LoadPressureReason> {
        self.placement_policy.last_load_pressure_reason_for_test()
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn remove_router_for_test(&mut self, router_id: RouterId) {
        self.routers.remove(&router_id);
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn remove_session_mapping_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.session_home_router.get(user_id).copied() else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_session_mapping_for_test(user_id);
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn remove_transport_mapping_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.session_home_router.get(user_id).copied() else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_transport_mapping_for_test(user_id);
    }
}
