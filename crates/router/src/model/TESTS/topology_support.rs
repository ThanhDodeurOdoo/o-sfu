use o_sfu_model::UserId;

use super::RoutingTopology;
use crate::model::{MediaCapabilities, RouterId};

impl RoutingTopology {
    #[must_use]
    pub fn new_for_test(primary_router_id: RouterId) -> Self {
        Self::new(
            primary_router_id,
            None,
            MediaCapabilities::new(Vec::new(), Vec::new()),
        )
    }

    #[must_use]
    pub fn user_count(&self) -> usize {
        self.sessions.active_connection_by_user.len()
    }

    #[must_use]
    pub fn router_count(&self) -> usize {
        self.routers.len()
    }

    #[must_use]
    pub fn mapped_session_count_for_router(&self, router_id: RouterId) -> Option<usize> {
        self.routers
            .get(&router_id)
            .map(super::RouterAdapterState::mapped_session_count_for_test)
    }

    #[must_use]
    pub fn home_router_id_for_user(&self, user_id: &UserId) -> Option<RouterId> {
        self.sessions
            .active(user_id)
            .map(|session| session.runtime.router)
    }

    pub fn remove_router_for_test(&mut self, router_id: RouterId) {
        self.routers.remove(&router_id);
    }

    pub fn remove_user_mappings_for_test(&mut self, user_id: &UserId) {
        let Some(router_id) = self.home_router_id_for_user(user_id) else {
            return;
        };
        let Some(router) = self.routers.get_mut(&router_id) else {
            return;
        };
        router.remove_user_mappings_for_test(user_id);
    }
}

#[cfg(kani)]
pub mod proof {
    use o_sfu_model::UserId;

    use super::super::{
        CommittedSessionPlacement, CommittedSessionPlacements, RoutedConsumerId, RouterPlacement,
        shadow::{ShadowSessionKey, ShadowSessionTracker},
    };
    use crate::model::{ConnectionId, ConsumerId, MediaWorkerId, RouterId};

    pub fn assert_routing_shadow_refcounts_prune_after_last_consumer() {
        let receiver_user_id = UserId::Integer(20);
        let source_router_id = RouterId(9);
        let shadow = ShadowSessionKey::new(source_router_id, receiver_user_id);
        let first_consumer = RoutedConsumerId::new(source_router_id, ConsumerId(1));
        let second_consumer = RoutedConsumerId::new(source_router_id, ConsumerId(2));
        let same_router_consumer = RoutedConsumerId::new(RouterId(10), ConsumerId(3));
        let mut tracker = ShadowSessionTracker::default();

        tracker.register_consumer(first_consumer, Some(shadow.clone()));
        tracker.register_consumer(second_consumer, Some(shadow.clone()));
        tracker.register_consumer(same_router_consumer, None);

        let (first_removed, second_removed) = if kani::any() {
            (first_consumer, second_consumer)
        } else {
            (second_consumer, first_consumer)
        };

        let first_prune = tracker.unregister_consumers([first_removed]);
        assert!(first_prune.is_empty());
        assert!(tracker.contains_shadow_session(&shadow));

        let second_prune = tracker.unregister_consumers([second_removed]);
        assert!(second_prune.len() == 1);
        assert!(second_prune.contains(&shadow));
        assert!(!tracker.contains_shadow_session(&shadow));

        let stale_prune = tracker.unregister_consumers([second_removed, same_router_consumer]);
        assert!(stale_prune.is_empty());
    }

    pub fn assert_routing_placement_replacement_retires_stale_connection() {
        let user_id = UserId::Integer(10);
        let first_connection = ConnectionId::from_raw(1);
        let second_connection = ConnectionId::from_raw(2);
        let mut placements = CommittedSessionPlacements::default();

        placements.insert(user_id.clone(), session(first_connection, 9, 0));
        assert!(placements.active(&user_id).is_some_and(|session| {
            session.connection_id == first_connection
                && session.runtime.media_worker == MediaWorkerId::from_raw(0)
        }));
        assert!(placements.by_connection.contains_key(&first_connection));

        placements.insert(user_id.clone(), session(second_connection, 10, 1));
        assert!(placements.active(&user_id).is_some_and(|session| {
            session.connection_id == second_connection
                && session.runtime.router == RouterId(10)
                && session.runtime.media_worker == MediaWorkerId::from_raw(1)
        }));
        assert!(!placements.by_connection.contains_key(&first_connection));
        assert!(placements.by_connection.contains_key(&second_connection));

        let removed = placements.remove(&user_id);
        assert!(removed.is_some_and(|session| session.connection_id == second_connection));
        assert!(placements.active(&user_id).is_none());
        assert!(!placements.by_connection.contains_key(&second_connection));
    }

    fn session(
        connection_id: ConnectionId,
        router: u64,
        media_worker: usize,
    ) -> CommittedSessionPlacement {
        CommittedSessionPlacement {
            connection_id,
            router_session_seed: connection_id.as_u64(),
            runtime: placement(router, media_worker),
        }
    }

    fn placement(router: u64, media_worker: usize) -> RouterPlacement {
        RouterPlacement {
            router: RouterId(router),
            media_worker: MediaWorkerId::from_raw(media_worker),
        }
    }
}
