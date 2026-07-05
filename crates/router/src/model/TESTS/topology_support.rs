#[cfg(test)]
use o_sfu_model::UserId;

#[cfg(any(test, feature = "test-support"))]
use super::RoutingTopology;
#[cfg(test)]
use crate::model::{MediaCapabilities, RouterId};

#[cfg(test)]
impl RoutingTopology {
    pub(crate) fn new_for_test(primary_router_id: RouterId) -> Self {
        Self::new(
            primary_router_id,
            None,
            MediaCapabilities::new(Vec::new(), Vec::new()),
        )
    }

    pub(crate) fn user_count(&self) -> usize {
        self.sessions.active_connection_by_user.len()
    }

    pub(crate) fn mapped_session_count_for_router(&self, router_id: RouterId) -> Option<usize> {
        self.routers
            .get(&router_id)
            .map(super::RouterAdapterState::mapped_session_count)
    }

    pub(crate) fn home_router_id_for_user(&self, user_id: &UserId) -> Option<RouterId> {
        self.sessions
            .active(user_id)
            .map(|session| session.placement.router)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl RoutingTopology {
    #[must_use]
    pub fn router_count(&self) -> usize {
        self.routers.len()
    }
}

#[cfg(kani)]
pub mod proof {
    use o_sfu_model::UserId;

    use super::super::{
        CommittedSessionPlacement, CommittedSessionPlacements, RoutedConsumerId, RoutedProducerId,
        RouterPlacement,
        shadow::{ShadowSessionKey, ShadowSessionTracker},
    };
    use crate::model::{ConnectionId, ConsumerId, MediaWorkerId, ProducerId, RouterId};

    pub fn assert_routing_shadow_tracker_prunes_by_producer() {
        let source_user_id = UserId::Integer(10);
        let receiver_user_id = UserId::Integer(20);
        let source_router_id = RouterId(9);
        let shadow = ShadowSessionKey::new(source_router_id, receiver_user_id.clone());
        let producer = RoutedProducerId::for_test(source_router_id, ProducerId(1));
        let consumer = RoutedConsumerId::for_test(source_router_id, ConsumerId(1));
        let mut tracker = ShadowSessionTracker::default();

        tracker.register_producer(producer, source_user_id);
        tracker.register_consumer(consumer, producer, Some(shadow.clone()));

        let prune = tracker.unregister_producer(producer);
        assert!(prune.len() == 1);
        assert!(prune.contains(&shadow));
        assert!(!tracker.contains_shadow_session(&shadow));
        assert!(tracker.unregister_producer(producer).is_empty());
    }

    pub fn assert_routing_shadow_tracker_prunes_by_receiver_user() {
        let source_user_id = UserId::Integer(10);
        let receiver_user_id = UserId::Integer(20);
        let source_router_id = RouterId(9);
        let shadow = ShadowSessionKey::new(source_router_id, receiver_user_id.clone());
        let producer = RoutedProducerId::for_test(source_router_id, ProducerId(1));
        let consumer = RoutedConsumerId::for_test(source_router_id, ConsumerId(1));
        let mut tracker = ShadowSessionTracker::default();

        tracker.register_producer(producer, source_user_id);
        tracker.register_consumer(consumer, producer, Some(shadow.clone()));

        let prune = tracker.unregister_user(&receiver_user_id);
        assert!(prune.len() == 1);
        assert!(prune.contains(&shadow));
        assert!(!tracker.contains_shadow_session(&shadow));
        assert!(tracker.unregister_user(&receiver_user_id).is_empty());
    }

    pub fn assert_routing_placement_replacement_retires_stale_connection() {
        let user_id = UserId::Integer(10);
        let first_connection = ConnectionId::from_raw(1);
        let second_connection = ConnectionId::from_raw(2);
        let mut placements = CommittedSessionPlacements::default();

        placements.insert(user_id.clone(), session(first_connection, 9, 0));
        assert!(placements.active(&user_id).is_some_and(|session| {
            session.connection_id == first_connection
                && session.placement.media_worker == MediaWorkerId::from_raw(0)
        }));
        assert!(placements.by_connection.contains_key(&first_connection));

        placements.insert(user_id.clone(), session(second_connection, 10, 1));
        assert!(placements.active(&user_id).is_some_and(|session| {
            session.connection_id == second_connection
                && session.placement.router == RouterId(10)
                && session.placement.media_worker == MediaWorkerId::from_raw(1)
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
            placement: placement(router, media_worker),
        }
    }

    fn placement(router: u64, media_worker: usize) -> RouterPlacement {
        RouterPlacement {
            router: RouterId(router),
            media_worker: MediaWorkerId::from_raw(media_worker),
        }
    }
}
