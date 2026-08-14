use o_sfu_model::UserId;

use super::{
    CommittedSession, LocalSession, RoutedConsumerId, RoutedProducerId, Router, RouterPlacement,
};
use crate::model::{ConnectionId, RouterId};

/// Read-only projection for routed graph invariants.
pub struct InvariantView<'a> {
    router: &'a Router,
}

impl<'a> InvariantView<'a> {
    #[must_use]
    pub const fn new(router: &'a Router) -> Self {
        Self { router }
    }

    fn local_session(&self, router: RouterId, connection: ConnectionId) -> Option<&LocalSession> {
        self.router.routers.get(&router)?.sessions.get(&connection)
    }

    fn committed_session(&self, connection: ConnectionId) -> Option<&CommittedSession> {
        let user = self.router.users.get(&connection)?;
        let session = self.router.sessions.get(user)?;
        (session.connection == connection).then_some(session)
    }

    /// Returns `true` when placements, session indexes and producer-consumer
    /// routes form one reciprocal graph.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        if !self.router.routers.contains_key(&self.router.primary) {
            return false;
        }
        if let Some(placements) = &self.router.placements {
            if placements.primary().router != self.router.primary {
                return false;
            }
            let mut routers = super::BTreeSet::new();
            for placement in placements.iter() {
                if !routers.insert(placement.router) {
                    return false;
                }
            }
            if self
                .router
                .routers
                .keys()
                .any(|router| !routers.contains(router))
            {
                return false;
            }
        } else if !self.router.sessions.is_empty() {
            return false;
        }
        for (user, session) in &self.router.sessions {
            if self.router.users.get(&session.connection) != Some(user) {
                return false;
            }
            if self.router.placements.as_ref().is_none_or(|placements| {
                !placements
                    .iter()
                    .any(|placement| placement == session.placement)
            }) {
                return false;
            }
            let Some(local) = self.router.routers.get(&session.placement.router) else {
                return false;
            };
            if !local.sessions.contains_key(&session.connection) {
                return false;
            }
        }
        for (connection, user) in &self.router.users {
            if self
                .router
                .sessions
                .get(user)
                .is_none_or(|session| session.connection != *connection)
            {
                return false;
            }
        }
        for (router, local) in &self.router.routers {
            for (connection, session) in &local.sessions {
                let Some(user) = self.router.users.get(connection) else {
                    return false;
                };
                let Some(committed) = self.router.sessions.get(user) else {
                    return false;
                };
                if committed.connection != *connection {
                    return false;
                }
                let home = committed.placement.router == *router;
                if !home && (!session.producers.is_empty() || session.consumers.is_empty()) {
                    return false;
                }
                for (producer, consumers) in &session.producers {
                    let routed = RoutedProducerId(*router, *connection, *producer);
                    for consumer in consumers {
                        if consumer.router_id() != *router {
                            return false;
                        }
                        let Some(receiver) = local.sessions.get(&consumer.connection_id()) else {
                            return false;
                        };
                        if receiver.consumers.get(&consumer.consumer_id()) != Some(&routed) {
                            return false;
                        }
                    }
                }
                for (consumer, producer) in &session.consumers {
                    if producer.router_id() != *router {
                        return false;
                    }
                    let routed = RoutedConsumerId(*router, *connection, *consumer);
                    let Some(source) = local.sessions.get(&producer.connection_id()) else {
                        return false;
                    };
                    if source
                        .producers
                        .get(&producer.producer_id())
                        .is_none_or(|consumers| !consumers.contains(&routed))
                    {
                        return false;
                    }
                }
            }
        }
        true
    }

    #[must_use]
    pub fn has_session(&self, user: &UserId, connection: ConnectionId, router: RouterId) -> bool {
        let Some(session) = self.router.sessions.get(user) else {
            return false;
        };
        if session.connection != connection || session.placement.router != router {
            return false;
        }
        if self.router.users.get(&connection) != Some(user) {
            return false;
        }
        self.local_session(router, connection).is_some()
    }

    #[must_use]
    pub fn has_empty_session(
        &self,
        user: &UserId,
        connection: ConnectionId,
        router: RouterId,
    ) -> bool {
        self.has_session(user, connection, router)
            && self
                .local_session(router, connection)
                .is_some_and(LocalSession::is_empty)
    }

    #[must_use]
    pub fn has_only_route(&self, producer: RoutedProducerId, consumer: RoutedConsumerId) -> bool {
        if producer.router_id() != consumer.router_id() {
            return false;
        }
        if self
            .committed_session(producer.connection_id())
            .is_none_or(|session| session.placement.router != producer.router_id())
            || self.committed_session(consumer.connection_id()).is_none()
        {
            return false;
        }
        let Some(source) = self.local_session(producer.router_id(), producer.connection_id())
        else {
            return false;
        };
        let Some(dependents) = source.producers.get(&producer.producer_id()) else {
            return false;
        };
        let Some(receiver) = self.local_session(consumer.router_id(), consumer.connection_id())
        else {
            return false;
        };
        source.producers.len() == 1
            && source.consumers.is_empty()
            && dependents.len() == 1
            && dependents.contains(&consumer)
            && receiver.producers.is_empty()
            && receiver.consumers.len() == 1
            && receiver.consumers.get(&consumer.consumer_id()) == Some(&producer)
    }

    #[must_use]
    pub fn has_only_producer(&self, producer: RoutedProducerId) -> bool {
        self.local_session(producer.router_id(), producer.connection_id())
            .is_some_and(|session| {
                session.producers.len() == 1
                    && session.consumers.is_empty()
                    && session.producers.contains_key(&producer.producer_id())
            })
    }

    #[must_use]
    pub fn has_committed_sessions(&self, count: usize) -> bool {
        self.router.sessions.len() == count && self.router.users.len() == count
    }

    #[must_use]
    pub fn has_placement_pair(&self, primary: RouterPlacement, spillover: RouterPlacement) -> bool {
        self.router.primary == primary.router
            && self.router.placements.as_ref().is_some_and(|placements| {
                placements.primary == primary
                    && placements.spillover.len() == 1
                    && placements.spillover.first() == Some(&spillover)
            })
    }

    #[must_use]
    pub fn session_count(&self, router: RouterId) -> Option<usize> {
        self.router
            .routers
            .get(&router)
            .map(|local| local.sessions.len())
    }

    #[must_use]
    pub fn home_router(&self, user: &UserId) -> Option<RouterId> {
        self.router
            .sessions
            .get(user)
            .map(|session| session.placement.router)
    }

    #[must_use]
    pub fn has_connection(&self, connection: ConnectionId) -> bool {
        self.router.users.contains_key(&connection)
    }

    #[must_use]
    pub fn has_producer(&self, producer: RoutedProducerId) -> bool {
        self.local_session(producer.router_id(), producer.connection_id())
            .is_some_and(|session| session.producers.contains_key(&producer.producer_id()))
    }

    #[must_use]
    pub fn has_consumer(&self, consumer: RoutedConsumerId) -> bool {
        self.local_session(consumer.router_id(), consumer.connection_id())
            .is_some_and(|session| session.consumers.contains_key(&consumer.consumer_id()))
    }

    #[must_use]
    pub fn dependent_count(&self, producer: RoutedProducerId) -> Option<usize> {
        self.local_session(producer.router_id(), producer.connection_id())?
            .producers
            .get(&producer.producer_id())
            .map(super::BTreeSet::len)
    }
}
