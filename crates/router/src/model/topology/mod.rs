//! room-local topology over pure router instances

#[cfg(not(kani))]
use std::collections::{BTreeMap, BTreeSet};
use std::{iter, sync::OnceLock};

use o_sfu_model::UserId;

#[cfg(kani)]
use crate::model::proof_storage::{BTreeMap, BTreeSet};
use crate::model::{
    ConnectionId, ConsumerCapability, ConsumerId as RouterConsumerId, ConsumerRouteState,
    MediaCapabilities, MediaKind as RouterMediaKind, MediaWorkerId, ProducerId as RouterProducerId,
    ProducerRouteState, RouterError, RouterId,
};

pub(super) mod router_state;
mod shadow;
#[cfg(any(test, feature = "test-support", kani))]
#[path = "../TESTS/topology_support.rs"]
pub(super) mod test_support;

use router_state::{RouterAdapterError, RouterAdapterState};
use shadow::{ShadowSessionKey, ShadowSessionTracker};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingError {
    /// A routed operation referenced a router that is no longer attached.
    MissingRouter { router_id: RouterId },
    /// The session has a home router, but the router state is absent.
    MissingRouterForSession {
        user_id: UserId,
        router_id: RouterId,
    },
    /// The user has no committed home router in this routing state.
    MissingSessionPlacement { user_id: UserId },
    /// The topology adapter has lost the pure-router session mapping for a user.
    MissingSessionMapping { user_id: UserId },
    /// The pure router rejected a topology operation.
    Router(RouterError),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoutingRepairReport(Vec<RoutingError>);

impl RoutingRepairReport {
    #[must_use]
    pub fn errors(&self) -> &[RoutingError] {
        &self.0
    }

    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<RouterAdapterError> for RoutingError {
    fn from(error: RouterAdapterError) -> Self {
        match error {
            RouterAdapterError::MissingSessionMapping { user_id } => {
                Self::MissingSessionMapping { user_id }
            }
            RouterAdapterError::Router(error) => Self::Router(error),
        }
    }
}

/// producer id plus its authoritative router
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutedProducerId(RouterId, RouterProducerId);

impl RoutedProducerId {
    #[cfg(any(test, feature = "test-support", kani))]
    #[must_use]
    pub const fn for_test(router_id: RouterId, producer_id: RouterProducerId) -> Self {
        Self(router_id, producer_id)
    }

    #[must_use]
    pub const fn producer_id(self) -> RouterProducerId {
        self.1
    }

    #[must_use]
    pub const fn router_id(self) -> RouterId {
        self.0
    }
}

/// consumer id plus its authoritative source router
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutedConsumerId(RouterId, RouterConsumerId);

impl RoutedConsumerId {
    #[cfg(any(test, feature = "test-support", kani))]
    #[must_use]
    pub const fn for_test(router_id: RouterId, consumer_id: RouterConsumerId) -> Self {
        Self(router_id, consumer_id)
    }

    #[must_use]
    pub const fn consumer_id(self) -> RouterConsumerId {
        self.1
    }

    #[must_use]
    pub const fn router_id(self) -> RouterId {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouterPlacement {
    pub router: RouterId,
    pub media_worker: MediaWorkerId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterPlacements {
    primary: RouterPlacement,
    spillover: Vec<RouterPlacement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterPlacementsError {
    Empty,
}

impl RouterPlacements {
    #[must_use]
    pub fn new(primary: RouterPlacement, spillover: Vec<RouterPlacement>) -> Self {
        Self { primary, spillover }
    }

    /// # Errors
    ///
    /// returns [`RouterPlacementsError::Empty`] when `placements` is empty
    pub fn try_from_vec(placements: Vec<RouterPlacement>) -> Result<Self, RouterPlacementsError> {
        let mut placements = placements.into_iter();
        let Some(primary) = placements.next() else {
            return Err(RouterPlacementsError::Empty);
        };
        Ok(Self::new(primary, placements.collect()))
    }

    #[must_use]
    pub const fn primary(&self) -> RouterPlacement {
        self.primary
    }

    pub fn upsert(&mut self, placement: RouterPlacement) {
        if self.primary.router == placement.router {
            self.primary = placement;
            return;
        }
        if let Some(existing) = self
            .spillover
            .iter_mut()
            .find(|existing| existing.router == placement.router)
        {
            *existing = placement;
            return;
        }
        self.spillover.push(placement);
    }

    pub fn iter(&self) -> impl Iterator<Item = RouterPlacement> + '_ {
        iter::once(self.primary).chain(self.spillover.iter().copied())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingPlacementSnapshot {
    primary_router: RouterId,
    has_assigned_placements: bool,
    placements: Vec<RouterPlacement>,
}

impl RoutingPlacementSnapshot {
    #[must_use]
    pub fn new(
        primary_router: RouterId,
        has_assigned_placements: bool,
        placements: Vec<RouterPlacement>,
    ) -> Self {
        Self {
            primary_router,
            has_assigned_placements,
            placements,
        }
    }

    #[must_use]
    pub const fn primary_router(&self) -> RouterId {
        self.primary_router
    }

    #[must_use]
    pub fn next_router_id(&self) -> RouterId {
        let router_id = self
            .placements
            .iter()
            .map(|placement| placement.router.0)
            .max()
            .map_or(self.primary_router.0, |router_id| {
                router_id.saturating_add(1)
            });
        RouterId(router_id)
    }

    #[must_use]
    pub fn assigned_placements(&self) -> &[RouterPlacement] {
        if self.has_assigned_placements {
            &self.placements
        } else {
            &[]
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoutingTopology {
    primary_router: RouterId,
    router_placements: Option<RouterPlacements>,
    routers: BTreeMap<RouterId, RouterAdapterState>,
    sessions: CommittedSessionPlacements,
    shadow_sessions: ShadowSessionTracker,
}

#[derive(Debug, Clone)]
struct CommittedSessionPlacement {
    connection_id: ConnectionId,
    router_session_seed: u64,
    placement: RouterPlacement,
}

#[derive(Debug, Clone, Default)]
struct CommittedSessionPlacements {
    by_connection: BTreeMap<ConnectionId, CommittedSessionPlacement>,
    active_connection_by_user: BTreeMap<UserId, ConnectionId>,
}

impl CommittedSessionPlacements {
    fn active(&self, user_id: &UserId) -> Option<&CommittedSessionPlacement> {
        let connection_id = self.active_connection_by_user.get(user_id)?;
        self.by_connection.get(connection_id)
    }

    fn insert(&mut self, user_id: UserId, session: CommittedSessionPlacement) {
        let connection_id = session.connection_id;
        if let Some(previous) = self
            .active_connection_by_user
            .insert(user_id, connection_id)
        {
            self.by_connection.remove(&previous);
        }
        self.by_connection.insert(connection_id, session);
    }

    fn remove(&mut self, user_id: &UserId) -> Option<CommittedSessionPlacement> {
        let connection_id = self.active_connection_by_user.remove(user_id)?;
        self.by_connection.remove(&connection_id)
    }

    fn remove_if_active(
        &mut self,
        user: &UserId,
        conn: ConnectionId,
    ) -> Option<CommittedSessionPlacement> {
        (self.active_connection_by_user.get(user) == Some(&conn)).then_some(())?;
        self.remove(user)
    }
}

impl RoutingTopology {
    #[must_use]
    pub fn new(
        primary_router_id: RouterId,
        router_placements: Option<RouterPlacements>,
        router_rtp_capabilities: MediaCapabilities,
    ) -> Self {
        let mut routers = BTreeMap::new();
        routers.insert(
            primary_router_id,
            RouterAdapterState::new(primary_router_id, router_rtp_capabilities),
        );
        Self {
            primary_router: primary_router_id,
            router_placements,
            routers,
            sessions: CommittedSessionPlacements::default(),
            shadow_sessions: ShadowSessionTracker::default(),
        }
    }

    #[must_use]
    pub fn rtp_capabilities(&self) -> &MediaCapabilities {
        let Some(primary_router) = self.routers.get(&self.primary_router) else {
            return empty_capabilities();
        };
        primary_router.rtp_capabilities()
    }

    #[must_use]
    pub fn committed_media_worker_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<MediaWorkerId> {
        let session = self.sessions.active(user_id)?;
        (session.connection_id == connection_id).then_some(session.placement.media_worker)
    }

    /// commit one user connection to a resolved router placement
    ///
    /// the routing state is unchanged when placement is rejected
    ///
    /// # Errors
    ///
    /// returns [`RoutingError::MissingRouterForSession`] for an absent home
    /// router or [`RoutingError::Router`] for rejected router setup
    pub fn commit_session_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        placement: RouterPlacement,
    ) -> Result<MediaWorkerId, RoutingError> {
        let old_placements = self.router_placements.clone();
        let old_session = self.sessions.active(user_id).cloned();
        let old_router = self.routers.get(&placement.router).cloned();
        let mut old_routers = vec![(placement.router, old_router)];
        for (router_id, router) in &self.routers {
            if *router_id != placement.router && router.has_user(user_id) {
                old_routers.push((*router_id, Some(router.clone())));
            }
        }
        let error = match self.apply_session_placement(user_id, connection_id, placement) {
            Ok(media_worker) => return Ok(media_worker),
            Err(error) => error,
        };
        self.router_placements = old_placements;
        self.sessions.remove(user_id);
        if let Some(old_session) = old_session {
            self.sessions.insert(user_id.clone(), old_session);
        }
        for (router_id, router) in old_routers {
            if let Some(router) = router {
                self.routers.insert(router_id, router);
            } else {
                self.routers.remove(&router_id);
            }
        }
        Err(error)
    }

    fn apply_session_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        placement: RouterPlacement,
    ) -> Result<MediaWorkerId, RoutingError> {
        let replacing = self.sessions.active(user_id).is_some();
        if replacing {
            self.remove_session_from_routers(user_id)?;
            let shadow_sessions = self.shadow_sessions.prunable_for_user(user_id);
            self.prune_shadow_sessions(shadow_sessions)?;
            self.sessions.remove(user_id);
        }
        self.attach_placement(placement);
        let router_session_seed = connection_id.as_u64();
        let router = self.router_mut_for_user(user_id, placement.router)?;
        router.ensure_session(user_id, router_session_seed)?;
        router.ensure_session_transports(user_id)?;
        let session = CommittedSessionPlacement {
            connection_id,
            router_session_seed,
            placement,
        };
        self.sessions.insert(user_id.clone(), session);
        if replacing {
            let _ = self.shadow_sessions.unregister_user(user_id);
        }
        Ok(placement.media_worker)
    }

    pub fn retire_committed_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<MediaWorkerId> {
        let session = self.sessions.remove_if_active(user_id, connection_id)?;
        Some(session.placement.media_worker)
    }

    #[expect(
        clippy::unreachable,
        reason = "current route planning requires committed connection placement and must not synthesize a media worker"
    )]
    #[must_use]
    pub fn media_worker_id_for_connection(&self, connection_id: ConnectionId) -> MediaWorkerId {
        let Some(session) = self.sessions.by_connection.get(&connection_id) else {
            unreachable!("media worker lookup requires committed connection placement");
        };
        session.placement.media_worker
    }

    #[must_use]
    pub fn assigned_primary_media_worker_id(&self) -> Option<MediaWorkerId> {
        self.router_placements
            .as_ref()
            .map(|router_placements| router_placements.primary().media_worker)
    }

    #[must_use]
    pub fn usage_snapshot(&self) -> RoutingPlacementSnapshot {
        let placements = self
            .router_placements
            .as_ref()
            .map(|router_placements| router_placements.iter().collect())
            .unwrap_or_default();
        RoutingPlacementSnapshot::new(
            self.primary_router,
            self.router_placements.is_some(),
            placements,
        )
    }

    fn attach_placement(&mut self, placement: RouterPlacement) {
        match &mut self.router_placements {
            Some(router_placements) => router_placements.upsert(placement),
            None => {
                self.router_placements = Some(RouterPlacements::new(placement, Vec::new()));
            }
        }
        let router_id = placement.router;
        if self.routers.contains_key(&router_id) {
            return;
        }
        let router_rtp_capabilities = self.rtp_capabilities().clone();
        self.routers.insert(
            router_id,
            RouterAdapterState::new(router_id, router_rtp_capabilities),
        );
    }

    /// create a routed producer on the user's home router
    ///
    /// # Errors
    ///
    /// returns [`RoutingError::MissingSessionPlacement`] when the user has no
    /// committed home placement or [`RoutingError::Router`] when the pure
    /// router rejects producer insertion
    pub fn add_producer(
        &mut self,
        user_id: &UserId,
        media_kind: RouterMediaKind,
    ) -> Result<RoutedProducerId, RoutingError> {
        let router_id = self.require_session(user_id)?.placement.router;
        let producer_id = self
            .router_mut_for_user(user_id, router_id)?
            .add_producer(user_id, media_kind)?;
        let routed_producer_id = RoutedProducerId(router_id, producer_id);
        self.shadow_sessions
            .register_producer(routed_producer_id, user_id.clone());
        Ok(routed_producer_id)
    }

    /// create a routed consumer on the producer's source router
    ///
    /// cross-router receivers get a rollback-safe shadow session
    ///
    /// # Errors
    ///
    /// returns missing placement, missing router or router rejection errors
    pub fn add_consumer_with_route_state(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RoutedProducerId,
        capability: ConsumerCapability,
        route_state: ConsumerRouteState,
    ) -> Result<RoutedConsumerId, RoutingError> {
        let receiver_session =
            self.ensure_session_on_router(consumer_user_id, producer_id.router_id())?;
        let consumer_result = self
            .router_mut(producer_id.router_id())?
            .add_consumer_with_route_state(
                consumer_user_id,
                producer_id.producer_id(),
                capability,
                route_state,
            );
        let consumer_id = match consumer_result {
            Ok(consumer_id) => consumer_id,
            Err(error) => {
                if receiver_session.created_untracked_shadow {
                    self.router_mut_for_user(consumer_user_id, producer_id.router_id())?
                        .remove_session(consumer_user_id)?;
                }
                return Err(error.into());
            }
        };
        let routed_consumer_id = RoutedConsumerId(producer_id.router_id(), consumer_id);
        self.shadow_sessions.register_consumer(
            routed_consumer_id,
            producer_id,
            receiver_session.shadow_key,
        );
        Ok(routed_consumer_id)
    }

    /// update the source route state for a routed producer
    ///
    /// # Errors
    ///
    /// returns missing router or router rejection errors
    pub fn set_producer_route_state(
        &mut self,
        producer_id: RoutedProducerId,
        route_state: ProducerRouteState,
    ) -> Result<(), RoutingError> {
        self.router_mut(producer_id.router_id())?
            .set_producer_route_state(producer_id.producer_id(), route_state)?;
        Ok(())
    }

    /// update the receiver-local route state for a routed consumer
    ///
    /// # Errors
    ///
    /// returns missing router or router rejection errors
    pub fn set_consumer_route_state(
        &mut self,
        consumer_id: RoutedConsumerId,
        route_state: ConsumerRouteState,
    ) -> Result<(), RoutingError> {
        self.router_mut(consumer_id.router_id())?
            .set_consumer_route_state(consumer_id.consumer_id(), route_state)?;
        Ok(())
    }

    /// remove a routed consumer and prune any unreferenced receiver shadow
    ///
    /// # Errors
    ///
    /// returns missing router or router rejection errors
    pub fn remove_consumer(&mut self, consumer_id: RoutedConsumerId) -> Result<(), RoutingError> {
        self.router_mut(consumer_id.router_id())?
            .remove_consumer(consumer_id.consumer_id())?;
        let shadow_sessions = self.shadow_sessions.release_consumers([consumer_id]);
        self.prune_shadow_sessions(shadow_sessions)?;
        Ok(())
    }

    /// remove a routed producer and release its known routed consumers
    ///
    /// # Errors
    ///
    /// returns missing router or router rejection errors
    pub fn remove_producer(&mut self, producer_id: RoutedProducerId) -> Result<(), RoutingError> {
        self.router_mut(producer_id.router_id())?
            .remove_producer(producer_id.producer_id())?;
        let shadow_sessions = self.shadow_sessions.unregister_producer(producer_id);
        self.prune_shadow_sessions(shadow_sessions)?;
        Ok(())
    }

    /// remove a user session from every attached router
    ///
    /// # Errors
    ///
    /// returns missing placement, missing home router or router rejection errors
    pub fn remove_session(&mut self, user_id: &UserId) -> Result<(), RoutingError> {
        self.remove_session_from_routers(user_id)?;
        let shadow_sessions = self.shadow_sessions.unregister_user(user_id);
        self.prune_shadow_sessions(shadow_sessions)?;
        self.sessions.remove(user_id);
        Ok(())
    }

    fn remove_session_from_routers(&mut self, user_id: &UserId) -> Result<(), RoutingError> {
        let home_router_id = self.require_session(user_id)?.placement.router;
        if !self.routers.contains_key(&home_router_id) {
            return Err(RoutingError::MissingRouterForSession {
                user_id: user_id.clone(),
                router_id: home_router_id,
            });
        }
        for router in self.routers.values_mut() {
            router.remove_session(user_id)?;
        }
        Ok(())
    }

    pub fn remove_session_repairing(&mut self, user_id: &UserId) -> RoutingRepairReport {
        let mut report = RoutingRepairReport::default();
        match self
            .require_session(user_id)
            .map(|session| session.placement.router)
        {
            Ok(home_router_id) if !self.routers.contains_key(&home_router_id) => {
                report.0.push(RoutingError::MissingRouterForSession {
                    user_id: user_id.clone(),
                    router_id: home_router_id,
                });
            }
            Err(error) => report.0.push(error),
            Ok(_) => {}
        }
        for router in self.routers.values_mut() {
            let removal = router
                .remove_session_repairing(user_id)
                .map_err(RoutingError::from);
            if let Err(error) = removal {
                report.0.push(error);
            }
        }
        let shadow_sessions = self.shadow_sessions.unregister_user(user_id);
        self.prune_shadow_sessions_repairing(shadow_sessions, &mut report);
        self.sessions.remove(user_id);
        report
    }

    fn require_session(
        &self,
        user_id: &UserId,
    ) -> Result<&CommittedSessionPlacement, RoutingError> {
        self.sessions
            .active(user_id)
            .ok_or_else(|| RoutingError::MissingSessionPlacement {
                user_id: user_id.clone(),
            })
    }

    /// create a receiver shadow when the source router differs from the home router
    fn ensure_session_on_router(
        &mut self,
        user_id: &UserId,
        router_id: RouterId,
    ) -> Result<ReceiverRouterSession, RoutingError> {
        let session = self.require_session(user_id)?;
        let router_session_seed = session.router_session_seed;
        let home_router_id = session.placement.router;
        let shadow_key = (home_router_id != router_id)
            .then(|| ShadowSessionKey::new(router_id, user_id.clone()));
        let created_untracked_shadow = shadow_key
            .as_ref()
            .is_some_and(|key| !self.shadow_sessions.contains_shadow_session(key));
        let router = self.router_mut(router_id)?;
        router.ensure_session(user_id, router_session_seed)?;
        if let Err(error) = router.ensure_session_transports(user_id) {
            if created_untracked_shadow {
                router.remove_session(user_id)?;
            }
            return Err(error.into());
        }
        Ok(ReceiverRouterSession {
            shadow_key,
            created_untracked_shadow,
        })
    }

    fn prune_shadow_sessions(
        &mut self,
        shadow_sessions: BTreeSet<ShadowSessionKey>,
    ) -> Result<(), RoutingError> {
        for shadow_session in shadow_sessions {
            self.router_mut_for_user(shadow_session.user_id(), shadow_session.router_id())?
                .remove_session(shadow_session.user_id())?;
        }
        Ok(())
    }

    fn prune_shadow_sessions_repairing(
        &mut self,
        shadow_sessions: BTreeSet<ShadowSessionKey>,
        report: &mut RoutingRepairReport,
    ) {
        for shadow_session in shadow_sessions {
            let removal = self
                .router_mut_for_user(shadow_session.user_id(), shadow_session.router_id())
                .and_then(|router| {
                    router
                        .remove_session_repairing(shadow_session.user_id())
                        .map_err(Into::into)
                });
            if let Err(error) = removal {
                report.0.push(error);
            }
        }
    }

    #[must_use]
    pub fn idle_spillover_routers(&self) -> Vec<RouterId> {
        let active_home_routers = self
            .sessions
            .by_connection
            .values()
            .map(|session| session.placement.router)
            .collect::<BTreeSet<_>>();
        self.routers
            .iter()
            .filter_map(|(router_id, router)| {
                if *router_id == self.primary_router || active_home_routers.contains(router_id) {
                    return None;
                }
                (router.mapped_session_count() == 0).then_some(*router_id)
            })
            .collect()
    }

    pub fn detach_spillover_routers(&mut self, router_ids: &[RouterId]) {
        for router_id in router_ids {
            if *router_id == self.primary_router {
                continue;
            }
            self.routers.remove(router_id);
        }
    }

    fn router_mut(&mut self, router_id: RouterId) -> Result<&mut RouterAdapterState, RoutingError> {
        self.routers
            .get_mut(&router_id)
            .ok_or(RoutingError::MissingRouter { router_id })
    }

    fn router_mut_for_user(
        &mut self,
        user_id: &UserId,
        router_id: RouterId,
    ) -> Result<&mut RouterAdapterState, RoutingError> {
        self.routers
            .get_mut(&router_id)
            .ok_or_else(|| RoutingError::MissingRouterForSession {
                user_id: user_id.clone(),
                router_id,
            })
    }
}

/// result of materializing a receiver on the source router for consumer setup
#[derive(Debug, Clone)]
struct ReceiverRouterSession {
    shadow_key: Option<ShadowSessionKey>,
    created_untracked_shadow: bool,
}

fn empty_capabilities() -> &'static MediaCapabilities {
    static EMPTY: OnceLock<MediaCapabilities> = OnceLock::new();
    EMPTY.get_or_init(|| MediaCapabilities::new(Vec::new(), Vec::new()))
}
