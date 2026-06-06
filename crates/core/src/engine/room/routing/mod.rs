//! room-local routing state between room membership and pure router instances
//!
//! the state owns committed connection placement, session homes, source-router
//! producer ids, source-router consumer ids and cross-router shadow sessions
//! it does not own transports or packet forwarding
//! consumers are routed on the source producer router even when their receiver
//! transport lives on another local media worker

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, OnceLock},
};

use o_sfu_router::{
    ConsumerCapability, ConsumerId as RouterConsumerId, ConsumerRouteState, MediaCapabilities,
    MediaKind as RouterMediaKind, ProducerId as RouterProducerId, ProducerRouteState, RouterId,
};

use super::{
    ResolvedPlacement,
    placement::{LocalRoomRouterPlacements, LocalRouterRuntimeContext, RoomPlacementUsageSnapshot},
};
use crate::engine::{
    ConnectionId, MediaWorkerId, RoomInstanceId, UserId, media_transport::TransportSessionKey,
    router_events::RoomRouterEventSink,
};

pub(in crate::engine::room) mod router_state;
mod shadow;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

use router_state::{RoomRouterState, RoomRouterStateError};
use shadow::{ShadowSessionKey, ShadowSessionTracker};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::engine::room) enum RoomRoutingError {
    /// A routed operation referenced a router that is no longer attached.
    MissingRouter { router_id: RouterId },
    /// The room has a home router for the user, but the router state is absent.
    MissingRouterForSession {
        user_id: UserId,
        router_id: RouterId,
    },
    /// The user has no committed home router in this routing state.
    MissingSessionPlacement { user_id: UserId },
    /// The pure router rejected or could not mirror a room routing operation.
    RouterState(RoomRouterStateError),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::engine::room) struct RoomRoutingRepairReport {
    errors: Vec<RoomRoutingError>,
}

impl RoomRoutingRepairReport {
    fn record(&mut self, error: RoomRoutingError) {
        self.errors.push(error);
    }

    pub fn errors(&self) -> &[RoomRoutingError] {
        &self.errors
    }

    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

impl From<RoomRouterStateError> for RoomRoutingError {
    fn from(error: RoomRouterStateError) -> Self {
        Self::RouterState(error)
    }
}

/// producer id plus its authoritative router
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RoutedProducerId {
    router_id: RouterId,
    producer_id: RouterProducerId,
}

impl RoutedProducerId {
    #[must_use]
    pub(super) const fn new(router_id: RouterId, producer_id: RouterProducerId) -> Self {
        Self {
            router_id,
            producer_id,
        }
    }

    #[must_use]
    pub(super) const fn producer_id(self) -> RouterProducerId {
        self.producer_id
    }

    #[must_use]
    pub(super) const fn router_id(self) -> RouterId {
        self.router_id
    }
}

/// consumer id plus its authoritative source router
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RoutedConsumerId {
    router_id: RouterId,
    consumer_id: RouterConsumerId,
}

impl RoutedConsumerId {
    #[must_use]
    pub(super) const fn new(router_id: RouterId, consumer_id: RouterConsumerId) -> Self {
        Self {
            router_id,
            consumer_id,
        }
    }

    #[must_use]
    pub(super) const fn consumer_id(self) -> RouterConsumerId {
        self.consumer_id
    }

    #[must_use]
    pub(super) const fn router_id(self) -> RouterId {
        self.router_id
    }
}

#[derive(Debug, Clone)]
pub(super) struct RoomRoutingState {
    instance_id: RoomInstanceId,
    primary_router: RouterId,
    local_routers: Option<LocalRoomRouterPlacements>,
    router_state_factory: RoomRouterStateFactory,
    routers: BTreeMap<RouterId, RoomRouterState>,
    sessions: CommittedSessionPlacements,
    shadow_sessions: ShadowSessionTracker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommittedRoutingReceipt {
    pub(super) connection_id: ConnectionId,
    pub(super) transport_session_key: TransportSessionKey,
}

#[derive(Debug)]
pub(super) struct DisplacedRoutingSession {
    pub(super) connection_id: ConnectionId,
    pub(super) transport_session_key: TransportSessionKey,
}

#[derive(Debug, Clone)]
struct CommittedSessionPlacement {
    connection_id: ConnectionId,
    router_session_seed: u64,
    runtime: LocalRouterRuntimeContext,
    transport_session_key: TransportSessionKey,
}

#[derive(Debug, Clone, Default)]
struct CommittedSessionPlacements {
    by_connection: BTreeMap<ConnectionId, CommittedSessionPlacement>,
    active_connection_by_user: BTreeMap<UserId, ConnectionId>,
}

#[derive(Clone)]
pub(super) struct RoomRouterStateFactory {
    event_sink: Arc<dyn RoomRouterEventSink>,
}

impl fmt::Debug for RoomRouterStateFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoomRouterStateFactory")
            .finish_non_exhaustive()
    }
}

impl RoomRouterStateFactory {
    #[must_use]
    pub(super) fn new(event_sink: Arc<dyn RoomRouterEventSink>) -> Self {
        Self { event_sink }
    }

    fn build_router_state(
        &self,
        router_id: RouterId,
        router_rtp_capabilities: MediaCapabilities,
    ) -> RoomRouterState {
        RoomRouterState::new(
            router_id,
            router_rtp_capabilities,
            Arc::clone(&self.event_sink),
        )
    }
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
}

impl RoomRoutingState {
    pub(super) fn new_with_router_state_factory(
        instance_id: RoomInstanceId,
        primary_router_id: RouterId,
        local_routers: Option<LocalRoomRouterPlacements>,
        router_rtp_capabilities: MediaCapabilities,
        router_state_factory: &RoomRouterStateFactory,
    ) -> Self {
        let mut routers = BTreeMap::new();
        routers.insert(
            primary_router_id,
            router_state_factory.build_router_state(primary_router_id, router_rtp_capabilities),
        );
        Self {
            instance_id,
            primary_router: primary_router_id,
            local_routers,
            router_state_factory: router_state_factory.clone(),
            routers,
            sessions: CommittedSessionPlacements::default(),
            shadow_sessions: ShadowSessionTracker::default(),
        }
    }

    pub(super) fn rtp_capabilities(&self) -> &MediaCapabilities {
        let Some(primary_router) = self.routers.get(&self.primary_router) else {
            return empty_capabilities();
        };
        primary_router.rtp_capabilities()
    }

    #[must_use]
    pub(super) fn committed_transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        let session = self.sessions.active(user_id)?;
        (session.connection_id == connection_id).then(|| session.transport_session_key.clone())
    }

    #[must_use]
    #[expect(
        clippy::unreachable,
        reason = "current room operations require committed connection placement and must not synthesize a transport worker"
    )]
    pub(super) fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        let Some(session_key) = self.committed_transport_user_key(user_id, connection_id) else {
            unreachable!("transport session key lookup requires committed connection placement");
        };
        session_key
    }

    pub(super) fn commit_session_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        placement: ResolvedPlacement,
        affected_consumers: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> Result<(CommittedRoutingReceipt, Option<DisplacedRoutingSession>), RoomRoutingError> {
        let placement = placement.into_context();
        let displaced_session =
            self.sessions
                .active(user_id)
                .map(|session| DisplacedRoutingSession {
                    connection_id: session.connection_id,
                    transport_session_key: session.transport_session_key.clone(),
                });
        if displaced_session.is_some() {
            self.remove_session(user_id, affected_consumers)?;
        }
        self.attach_placement(placement);
        let router_session_seed = connection_id.as_u64();
        let router = self.router_mut_for_user(user_id, placement.router)?;
        router.ensure_session(user_id, router_session_seed)?;
        router.ensure_session_transports(user_id)?;
        let transport_session_key = TransportSessionKey::new(
            self.instance_id,
            placement.media_worker,
            connection_id,
            user_id.clone(),
        );
        let session = CommittedSessionPlacement {
            connection_id,
            router_session_seed,
            runtime: placement,
            transport_session_key: transport_session_key.clone(),
        };
        let receipt = CommittedRoutingReceipt {
            connection_id,
            transport_session_key,
        };
        self.sessions.insert(user_id.clone(), session);
        Ok((receipt, displaced_session))
    }

    pub(super) fn unregister_committed_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) {
        if self.sessions.active_connection_by_user.get(user_id) == Some(&connection_id) {
            self.sessions.remove(user_id);
        }
    }

    #[expect(
        clippy::unreachable,
        reason = "current route planning requires committed connection placement and must not synthesize a media worker"
    )]
    pub(super) fn media_worker_id_for_connection(
        &self,
        connection_id: ConnectionId,
    ) -> MediaWorkerId {
        let Some(session) = self.sessions.by_connection.get(&connection_id) else {
            unreachable!("media worker lookup requires committed connection placement");
        };
        session.runtime.media_worker
    }

    pub(super) fn assigned_primary_media_worker_id(&self) -> Option<MediaWorkerId> {
        self.local_routers
            .as_ref()
            .map(|local_routers| local_routers.primary().media_worker)
    }

    #[expect(
        clippy::unreachable,
        reason = "source fanout planning requires committed connection placement and must not synthesize worker identity"
    )]
    pub(super) fn worker_lookup(&self) -> impl Fn(ConnectionId) -> MediaWorkerId + use<> {
        let media_worker_by_connection = self
            .sessions
            .by_connection
            .iter()
            .map(|(connection_id, session)| (*connection_id, session.runtime.media_worker))
            .collect::<BTreeMap<_, _>>();
        move |connection_id| {
            let Some(media_worker) = media_worker_by_connection.get(&connection_id).copied() else {
                unreachable!("worker lookup requires committed connection placement");
            };
            media_worker
        }
    }

    #[must_use]
    pub(super) fn usage_snapshot(&self) -> RoomPlacementUsageSnapshot {
        let placements = self
            .local_routers
            .as_ref()
            .map(|local_routers| local_routers.iter().collect())
            .unwrap_or_default();
        RoomPlacementUsageSnapshot::new(
            self.primary_router,
            self.local_routers.is_some(),
            placements,
        )
    }

    #[cfg(test)]
    pub(super) fn primary_router_id(&self) -> RouterId {
        self.primary_router
    }

    fn attach_placement(&mut self, placement: LocalRouterRuntimeContext) {
        match &mut self.local_routers {
            Some(local_routers) => local_routers.upsert(placement),
            None => {
                self.local_routers = Some(LocalRoomRouterPlacements::new(placement, Vec::new()));
            }
        }
        let router_id = placement.router;
        if self.routers.contains_key(&router_id) {
            return;
        }
        let router_rtp_capabilities = self.rtp_capabilities().clone();
        self.routers.insert(
            router_id,
            self.router_state_factory
                .build_router_state(router_id, router_rtp_capabilities),
        );
    }

    pub(super) fn add_producer(
        &mut self,
        user_id: &UserId,
        media_kind: RouterMediaKind,
    ) -> Result<RoutedProducerId, RoomRoutingError> {
        let router_id = self.require_session(user_id)?.runtime.router;
        let producer_id = self
            .router_mut_for_user(user_id, router_id)?
            .add_producer(user_id, media_kind)?;
        Ok(RoutedProducerId::new(router_id, producer_id))
    }

    #[cfg(test)]
    pub(super) fn add_consumer(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RoutedProducerId,
        media_kind: RouterMediaKind,
        capability: ConsumerCapability,
    ) -> Result<RoutedConsumerId, RoomRoutingError> {
        self.add_consumer_with_route_state(
            consumer_user_id,
            producer_id,
            media_kind,
            capability,
            ConsumerRouteState::Active,
        )
    }

    pub(super) fn add_consumer_with_route_state(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RoutedProducerId,
        media_kind: RouterMediaKind,
        capability: ConsumerCapability,
        route_state: ConsumerRouteState,
    ) -> Result<RoutedConsumerId, RoomRoutingError> {
        let receiver_session =
            self.ensure_session_on_router(consumer_user_id, producer_id.router_id())?;
        let consumer_result = self
            .router_mut(producer_id.router_id())?
            .add_consumer_with_route_state(
                consumer_user_id,
                producer_id.producer_id(),
                media_kind,
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
        let routed_consumer_id = RoutedConsumerId::new(producer_id.router_id(), consumer_id);
        self.shadow_sessions
            .register_consumer(routed_consumer_id, receiver_session.shadow_key);
        Ok(routed_consumer_id)
    }

    pub(super) fn set_producer_route_state(
        &mut self,
        producer_id: RoutedProducerId,
        route_state: ProducerRouteState,
    ) -> Result<(), RoomRoutingError> {
        self.router_mut(producer_id.router_id())?
            .set_producer_route_state(producer_id.producer_id(), route_state)?;
        Ok(())
    }

    pub(super) fn set_consumer_route_state(
        &mut self,
        consumer_id: RoutedConsumerId,
        route_state: ConsumerRouteState,
    ) -> Result<(), RoomRoutingError> {
        self.router_mut(consumer_id.router_id())?
            .set_consumer_route_state(consumer_id.consumer_id(), route_state)?;
        Ok(())
    }

    pub(super) fn remove_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
    ) -> Result<(), RoomRoutingError> {
        self.router_mut(consumer_id.router_id())?
            .remove_consumer(consumer_id.consumer_id())?;
        let shadow_sessions = self.shadow_sessions.unregister_consumers([consumer_id]);
        self.prune_shadow_sessions(shadow_sessions)?;
        Ok(())
    }

    pub(super) fn remove_producer(
        &mut self,
        producer_id: RoutedProducerId,
        affected_consumers: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> Result<(), RoomRoutingError> {
        self.router_mut(producer_id.router_id())?
            .remove_producer(producer_id.producer_id())?;
        let shadow_sessions = self
            .shadow_sessions
            .unregister_consumers(affected_consumers);
        self.prune_shadow_sessions(shadow_sessions)?;
        Ok(())
    }

    pub(super) fn remove_session(
        &mut self,
        user_id: &UserId,
        affected_consumers: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> Result<(), RoomRoutingError> {
        let home_router_id = self.require_session(user_id)?.runtime.router;
        if !self.routers.contains_key(&home_router_id) {
            return Err(RoomRoutingError::MissingRouterForSession {
                user_id: user_id.clone(),
                router_id: home_router_id,
            });
        }
        for router in self.routers.values_mut() {
            router.remove_session(user_id)?;
        }
        let shadow_sessions = self
            .shadow_sessions
            .unregister_consumers(affected_consumers);
        self.prune_shadow_sessions(shadow_sessions)?;
        self.sessions.remove(user_id);
        Ok(())
    }

    pub(super) fn remove_session_repairing(
        &mut self,
        user_id: &UserId,
        affected_consumers: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> RoomRoutingRepairReport {
        let mut report = RoomRoutingRepairReport::default();
        match self
            .require_session(user_id)
            .map(|session| session.runtime.router)
        {
            Ok(home_router_id) if !self.routers.contains_key(&home_router_id) => {
                report.record(RoomRoutingError::MissingRouterForSession {
                    user_id: user_id.clone(),
                    router_id: home_router_id,
                });
            }
            Err(error) => report.record(error),
            Ok(_) => {}
        }
        for router in self.routers.values_mut() {
            let removal = router
                .remove_session_repairing(user_id)
                .map_err(RoomRoutingError::from);
            if let Err(error) = removal {
                report.record(error);
            }
        }
        let shadow_sessions = self
            .shadow_sessions
            .unregister_consumers(affected_consumers);
        self.prune_shadow_sessions_repairing(shadow_sessions, &mut report);
        self.sessions.remove(user_id);
        report
    }

    fn require_session(
        &self,
        user_id: &UserId,
    ) -> Result<&CommittedSessionPlacement, RoomRoutingError> {
        self.sessions
            .active(user_id)
            .ok_or_else(|| RoomRoutingError::MissingSessionPlacement {
                user_id: user_id.clone(),
            })
    }

    /// create a receiver shadow when the source router differs from the home router
    fn ensure_session_on_router(
        &mut self,
        user_id: &UserId,
        router_id: RouterId,
    ) -> Result<ReceiverRouterSession, RoomRoutingError> {
        let session = self.require_session(user_id)?;
        let router_session_seed = session.router_session_seed;
        let home_router_id = session.runtime.router;
        let shadow_key = (home_router_id != router_id)
            .then(|| ShadowSessionKey::new(router_id, user_id.clone()));
        if !self.routers.contains_key(&router_id) {
            return Err(RoomRoutingError::MissingRouter { router_id });
        }
        let created_untracked_shadow = shadow_key
            .as_ref()
            .is_some_and(|key| !self.shadow_sessions.contains_shadow_session(key));
        self.router_mut_for_user(user_id, router_id)?
            .ensure_session(user_id, router_session_seed)?;
        if let Err(error) = self
            .router_mut_for_user(user_id, router_id)?
            .ensure_session_transports(user_id)
        {
            if created_untracked_shadow {
                self.router_mut_for_user(user_id, router_id)?
                    .remove_session(user_id)?;
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
    ) -> Result<(), RoomRoutingError> {
        for shadow_session in shadow_sessions {
            self.router_mut_for_user(shadow_session.user_id(), shadow_session.router_id())?
                .remove_session(shadow_session.user_id())?;
        }
        Ok(())
    }

    fn prune_shadow_sessions_repairing(
        &mut self,
        shadow_sessions: BTreeSet<ShadowSessionKey>,
        report: &mut RoomRoutingRepairReport,
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
                report.record(error);
            }
        }
    }

    pub(in crate::engine::room) fn idle_spillover_routers(&self) -> Vec<RouterId> {
        let active_home_routers = self
            .sessions
            .by_connection
            .values()
            .map(|session| session.runtime.router)
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

    pub(in crate::engine::room) fn detach_spillover_routers(&mut self, router_ids: &[RouterId]) {
        for router_id in router_ids {
            if *router_id == self.primary_router {
                continue;
            }
            self.routers.remove(router_id);
        }
    }

    fn router_mut(
        &mut self,
        router_id: RouterId,
    ) -> Result<&mut RoomRouterState, RoomRoutingError> {
        self.routers
            .get_mut(&router_id)
            .ok_or(RoomRoutingError::MissingRouter { router_id })
    }

    fn router_mut_for_user(
        &mut self,
        user_id: &UserId,
        router_id: RouterId,
    ) -> Result<&mut RoomRouterState, RoomRoutingError> {
        self.routers
            .get_mut(&router_id)
            .ok_or_else(|| RoomRoutingError::MissingRouterForSession {
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
