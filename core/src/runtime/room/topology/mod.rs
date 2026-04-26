use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use o_sfu_protocol::shared::UserId;
use o_sfu_router::{
    ConsumerCapability, ConsumerId as RouterConsumerId, MediaCapabilities,
    MediaKind as RouterMediaKind, ProducerId as RouterProducerId, RouterId,
};

use super::router_state::{RoomRouterState, RoomRouterStateError};
use crate::runtime::recording::RecordingService;

#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) enum RoomTopologyError {
    MissingRouter {
        router_id: RouterId,
    },
    MissingRouterForSession {
        user_id: UserId,
        router_id: RouterId,
    },
    RouterState(RoomRouterStateError),
}

impl From<RoomRouterStateError> for RoomTopologyError {
    fn from(error: RoomRouterStateError) -> Self {
        Self::RouterState(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
/// Room-local routing placement over one or more pure router instances.
///
/// The current implementation is single-router, but the type owns the user
/// placement map and router-facing operations so future sharding does not leak
/// into room-state mutation code.
pub(super) struct RoomTopology {
    primary_router: RouterId,
    routers: BTreeMap<RouterId, RoomRouterState>,
    session_home_router: BTreeMap<UserId, RouterId>,
}

#[derive(Debug, Clone)]
pub(super) struct RoomRouterObserverFactory {
    recording_service: Arc<RecordingService>,
}

impl RoomRouterObserverFactory {
    #[must_use]
    pub(super) fn new(recording_service: Arc<RecordingService>) -> Self {
        Self { recording_service }
    }

    fn build_router_state(
        &self,
        router_id: RouterId,
        router_rtp_capabilities: MediaCapabilities,
    ) -> RoomRouterState {
        RoomRouterState::new_with_recording_service(
            router_id,
            router_rtp_capabilities,
            Arc::clone(&self.recording_service),
        )
    }
}

impl RoomTopology {
    pub(super) fn new_with_recording_observer_factory(
        primary_router_id: RouterId,
        router_rtp_capabilities: MediaCapabilities,
        router_observer_factory: &RoomRouterObserverFactory,
    ) -> Self {
        let mut routers = BTreeMap::new();
        routers.insert(
            primary_router_id,
            router_observer_factory.build_router_state(primary_router_id, router_rtp_capabilities),
        );
        Self {
            primary_router: primary_router_id,
            routers,
            session_home_router: BTreeMap::new(),
        }
    }

    pub(super) fn rtp_capabilities(&self) -> &MediaCapabilities {
        let Some(primary_router) = self.routers.get(&self.primary_router) else {
            return empty_router_capabilities();
        };
        primary_router.rtp_capabilities()
    }

    pub(super) fn ensure_session(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
    ) -> Result<(), RoomTopologyError> {
        let router_id = self
            .session_home_router
            .get(user_id)
            .copied()
            .unwrap_or(self.primary_router);
        let router = self.router_mut_for_user(user_id, router_id)?;
        router.ensure_session(user_id, router_session_seed)?;
        self.session_home_router.insert(user_id.clone(), router_id);
        Ok(())
    }

    pub(super) fn ensure_session_transports(
        &mut self,
        user_id: &UserId,
    ) -> Result<(), RoomTopologyError> {
        let router_id = self.router_id_for_user(user_id);
        self.router_mut_for_user(user_id, router_id)?
            .ensure_session_transports(user_id)?;
        Ok(())
    }

    pub(super) fn apply_client_join(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
    ) -> Result<(), RoomTopologyError> {
        self.ensure_session(user_id, router_session_seed)?;
        self.ensure_session_transports(user_id)?;
        Ok(())
    }

    pub(super) fn apply_client_leave(&mut self, user_id: &UserId) -> Result<(), RoomTopologyError> {
        self.remove_session(user_id)
    }

    pub(super) fn add_producer(
        &mut self,
        user_id: &UserId,
        media_kind: RouterMediaKind,
    ) -> Result<RoutedProducerId, RoomTopologyError> {
        let router_id = self.router_id_for_user(user_id);
        let producer_id = self
            .router_mut_for_user(user_id, router_id)?
            .add_producer(user_id, media_kind)?;
        Ok(RoutedProducerId::new(router_id, producer_id))
    }

    pub(super) fn add_consumer(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RoutedProducerId,
        media_kind: RouterMediaKind,
        capability: ConsumerCapability,
    ) -> Result<RoutedConsumerId, RoomTopologyError> {
        let consumer_id = self.router_mut(producer_id.router_id())?.add_consumer(
            consumer_user_id,
            producer_id.producer_id(),
            media_kind,
            capability,
        )?;
        Ok(RoutedConsumerId::new(producer_id.router_id(), consumer_id))
    }

    pub(super) fn set_producer_paused(
        &mut self,
        producer_id: RoutedProducerId,
        paused: bool,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(producer_id.router_id())?
            .set_producer_paused(producer_id.producer_id(), paused)?;
        Ok(())
    }

    pub(super) fn set_consumer_paused(
        &mut self,
        consumer_id: RoutedConsumerId,
        paused: bool,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(consumer_id.router_id())?
            .set_consumer_paused(consumer_id.consumer_id(), paused)?;
        Ok(())
    }

    pub(super) fn remove_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(consumer_id.router_id())?
            .remove_consumer(consumer_id.consumer_id())?;
        Ok(())
    }

    pub(super) fn remove_producer(
        &mut self,
        producer_id: RoutedProducerId,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(producer_id.router_id())?
            .remove_producer(producer_id.producer_id())?;
        Ok(())
    }

    pub(super) fn remove_session(&mut self, user_id: &UserId) -> Result<(), RoomTopologyError> {
        let router_id = self.router_id_for_user(user_id);
        self.router_mut_for_user(user_id, router_id)?
            .remove_session(user_id)?;
        self.session_home_router.remove(user_id);
        Ok(())
    }

    fn router_id_for_user(&self, user_id: &UserId) -> RouterId {
        self.session_home_router
            .get(user_id)
            .copied()
            .unwrap_or(self.primary_router)
    }

    fn router_mut(
        &mut self,
        router_id: RouterId,
    ) -> Result<&mut RoomRouterState, RoomTopologyError> {
        self.routers
            .get_mut(&router_id)
            .ok_or(RoomTopologyError::MissingRouter { router_id })
    }

    fn router_mut_for_user(
        &mut self,
        user_id: &UserId,
        router_id: RouterId,
    ) -> Result<&mut RoomRouterState, RoomTopologyError> {
        self.routers
            .get_mut(&router_id)
            .ok_or_else(|| RoomTopologyError::MissingRouterForSession {
                user_id: user_id.clone(),
                router_id,
            })
    }
}

fn empty_router_capabilities() -> &'static MediaCapabilities {
    static EMPTY: OnceLock<MediaCapabilities> = OnceLock::new();
    EMPTY.get_or_init(|| MediaCapabilities::new(Vec::new(), Vec::new()))
}
