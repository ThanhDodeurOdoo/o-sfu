use std::{collections::BTreeMap, sync::OnceLock};

#[cfg(test)]
use o_sfu_router::SessionPermissions as RouterSessionPermissions;
use o_sfu_router::{
    ConsumerId as RouterConsumerId, MediaKind as RouterMediaKind, ProducerId as RouterProducerId,
    RouterError, RouterId, RtpCapabilities, SessionId as RouterSessionId,
    StreamType as RouterStreamType,
};

use super::router_state::ChannelRouterState;
use crate::signaling::shared::{
    SessionId, SessionInfo as SignalingSessionInfo,
    SessionPermissions as SignalingSessionPermissions,
};

const MISSING_ROUTER_SESSION_FALLBACK: RouterSessionId = RouterSessionId(0);

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

#[derive(Debug)]
pub(super) struct ChannelTopology {
    primary_router: RouterId,
    routers: BTreeMap<RouterId, ChannelRouterState>,
    session_home_router: BTreeMap<SessionId, RouterId>,
}

impl ChannelTopology {
    pub(super) fn new(primary_router_id: RouterId) -> Self {
        let mut routers = BTreeMap::new();
        routers.insert(
            primary_router_id,
            ChannelRouterState::new(primary_router_id),
        );
        Self {
            primary_router: primary_router_id,
            routers,
            session_home_router: BTreeMap::new(),
        }
    }

    pub(super) fn rtp_capabilities(&self) -> &RtpCapabilities {
        let Some(primary_router) = self.routers.get(&self.primary_router) else {
            return empty_router_capabilities();
        };
        primary_router.rtp_capabilities()
    }

    pub(super) fn ensure_session(
        &mut self,
        session_id: &SessionId,
        router_session_seed: u64,
        permissions: &SignalingSessionPermissions,
    ) -> Result<(), RouterError> {
        let router_id = self
            .session_home_router
            .get(session_id)
            .copied()
            .unwrap_or(self.primary_router);
        let router = self.router_mut(router_id)?;
        router.ensure_session(session_id, router_session_seed, permissions)?;
        self.session_home_router
            .insert(session_id.clone(), router_id);
        Ok(())
    }

    pub(super) fn ensure_session_transports(
        &mut self,
        session_id: &SessionId,
    ) -> Result<(), RouterError> {
        let router_id = self.router_id_for_session(session_id);
        self.router_mut(router_id)?
            .ensure_session_transports(session_id)
    }

    pub(super) fn apply_client_join(
        &mut self,
        session_id: &SessionId,
        router_session_seed: u64,
        permissions: &SignalingSessionPermissions,
    ) -> Result<(), RouterError> {
        let is_existing_session = self.session_home_router.contains_key(session_id);
        self.ensure_session(session_id, router_session_seed, permissions)?;
        self.ensure_session_transports(session_id)?;
        if is_existing_session {
            self.update_session_info(session_id, &SignalingSessionInfo::default())?;
        }
        Ok(())
    }

    pub(super) fn apply_client_leave(&mut self, session_id: &SessionId) -> Result<(), RouterError> {
        self.remove_session(session_id)
    }

    pub(super) fn add_producer(
        &mut self,
        session_id: &SessionId,
        media_kind: RouterMediaKind,
        stream_type: RouterStreamType,
    ) -> Result<RoutedProducerId, RouterError> {
        let router_id = self.router_id_for_session(session_id);
        let producer_id =
            self.router_mut(router_id)?
                .add_producer(session_id, media_kind, stream_type)?;
        Ok(RoutedProducerId::new(router_id, producer_id))
    }

    pub(super) fn add_consumer(
        &mut self,
        consumer_session_id: &SessionId,
        producer_id: RoutedProducerId,
        media_kind: RouterMediaKind,
        stream_type: RouterStreamType,
        capable: bool,
    ) -> Result<RoutedConsumerId, RouterError> {
        let consumer_id = self.router_mut(producer_id.router_id())?.add_consumer(
            consumer_session_id,
            producer_id.producer_id(),
            media_kind,
            stream_type,
            capable,
        )?;
        Ok(RoutedConsumerId::new(producer_id.router_id(), consumer_id))
    }

    pub(super) fn update_session_info(
        &mut self,
        session_id: &SessionId,
        info: &SignalingSessionInfo,
    ) -> Result<(), RouterError> {
        let router_id = self.router_id_for_session(session_id);
        self.router_mut(router_id)?
            .update_session_info(session_id, info)
    }

    pub(super) fn set_producer_paused(
        &mut self,
        producer_id: RoutedProducerId,
        paused: bool,
    ) -> Result<(), RouterError> {
        self.router_mut(producer_id.router_id())?
            .set_producer_paused(producer_id.producer_id(), paused)
    }

    pub(super) fn set_consumer_paused(
        &mut self,
        consumer_id: RoutedConsumerId,
        paused: bool,
    ) -> Result<(), RouterError> {
        self.router_mut(consumer_id.router_id())?
            .set_consumer_paused(consumer_id.consumer_id(), paused)
    }

    pub(super) fn remove_session(&mut self, session_id: &SessionId) -> Result<(), RouterError> {
        let router_id = self.router_id_for_session(session_id);
        self.router_mut(router_id)?.remove_session(session_id)?;
        self.session_home_router.remove(session_id);
        Ok(())
    }

    pub(super) fn session_count(&self) -> u64 {
        self.routers
            .values()
            .map(ChannelRouterState::session_count)
            .sum()
    }

    pub(super) fn camera_count(&self) -> u64 {
        self.routers
            .values()
            .map(ChannelRouterState::camera_count)
            .sum()
    }

    pub(super) fn screen_count(&self) -> u64 {
        self.routers
            .values()
            .map(ChannelRouterState::screen_count)
            .sum()
    }

    #[cfg(test)]
    pub(super) fn home_router_id_for_session(&self, session_id: &SessionId) -> Option<RouterId> {
        self.session_home_router.get(session_id).copied()
    }

    #[cfg(test)]
    pub(super) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<RouterSessionPermissions> {
        let router_id = self.session_home_router.get(session_id).copied()?;
        self.routers
            .get(&router_id)?
            .session_permissions(session_id)
    }

    fn router_id_for_session(&self, session_id: &SessionId) -> RouterId {
        self.session_home_router
            .get(session_id)
            .copied()
            .unwrap_or(self.primary_router)
    }

    fn router_mut(&mut self, router_id: RouterId) -> Result<&mut ChannelRouterState, RouterError> {
        self.routers
            .get_mut(&router_id)
            .ok_or(RouterError::MissingSession(MISSING_ROUTER_SESSION_FALLBACK))
    }
}

fn empty_router_capabilities() -> &'static RtpCapabilities {
    static EMPTY: OnceLock<RtpCapabilities> = OnceLock::new();
    EMPTY.get_or_init(|| RtpCapabilities::new(Vec::new(), Vec::new()))
}
