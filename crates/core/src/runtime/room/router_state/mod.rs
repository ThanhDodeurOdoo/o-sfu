use std::{collections::BTreeMap, sync::Arc};

use o_sfu_router::{
    Consumer as RouterConsumer, ConsumerCapability, ConsumerId as RouterConsumerId,
    ConsumerRouteState, MediaCapabilities, MediaKind as RouterMediaKind,
    Producer as RouterProducer, ProducerId as RouterProducerId, ProducerRouteState, Router,
    RouterError, RouterId, Session as RouterSession, SessionId as RouterSessionId,
    Transport as RouterTransport, TransportDirection as RouterTransportDirection,
    TransportId as RouterTransportId,
};

use crate::runtime::{
    UserId,
    router_events::{RoomRouterEventSink, RoomRouterObserver},
};

#[cfg(test)]
mod test_support;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) enum RoomRouterStateError {
    MissingSessionMapping { user_id: UserId },
    Router(RouterError),
}

impl From<RouterError> for RoomRouterStateError {
    fn from(error: RouterError) -> Self {
        Self::Router(error)
    }
}

#[derive(Debug, Clone)]
/// Room-owned adapter around one pure router instance.
///
/// The adapter translates compatibility-facing room identities into compact
/// router identifiers and keeps the upload and download transport pair for
/// each user. It owns no transport runtime resources. Its job is to make pure
/// router mutations line up with the room state that already accepted the
/// signaling transition.
pub(super) struct RoomRouterState {
    router: Router<RoomRouterObserver>,
    rtp_capabilities: MediaCapabilities,
    router_user_ids_by_user_id: BTreeMap<UserId, RouterSessionId>,
    transport_ids_by_user_id: BTreeMap<UserId, SessionTransportIds>,
    next_transport_id: u64,
    next_producer_id: u64,
    next_consumer_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionTransportIds {
    upload: RouterTransportId,
    download: RouterTransportId,
}

impl RoomRouterState {
    pub(super) fn new(
        router_id: RouterId,
        rtp_capabilities: MediaCapabilities,
        event_sink: Arc<dyn RoomRouterEventSink>,
    ) -> Self {
        Self {
            router: Router::new_with_observer(router_id, RoomRouterObserver::new(event_sink)),
            rtp_capabilities,
            router_user_ids_by_user_id: BTreeMap::new(),
            transport_ids_by_user_id: BTreeMap::new(),
            next_transport_id: 1,
            next_producer_id: 1,
            next_consumer_id: 1,
        }
    }

    pub(super) fn rtp_capabilities(&self) -> &MediaCapabilities {
        &self.rtp_capabilities
    }

    pub(super) fn mapped_session_count(&self) -> usize {
        self.router_user_ids_by_user_id.len()
    }

    /// Ensure the pure router contains a user matching the signaling-layer user.
    ///
    /// The runtime still accepts integer and string signaling user IDs, so this
    /// room-local map keeps that compatibility at the edge while the pure router
    /// continues to use compact numeric identifiers internally.
    ///
    /// TODO: once Discuss only sends database-backed numeric user IDs, remove
    /// the string compatibility branch and use one identity type across the
    /// room boundary.
    ///
    /// # Errors
    ///
    /// potentially the underlying [`RouterError`] if joining the pure router fails
    pub(super) fn ensure_session(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
    ) -> Result<(), RoomRouterStateError> {
        if self.router_user_ids_by_user_id.contains_key(user_id) {
            return Ok(());
        }
        let router_user_id = RouterSessionId(router_session_seed);
        self.router
            .join_session(RouterSession::new(router_user_id))
            .map_err(RoomRouterStateError::from)?;
        self.router_user_ids_by_user_id
            .insert(user_id.clone(), router_user_id);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] when the pure router cannot open
    /// either directional transport for the user.
    pub(super) fn ensure_session_transports(
        &mut self,
        user_id: &UserId,
    ) -> Result<(), RoomRouterStateError> {
        self.ensure_session_transport_ids(user_id).map(|_| ())
    }

    fn ensure_session_transport_ids(
        &mut self,
        user_id: &UserId,
    ) -> Result<SessionTransportIds, RoomRouterStateError> {
        if let Some(transport_ids) = self.transport_ids_by_user_id.get(user_id).copied() {
            return Ok(transport_ids);
        }
        let router_user_id = self.router_user_id(user_id)?;
        let upload_transport_id = self.allocate_transport_id();
        let download_transport_id = self.allocate_transport_id();
        self.router
            .open_transport(RouterTransport::new(
                upload_transport_id,
                router_user_id,
                RouterTransportDirection::Receive,
            ))
            .map_err(RoomRouterStateError::from)?;
        self.router
            .open_transport(RouterTransport::new(
                download_transport_id,
                router_user_id,
                RouterTransportDirection::Send,
            ))
            .map_err(RoomRouterStateError::from)?;
        let transport_ids = SessionTransportIds {
            upload: upload_transport_id,
            download: download_transport_id,
        };
        self.transport_ids_by_user_id
            .insert(user_id.clone(), transport_ids);
        Ok(transport_ids)
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] when the user does not exist,
    /// no upload transport is available or producer insertion fails.
    pub(super) fn add_producer(
        &mut self,
        user_id: &UserId,
        media_kind: RouterMediaKind,
    ) -> Result<RouterProducerId, RoomRouterStateError> {
        let transport_ids = self.ensure_session_transport_ids(user_id)?;
        let producer_id = self.allocate_producer_id();
        self.router
            .add_producer(RouterProducer::new(
                producer_id,
                transport_ids.upload,
                media_kind,
            ))
            .map_err(RoomRouterStateError::from)?;
        Ok(producer_id)
    }

    pub(super) fn add_consumer_with_route_state(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RouterProducerId,
        media_kind: RouterMediaKind,
        capability: ConsumerCapability,
        route_state: ConsumerRouteState,
    ) -> Result<RouterConsumerId, RoomRouterStateError> {
        let transport_ids = self.ensure_session_transport_ids(consumer_user_id)?;
        let consumer_id = self.allocate_consumer_id();
        self.router
            .add_consumer(
                RouterConsumer::new(consumer_id, producer_id, transport_ids.download, media_kind)
                    .with_route_state(route_state),
                capability,
            )
            .map_err(RoomRouterStateError::from)?;
        Ok(consumer_id)
    }

    /// Update the source route state of a producer in the pure router.
    ///
    /// This is the room boundary for producer activity changes. The pure router
    /// propagates the producer route state to each dependent consumer's source
    /// shadow while preserving every consumer-local subscription state.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the producer does not exist.
    pub(super) fn set_producer_route_state(
        &mut self,
        producer_id: RouterProducerId,
        route_state: ProducerRouteState,
    ) -> Result<(), RoomRouterStateError> {
        self.router
            .set_producer_route_state(producer_id, route_state)
            .map_err(RoomRouterStateError::from)
    }

    /// Update the local route state of a consumer in the pure router.
    ///
    /// This controls the receiver's own route choice independently of the
    /// producer-side shadow stored on the same consumer.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the consumer does not exist.
    pub(super) fn set_consumer_route_state(
        &mut self,
        consumer_id: RouterConsumerId,
        route_state: ConsumerRouteState,
    ) -> Result<(), RoomRouterStateError> {
        self.router
            .set_consumer_route_state(consumer_id, route_state)
            .map_err(RoomRouterStateError::from)
    }

    /// Remove a producer from the pure router.
    ///
    /// The router tears down dependent consumers as part of the same transition.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the producer does not exist.
    pub(super) fn remove_producer(
        &mut self,
        producer_id: RouterProducerId,
    ) -> Result<(), RoomRouterStateError> {
        self.router
            .remove_producer(producer_id)
            .map_err(RoomRouterStateError::from)
    }

    /// Remove the pure-router user for the signaling-layer user if one exists.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the runtime/user map and router
    /// state ever diverge.
    pub(super) fn remove_session(&mut self, user_id: &UserId) -> Result<(), RoomRouterStateError> {
        let Some(router_user_id) = self.router_user_ids_by_user_id.get(user_id).copied() else {
            return Ok(());
        };
        self.router
            .remove_session(router_user_id)
            .map_err(RoomRouterStateError::from)?;
        self.router_user_ids_by_user_id.remove(user_id);
        self.transport_ids_by_user_id.remove(user_id);
        Ok(())
    }

    pub(super) fn remove_session_repairing(
        &mut self,
        user_id: &UserId,
    ) -> Result<(), RoomRouterStateError> {
        match self.remove_session(user_id) {
            Ok(()) => Ok(()),
            Err(error @ RoomRouterStateError::Router(RouterError::MissingSession(_))) => {
                self.forget_session_indexes(user_id);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    fn forget_session_indexes(&mut self, user_id: &UserId) {
        self.router_user_ids_by_user_id.remove(user_id);
        self.transport_ids_by_user_id.remove(user_id);
    }

    fn router_user_id(&self, user_id: &UserId) -> Result<RouterSessionId, RoomRouterStateError> {
        self.router_user_ids_by_user_id
            .get(user_id)
            .copied()
            .ok_or_else(|| RoomRouterStateError::MissingSessionMapping {
                user_id: user_id.clone(),
            })
    }

    fn allocate_transport_id(&mut self) -> RouterTransportId {
        let transport_id = RouterTransportId(self.next_transport_id);
        self.next_transport_id = self.next_transport_id.saturating_add(1);
        transport_id
    }

    fn allocate_producer_id(&mut self) -> RouterProducerId {
        let producer_id = RouterProducerId(self.next_producer_id);
        self.next_producer_id = self.next_producer_id.saturating_add(1);
        producer_id
    }

    fn allocate_consumer_id(&mut self) -> RouterConsumerId {
        let consumer_id = RouterConsumerId(self.next_consumer_id);
        self.next_consumer_id = self.next_consumer_id.saturating_add(1);
        consumer_id
    }
}
