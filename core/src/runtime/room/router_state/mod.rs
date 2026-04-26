use std::{collections::BTreeMap, sync::Arc};

use o_sfu_router::{
    Consumer as RouterConsumer, ConsumerCapability, ConsumerId as RouterConsumerId,
    MediaCapabilities, MediaKind as RouterMediaKind, Producer as RouterProducer,
    ProducerId as RouterProducerId, Router, RouterError, RouterId, Session as RouterSession,
    SessionId as RouterSessionId, Transport as RouterTransport,
    TransportDirection as RouterTransportDirection, TransportId as RouterTransportId,
};

use crate::runtime::{
    UserId,
    recording::{RecordingRouterObserver, RecordingService},
};

#[cfg(any(test, feature = "testing-transport"))]
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
pub(super) struct RoomRouterState {
    router: Router<RecordingRouterObserver>,
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
    pub(super) fn new_with_recording_service(
        router_id: RouterId,
        rtp_capabilities: MediaCapabilities,
        recording_service: Arc<RecordingService>,
    ) -> Self {
        Self {
            router: Router::new_with_observer(
                router_id,
                RecordingRouterObserver::new(recording_service),
            ),
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

    /// Ensure the pure router contains a user matching the signaling-layer user.
    ///
    /// The runtime still accepts integer and string signaling user IDs, so this
    /// room-local map keeps that compatibility at the edge while the pure router
    /// continues to use compact numeric identifiers internally.
    ///
    /// TODO: maybe will deprecate the use of string sessiond id and make the code simpler,
    /// in practice in discuss, user ids are number (their actual postgrsql id)
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
    /// no upload transport is available, or producer insertion fails.
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

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] when the consumer user does not
    /// exist, no download transport is available, or consumer insertion fails.
    pub(super) fn add_consumer(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RouterProducerId,
        media_kind: RouterMediaKind,
        capability: ConsumerCapability,
    ) -> Result<RouterConsumerId, RoomRouterStateError> {
        let transport_ids = self.ensure_session_transport_ids(consumer_user_id)?;
        let consumer_id = self.allocate_consumer_id();
        self.router
            .add_consumer(
                RouterConsumer::new(consumer_id, producer_id, transport_ids.download, media_kind),
                capability,
            )
            .map_err(RoomRouterStateError::from)?;
        Ok(consumer_id)
    }

    /// Update the pause state of a producer in the pure router.
    ///
    /// When a producer is paused, the router propagates the pause state to all
    /// dependent consumers (`producer_paused` shadow).
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the producer does not exist.
    pub(super) fn set_producer_paused(
        &mut self,
        producer_id: RouterProducerId,
        paused: bool,
    ) -> Result<(), RoomRouterStateError> {
        self.router
            .set_producer_paused(producer_id, paused)
            .map_err(RoomRouterStateError::from)
    }

    /// Update the local pause state of a consumer in the pure router.
    ///
    /// This controls the consumer's own pause flag independently of the
    /// producer-side pause shadow.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the consumer does not exist.
    pub(super) fn set_consumer_paused(
        &mut self,
        consumer_id: RouterConsumerId,
        paused: bool,
    ) -> Result<(), RoomRouterStateError> {
        self.router
            .set_consumer_paused(consumer_id, paused)
            .map_err(RoomRouterStateError::from)
    }

    /// Remove a consumer from the pure router.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the consumer does not exist.
    pub(super) fn remove_consumer(
        &mut self,
        consumer_id: RouterConsumerId,
    ) -> Result<(), RoomRouterStateError> {
        self.router
            .remove_consumer(consumer_id)
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
