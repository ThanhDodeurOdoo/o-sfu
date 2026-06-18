#[cfg(not(kani))]
use std::collections::BTreeMap;

use o_sfu_model::UserId;

#[cfg(kani)]
use crate::model::proof_storage::BTreeMap;
use crate::model::{
    ConsumerCapability, ConsumerId as RouterConsumerId, ConsumerRouteState, ConsumerSpec,
    MediaCapabilities, MediaKind as RouterMediaKind, ProducerId as RouterProducerId,
    ProducerRouteState, ProducerSpec, Router, RouterError, RouterId, Session as RouterSession,
    SessionId as RouterSessionId, TransportId as RouterTransportId,
};

#[cfg(any(test, feature = "test-support"))]
#[path = "../TESTS/topology_router_state_support.rs"]
mod test_support;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterAdapterError {
    MissingSessionMapping { user_id: UserId },
    Router(RouterError),
}

impl From<RouterError> for RouterAdapterError {
    fn from(error: RouterError) -> Self {
        Self::Router(error)
    }
}

/// Adapter around one pure router instance.
///
/// The adapter translates compatibility-facing user identities into compact
/// router identifiers and keeps the upload and download transport pair for
/// each user. It owns no transport runtime resources. Its job is to make pure
/// router mutations line up with the topology state that already accepted the
/// placement transition.
#[derive(Debug, Clone)]
pub struct RouterAdapterState {
    router: Router,
    rtp_capabilities: MediaCapabilities,
    sessions_by_user: BTreeMap<UserId, RouterSessionId>,
    transports_by_user: BTreeMap<UserId, SessionTransportIds>,
    next_transport_id: u64,
    next_producer_id: u64,
    next_consumer_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionTransportIds {
    upload_id: RouterTransportId,
    download_id: RouterTransportId,
}

impl RouterAdapterState {
    pub(super) fn new(router_id: RouterId, rtp_capabilities: MediaCapabilities) -> Self {
        Self {
            router: Router::new(router_id),
            rtp_capabilities,
            sessions_by_user: BTreeMap::new(),
            transports_by_user: BTreeMap::new(),
            next_transport_id: 1,
            next_producer_id: 1,
            next_consumer_id: 1,
        }
    }

    pub(super) fn rtp_capabilities(&self) -> &MediaCapabilities {
        &self.rtp_capabilities
    }

    pub(super) fn mapped_session_count(&self) -> usize {
        self.sessions_by_user.len()
    }

    /// Ensure the pure router contains a user matching the signaling-layer user.
    ///
    /// The runtime still accepts integer and string signaling user IDs, so this
    /// adapter-local map keeps that compatibility at the edge while the pure
    /// router continues to use compact numeric identifiers internally.
    ///
    /// TODO: once Discuss only sends database-backed numeric user IDs, remove
    /// the string compatibility branch and use one identity type across the
    /// signaling boundary.
    ///
    /// # Errors
    ///
    /// potentially the underlying [`RouterError`] if joining the pure router fails
    pub(super) fn ensure_session(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
    ) -> Result<(), RouterAdapterError> {
        if self.sessions_by_user.contains_key(user_id) {
            return Ok(());
        }
        let session_id = RouterSessionId(router_session_seed);
        self.router
            .join(RouterSession::new(session_id))
            .map_err(RouterAdapterError::from)?;
        self.sessions_by_user.insert(user_id.clone(), session_id);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] when the pure router cannot open
    /// either directional transport for the user.
    pub fn ensure_session_transports(
        &mut self,
        user_id: &UserId,
    ) -> Result<(), RouterAdapterError> {
        self.ensure_transport_ids(user_id).map(|_| ())
    }

    fn ensure_transport_ids(
        &mut self,
        user_id: &UserId,
    ) -> Result<SessionTransportIds, RouterAdapterError> {
        if let Some(transport_ids) = self.transports_by_user.get(user_id).copied() {
            return Ok(transport_ids);
        }
        let session_id = self.session_id(user_id)?;
        let upload_id = self.allocate_transport_id();
        let download_id = self.allocate_transport_id();
        self.router
            .session(session_id)
            .and_then(|session| session.open_receive_transport(upload_id))
            .map_err(RouterAdapterError::from)?;
        self.router
            .session(session_id)
            .and_then(|session| session.open_send_transport(download_id))
            .map_err(RouterAdapterError::from)?;
        let transport_ids = SessionTransportIds {
            upload_id,
            download_id,
        };
        self.transports_by_user
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
    ) -> Result<RouterProducerId, RouterAdapterError> {
        let transport_ids = self.ensure_transport_ids(user_id)?;
        let producer_id = self.allocate_producer_id();
        self.router
            .receive_transport(transport_ids.upload_id)
            .and_then(|transport| transport.publish(ProducerSpec::new(producer_id, media_kind)))
            .map_err(RouterAdapterError::from)?;
        Ok(producer_id)
    }

    pub(super) fn add_consumer_with_route_state(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RouterProducerId,
        capability: ConsumerCapability,
        route_state: ConsumerRouteState,
    ) -> Result<RouterConsumerId, RouterAdapterError> {
        let transport_ids = self.ensure_transport_ids(consumer_user_id)?;
        let consumer_id = self.allocate_consumer_id();
        self.router
            .send_transport(transport_ids.download_id)
            .and_then(|transport| {
                transport.consume(
                    ConsumerSpec::new(consumer_id, producer_id, capability)
                        .with_route_state(route_state),
                )
            })
            .map_err(RouterAdapterError::from)?;
        Ok(consumer_id)
    }

    /// Update the source route state of a producer in the pure router.
    ///
    /// This is the topology boundary for producer activity changes. The pure router
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
    ) -> Result<(), RouterAdapterError> {
        self.router
            .set_producer_route_state(producer_id, route_state)
            .map_err(RouterAdapterError::from)
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
    ) -> Result<(), RouterAdapterError> {
        self.router
            .set_consumer_route_state(consumer_id, route_state)
            .map_err(RouterAdapterError::from)
    }

    /// Remove a consumer from the pure router.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the consumer does not exist.
    pub(super) fn remove_consumer(
        &mut self,
        consumer_id: RouterConsumerId,
    ) -> Result<(), RouterAdapterError> {
        self.router
            .remove_consumer(consumer_id)
            .map_err(RouterAdapterError::from)
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
    ) -> Result<(), RouterAdapterError> {
        self.router
            .remove_producer(producer_id)
            .map_err(RouterAdapterError::from)
    }

    /// Remove the pure-router user for the signaling-layer user if one exists.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the runtime/user map and router
    /// state ever diverge.
    pub(super) fn remove_session(&mut self, user_id: &UserId) -> Result<(), RouterAdapterError> {
        let Some(session_id) = self.sessions_by_user.get(user_id).copied() else {
            return Ok(());
        };
        self.router
            .remove_session(session_id)
            .map_err(RouterAdapterError::from)?;
        self.sessions_by_user.remove(user_id);
        self.transports_by_user.remove(user_id);
        Ok(())
    }

    pub(super) fn remove_session_repairing(
        &mut self,
        user_id: &UserId,
    ) -> Result<(), RouterAdapterError> {
        match self.remove_session(user_id) {
            Ok(()) => Ok(()),
            Err(error @ RouterAdapterError::Router(RouterError::MissingSession(_))) => {
                self.forget_session_indexes(user_id);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    fn forget_session_indexes(&mut self, user_id: &UserId) {
        self.sessions_by_user.remove(user_id);
        self.transports_by_user.remove(user_id);
    }

    fn session_id(&self, user_id: &UserId) -> Result<RouterSessionId, RouterAdapterError> {
        self.sessions_by_user.get(user_id).copied().ok_or_else(|| {
            RouterAdapterError::MissingSessionMapping {
                user_id: user_id.clone(),
            }
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
