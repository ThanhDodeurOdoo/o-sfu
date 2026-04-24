use std::{collections::BTreeMap, sync::Arc};

use o_sfu_protocol::shared::SessionId;
use o_sfu_router::{
    Consumer as RouterConsumer, ConsumerCapability, ConsumerId as RouterConsumerId,
    MediaCapabilities, MediaKind as RouterMediaKind, Producer as RouterProducer,
    ProducerId as RouterProducerId, Router, RouterError, RouterId, Session as RouterSession,
    SessionId as RouterSessionId, SessionPermissions as RouterSessionPermissions,
    StreamType as RouterStreamType, Transport as RouterTransport,
    TransportDirection as RouterTransportDirection, TransportId as RouterTransportId,
};

use crate::runtime::recording::{RecordingRouterObserver, RecordingService};

#[cfg(test)]
mod test_support;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) enum ChannelRouterStateError {
    MissingSessionMapping { session_id: SessionId },
    Router(RouterError),
}

impl From<RouterError> for ChannelRouterStateError {
    fn from(error: RouterError) -> Self {
        Self::Router(error)
    }
}

#[derive(Debug, Clone)]
pub(super) struct ChannelRouterState {
    router: Router<RecordingRouterObserver>,
    rtp_capabilities: MediaCapabilities,
    router_session_ids_by_session_id: BTreeMap<SessionId, RouterSessionId>,
    transport_ids_by_session_id: BTreeMap<SessionId, SessionTransportIds>,
    next_transport_id: u64,
    next_producer_id: u64,
    next_consumer_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionTransportIds {
    upload: RouterTransportId,
    download: RouterTransportId,
}

impl ChannelRouterState {
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
            router_session_ids_by_session_id: BTreeMap::new(),
            transport_ids_by_session_id: BTreeMap::new(),
            next_transport_id: 1,
            next_producer_id: 1,
            next_consumer_id: 1,
        }
    }

    pub(super) fn rtp_capabilities(&self) -> &MediaCapabilities {
        &self.rtp_capabilities
    }

    /// Ensure the pure router contains a session matching the signaling-layer session.
    ///
    /// The runtime still accepts integer and string signaling session IDs, so this
    /// channel-local map keeps that compatibility at the edge while the pure router
    /// continues to use compact numeric identifiers internally.
    ///
    /// TODO: maybe will deprecate the use of string sessiond id and make the code simpler,
    /// in practice in discuss, session ids are number (their actual postgrsql id)
    ///
    /// # Errors
    ///
    /// potentially the underlying [`RouterError`] if joining the pure router fails
    pub(super) fn ensure_session(
        &mut self,
        session_id: &SessionId,
        router_session_seed: u64,
        permissions: RouterSessionPermissions,
    ) -> Result<(), ChannelRouterStateError> {
        if self
            .router_session_ids_by_session_id
            .contains_key(session_id)
        {
            return self.update_session_permissions(session_id, permissions);
        }
        let router_session_id = RouterSessionId(router_session_seed);
        self.router
            .join_session(RouterSession::new(router_session_id, permissions))
            .map_err(ChannelRouterStateError::from)?;
        self.router_session_ids_by_session_id
            .insert(session_id.clone(), router_session_id);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] when the pure router cannot open
    /// either directional transport for the session.
    pub(super) fn ensure_session_transports(
        &mut self,
        session_id: &SessionId,
    ) -> Result<(), ChannelRouterStateError> {
        self.ensure_session_transport_ids(session_id).map(|_| ())
    }

    fn ensure_session_transport_ids(
        &mut self,
        session_id: &SessionId,
    ) -> Result<SessionTransportIds, ChannelRouterStateError> {
        if let Some(transport_ids) = self.transport_ids_by_session_id.get(session_id).copied() {
            return Ok(transport_ids);
        }
        let router_session_id = self.router_session_id(session_id)?;
        let upload_transport_id = self.allocate_transport_id();
        let download_transport_id = self.allocate_transport_id();
        self.router
            .open_transport(RouterTransport::new(
                upload_transport_id,
                router_session_id,
                RouterTransportDirection::Receive,
            ))
            .map_err(ChannelRouterStateError::from)?;
        self.router
            .open_transport(RouterTransport::new(
                download_transport_id,
                router_session_id,
                RouterTransportDirection::Send,
            ))
            .map_err(ChannelRouterStateError::from)?;
        let transport_ids = SessionTransportIds {
            upload: upload_transport_id,
            download: download_transport_id,
        };
        self.transport_ids_by_session_id
            .insert(session_id.clone(), transport_ids);
        Ok(transport_ids)
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] when the session does not exist,
    /// no upload transport is available, or producer insertion fails.
    pub(super) fn add_producer(
        &mut self,
        session_id: &SessionId,
        media_kind: RouterMediaKind,
        stream_type: RouterStreamType,
    ) -> Result<RouterProducerId, ChannelRouterStateError> {
        let transport_ids = self.ensure_session_transport_ids(session_id)?;
        let producer_id = self.allocate_producer_id();
        self.router
            .add_producer(RouterProducer::new(
                producer_id,
                transport_ids.upload,
                media_kind,
                stream_type,
            ))
            .map_err(ChannelRouterStateError::from)?;
        Ok(producer_id)
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] when the consumer session does not
    /// exist, no download transport is available, or consumer insertion fails.
    pub(super) fn add_consumer(
        &mut self,
        consumer_session_id: &SessionId,
        producer_id: RouterProducerId,
        media_kind: RouterMediaKind,
        stream_type: RouterStreamType,
        capability: ConsumerCapability,
    ) -> Result<RouterConsumerId, ChannelRouterStateError> {
        let transport_ids = self.ensure_session_transport_ids(consumer_session_id)?;
        let consumer_id = self.allocate_consumer_id();
        self.router
            .add_consumer(
                RouterConsumer::new(
                    consumer_id,
                    producer_id,
                    transport_ids.download,
                    media_kind,
                    stream_type,
                ),
                capability,
            )
            .map_err(ChannelRouterStateError::from)?;
        Ok(consumer_id)
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the signaling/session map and router
    /// state ever diverge.
    pub(super) fn update_session_permissions(
        &mut self,
        session_id: &SessionId,
        permissions: RouterSessionPermissions,
    ) -> Result<(), ChannelRouterStateError> {
        let Some(router_session_id) = self
            .router_session_ids_by_session_id
            .get(session_id)
            .copied()
        else {
            return Ok(());
        };
        self.router
            .update_session_permissions(router_session_id, permissions)
            .map_err(ChannelRouterStateError::from)
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
    ) -> Result<(), ChannelRouterStateError> {
        self.router
            .set_producer_paused(producer_id, paused)
            .map_err(ChannelRouterStateError::from)
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
    ) -> Result<(), ChannelRouterStateError> {
        self.router
            .set_consumer_paused(consumer_id, paused)
            .map_err(ChannelRouterStateError::from)
    }

    /// Remove a consumer from the pure router.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the consumer does not exist.
    pub(super) fn remove_consumer(
        &mut self,
        consumer_id: RouterConsumerId,
    ) -> Result<(), ChannelRouterStateError> {
        self.router
            .remove_consumer(consumer_id)
            .map_err(ChannelRouterStateError::from)
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
    ) -> Result<(), ChannelRouterStateError> {
        self.router
            .remove_producer(producer_id)
            .map_err(ChannelRouterStateError::from)
    }

    /// Remove the pure-router session for the signaling-layer session if one exists.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the runtime/session map and router
    /// state ever diverge.
    pub(super) fn remove_session(
        &mut self,
        session_id: &SessionId,
    ) -> Result<(), ChannelRouterStateError> {
        let Some(router_session_id) = self
            .router_session_ids_by_session_id
            .get(session_id)
            .copied()
        else {
            return Ok(());
        };
        self.router
            .remove_session(router_session_id)
            .map_err(ChannelRouterStateError::from)?;
        self.router_session_ids_by_session_id.remove(session_id);
        self.transport_ids_by_session_id.remove(session_id);
        Ok(())
    }

    fn router_session_id(
        &self,
        session_id: &SessionId,
    ) -> Result<RouterSessionId, ChannelRouterStateError> {
        self.router_session_ids_by_session_id
            .get(session_id)
            .copied()
            .ok_or_else(|| ChannelRouterStateError::MissingSessionMapping {
                session_id: session_id.clone(),
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
