use std::collections::BTreeMap;
use std::sync::Arc;

use o_sfu_router::{
    Consumer as RouterConsumer, ConsumerCapability, ConsumerId as RouterConsumerId,
    MediaKind as RouterMediaKind, Producer as RouterProducer, ProducerId as RouterProducerId,
    Router, RouterError, RouterId, RtpCapabilities, Session as RouterSession,
    SessionId as RouterSessionId, SessionPermissionFlags as RouterSessionPermissionFlags,
    SessionPermissions as RouterSessionPermissions, StreamType as RouterStreamType,
    Transport as RouterTransport, TransportDirection as RouterTransportDirection,
    TransportId as RouterTransportId,
};

use crate::runtime::recording::{RecordingRouterObserver, RecordingService};
use crate::signaling::shared::{SessionId, SessionPermissions as SignalingSessionPermissions};
const MISSING_ROUTER_SESSION_FALLBACK: RouterSessionId = RouterSessionId(0);

#[derive(Debug)]
pub(super) struct ChannelRouterState {
    router: Router<RecordingRouterObserver>,
    rtp_capabilities: RtpCapabilities,
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
        rtp_capabilities: RtpCapabilities,
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

    pub(super) fn rtp_capabilities(&self) -> &RtpCapabilities {
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
        permissions: &SignalingSessionPermissions,
    ) -> Result<(), RouterError> {
        if self
            .router_session_ids_by_session_id
            .contains_key(session_id)
        {
            return self.update_session_permissions(session_id, permissions);
        }
        let router_session_id = RouterSessionId(router_session_seed);
        self.router.join_session(RouterSession::new(
            router_session_id,
            router_permissions(permissions),
        ))?;
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
    ) -> Result<(), RouterError> {
        if self.transport_ids_by_session_id.contains_key(session_id) {
            return Ok(());
        }
        let Some(router_session_id) = self
            .router_session_ids_by_session_id
            .get(session_id)
            .copied()
        else {
            return Err(RouterError::MissingSession(MISSING_ROUTER_SESSION_FALLBACK));
        };
        let upload_transport_id = self.allocate_transport_id();
        let download_transport_id = self.allocate_transport_id();
        self.router.open_transport(RouterTransport::new(
            upload_transport_id,
            router_session_id,
            RouterTransportDirection::Receive,
        ))?;
        self.router.open_transport(RouterTransport::new(
            download_transport_id,
            router_session_id,
            RouterTransportDirection::Send,
        ))?;
        self.transport_ids_by_session_id.insert(
            session_id.clone(),
            SessionTransportIds {
                upload: upload_transport_id,
                download: download_transport_id,
            },
        );
        Ok(())
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
    ) -> Result<RouterProducerId, RouterError> {
        self.ensure_session_transports(session_id)?;
        let Some(transport_ids) = self.transport_ids_by_session_id.get(session_id).copied() else {
            let router_session_id = self
                .router_session_ids_by_session_id
                .get(session_id)
                .copied()
                .unwrap_or(MISSING_ROUTER_SESSION_FALLBACK);
            return Err(RouterError::MissingSession(router_session_id));
        };
        let producer_id = self.allocate_producer_id();
        self.router.add_producer(RouterProducer::new(
            producer_id,
            transport_ids.upload,
            media_kind,
            stream_type,
        ))?;
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
    ) -> Result<RouterConsumerId, RouterError> {
        self.ensure_session_transports(consumer_session_id)?;
        let Some(transport_ids) = self
            .transport_ids_by_session_id
            .get(consumer_session_id)
            .copied()
        else {
            let router_session_id = self
                .router_session_ids_by_session_id
                .get(consumer_session_id)
                .copied()
                .unwrap_or(MISSING_ROUTER_SESSION_FALLBACK);
            return Err(RouterError::MissingSession(router_session_id));
        };
        let consumer_id = self.allocate_consumer_id();
        self.router.add_consumer(
            RouterConsumer::new(
                consumer_id,
                producer_id,
                transport_ids.download,
                media_kind,
                stream_type,
            ),
            capability,
        )?;
        Ok(consumer_id)
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the signaling/session map and router
    /// state ever diverge.
    pub(super) fn update_session_permissions(
        &mut self,
        session_id: &SessionId,
        permissions: &SignalingSessionPermissions,
    ) -> Result<(), RouterError> {
        let Some(router_session_id) = self
            .router_session_ids_by_session_id
            .get(session_id)
            .copied()
        else {
            return Ok(());
        };
        self.router
            .update_session_permissions(router_session_id, router_permissions(permissions))
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
    ) -> Result<(), RouterError> {
        self.router.set_producer_paused(producer_id, paused)
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
    ) -> Result<(), RouterError> {
        self.router.set_consumer_paused(consumer_id, paused)
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
    ) -> Result<(), RouterError> {
        self.router.remove_producer(producer_id)
    }

    /// Remove the pure-router session for the signaling-layer session if one exists.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the runtime/session map and router
    /// state ever diverge.
    pub(super) fn remove_session(&mut self, session_id: &SessionId) -> Result<(), RouterError> {
        let Some(router_session_id) = self
            .router_session_ids_by_session_id
            .get(session_id)
            .copied()
        else {
            return Ok(());
        };
        self.router.remove_session(router_session_id)?;
        self.router_session_ids_by_session_id.remove(session_id);
        self.transport_ids_by_session_id.remove(session_id);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn session_count(&self) -> u64 {
        u64::try_from(self.router.session_count()).unwrap_or(u64::MAX)
    }

    #[cfg(test)]
    pub(super) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<RouterSessionPermissions> {
        let router_session_id = self.router_session_ids_by_session_id.get(session_id)?;
        self.router
            .sessions()
            .find(|session| session.id() == *router_session_id)
            .map(o_sfu_router::Session::permissions)
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

fn router_permissions(permissions: &SignalingSessionPermissions) -> RouterSessionPermissions {
    RouterSessionPermissions::from_flags(RouterSessionPermissionFlags {
        transcription: permissions.transcription.unwrap_or(false),
        audio_recording: permissions.audio_recording.unwrap_or(false),
        video_recording: permissions.video_recording.unwrap_or(false),
    })
}
