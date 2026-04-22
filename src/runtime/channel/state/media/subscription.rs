use o_sfu_router::{
    ConsumerCapability, MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters,
    can_consume, negotiate_consumer_rtp_parameters,
};
use tracing::{error, warn};

use crate::runtime::ConnectionId;
use crate::runtime::transport_adapter::TransportMediaId;
use o_sfu_protocol::shared::{DownloadStates, SessionId, StreamType};

use super::super::{
    super::{ChannelEventRequest, outbound::OutboundSender, topology::RoutedProducerId},
    ids::{ConsumerRuntimeId, ProducerRuntimeId},
    shared::{ChannelState, ConsumerKey, ConsumerState, PublishedProducer},
};
use super::router_stream_type::to_router_stream_type;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ConsumerRouteUpdate {
    consumer_state: ConsumerState,
    stream_type: StreamType,
    active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumerRouteState {
    Absent,
    Inactive,
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ConsumerKeyframeRefreshTarget {
    consumer_media: TransportMediaId,
    producer_session_id: SessionId,
    producer_connection_id: ConnectionId,
    source_media: TransportMediaId,
}

#[derive(Debug, Default)]
pub(in crate::runtime::channel) struct PlannedSubscriptionChange {
    route_updates: Vec<ConsumerRouteUpdate>,
    bootstraps: Vec<PlannedConsumerBootstrap>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PendingConsumerBootstrapTarget {
    pub(super) consumer_session_id: SessionId,
    pub(super) consumer_connection_id: ConnectionId,
    producer: ConsumerBootstrapProducerSnapshot,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct ConsumerBootstrapProducerSnapshot {
    owner_session_id: SessionId,
    owner_connection_id: ConnectionId,
    producer_id: ProducerRuntimeId,
    stream_type: StreamType,
    media_kind: RouterMediaKind,
    transport_media_id: TransportMediaId,
    routed_producer_id: Option<RoutedProducerId>,
    active: Option<bool>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PreparedConsumerBootstrap {
    consumer_rtp_parameters: RouterRtpParameters,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PendingConsumerBootstrap {
    consumer_key: ConsumerKey,
    sender: OutboundSender,
    bootstrap: RemoteTrackBootstrap,
    consumer_active: bool,
    producer: ConsumerBootstrapProducerSnapshot,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PlannedConsumerBootstrap {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending_bootstrap: PendingConsumerBootstrap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteTrackBootstrap {
    consumer_id: ConsumerRuntimeId,
    media_kind: RouterMediaKind,
    mid: String,
    producer_id: ProducerRuntimeId,
    rtp_parameters: RouterRtpParameters,
    session_id: SessionId,
    active: bool,
    stream_type: StreamType,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::channel) enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
    Subscribe,
}

impl ChannelState {
    pub(in crate::runtime::channel) fn plan_subscription_change(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
    ) -> PlannedSubscriptionChange {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return PlannedSubscriptionChange::default();
        };
        let existing_states = session
            .desired_download_states
            .entry(target_session_id.clone())
            .or_default();
        merge_download_states(existing_states, states);
        if download_states_are_empty(existing_states) {
            session.desired_download_states.remove(target_session_id);
        }
        let route_updates = self.apply_subscription_route_updates(
            session_id,
            connection_id,
            target_session_id,
            states,
        );
        let bootstraps = self.plan_consumer_bootstraps_for_targets(
            self.collect_missing_consumer_targets_for_peer(
                session_id,
                connection_id,
                target_session_id,
            ),
        );
        PlannedSubscriptionChange {
            route_updates,
            bootstraps,
        }
    }

    pub(in crate::runtime::channel) fn plan_missing_consumer_bootstraps_for_connection(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> Option<Vec<PlannedConsumerBootstrap>> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != connection_id {
            return None;
        }
        if !session.negotiation.can_consume() {
            return Some(Vec::new());
        }
        Some(self.plan_consumer_bootstraps_for_targets(
            self.collect_missing_consumer_targets(session_id, connection_id),
        ))
    }

    pub(in crate::runtime::channel) fn plan_consumer_bootstraps_for_targets(
        &mut self,
        targets: Vec<PendingConsumerBootstrapTarget>,
    ) -> Vec<PlannedConsumerBootstrap> {
        targets
            .into_iter()
            .filter_map(|target| self.plan_consumer_bootstrap(&target))
            .collect()
    }

    fn apply_subscription_route_updates(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
    ) -> Vec<ConsumerRouteUpdate> {
        let mut accepted_updates = Vec::new();
        for (stream_type, active) in states.iter() {
            let key = ConsumerKey::new(session_id, target_session_id, stream_type);
            let Some(current_consumer_state) = self.consumer_index.get(&key).copied() else {
                continue;
            };
            if current_consumer_state.consumer_connection_id != connection_id {
                continue;
            }
            let paused = !active;
            if self
                .topology
                .set_consumer_paused(current_consumer_state.routed_consumer_id, paused)
                .is_err()
            {
                error!(
                    ?session_id,
                    ?target_session_id,
                    ?stream_type,
                    "failed to set consumer pause state in channel router"
                );
                continue;
            }
            accepted_updates.push(ConsumerRouteUpdate {
                consumer_state: current_consumer_state,
                stream_type,
                active,
            });
        }
        accepted_updates
    }

    fn collect_missing_consumer_targets(
        &self,
        session_id: &SessionId,
        consumer_connection_id: ConnectionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                self.pending_consumer_target(
                    session_id,
                    consumer_connection_id,
                    *producer_id,
                    producer,
                )
            })
            .collect()
    }

    fn collect_missing_consumer_targets_for_peer(
        &self,
        session_id: &SessionId,
        consumer_connection_id: ConnectionId,
        target_session_id: &SessionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                if producer.owner_session_id != *target_session_id {
                    return None;
                }
                self.pending_consumer_target(
                    session_id,
                    consumer_connection_id,
                    *producer_id,
                    producer,
                )
            })
            .collect()
    }

    fn pending_consumer_target(
        &self,
        session_id: &SessionId,
        consumer_connection_id: ConnectionId,
        producer_id: ProducerRuntimeId,
        producer: &PublishedProducer,
    ) -> Option<PendingConsumerBootstrapTarget> {
        let transport_media_id = producer.transport_media_id?;
        if producer.owner_session_id == *session_id {
            return None;
        }
        let consumer_key =
            ConsumerKey::new(session_id, &producer.owner_session_id, producer.stream_type);
        if self.consumer_bootstrap_exists(&consumer_key) {
            return None;
        }
        Some(PendingConsumerBootstrapTarget::new(
            session_id.clone(),
            consumer_connection_id,
            ConsumerBootstrapProducerSnapshot::pending(
                producer.owner_session_id.clone(),
                producer.owner_connection_id,
                producer_id,
                producer.stream_type,
                producer.media_kind,
                transport_media_id,
            ),
        ))
    }

    fn plan_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Option<PlannedConsumerBootstrap> {
        let (sender, client_capabilities) = {
            let session = self.sessions.get(&target.consumer_session_id)?;
            if session.connection_id != target.consumer_connection_id
                || !session.negotiation.can_consume()
            {
                return None;
            }
            (
                session.sender.clone(),
                session.parsed_client_rtp_capabilities.clone()?,
            )
        };
        let producer = self.producers.get(&target.producer.producer_id)?;
        if !target.producer.matches_pending_producer(producer) {
            return None;
        }
        let producer_consumable_rtp_parameters = producer.consumable_rtp_parameters.clone();
        let prepared_producer = target
            .producer
            .with_commit_snapshot(producer.routed_producer_id, producer.active);
        let consumer_active = self.desired_download_active(
            &target.consumer_session_id,
            target.producer_session_id(),
            target.stream_type(),
        );
        if !can_consume(&producer_consumable_rtp_parameters, &client_capabilities) {
            return None;
        }
        let negotiated_rtp_parameters = negotiate_consumer_rtp_parameters(
            &producer_consumable_rtp_parameters,
            &client_capabilities,
        )
        .ok()?;
        let consumer_key = ConsumerKey::new(
            &target.consumer_session_id,
            target.producer_session_id(),
            target.stream_type(),
        );
        if self.consumer_bootstrap_exists(&consumer_key) {
            return None;
        }
        self.pending_consumer_bootstraps
            .insert(consumer_key.clone());
        let consumer_id = ConsumerRuntimeId::allocate(&mut self.next_consumer_id);
        Some(PlannedConsumerBootstrap {
            target: target.clone(),
            prepared: PreparedConsumerBootstrap {
                consumer_rtp_parameters: negotiated_rtp_parameters.clone(),
            },
            pending_bootstrap: PendingConsumerBootstrap {
                consumer_key,
                sender,
                bootstrap: RemoteTrackBootstrap {
                    consumer_id,
                    media_kind: prepared_producer.media_kind,
                    mid: negotiated_rtp_parameters
                        .mid()
                        .map_or_else(|| consumer_id.into_wire_id(), ToOwned::to_owned),
                    producer_id: prepared_producer.producer_id,
                    rtp_parameters: negotiated_rtp_parameters,
                    session_id: prepared_producer.owner_session_id.clone(),
                    active: prepared_producer.active.unwrap_or(true),
                    stream_type: prepared_producer.stream_type,
                },
                consumer_active,
                producer: prepared_producer,
            },
        })
    }

    pub(in crate::runtime::channel) fn commit_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        mut pending: PendingConsumerBootstrap,
        consumer_transport_media_id: TransportMediaId,
        consumer_mid: Option<String>,
    ) -> Option<(OutboundSender, RemoteTrackBootstrap, bool)> {
        self.pending_consumer_bootstraps
            .remove(&pending.consumer_key);
        let session = self.sessions.get(&target.consumer_session_id)?;
        if session.connection_id != target.consumer_connection_id
            || !session.negotiation.can_consume()
        {
            return None;
        }
        let producer = self.producers.get(&pending.producer.producer_id)?;
        if !pending.producer.matches_committed_producer(producer) {
            return None;
        }
        if self.consumer_index.contains_key(&pending.consumer_key) {
            return None;
        }
        let routed_consumer_id = match self.topology.add_consumer(
            &target.consumer_session_id,
            pending.producer.routed_producer_id?,
            pending.producer.media_kind,
            to_router_stream_type(pending.producer.stream_type),
            ConsumerCapability::Compatible,
        ) {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    consumer_session_id = ?target.consumer_session_id,
                    producer_id = %pending.producer.producer_id,
                    "router rejected consumer creation"
                );
                return None;
            }
        };
        if let Some(consumer_mid) = consumer_mid {
            pending.bootstrap.mid = consumer_mid;
        }
        if !pending.consumer_active
            && self
                .topology
                .set_consumer_paused(routed_consumer_id, true)
                .is_err()
        {
            error!(
                consumer_session_id = ?target.consumer_session_id,
                producer_id = %pending.producer.producer_id,
                "failed to mirror initial consumer pause state into channel router"
            );
            return None;
        }
        self.consumer_index.insert(
            pending.consumer_key,
            ConsumerState {
                routed_consumer_id,
                consumer_connection_id: target.consumer_connection_id,
                source_connection_id: pending.producer.owner_connection_id,
                source_media: target.transport_media_id(),
                consumer_media: consumer_transport_media_id,
            },
        );
        Some((pending.sender, pending.bootstrap, pending.consumer_active))
    }

    pub(in crate::runtime::channel) fn release_pending_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) {
        self.pending_consumer_bootstraps.remove(&ConsumerKey::new(
            &target.consumer_session_id,
            target.producer_session_id(),
            target.stream_type(),
        ));
    }

    pub(in crate::runtime::channel) fn desired_download_active(
        &self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        stream_type: StreamType,
    ) -> bool {
        self.sessions
            .get(session_id)
            .and_then(|session| session.desired_download_states.get(target_session_id))
            .and_then(|states| download_state_for_stream_type(states, stream_type))
            .unwrap_or(true)
    }

    pub(in crate::runtime::channel) fn consumer_route_state(
        &self,
        consumer_session_id: &SessionId,
        producer_session_id: &SessionId,
        stream_type: StreamType,
    ) -> Option<ConsumerRouteState> {
        self.sessions.get(consumer_session_id)?;
        let consumer_key = ConsumerKey::new(consumer_session_id, producer_session_id, stream_type);
        if !self.consumer_index.contains_key(&consumer_key) {
            return Some(ConsumerRouteState::Absent);
        }
        let Some(producer_id) =
            self.producer_ids_by_owner_stream
                .get(&super::super::shared::ProducerKey::new(
                    producer_session_id,
                    stream_type,
                ))
        else {
            return Some(ConsumerRouteState::Absent);
        };
        let Some(producer) = self.producers.get(producer_id) else {
            return Some(ConsumerRouteState::Absent);
        };
        let route_active = producer.active
            && self.desired_download_active(consumer_session_id, producer_session_id, stream_type);
        Some(if route_active {
            ConsumerRouteState::Active
        } else {
            ConsumerRouteState::Inactive
        })
    }

    pub(in crate::runtime::channel) fn active_video_consumer_keyframe_refresh_targets(
        &self,
        consumer_session_id: &SessionId,
        consumer_connection_id: ConnectionId,
    ) -> Option<Vec<ConsumerKeyframeRefreshTarget>> {
        let session = self.sessions.get(consumer_session_id)?;
        if session.connection_id != consumer_connection_id {
            return None;
        }
        Some(
            self.consumer_index
                .iter()
                .filter_map(|(key, consumer_state)| {
                    if key.consumer_session_id != *consumer_session_id
                        || consumer_state.consumer_connection_id != consumer_connection_id
                        || !matches!(key.stream_type, StreamType::Camera | StreamType::Screen)
                    {
                        return None;
                    }
                    let producer_id = self.producer_ids_by_owner_stream.get(
                        &super::super::shared::ProducerKey::new(
                            &key.producer_session_id,
                            key.stream_type,
                        ),
                    )?;
                    let producer = self.producers.get(producer_id)?;
                    if !producer.active
                        || !self.desired_download_active(
                            consumer_session_id,
                            &key.producer_session_id,
                            key.stream_type,
                        )
                    {
                        return None;
                    }
                    Some(ConsumerKeyframeRefreshTarget {
                        consumer_media: consumer_state.consumer_media,
                        producer_session_id: key.producer_session_id.clone(),
                        producer_connection_id: consumer_state.source_connection_id,
                        source_media: consumer_state.source_media,
                    })
                })
                .collect(),
        )
    }

    pub(in crate::runtime::channel) fn consumer_bootstrap_exists(
        &self,
        consumer_key: &ConsumerKey,
    ) -> bool {
        self.consumer_index.contains_key(consumer_key)
            || self.pending_consumer_bootstraps.contains(consumer_key)
    }
}

impl PlannedSubscriptionChange {
    pub(in crate::runtime::channel) fn into_parts(
        self,
    ) -> (Vec<ConsumerRouteUpdate>, Vec<PlannedConsumerBootstrap>) {
        (self.route_updates, self.bootstraps)
    }
}

impl ConsumerRouteUpdate {
    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_state.consumer_connection_id
    }

    pub(in crate::runtime::channel) const fn consumer_media(&self) -> TransportMediaId {
        self.consumer_state.consumer_media
    }

    pub(in crate::runtime::channel) const fn source_connection_id(&self) -> ConnectionId {
        self.consumer_state.source_connection_id
    }

    pub(in crate::runtime::channel) const fn source_media(&self) -> TransportMediaId {
        self.consumer_state.source_media
    }

    pub(in crate::runtime::channel) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(in crate::runtime::channel) const fn active(&self) -> bool {
        self.active
    }
}

impl PendingConsumerBootstrapTarget {
    pub(in crate::runtime::channel) fn new(
        consumer_session_id: SessionId,
        consumer_connection_id: ConnectionId,
        producer: ConsumerBootstrapProducerSnapshot,
    ) -> Self {
        Self {
            consumer_session_id,
            consumer_connection_id,
            producer,
        }
    }

    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub(in crate::runtime::channel) fn consumer_session_id(&self) -> &SessionId {
        &self.consumer_session_id
    }

    pub(in crate::runtime::channel) const fn media_kind(&self) -> RouterMediaKind {
        self.producer.media_kind
    }

    pub(in crate::runtime::channel) const fn producer_connection_id(&self) -> ConnectionId {
        self.producer.owner_connection_id
    }

    pub(in crate::runtime::channel) fn producer_session_id(&self) -> &SessionId {
        &self.producer.owner_session_id
    }

    pub(in crate::runtime::channel) const fn transport_media_id(&self) -> TransportMediaId {
        self.producer.transport_media_id
    }

    pub(in crate::runtime::channel) const fn stream_type(&self) -> StreamType {
        self.producer.stream_type
    }
}

impl PreparedConsumerBootstrap {
    pub(in crate::runtime::channel) fn consumer_rtp_parameters(&self) -> &RouterRtpParameters {
        &self.consumer_rtp_parameters
    }
}

impl PlannedConsumerBootstrap {
    pub(in crate::runtime::channel) fn into_parts(
        self,
    ) -> (
        PendingConsumerBootstrapTarget,
        PreparedConsumerBootstrap,
        PendingConsumerBootstrap,
    ) {
        (self.target, self.prepared, self.pending_bootstrap)
    }
}

impl ConsumerBootstrapProducerSnapshot {
    pub(in crate::runtime::channel) fn pending(
        owner_session_id: SessionId,
        owner_connection_id: ConnectionId,
        producer_id: ProducerRuntimeId,
        stream_type: StreamType,
        media_kind: RouterMediaKind,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            owner_session_id,
            owner_connection_id,
            producer_id,
            stream_type,
            media_kind,
            transport_media_id,
            routed_producer_id: None,
            active: None,
        }
    }

    fn with_commit_snapshot(&self, routed_producer_id: RoutedProducerId, active: bool) -> Self {
        Self {
            owner_session_id: self.owner_session_id.clone(),
            owner_connection_id: self.owner_connection_id,
            producer_id: self.producer_id,
            stream_type: self.stream_type,
            media_kind: self.media_kind,
            transport_media_id: self.transport_media_id,
            routed_producer_id: Some(routed_producer_id),
            active: Some(active),
        }
    }

    fn matches_pending_producer(&self, producer: &PublishedProducer) -> bool {
        producer.owner_session_id == self.owner_session_id
            && producer.owner_connection_id == self.owner_connection_id
            && producer.stream_type == self.stream_type
            && producer.media_kind == self.media_kind
            && producer.transport_media_id == Some(self.transport_media_id)
    }

    fn matches_committed_producer(&self, producer: &PublishedProducer) -> bool {
        let Some(routed_producer_id) = self.routed_producer_id else {
            return false;
        };
        let Some(active) = self.active else {
            return false;
        };
        self.matches_pending_producer(producer)
            && producer.routed_producer_id == routed_producer_id
            && producer.active == active
    }
}

impl RemoteTrackBootstrap {
    pub(crate) fn mid(&self) -> &str {
        &self.mid
    }

    #[cfg(test)]
    pub(crate) fn rtp_parameters(&self) -> &RouterRtpParameters {
        &self.rtp_parameters
    }

    pub(crate) fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub(crate) const fn active(&self) -> bool {
        self.active
    }

    pub(crate) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(crate) fn into_channel_event_request(self) -> ChannelEventRequest {
        ChannelEventRequest::BootstrapRemoteTrack(self)
    }
}

impl ConsumerKeyframeRefreshTarget {
    pub(in crate::runtime::channel) const fn consumer_media(&self) -> TransportMediaId {
        self.consumer_media
    }

    pub(in crate::runtime::channel) fn producer_session_id(&self) -> &SessionId {
        &self.producer_session_id
    }

    pub(in crate::runtime::channel) const fn producer_connection_id(&self) -> ConnectionId {
        self.producer_connection_id
    }

    pub(in crate::runtime::channel) const fn source_media(&self) -> TransportMediaId {
        self.source_media
    }
}

fn merge_download_states(target: &mut DownloadStates, update: &DownloadStates) {
    if let Some(audio) = update.audio {
        target.audio = Some(audio);
    }
    if let Some(camera) = update.camera {
        target.camera = Some(camera);
    }
    if let Some(screen) = update.screen {
        target.screen = Some(screen);
    }
}

fn download_states_are_empty(states: &DownloadStates) -> bool {
    states.audio.is_none() && states.camera.is_none() && states.screen.is_none()
}

fn download_state_for_stream_type(
    states: &DownloadStates,
    stream_type: StreamType,
) -> Option<bool> {
    match stream_type {
        StreamType::Audio => states.audio,
        StreamType::Camera => states.camera,
        StreamType::Screen => states.screen,
    }
}
