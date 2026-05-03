use o_sfu_router::{
    ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState,
    MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters, can_consume,
    negotiate_consumer_rtp_parameters,
};
use tracing::{error, warn};

use super::super::{
    super::{RoomEventRequest, outbound::OutboundSender, topology::RoutedProducerId},
    ids::{ConsumerRuntimeId, ProducerRuntimeId},
    shared::{ConsumerKey, ConsumerState, PublishedProducer, RoomState, SourceKey},
};
use crate::runtime::{
    ConnectionId, DownloadStates, StreamType, UserId,
    media_transport::TransportMediaId,
    source_model::{ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Accepted consumer-route update that should be fanned out after state commit.
///
/// The route update only represents the receiver-local route choice. Producer
/// activity is handled through producer state and is combined with this value
/// when callers ask for the effective route.
pub(in crate::runtime::room) struct ConsumerRouteUpdate {
    consumer_state: ConsumerState,
    producer_user_id: UserId,
    stream_type: StreamType,
    active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Effective room-level route state exposed to compatibility callers.
///
/// This is not the same type as the pure router's
/// [`RouterConsumerRouteState`]. The router type stores only the
/// receiver-local route choice. This value folds together producer activity,
/// consumer source selection and whether the consumer route exists at all.
pub enum ConsumerRouteState {
    /// No committed consumer route exists for the requested source.
    Absent,
    /// A route exists, but either the producer or the consumer-local selection
    /// currently prevents forwarding.
    Inactive,
    /// A committed route exists and all room-level activity inputs allow it.
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct ConsumerKeyframeRefreshTarget {
    consumer_media: TransportMediaId,
    producer_user_id: UserId,
    producer_connection_id: ConnectionId,
    source_media: TransportMediaId,
}

#[derive(Debug, Default)]
pub(in crate::runtime::room) struct PlannedSubscriptionChange {
    route_updates: Vec<ConsumerRouteUpdate>,
    bootstraps: Vec<PlannedConsumerBootstrap>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PendingConsumerBootstrapTarget {
    pub(super) consumer_user_id: UserId,
    pub(super) consumer_connection_id: ConnectionId,
    producer: ConsumerBootstrapProducerSnapshot,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ConsumerBootstrapProducerSnapshot {
    source_id: PublishedSourceId,
    owner_user_id: UserId,
    owner_connection_id: ConnectionId,
    producer_id: ProducerRuntimeId,
    stream_type: StreamType,
    media_kind: RouterMediaKind,
    transport_media_id: TransportMediaId,
    routed_producer_id: Option<RoutedProducerId>,
    active: Option<bool>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PreparedConsumerBootstrap {
    consumer_rtp_parameters: RouterRtpParameters,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PendingConsumerBootstrap {
    consumer_key: ConsumerKey,
    sender: OutboundSender,
    bootstrap: RemoteTrackBootstrap,
    consumer_active: bool,
    producer: ConsumerBootstrapProducerSnapshot,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PlannedConsumerBootstrap {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending_bootstrap: PendingConsumerBootstrap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackBootstrap {
    consumer_id: ConsumerRuntimeId,
    media_kind: RouterMediaKind,
    mid: String,
    producer_id: ProducerRuntimeId,
    rtp_parameters: RouterRtpParameters,
    source_descriptor: PublishedSourceDescriptor,
    user_id: UserId,
    active: bool,
    stream_type: StreamType,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
    Subscribe,
}

impl RoomState {
    pub(in crate::runtime::room) fn plan_subscription_change(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) -> PlannedSubscriptionChange {
        if self.user_for_connection(user_id, connection_id).is_none() {
            return PlannedSubscriptionChange::default();
        }
        self.persist_compat_download_states(user_id, target_user_id, states);
        let route_updates =
            self.apply_subscription_route_updates(user_id, connection_id, target_user_id, states);
        let bootstraps = self.plan_consumer_bootstraps_for_targets(
            self.collect_missing_consumer_targets_for_peer(user_id, connection_id, target_user_id),
        );
        PlannedSubscriptionChange {
            route_updates,
            bootstraps,
        }
    }

    fn persist_compat_download_states(
        &mut self,
        user_id: &UserId,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) {
        let Some(user) = self.users.get_mut(user_id) else {
            return;
        };
        let existing_states = user
            .desired_download_states
            .entry(target_user_id.clone())
            .or_default();
        merge_download_states(existing_states, states);
        if download_states_are_empty(existing_states) {
            user.desired_download_states.remove(target_user_id);
        }
    }

    pub(in crate::runtime::room) fn plan_missing_consumer_bootstraps_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<Vec<PlannedConsumerBootstrap>> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        if !user.negotiation.can_consume() {
            return Some(Vec::new());
        }
        Some(self.plan_consumer_bootstraps_for_targets(
            self.collect_missing_consumer_targets(user_id, connection_id),
        ))
    }

    pub(in crate::runtime::room) fn plan_consumer_bootstraps_for_targets(
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
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) -> Vec<ConsumerRouteUpdate> {
        let mut accepted_updates = Vec::new();
        for (stream_type, active) in states.iter() {
            let Some(source_id) =
                self.source_id_for_compat_subscription(target_user_id, stream_type)
            else {
                continue;
            };
            let key = ConsumerKey::new(user_id, source_id);
            self.set_consumer_source_selection(&key, active);
            let Some(current_consumer_state) = self.consumer_index.get(&key).copied() else {
                continue;
            };
            if current_consumer_state.consumer_connection_id != connection_id {
                continue;
            }
            let route_state = if active {
                RouterConsumerRouteState::Active
            } else {
                RouterConsumerRouteState::Paused
            };
            if self
                .topology
                .set_consumer_route_state(current_consumer_state.routed_consumer_id, route_state)
                .is_err()
            {
                error!(
                    ?user_id,
                    ?target_user_id,
                    ?stream_type,
                    "failed to set consumer pause state in room router"
                );
                continue;
            }
            accepted_updates.push(ConsumerRouteUpdate {
                consumer_state: current_consumer_state,
                producer_user_id: target_user_id.clone(),
                stream_type,
                active,
            });
        }
        accepted_updates
    }

    fn collect_missing_consumer_targets(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                self.pending_consumer_target(
                    user_id,
                    consumer_connection_id,
                    *producer_id,
                    producer,
                )
            })
            .collect()
    }

    fn collect_missing_consumer_targets_for_peer(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        target_user_id: &UserId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                if producer.owner_user_id != *target_user_id {
                    return None;
                }
                self.pending_consumer_target(
                    user_id,
                    consumer_connection_id,
                    *producer_id,
                    producer,
                )
            })
            .collect()
    }

    fn pending_consumer_target(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        producer_id: ProducerRuntimeId,
        producer: &PublishedProducer,
    ) -> Option<PendingConsumerBootstrapTarget> {
        let transport_media_id = producer.transport_media_id?;
        if producer.owner_user_id == *user_id {
            return None;
        }
        let consumer_key = ConsumerKey::new(user_id, producer.source_id);
        if self.consumer_bootstrap_exists(&consumer_key) {
            return None;
        }
        Some(PendingConsumerBootstrapTarget::new(
            user_id.clone(),
            consumer_connection_id,
            ConsumerBootstrapProducerSnapshot::pending(
                producer.source_id,
                producer.owner_user_id.clone(),
                producer.owner_connection_id,
                producer_id,
                producer.stream_type,
                producer.media_kind,
                transport_media_id,
            ),
        ))
    }

    fn source_id_for_compat_subscription(
        &self,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> Option<PublishedSourceId> {
        self.source_ids_by_owner_stream
            .get(&SourceKey::new(producer_user_id, stream_type))
            .copied()
    }

    fn set_consumer_source_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.consumer_source_selections
            .entry(key.clone())
            .and_modify(|selection| selection.set_active(active))
            .or_insert_with(|| ConsumerSourceSelection::open(active));
        self.register_consumer_key(key);
    }

    fn plan_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Option<PlannedConsumerBootstrap> {
        let (sender, client_capabilities) = {
            let user = self.users.get(&target.consumer_user_id)?;
            if user.connection_id != target.consumer_connection_id
                || !user.negotiation.can_consume()
            {
                return None;
            }
            (
                user.sender.clone(),
                user.parsed_client_rtp_capabilities.clone()?,
            )
        };
        let producer = self.producers.get(&target.producer.producer_id)?;
        if !target.producer.matches_pending_producer(producer) {
            return None;
        }
        let source_descriptor = self.sources.get(&target.producer.source_id)?.clone();
        let consumer_key = ConsumerKey::new(&target.consumer_user_id, target.source_id());
        if self.consumer_bootstrap_exists(&consumer_key) {
            return None;
        }
        let consumer_active = self
            .consumer_source_selection_for_bootstrap(target)
            .active();
        let producer_consumable_rtp_parameters = producer.consumable_rtp_parameters.clone();
        let prepared_producer = target
            .producer
            .with_commit_snapshot(producer.routed_producer_id, producer.active);
        if !can_consume(&producer_consumable_rtp_parameters, &client_capabilities) {
            return None;
        }
        let negotiated_rtp_parameters = negotiate_consumer_rtp_parameters(
            &producer_consumable_rtp_parameters,
            &client_capabilities,
        )
        .ok()?;
        self.consumer_source_selections
            .entry(consumer_key.clone())
            .or_insert_with(|| ConsumerSourceSelection::open(consumer_active));
        self.register_consumer_key(&consumer_key);
        self.pending_consumer_bootstraps
            .insert(consumer_key.clone());
        self.register_consumer_key(&consumer_key);
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
                    source_descriptor,
                    user_id: prepared_producer.owner_user_id.clone(),
                    active: prepared_producer.active.unwrap_or(true),
                    stream_type: prepared_producer.stream_type,
                },
                consumer_active,
                producer: prepared_producer,
            },
        })
    }

    fn consumer_source_selection_for_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) -> ConsumerSourceSelection {
        let consumer_key = ConsumerKey::new(&target.consumer_user_id, target.source_id());
        self.consumer_source_selections
            .get(&consumer_key)
            .copied()
            .unwrap_or_else(|| {
                ConsumerSourceSelection::open(self.desired_download_active(
                    &target.consumer_user_id,
                    target.producer_user_id(),
                    target.stream_type(),
                ))
            })
    }

    pub(in crate::runtime::room) fn commit_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        mut pending: PendingConsumerBootstrap,
        consumer_transport_media_id: TransportMediaId,
        consumer_mid: Option<String>,
    ) -> Option<(OutboundSender, RemoteTrackBootstrap, bool)> {
        self.pending_consumer_bootstraps
            .remove(&pending.consumer_key);
        self.prune_consumer_key_indexes_if_unused(&pending.consumer_key);
        let user = self.users.get(&target.consumer_user_id)?;
        if user.connection_id != target.consumer_connection_id || !user.negotiation.can_consume() {
            return None;
        }
        let producer = self.producers.get(&pending.producer.producer_id)?;
        if !pending.producer.matches_committed_producer(producer) {
            return None;
        }
        if self.consumer_index.contains_key(&pending.consumer_key) {
            return None;
        }
        self.consumer_source_selections
            .entry(pending.consumer_key.clone())
            .or_insert_with(|| ConsumerSourceSelection::open(pending.consumer_active));
        self.register_consumer_key(&pending.consumer_key);
        let routed_consumer_id = match self.topology.add_consumer(
            &target.consumer_user_id,
            pending.producer.routed_producer_id?,
            pending.producer.media_kind,
            ConsumerCapability::Compatible,
        ) {
            Ok(id) => id,
            Err(error) => {
                warn!(
                    consumer_user_id = ?target.consumer_user_id,
                    producer_id = %pending.producer.producer_id,
                    ?error,
                    "router rejected consumer creation"
                );
                return None;
            }
        };
        if let Some(consumer_mid) = consumer_mid {
            pending.bootstrap.mid = consumer_mid;
        }
        // The router inserts consumers as locally active, then mirrors the
        // stored download selection if this receiver had already paused it.
        if !pending.consumer_active
            && self
                .topology
                .set_consumer_route_state(routed_consumer_id, RouterConsumerRouteState::Paused)
                .is_err()
        {
            error!(
                consumer_user_id = ?target.consumer_user_id,
                producer_id = %pending.producer.producer_id,
                "failed to mirror initial consumer pause state into room router"
            );
            return None;
        }
        let consumer_key = pending.consumer_key;
        self.consumer_index.insert(
            consumer_key.clone(),
            ConsumerState {
                routed_consumer_id,
                consumer_connection_id: target.consumer_connection_id,
                source_connection_id: pending.producer.owner_connection_id,
                source_media: target.transport_media_id(),
                consumer_media: consumer_transport_media_id,
            },
        );
        self.register_consumer_key(&consumer_key);
        Some((pending.sender, pending.bootstrap, pending.consumer_active))
    }

    pub(in crate::runtime::room) fn release_pending_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) {
        let consumer_key = ConsumerKey::new(&target.consumer_user_id, target.source_id());
        self.pending_consumer_bootstraps.remove(&consumer_key);
        self.prune_consumer_key_indexes_if_unused(&consumer_key);
    }

    pub(in crate::runtime::room) fn desired_download_active(
        &self,
        user_id: &UserId,
        target_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.users
            .get(user_id)
            .and_then(|user| user.desired_download_states.get(target_user_id))
            .and_then(|states| download_state_for_stream_type(states, stream_type))
            .unwrap_or(true)
    }

    /// Returns the effective room route state for a compatibility subscription.
    ///
    /// This is a cold-path query for signaling and diagnostics. It resolves the
    /// compatibility stream to current room indexes and combines producer
    /// activity with the receiver-local source selection. Missing users return
    /// `None`, while missing routes for an existing user return
    /// [`ConsumerRouteState::Absent`].
    pub(in crate::runtime::room) fn consumer_route_state(
        &self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> Option<ConsumerRouteState> {
        self.users.get(consumer_user_id)?;
        let Some(source_id) = self.source_id_for_compat_subscription(producer_user_id, stream_type)
        else {
            return Some(ConsumerRouteState::Absent);
        };
        let consumer_key = ConsumerKey::new(consumer_user_id, source_id);
        if !self.consumer_index.contains_key(&consumer_key) {
            return Some(ConsumerRouteState::Absent);
        }
        let Some(producer_id) = self.producer_id_by_source_id.get(&source_id).copied() else {
            return Some(ConsumerRouteState::Absent);
        };
        let Some(producer) = self.producers.get(&producer_id) else {
            return Some(ConsumerRouteState::Absent);
        };
        let route_active = producer.active
            && self
                .consumer_source_selections
                .get(&consumer_key)
                .map_or_else(
                    || {
                        self.desired_download_active(
                            consumer_user_id,
                            producer_user_id,
                            stream_type,
                        )
                    },
                    |selection| selection.active(),
                );
        Some(if route_active {
            ConsumerRouteState::Active
        } else {
            ConsumerRouteState::Inactive
        })
    }

    pub(in crate::runtime::room) fn active_video_consumer_keyframe_refresh_targets(
        &self,
        consumer_user_id: &UserId,
        consumer_connection_id: ConnectionId,
    ) -> Option<Vec<ConsumerKeyframeRefreshTarget>> {
        let user = self.users.get(consumer_user_id)?;
        if user.connection_id != consumer_connection_id {
            return None;
        }
        Some(
            self.consumer_keys_for_user(consumer_user_id)
                .into_iter()
                .filter_map(|key| {
                    let consumer_state = self.consumer_index.get(&key)?;
                    let source = self.sources.get(&key.source_id)?;
                    if key.consumer_user_id != *consumer_user_id
                        || consumer_state.consumer_connection_id != consumer_connection_id
                        || !matches!(
                            source.stream_type(),
                            StreamType::Camera | StreamType::Screen
                        )
                    {
                        return None;
                    }
                    let producer_id = self.producer_id_by_source_id.get(&key.source_id)?;
                    let producer = self.producers.get(producer_id)?;
                    if !producer.active
                        || !self
                            .consumer_source_selections
                            .get(&key)
                            .is_none_or(|selection| selection.active())
                    {
                        return None;
                    }
                    Some(ConsumerKeyframeRefreshTarget {
                        consumer_media: consumer_state.consumer_media,
                        producer_user_id: source.owner().user_id().clone(),
                        producer_connection_id: consumer_state.source_connection_id,
                        source_media: consumer_state.source_media,
                    })
                })
                .collect(),
        )
    }

    pub(in crate::runtime::room) fn consumer_bootstrap_exists(
        &self,
        consumer_key: &ConsumerKey,
    ) -> bool {
        self.consumer_index.contains_key(consumer_key)
            || self.pending_consumer_bootstraps.contains(consumer_key)
    }
}

impl PlannedSubscriptionChange {
    pub(in crate::runtime::room) fn into_parts(
        self,
    ) -> (Vec<ConsumerRouteUpdate>, Vec<PlannedConsumerBootstrap>) {
        (self.route_updates, self.bootstraps)
    }
}

impl ConsumerRouteUpdate {
    pub(in crate::runtime::room) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_state.consumer_connection_id
    }

    pub(in crate::runtime::room) const fn consumer_media(&self) -> TransportMediaId {
        self.consumer_state.consumer_media
    }

    pub(in crate::runtime::room) const fn source_connection_id(&self) -> ConnectionId {
        self.consumer_state.source_connection_id
    }

    pub(in crate::runtime::room) fn producer_user_id(&self) -> &UserId {
        &self.producer_user_id
    }

    pub(in crate::runtime::room) const fn source_media(&self) -> TransportMediaId {
        self.consumer_state.source_media
    }

    pub(in crate::runtime::room) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(in crate::runtime::room) const fn active(&self) -> bool {
        self.active
    }
}

impl PendingConsumerBootstrapTarget {
    pub(in crate::runtime::room) fn new(
        consumer_user_id: UserId,
        consumer_connection_id: ConnectionId,
        producer: ConsumerBootstrapProducerSnapshot,
    ) -> Self {
        Self {
            consumer_user_id,
            consumer_connection_id,
            producer,
        }
    }

    pub(in crate::runtime::room) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub(in crate::runtime::room) fn consumer_user_id(&self) -> &UserId {
        &self.consumer_user_id
    }

    pub(in crate::runtime::room) const fn source_id(&self) -> PublishedSourceId {
        self.producer.source_id
    }

    pub(in crate::runtime::room) const fn media_kind(&self) -> RouterMediaKind {
        self.producer.media_kind
    }

    pub(in crate::runtime::room) const fn producer_connection_id(&self) -> ConnectionId {
        self.producer.owner_connection_id
    }

    pub(in crate::runtime::room) fn producer_user_id(&self) -> &UserId {
        &self.producer.owner_user_id
    }

    pub(in crate::runtime::room) const fn transport_media_id(&self) -> TransportMediaId {
        self.producer.transport_media_id
    }

    pub(in crate::runtime::room) const fn stream_type(&self) -> StreamType {
        self.producer.stream_type
    }
}

impl PreparedConsumerBootstrap {
    pub(in crate::runtime::room) fn consumer_rtp_parameters(&self) -> &RouterRtpParameters {
        &self.consumer_rtp_parameters
    }
}

impl PlannedConsumerBootstrap {
    pub(in crate::runtime::room) fn into_parts(
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
    pub(in crate::runtime::room) fn pending(
        source_id: PublishedSourceId,
        owner_user_id: UserId,
        owner_connection_id: ConnectionId,
        producer_id: ProducerRuntimeId,
        stream_type: StreamType,
        media_kind: RouterMediaKind,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            source_id,
            owner_user_id,
            owner_connection_id,
            producer_id,
            stream_type,
            media_kind,
            transport_media_id,
            routed_producer_id: None,
            active: None,
        }
    }

    pub(in crate::runtime::room) const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    pub(in crate::runtime::room) fn owner_user_id(&self) -> &UserId {
        &self.owner_user_id
    }

    fn with_commit_snapshot(&self, routed_producer_id: RoutedProducerId, active: bool) -> Self {
        Self {
            source_id: self.source_id,
            owner_user_id: self.owner_user_id.clone(),
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
        producer.source_id == self.source_id
            && producer.owner_user_id == self.owner_user_id
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
    pub fn mid(&self) -> &str {
        &self.mid
    }

    #[cfg(test)]
    pub(crate) fn rtp_parameters(&self) -> &RouterRtpParameters {
        &self.rtp_parameters
    }

    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    pub fn source_descriptor(&self) -> &PublishedSourceDescriptor {
        &self.source_descriptor
    }

    pub const fn active(&self) -> bool {
        self.active
    }

    pub const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub fn into_room_event_request(self) -> RoomEventRequest {
        RoomEventRequest::BootstrapRemoteTrack(self)
    }
}

impl ConsumerKeyframeRefreshTarget {
    pub(in crate::runtime::room) const fn consumer_media(&self) -> TransportMediaId {
        self.consumer_media
    }

    pub(in crate::runtime::room) fn producer_user_id(&self) -> &UserId {
        &self.producer_user_id
    }

    pub(in crate::runtime::room) const fn producer_connection_id(&self) -> ConnectionId {
        self.producer_connection_id
    }

    pub(in crate::runtime::room) const fn source_media(&self) -> TransportMediaId {
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
    if let Some(camera_layout) = update.camera_layout {
        target.camera_layout = Some(camera_layout);
    }
    if let Some(screen_layout) = update.screen_layout {
        target.screen_layout = Some(screen_layout);
    }
}

fn download_states_are_empty(states: &DownloadStates) -> bool {
    states.audio.is_none()
        && states.camera.is_none()
        && states.screen.is_none()
        && states.camera_layout.is_none()
        && states.screen_layout.is_none()
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
