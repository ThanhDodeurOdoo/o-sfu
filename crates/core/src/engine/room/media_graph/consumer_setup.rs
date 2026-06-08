use o_sfu_router::{MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters};

use super::{
    super::{
        RoomMediaCounts,
        outbound::OutboundSender,
        routing::{RoutedConsumerId, RoutedProducerId},
        state::RoomState,
    },
    ConsumerKey, ConsumerRuntimeId, ConsumerState, ProducerRuntimeId, PublishedProducer,
    route_graph::{ConsumerRouteReservation, RelayRouteKey, ResolvedRelayRouteEffect},
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{
        TransportConsumerRoute, TransportMediaId, TransportSessionKey, TransportSourceKey,
    },
    source_model::{PublishedSourceDescriptor, PublishedSourceId, UserStreamId},
};

#[derive(Debug, Clone)]
pub struct ConsumerSetupTarget {
    pub user: UserId,
    pub connection: ConnectionId,
    pub user_session: TransportSessionKey,
    pub producer_session: TransportSessionKey,
    pub source_id: PublishedSourceId,
    pub producer_user: UserId,
    pub producer_connection: ConnectionId,
    pub producer_id: ProducerRuntimeId,
    pub stream: UserStreamId,
    pub kind: RouterMediaKind,
    pub media: TransportMediaId,
    pub routed: RoutedProducerId,
}

#[derive(Debug)]
#[must_use = "pending consumer setups reserve route graph state and must be committed or released"]
pub struct PendingConsumerSetup {
    pub target: ConsumerSetupTarget,
    pub reservation: ConsumerRouteReservation,
    pub sender: OutboundSender,
    pub track: RemoteTrackSetup,
    pub relays: Vec<ResolvedRelayRouteEffect>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "consumer setup outcomes are returned and matched immediately so boxing the committed setup would allocate on every successful consumer setup"
)]
#[derive(Debug)]
pub enum ConsumerSetupOutcome {
    Committed {
        sender: OutboundSender,
        track: RemoteTrackSetup,
        transport_activity_update: Option<bool>,
    },
    Released(Vec<ResolvedRelayRouteEffect>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackSetup {
    pub(super) consumer: ConsumerRuntimeId,
    pub(super) kind: RouterMediaKind,
    pub mid: String,
    pub(super) producer: ProducerRuntimeId,
    pub rtp: RouterRtpParameters,
    pub source: PublishedSourceDescriptor,
    pub user: UserId,
    pub active: bool,
    pub stream: UserStreamId,
}

#[derive(Debug, Clone, Copy)]
pub enum ConsumerSetupOrigin {
    Readiness,
    Publish,
    Subscribe,
}

impl ConsumerSetupOrigin {
    pub const fn as_diagnostic_str(self) -> &'static str {
        match self {
            Self::Readiness => "readiness",
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
        }
    }
}

impl PendingConsumerSetup {
    fn commit(
        mut self,
        state: &mut RoomState,
        media: TransportMediaId,
        mid: Option<String>,
    ) -> ConsumerSetupOutcome {
        let Some(user) = state.users.get(&self.target.user) else {
            return self.release_into_outcome(state);
        };
        if user.connection_id != self.target.connection || !user.negotiation.can_consume() {
            return self.release_into_outcome(state);
        }
        let Some(producer) = state.media.producer(self.target.producer_id) else {
            return self.release_into_outcome(state);
        };
        if !self.target.matches_identity(producer) {
            return self.release_into_outcome(state);
        }
        let producer_active = producer.active;
        if state.media.contains_consumer(self.reservation.key()) {
            return self.release_into_outcome(state);
        }
        let selection = state.setup_selection(&self.target, producer_active);
        let Ok(topology_commit) =
            state.commit_consumer_setup(&self.reservation, &self.target, selection, media)
        else {
            return self.release_into_outcome(state);
        };
        if let Some(mid) = mid {
            self.track.mid = mid;
        }
        self.track.active = producer_active;
        ConsumerSetupOutcome::Committed {
            sender: self.sender,
            track: self.track,
            transport_activity_update: topology_commit,
        }
    }

    fn release(self, state: &mut RoomState) -> Vec<ResolvedRelayRouteEffect> {
        state.release_consumer_setup(self.reservation)
    }

    fn release_into_outcome(self, state: &mut RoomState) -> ConsumerSetupOutcome {
        ConsumerSetupOutcome::Released(self.release(state))
    }
}

impl RoomState {
    pub fn commit_pending_consumer_setup(
        &mut self,
        setup: PendingConsumerSetup,
        media: TransportMediaId,
        mid: Option<String>,
    ) -> (RoomMediaCounts, RoomMediaCounts, ConsumerSetupOutcome) {
        let before = self.media_counts();
        let outcome = setup.commit(self, media, mid);
        let after = self.media_counts();
        (before, after, outcome)
    }

    pub fn release_pending_consumer_setup(
        &mut self,
        setup: PendingConsumerSetup,
    ) -> (
        RoomMediaCounts,
        RoomMediaCounts,
        Vec<ResolvedRelayRouteEffect>,
    ) {
        let before = self.media_counts();
        let relays = setup.release(self);
        let after = self.media_counts();
        (before, after, relays)
    }
}

impl ConsumerSetupTarget {
    pub fn new(
        consumer_user_id: UserId,
        consumer_connection_id: ConnectionId,
        consumer_session: TransportSessionKey,
        producer_session: TransportSessionKey,
        producer_id: ProducerRuntimeId,
        producer: &PublishedProducer,
        producer_media: TransportMediaId,
    ) -> Self {
        Self {
            user: consumer_user_id,
            connection: consumer_connection_id,
            user_session: consumer_session,
            producer_session,
            source_id: producer.source_id,
            producer_user: producer.owner_user_id.clone(),
            producer_connection: producer.owner_connection_id,
            producer_id,
            stream: producer.stream_id.clone(),
            kind: producer.media_kind,
            media: producer_media,
            routed: producer.routed_producer_id,
        }
    }

    pub(super) fn consumer_key(&self) -> ConsumerKey {
        ConsumerKey::new(&self.user, self.source_id)
    }

    pub(super) fn consumer_state(
        &self,
        routed_consumer_id: RoutedConsumerId,
        consumer_media: TransportMediaId,
    ) -> ConsumerState {
        ConsumerState {
            routed_consumer_id,
            consumer_connection_id: self.connection,
            source_connection_id: self.producer_connection,
            source_media: self.media,
            consumer_media,
        }
    }

    pub fn transport_consumer_route(
        &self,
        consumer_media: TransportMediaId,
    ) -> TransportConsumerRoute {
        TransportConsumerRoute::new(
            self.user_session.clone(),
            consumer_media,
            TransportSourceKey::new(self.producer_session.clone(), self.media),
        )
    }

    pub(super) fn relay_route_key(&self, target_worker: MediaWorkerId) -> RelayRouteKey {
        RelayRouteKey {
            source_user: self.producer_user.clone(),
            source_connection: self.producer_connection,
            source_media: self.media,
            target_worker,
        }
    }

    pub(super) fn matches_identity(&self, producer: &PublishedProducer) -> bool {
        producer.source_id == self.source_id
            && producer.owner_user_id == self.producer_user
            && producer.owner_connection_id == self.producer_connection
            && producer.stream_id == self.stream
            && producer.media_kind == self.kind
            && producer.transport_media_id == Some(self.media)
            && producer.routed_producer_id == self.routed
    }
}
