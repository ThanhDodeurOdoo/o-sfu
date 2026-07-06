use std::mem;

use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::MediaStream as RouterRtpParameters,
    topology::{RoutedConsumerId, RoutedProducerId},
};
use tracing::warn;

use super::{
    super::{
        RoomMediaCounts,
        outbound::{OutboundSender, RemoteSourceSnapshot},
        state::RoomState,
    },
    ConsumerKey, ConsumerState, ProducerRuntimeId, PublishedProducer,
    route_graph::{ConsumerRouteReservation, RelayRouteKey, ResolvedRelayRouteEffect},
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{
        ConsumerActivity, MediaTransport, TransportConsumerRoute, TransportMediaId,
        TransportSessionKey, TransportSourceKey,
    },
    source_model::{PublishedSourceId, UserStreamId},
};

#[derive(Debug)]
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

/// pending consumer route transaction between room reservation and transport declaration
#[derive(Debug)]
#[must_use = "pending consumer setups reserve route graph state and must be committed or released"]
pub struct PendingConsumerSetup {
    pub(super) target: ConsumerSetupTarget,
    pub(super) reservation: ConsumerRouteReservation,
    pub(super) sender: OutboundSender,
    pub(super) fallback_mid: String,
    pub(super) rtp: RouterRtpParameters,
    pub(super) relays: Vec<ResolvedRelayRouteEffect>,
}

pub struct DeclaredConsumerSetup {
    pub(super) pending: PendingConsumerSetup,
    pub(super) route: TransportConsumerRoute,
    pub(super) mid: Option<String>,
}

pub struct CommittedConsumerSetup {
    pub(super) target: ConsumerSetupTarget,
    pub(super) route: TransportConsumerRoute,
    pub(super) sender: OutboundSender,
    pub(super) transport_activity_update: Option<bool>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "consumer setup outcomes are returned and matched immediately so boxing the committed setup would allocate on every successful consumer setup"
)]
#[derive(Debug)]
pub enum ConsumerSetupOutcome {
    Committed {
        target: ConsumerSetupTarget,
        route: TransportConsumerRoute,
        sender: OutboundSender,
        snapshot: RemoteSourceSnapshot,
        transport_activity_update: Option<bool>,
    },
    Released(TransportConsumerRoute, Vec<ResolvedRelayRouteEffect>),
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

impl RoomState {
    pub fn commit_declared_consumer_setup(
        &mut self,
        setup: DeclaredConsumerSetup,
    ) -> (RoomMediaCounts, RoomMediaCounts, ConsumerSetupOutcome) {
        let before = self.media_counts();
        let target = &setup.pending.target;
        let outcome = if self.users.get(&target.user).is_some_and(|user| {
            user.connection_id == target.connection && user.negotiation.can_consume()
        }) && let Some(producer_active) = self
            .topology
            .producer(target.producer_id)
            .filter(|producer| target.matches_identity(producer))
            .map(|producer| producer.active)
        {
            let selection = self.setup_selection(target, producer_active);
            match self.topology.commit_consumer_setup(setup, selection) {
                Ok(commit) => {
                    let snapshot = self.remote_source_snapshot_for_user(&commit.target.user, true);
                    ConsumerSetupOutcome::Committed {
                        target: commit.target,
                        route: commit.route,
                        sender: commit.sender,
                        snapshot,
                        transport_activity_update: commit.transport_activity_update,
                    }
                }
                Err((route, relays)) => ConsumerSetupOutcome::Released(route, relays),
            }
        } else {
            let DeclaredConsumerSetup { pending, route, .. } = setup;
            ConsumerSetupOutcome::Released(route, self.topology.release_consumer_setup(pending))
        };
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
        let relays = self.topology.release_consumer_setup(setup);
        let after = self.media_counts();
        (before, after, relays)
    }
}

impl PendingConsumerSetup {
    pub(in crate::engine::room) fn take_relays(&mut self) -> Vec<ResolvedRelayRouteEffect> {
        mem::take(&mut self.relays)
    }

    pub(in crate::engine::room) async fn declare(
        self,
        media_transport: &MediaTransport,
        origin: ConsumerSetupOrigin,
    ) -> Result<DeclaredConsumerSetup, Self> {
        let activity =
            ConsumerActivity::from_active(self.reservation.selection().delivery_active());
        match media_transport
            .consume_media(
                &self.target.user_session,
                self.target.kind,
                &self.target.producer_session,
                self.target.media,
                &self.rtp,
                activity,
            )
            .await
        {
            Ok(media) => {
                let mid = media_transport
                    .transport_media_mid(&self.target.user_session, media)
                    .await;
                Ok(DeclaredConsumerSetup {
                    route: self.target.transport_consumer_route(media),
                    pending: self,
                    mid,
                })
            }
            Err(error) => {
                warn!(
                    consumer_user_id = ?self.target.user,
                    consumer_connection_id = ?self.target.connection,
                    producer_user_id = ?self.target.producer_user,
                    producer_connection_id = ?self.target.producer_connection,
                    source_transport_media_id = ?self.target.media,
                    error = ?error,
                    consumer_mid = self.rtp.mid(),
                    ?origin,
                    "media transport rejected consume media declaration"
                );
                Err(self)
            }
        }
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
        consumer_mid: String,
    ) -> ConsumerState {
        ConsumerState {
            routed_consumer_id,
            consumer_connection_id: self.connection,
            source_connection_id: self.producer_connection,
            source_media: self.media,
            consumer_media,
            consumer_mid,
        }
    }

    pub(super) fn transport_consumer_route(
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
