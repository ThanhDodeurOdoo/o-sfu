use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{ConsumerId, MediaKind, rtp, topology::RoutedProducerId};

use crate::engine::{
    ConnectionId, UserId,
    media_transport::{TransportConsumerRoute, TransportMediaId, TransportSourceKey},
    source_model::{
        ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId, UserStreamId,
    },
};

mod consumer_setup;
mod producer;
mod route_graph;
mod source_index;
mod subscription;
mod topology;

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
#[cfg(test)]
#[path = "TESTS/route_graph.rs"]
mod route_graph_tests;

#[cfg(any(test, feature = "testing-transport"))]
pub use self::subscription::ConsumerRouteState;
pub(super) use self::{
    consumer_setup::{
        CommittedConsumerSetup, ConsumerSetupOrigin, ConsumerSetupOutcome, ConsumerSetupTarget,
        DeclaredConsumerSetup, PendingConsumerSetup,
    },
    producer::{
        ProducerActivityCommit, PublishCommit, PublishIntentPlan, UnpublishCommit, ValidatedPublish,
    },
    subscription::{ReceiverRouteActivity, ReceiverRouteCommit, ReceiverRouteWork},
    topology::{
        CommittedTransportReceipt, RoomTopology, SessionPlacementCommit, SessionPlacementRejection,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ConsumerKey {
    pub consumer_user_id: UserId,
    pub source_id: PublishedSourceId,
}

impl ConsumerKey {
    pub fn new(consumer_user_id: &UserId, source_id: PublishedSourceId) -> Self {
        Self {
            consumer_user_id: consumer_user_id.clone(),
            source_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct SourceKey {
    owner_user_id: UserId,
    stream_id: UserStreamId,
}

#[derive(Debug)]
pub(super) struct PublishedSource {
    pub descriptor: PublishedSourceDescriptor,
    pub transport: TransportSourceKey,
    pub rtp: rtp::MediaStream,
    pub routed: RoutedProducerId,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TransportMediaRemoval {
    pub user: UserId,
    pub connection: ConnectionId,
    pub transport_media: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConsumerState {
    pub consumer_connection_id: ConnectionId,
    pub source_connection_id: ConnectionId,
    pub source_media: TransportMediaId,
    pub consumer_media: TransportMediaId,
    pub consumer_mid: String,
}

#[derive(Debug, Clone)]
pub(super) struct ConsumerRouteView<'a> {
    pub consumer_user_id: UserId,
    pub state: &'a ConsumerState,
    pub source: &'a PublishedSource,
    pub selection: Option<ConsumerSourceSelection>,
}

impl ConsumerRouteView<'_> {
    pub fn selection_or_open(&self, active: bool) -> ConsumerSourceSelection {
        self.selection
            .unwrap_or_else(|| ConsumerSourceSelection::open(active))
    }

    pub fn transport_ref(&self) -> ConsumerRouteTransportRef {
        ConsumerRouteTransportRef {
            consumer_user_id: self.consumer_user_id.clone(),
            consumer_connection_id: self.state.consumer_connection_id,
            consumer_media: self.state.consumer_media,
            source_user_id: self.source.descriptor.owner().user_id().clone(),
            source_connection_id: self.state.source_connection_id,
            source_media: self.state.source_media,
        }
    }

    pub fn matches_transport_ref(&self, route: &ConsumerRouteTransportRef) -> bool {
        self.consumer_user_id == route.consumer_user_id
            && self.state.consumer_connection_id == route.consumer_connection_id
            && self.state.consumer_media == route.consumer_media
            && self.source.descriptor.owner().user_id() == &route.source_user_id
            && self.state.source_connection_id == route.source_connection_id
            && self.state.source_media == route.source_media
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PendingConsumerRouteView<'a> {
    pub source: &'a PublishedSource,
    pub selection: Option<ConsumerSourceSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConsumerRouteTransportRef {
    pub consumer_user_id: UserId,
    pub consumer_connection_id: ConnectionId,
    pub consumer_media: TransportMediaId,
    pub source_user_id: UserId,
    pub source_connection_id: ConnectionId,
    pub source_media: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRouteTarget {
    transport_route: TransportConsumerRoute,
    stream_id: UserStreamId,
    kind: MediaKind,
}

impl ConsumerRouteTarget {
    fn new(
        transport_route: TransportConsumerRoute,
        stream_id: UserStreamId,
        kind: MediaKind,
    ) -> Self {
        Self {
            transport_route,
            stream_id,
            kind,
        }
    }

    pub const fn transport_route(&self) -> &TransportConsumerRoute {
        &self.transport_route
    }

    pub const fn consumer_media_id(&self) -> TransportMediaId {
        self.transport_route.consumer_transport_media_id()
    }

    pub fn producer_user_id(&self) -> &UserId {
        self.transport_route.source_session_key().user_id()
    }

    pub const fn source_media_id(&self) -> TransportMediaId {
        self.transport_route.source_transport_media_id()
    }

    pub fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }

    pub fn request_keyframe_after_activity(&self, active: bool) -> bool {
        active && self.kind == MediaKind::Video
    }
}

impl SourceKey {
    pub fn new(owner_user_id: &UserId, stream_id: &UserStreamId) -> Self {
        Self {
            owner_user_id: owner_user_id.clone(),
            stream_id: stream_id.clone(),
        }
    }
}

impl TransportMediaRemoval {
    pub fn new(user: UserId, connection: ConnectionId, transport_media: TransportMediaId) -> Self {
        Self {
            user,
            connection,
            transport_media,
        }
    }
}

fn remove_from_index_set<K, V>(index: &mut BTreeMap<K, BTreeSet<V>>, key: &K, value: &V)
where
    K: Ord,
    V: Ord,
{
    let Some(values) = index.get_mut(key) else {
        return;
    };
    values.remove(value);
    if values.is_empty() {
        index.remove(key);
    }
}
