//! Pure room media state split by responsibility.
//!
//! - `subscription` owns consumer-side subscription intent, route activity
//!   updates, bootstrap planning, and bootstrap commit paths.
//! - `producer` owns producer publish lifecycle, unpublish cleanup, and activity fan-out.
//! - `graph` owns source, producer, consumer and pending-bootstrap indexes.

mod graph;
mod producer;
pub(super) mod relay;
mod subscription;

#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

#[cfg(test)]
mod tests;

pub(super) use self::graph::{
    ConsumerKey, ConsumerRouteView, ConsumerState, ProducerRouteTarget, PublishedProducer,
    PublishedSourceInstall, RoomMediaGraph, SourceKey, SourceTransportMediaIndexEntry,
};
pub use self::subscription::{ConsumerRouteState, RemoteTrackBootstrap};
pub(in crate::runtime::room) use self::{
    graph::{ConsumerRouteTransportRef, TransportMediaRemoval},
    producer::ValidatedPublishDescriptor,
    relay::{RelayRouteEffect, RelayRouteKey},
    subscription::{
        ConsumerBootstrapOrigin, ConsumerRouteUpdate, PendingConsumerBootstrap,
        PendingConsumerBootstrapTarget, PlannedConsumerBootstrap, PlannedSubscriptionChange,
        PreparedConsumerBootstrap,
    },
};
