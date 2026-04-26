//! Pure room media state split by responsibility.
//!
//! - `subscription` owns consumer-side subscription intent, route activity
//!   updates, bootstrap planning, and bootstrap commit paths.
//! - `producer` owns producer publish lifecycle, unpublish cleanup, and activity fan-out.

mod producer;
mod subscription;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests;

pub(crate) use self::subscription::{ConsumerRouteState, RemoteTrackBootstrap};
pub(in crate::runtime::room) use self::{
    producer::ValidatedPublishDescriptor,
    subscription::{
        ConsumerBootstrapOrigin, ConsumerRouteUpdate, PendingConsumerBootstrap,
        PendingConsumerBootstrapTarget, PlannedConsumerBootstrap, PlannedSubscriptionChange,
        PreparedConsumerBootstrap,
    },
};
