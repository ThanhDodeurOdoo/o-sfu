//! Pure channel media state split by responsibility.
//!
//! - `bootstrap` owns consumer bootstrap planning and commit paths.
//! - `download` owns desired-download persistence and route activity updates.
//! - `producer` owns producer publish lifecycle, unpublish cleanup, and activity fan-out.
//! - `router_stream_type` keeps the shared router-native stream-type translation local to this boundary.

mod bootstrap;
mod download;
mod producer;
mod router_stream_type;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests;

pub(crate) use self::bootstrap::RemoteTrackBootstrap;
pub(in crate::runtime::channel) use self::bootstrap::{
    ConsumerBootstrapOrigin, PendingConsumerBootstrap, PendingConsumerBootstrapTarget,
    PreparedConsumerBootstrap,
};
pub(crate) use self::download::ConsumerRouteState;
pub(in crate::runtime::channel) use self::download::ConsumerRouteUpdate;
pub(in crate::runtime::channel) use self::producer::{
    PreparedPublishedTrack, ValidatedPublishDescriptor,
};
