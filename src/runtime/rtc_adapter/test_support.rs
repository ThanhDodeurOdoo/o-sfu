#[cfg(test)]
pub(crate) use super::commands::debug::DebugRouteEntry;

use crate::runtime::{ChannelInstanceId, ConnectionId, transport_adapter::TransportSessionKey};
use o_sfu_protocol::shared::SessionId;

#[must_use]
pub fn test_transport_session_key(
    channel_instance_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    session_id: SessionId,
) -> TransportSessionKey {
    TransportSessionKey::new(
        ChannelInstanceId::from_raw(channel_instance_id),
        media_worker_id,
        ConnectionId::from_raw(connection_id),
        session_id,
    )
}
