use o_sfu_protocol::shared::SessionId;

#[cfg(test)]
pub(crate) use super::commands::debug::DebugRouteEntry;
#[cfg(test)]
pub(crate) use super::forwarded_packet::test_support::{
    sample_forwarded_packet, sample_forwarded_packet_with_audio_activity,
    sample_forwarded_packet_with_frame_mark, sample_forwarded_packet_with_rid,
};
use crate::runtime::{ChannelInstanceId, ConnectionId, transport_adapter::TransportSessionKey};

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
