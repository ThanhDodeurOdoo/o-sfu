#[cfg(any(test, feature = "testing-transport"))]
pub use super::commands::debug::DebugRouteEntry;
#[cfg(any(test, feature = "testing-transport"))]
pub use super::forwarded_packet::test_support::{
    sample_forwarded_packet, sample_forwarded_packet_with_audio_activity,
    sample_forwarded_packet_with_frame_mark, sample_forwarded_packet_with_rid,
    sample_forwarded_packet_without_mid,
};
use crate::runtime::{
    ConnectionId, RoomInstanceId, UserId, transport_adapter::TransportSessionKey,
};

#[must_use]
pub fn test_transport_session_key(
    room_instance_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(room_instance_id),
        media_worker_id,
        ConnectionId::from_raw(connection_id),
        user_id,
    )
}
