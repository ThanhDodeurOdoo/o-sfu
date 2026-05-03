use std::{sync::Arc, time::Instant};

use crate::runtime::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
    recording::{
        MediaPacketSink, MediaTap,
        test_support::{into_media_source, is_room_active},
    },
};

struct NoopSink;

impl MediaPacketSink for NoopSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
    }
}

#[test]
fn media_source_trait_object_can_activate_and_deactivate_rooms() {
    let tap = Arc::new(MediaTap::default());
    let media_source = into_media_source(Arc::<MediaTap>::clone(&tap));
    let sink: Arc<dyn MediaPacketSink> = Arc::new(NoopSink);

    media_source.activate_room(RoomInstanceId::from_raw(7), sink);
    assert!(is_room_active(&tap, RoomInstanceId::from_raw(7)));

    media_source.deactivate_room(RoomInstanceId::from_raw(7));
    assert!(!is_room_active(&tap, RoomInstanceId::from_raw(7)));
}
