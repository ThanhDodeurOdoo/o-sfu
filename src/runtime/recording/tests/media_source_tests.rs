use std::{sync::Arc, time::Instant};

use crate::runtime::{
    ChannelInstanceId,
    recording::{
        MediaPacketSink, MediaTap,
        test_support::{into_media_source, is_channel_active},
    },
    transport_adapter::{TransportMediaId, TransportSessionKey},
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
fn media_source_trait_object_can_activate_and_deactivate_channels() {
    let tap = Arc::new(MediaTap::default());
    let media_source = into_media_source(Arc::<MediaTap>::clone(&tap));
    let sink: Arc<dyn MediaPacketSink> = Arc::new(NoopSink);

    media_source.activate_channel(ChannelInstanceId::from_raw(7), sink);
    assert!(is_channel_active(&tap, ChannelInstanceId::from_raw(7)));

    media_source.deactivate_channel(ChannelInstanceId::from_raw(7));
    assert!(!is_channel_active(&tap, ChannelInstanceId::from_raw(7)));
}
