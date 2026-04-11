use std::sync::Arc;
use std::time::Instant;

use crate::runtime::recording::{MediaFrameSink, MediaTap, into_media_source};
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

struct NoopSink;

impl MediaFrameSink for NoopSink {
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
    let sink: Arc<dyn MediaFrameSink> = Arc::new(NoopSink);

    media_source.activate_channel(7, sink);
    assert!(tap.is_channel_active(7));

    media_source.deactivate_channel(7);
    assert!(!tap.is_channel_active(7));
}
