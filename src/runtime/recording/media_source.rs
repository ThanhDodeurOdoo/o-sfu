use std::{sync::Arc, time::Instant};

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

pub(crate) trait MediaFrameSink: Send + Sync {
    fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        received_at: Instant,
        payload: &[u8],
    );
}

pub(crate) trait MediaSource: Send + Sync {
    fn activate_channel(&self, channel_runtime_id: u64, sink: Arc<dyn MediaFrameSink>);
    fn deactivate_channel(&self, channel_runtime_id: u64);
}

pub(crate) fn into_frame_sink<T>(sink: Arc<T>) -> Arc<dyn MediaFrameSink>
where
    T: MediaFrameSink + 'static,
{
    sink
}

pub(crate) fn into_media_source<T>(source: Arc<T>) -> Arc<dyn MediaSource>
where
    T: MediaSource + 'static,
{
    source
}
