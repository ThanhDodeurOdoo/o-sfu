use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::{MediaFrameSink, MediaSource};

pub(crate) struct MediaTap {
    any_active: AtomicBool,
    active_channels: Mutex<BTreeMap<u64, Arc<dyn MediaFrameSink>>>,
}

impl Default for MediaTap {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            active_channels: Mutex::new(BTreeMap::new()),
        }
    }
}

impl MediaTap {
    pub(crate) fn write_frame(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        received_at: Instant,
        payload: &[u8],
    ) {
        if !self.any_active.load(Ordering::Relaxed) {
            return;
        }
        let sink = self
            .active_channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&session_key.channel_runtime_id())
            .cloned();
        if let Some(sink) = sink {
            sink.record_packet(session_key, transport_media_id, received_at, payload);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_channel_active(&self, channel_runtime_id: u64) -> bool {
        self.active_channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&channel_runtime_id)
    }

    fn active_channel_count(&self) -> usize {
        self.active_channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl MediaSource for MediaTap {
    fn activate_channel(&self, channel_runtime_id: u64, sink: Arc<dyn MediaFrameSink>) {
        self.active_channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(channel_runtime_id, sink);
        self.any_active.store(true, Ordering::Release);
    }

    fn deactivate_channel(&self, channel_runtime_id: u64) {
        let mut active_channels = self
            .active_channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        active_channels.remove(&channel_runtime_id);
        let has_active_channels = !active_channels.is_empty();
        drop(active_channels);
        self.any_active
            .store(has_active_channels, Ordering::Release);
    }
}

impl fmt::Debug for MediaTap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MediaTap")
            .field("any_active", &self.any_active.load(Ordering::Relaxed))
            .field("active_channel_count", &self.active_channel_count())
            .finish_non_exhaustive()
    }
}
