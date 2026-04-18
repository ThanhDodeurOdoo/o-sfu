use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, PoisonError, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::runtime::{rtc_adapter::ForwardedPacket, transport_adapter::TransportMediaId};

use super::{MediaPacketSink, MediaSource};

pub(crate) struct MediaTap {
    any_active: AtomicBool,
    active_channels: RwLock<HashMap<u64, Arc<dyn MediaPacketSink>>>,
}

impl Default for MediaTap {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            active_channels: RwLock::new(HashMap::new()),
        }
    }
}

impl MediaTap {
    pub(crate) fn sink_for_channel(
        &self,
        channel_runtime_id: u64,
    ) -> Option<Arc<dyn MediaPacketSink>> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&channel_runtime_id)
            .cloned()
    }

    pub(crate) fn write_packet(
        &self,
        packet: &ForwardedPacket,
        transport_media_id: TransportMediaId,
    ) {
        let Some(sink) = self.sink_for_channel(packet.source_session_key().channel_runtime_id())
        else {
            return;
        };
        sink.record_packet(
            packet.source_session_key(),
            transport_media_id,
            packet.received_at(),
            packet.payload().as_slice(),
        );
    }

    pub(super) fn has_active_channel(&self, channel_runtime_id: u64) -> bool {
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&channel_runtime_id)
    }

    fn active_channel_count(&self) -> usize {
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl MediaSource for MediaTap {
    fn activate_channel(&self, channel_runtime_id: u64, sink: Arc<dyn MediaPacketSink>) {
        self.active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(channel_runtime_id, sink);
        self.any_active.store(true, Ordering::Release);
    }

    fn deactivate_channel(&self, channel_runtime_id: u64) {
        let mut active_channels = self
            .active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_channels.remove(&channel_runtime_id);
        self.any_active
            .store(!active_channels.is_empty(), Ordering::Release);
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
