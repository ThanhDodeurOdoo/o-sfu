use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    sync::{
        Arc, PoisonError, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::runtime::{
    ChannelInstanceId, rtc_adapter::ForwardedPacket, transport_adapter::TransportMediaId,
};

use super::{MediaPacketSink, MediaSource};

/// Shared channel-to-sink registry used by the production media tap and the
/// Loom model that exercises its visibility rules.
#[derive(Debug, Clone)]
pub struct ActiveChannelRegistry<K, V> {
    channels: HashMap<K, V>,
}

impl<K, V> Default for ActiveChannelRegistry<K, V> {
    fn default() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }
}

impl<K, V> ActiveChannelRegistry<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    pub fn insert(&mut self, channel_instance_id: K, sink: V) {
        self.channels.insert(channel_instance_id, sink);
    }

    pub fn remove(&mut self, channel_instance_id: &K) -> bool {
        self.channels.remove(channel_instance_id).is_some()
    }

    pub fn get(&self, channel_instance_id: &K) -> Option<V> {
        self.channels.get(channel_instance_id).cloned()
    }

    pub fn contains_key(&self, channel_instance_id: &K) -> bool {
        self.channels.contains_key(channel_instance_id)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.channels.len()
    }
}

pub(crate) struct MediaTap {
    any_active: AtomicBool,
    active_channels: RwLock<ActiveChannelRegistry<ChannelInstanceId, Arc<dyn MediaPacketSink>>>,
}

impl Default for MediaTap {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            active_channels: RwLock::new(ActiveChannelRegistry::default()),
        }
    }
}

impl MediaTap {
    pub(crate) fn sink_for_channel(
        &self,
        channel_instance_id: ChannelInstanceId,
    ) -> Option<Arc<dyn MediaPacketSink>> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&channel_instance_id)
    }

    pub(crate) fn write_packet(
        &self,
        packet: &ForwardedPacket,
        transport_media_id: TransportMediaId,
    ) {
        let Some(sink) = self.sink_for_channel(packet.source_session_key().channel_instance_id())
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

    pub(super) fn has_active_channel(&self, channel_instance_id: ChannelInstanceId) -> bool {
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&channel_instance_id)
    }

    fn active_channel_count(&self) -> usize {
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl MediaSource for MediaTap {
    fn activate_channel(
        &self,
        channel_instance_id: ChannelInstanceId,
        sink: Arc<dyn MediaPacketSink>,
    ) {
        self.active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(channel_instance_id, sink);
        self.any_active.store(true, Ordering::Release);
    }

    fn deactivate_channel(&self, channel_instance_id: ChannelInstanceId) {
        let mut active_channels = self
            .active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_channels.remove(&channel_instance_id);
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
