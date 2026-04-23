use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    sync::{
        Arc, PoisonError, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

#[cfg(test)]
use super::rtc_adapter::ForwardedPacket;
use super::{
    ChannelInstanceId,
    metrics::RtpForwardDestinationKind,
    transport_adapter::{TransportMediaId, TransportSessionKey},
};

pub(crate) trait PacketSink: Send + Sync {
    fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        received_at: Instant,
        payload: &[u8],
    );
}

pub(crate) fn into_packet_sink<T>(sink: Arc<T>) -> Arc<dyn PacketSink>
where
    T: PacketSink + 'static,
{
    sink
}

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

#[derive(Clone)]
pub(crate) struct RegisteredPacketSink {
    sink: Arc<dyn PacketSink>,
    forward_destination_kind: RtpForwardDestinationKind,
}

impl RegisteredPacketSink {
    pub(crate) fn new(
        sink: Arc<dyn PacketSink>,
        forward_destination_kind: RtpForwardDestinationKind,
    ) -> Self {
        Self {
            sink,
            forward_destination_kind,
        }
    }

    pub(crate) fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        received_at: Instant,
        payload: &[u8],
    ) {
        self.sink
            .record_packet(session_key, transport_media_id, received_at, payload);
    }

    #[must_use]
    pub(crate) const fn forward_destination_kind(&self) -> RtpForwardDestinationKind {
        self.forward_destination_kind
    }
}

impl fmt::Debug for RegisteredPacketSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredPacketSink")
            .field("forward_destination_kind", &self.forward_destination_kind)
            .finish_non_exhaustive()
    }
}

pub(crate) struct ChannelPacketSinkRegistry {
    any_active: AtomicBool,
    active_channels: RwLock<ActiveChannelRegistry<ChannelInstanceId, RegisteredPacketSink>>,
}

impl Default for ChannelPacketSinkRegistry {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            active_channels: RwLock::new(ActiveChannelRegistry::default()),
        }
    }
}

impl ChannelPacketSinkRegistry {
    pub(crate) fn sink_for_channel(
        &self,
        channel_instance_id: ChannelInstanceId,
    ) -> Option<RegisteredPacketSink> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&channel_instance_id)
    }

    pub(crate) fn register_channel(
        &self,
        channel_instance_id: ChannelInstanceId,
        sink: Arc<dyn PacketSink>,
        forward_destination_kind: RtpForwardDestinationKind,
    ) {
        self.active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                channel_instance_id,
                RegisteredPacketSink::new(sink, forward_destination_kind),
            );
        self.any_active.store(true, Ordering::Release);
    }

    pub(crate) fn unregister_channel(&self, channel_instance_id: ChannelInstanceId) {
        let mut active_channels = self
            .active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_channels.remove(&channel_instance_id);
        self.any_active
            .store(!active_channels.is_empty(), Ordering::Release);
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub(crate) fn has_active_channel(&self, channel_instance_id: ChannelInstanceId) -> bool {
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

impl fmt::Debug for ChannelPacketSinkRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChannelPacketSinkRegistry")
            .field("any_active", &self.any_active.load(Ordering::Relaxed))
            .field("active_channel_count", &self.active_channel_count())
            .finish_non_exhaustive()
    }
}
