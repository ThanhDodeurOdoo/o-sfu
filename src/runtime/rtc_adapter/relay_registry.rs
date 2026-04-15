use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, PoisonError, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::runtime::transport_adapter::TransportMediaId;

use super::forwarded_packet::ForwardedPacket;

pub(super) trait RelayPacketSink: Send + Sync {
    fn forward_packet(&self, packet: &ForwardedPacket, source_transport_media_id: TransportMediaId);
}

pub(super) struct RelayRegistry {
    any_active: AtomicBool,
    active_channels: RwLock<HashMap<u64, Arc<dyn RelayPacketSink>>>,
}

impl Default for RelayRegistry {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            active_channels: RwLock::new(HashMap::new()),
        }
    }
}

impl RelayRegistry {
    pub(super) fn sink_for_channel(
        &self,
        channel_runtime_id: u64,
    ) -> Option<Arc<dyn RelayPacketSink>> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&channel_runtime_id)
            .cloned()
    }

    #[cfg(test)]
    pub(super) fn activate_channel(&self, channel_runtime_id: u64, sink: Arc<dyn RelayPacketSink>) {
        self.active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(channel_runtime_id, sink);
        self.any_active.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn deactivate_channel(&self, channel_runtime_id: u64) {
        let mut active_channels = self
            .active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_channels.remove(&channel_runtime_id);
        self.any_active
            .store(!active_channels.is_empty(), Ordering::Release);
    }

    fn active_channel_count(&self) -> usize {
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl fmt::Debug for RelayRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayRegistry")
            .field("any_active", &self.any_active.load(Ordering::Relaxed))
            .field("active_channel_count", &self.active_channel_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::runtime::rtc_adapter::sample_forwarded_packet;
    use crate::runtime::transport_adapter::TransportSessionKey;
    use crate::signaling::shared::SessionId;

    struct CountingRelaySink {
        packets: AtomicUsize,
    }

    impl CountingRelaySink {
        fn new() -> Self {
            Self {
                packets: AtomicUsize::new(0),
            }
        }
    }

    impl RelayPacketSink for CountingRelaySink {
        fn forward_packet(
            &self,
            _packet: &ForwardedPacket,
            _source_transport_media_id: TransportMediaId,
        ) {
            self.packets.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn relay_registry_tracks_active_channels() {
        let registry = RelayRegistry::default();
        let sink = Arc::new(CountingRelaySink::new());
        let channel_runtime_id = 12;

        registry.activate_channel(channel_runtime_id, sink);
        assert!(registry.sink_for_channel(channel_runtime_id).is_some());

        registry.deactivate_channel(channel_runtime_id);
        assert!(registry.sink_for_channel(channel_runtime_id).is_none());
    }

    #[test]
    fn relay_registry_returns_registered_sink() {
        let registry = RelayRegistry::default();
        let sink = Arc::new(CountingRelaySink::new());
        let channel_runtime_id = 13;
        let session_key = TransportSessionKey::new(13, 0, 14, SessionId::Integer(15));
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        registry.activate_channel(channel_runtime_id, Arc::<CountingRelaySink>::clone(&sink));

        let relay_sink = registry.sink_for_channel(channel_runtime_id);
        assert!(relay_sink.is_some());
        if let Some(relay_sink) = relay_sink {
            relay_sink.forward_packet(&packet, TransportMediaId::new(9));
        }

        assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    }
}
