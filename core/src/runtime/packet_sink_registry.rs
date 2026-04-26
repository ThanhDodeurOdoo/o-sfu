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

#[cfg(any(test, feature = "testing-transport"))]
use super::rtc_adapter::ForwardedPacket;
use super::{
    RoomInstanceId,
    metrics::RtpForwardDestinationKind,
    transport_adapter::{TransportMediaId, TransportSessionKey},
};

pub trait PacketSink: Send + Sync {
    fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        received_at: Instant,
        payload: &[u8],
    );
}

pub fn into_packet_sink<T>(sink: Arc<T>) -> Arc<dyn PacketSink>
where
    T: PacketSink + 'static,
{
    sink
}

#[derive(Debug, Clone)]
pub struct ActiveRoomRegistry<K, V> {
    rooms: HashMap<K, V>,
}

impl<K, V> Default for ActiveRoomRegistry<K, V> {
    fn default() -> Self {
        Self {
            rooms: HashMap::new(),
        }
    }
}

impl<K, V> ActiveRoomRegistry<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    pub fn insert(&mut self, room_instance_id: K, sink: V) {
        self.rooms.insert(room_instance_id, sink);
    }

    pub fn remove(&mut self, room_instance_id: &K) -> bool {
        self.rooms.remove(room_instance_id).is_some()
    }

    pub fn get(&self, room_instance_id: &K) -> Option<V> {
        self.rooms.get(room_instance_id).cloned()
    }

    pub fn contains_key(&self, room_instance_id: &K) -> bool {
        self.rooms.contains_key(room_instance_id)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rooms.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rooms.len()
    }
}

#[derive(Clone)]
pub struct RegisteredPacketSink {
    sink: Arc<dyn PacketSink>,
    forward_destination_kind: RtpForwardDestinationKind,
}

impl RegisteredPacketSink {
    pub fn new(
        sink: Arc<dyn PacketSink>,
        forward_destination_kind: RtpForwardDestinationKind,
    ) -> Self {
        Self {
            sink,
            forward_destination_kind,
        }
    }

    pub fn record_packet(
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
    pub const fn forward_destination_kind(&self) -> RtpForwardDestinationKind {
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

pub struct RoomPacketSinkRegistry {
    any_active: AtomicBool,
    active_rooms: RwLock<ActiveRoomRegistry<RoomInstanceId, RegisteredPacketSink>>,
}

impl Default for RoomPacketSinkRegistry {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            active_rooms: RwLock::new(ActiveRoomRegistry::default()),
        }
    }
}

impl RoomPacketSinkRegistry {
    pub fn sink_for_room(&self, room_instance_id: RoomInstanceId) -> Option<RegisteredPacketSink> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_rooms
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&room_instance_id)
    }

    pub fn register_room(
        &self,
        room_instance_id: RoomInstanceId,
        sink: Arc<dyn PacketSink>,
        forward_destination_kind: RtpForwardDestinationKind,
    ) {
        self.active_rooms
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                room_instance_id,
                RegisteredPacketSink::new(sink, forward_destination_kind),
            );
        self.any_active.store(true, Ordering::Release);
    }

    pub fn unregister_room(&self, room_instance_id: RoomInstanceId) {
        let mut active_rooms = self
            .active_rooms
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_rooms.remove(&room_instance_id);
        self.any_active
            .store(!active_rooms.is_empty(), Ordering::Release);
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn write_packet(&self, packet: &ForwardedPacket, transport_media_id: TransportMediaId) {
        let Some(sink) = self.sink_for_room(packet.source_session_key().room_instance_id()) else {
            return;
        };
        sink.record_packet(
            packet.source_session_key(),
            transport_media_id,
            packet.received_at(),
            packet.payload().as_slice(),
        );
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn has_active_room(&self, room_instance_id: RoomInstanceId) -> bool {
        self.active_rooms
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&room_instance_id)
    }

    fn active_room_count(&self) -> usize {
        self.active_rooms
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl fmt::Debug for RoomPacketSinkRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoomPacketSinkRegistry")
            .field("any_active", &self.any_active.load(Ordering::Relaxed))
            .field("active_room_count", &self.active_room_count())
            .finish_non_exhaustive()
    }
}
