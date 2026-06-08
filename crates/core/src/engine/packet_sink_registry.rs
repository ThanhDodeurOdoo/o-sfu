use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

#[cfg(any(test, feature = "testing-transport"))]
use super::media_transport::ForwardedPacket;
use super::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::RtpForwardDestinationKind,
    sync::{read_unpoisoned, write_unpoisoned},
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
    generation: AtomicU64,
    active_rooms: RwLock<HashMap<RoomInstanceId, RegisteredPacketSink>>,
}

impl Default for RoomPacketSinkRegistry {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            active_rooms: RwLock::new(HashMap::new()),
        }
    }
}

pub trait PacketSinkLookup {
    fn sink_for_room(&self, room_instance_id: RoomInstanceId) -> Option<RegisteredPacketSink>;
}

#[derive(Default)]
pub struct PacketSinkRouteCache {
    generation: u64,
    active_rooms: HashMap<RoomInstanceId, RegisteredPacketSink>,
}

impl PacketSinkRouteCache {
    pub fn refresh_from(&mut self, registry: &RoomPacketSinkRegistry) {
        let generation = registry.generation();
        if self.generation == generation {
            return;
        }
        let snapshot = registry.snapshot();
        self.generation = snapshot.generation;
        self.active_rooms = snapshot.active_rooms;
    }
}

impl PacketSinkLookup for PacketSinkRouteCache {
    #[inline]
    fn sink_for_room(&self, room_instance_id: RoomInstanceId) -> Option<RegisteredPacketSink> {
        self.active_rooms.get(&room_instance_id).cloned()
    }
}

impl PacketSinkLookup for RoomPacketSinkRegistry {
    #[inline]
    fn sink_for_room(&self, room_instance_id: RoomInstanceId) -> Option<RegisteredPacketSink> {
        Self::sink_for_room(self, room_instance_id)
    }
}

struct PacketSinkRegistrySnapshot {
    generation: u64,
    active_rooms: HashMap<RoomInstanceId, RegisteredPacketSink>,
}

impl RoomPacketSinkRegistry {
    #[inline]
    pub fn sink_for_room(&self, room_instance_id: RoomInstanceId) -> Option<RegisteredPacketSink> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        read_unpoisoned(&self.active_rooms)
            .get(&room_instance_id)
            .cloned()
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn snapshot(&self) -> PacketSinkRegistrySnapshot {
        loop {
            let generation = self.generation();
            let active_rooms = read_unpoisoned(&self.active_rooms).clone();
            if generation == self.generation() {
                return PacketSinkRegistrySnapshot {
                    generation,
                    active_rooms,
                };
            }
        }
    }

    pub fn register_room(
        &self,
        room_instance_id: RoomInstanceId,
        sink: Arc<dyn PacketSink>,
        forward_destination_kind: RtpForwardDestinationKind,
    ) {
        let mut active_rooms = write_unpoisoned(&self.active_rooms);
        active_rooms.insert(
            room_instance_id,
            RegisteredPacketSink::new(sink, forward_destination_kind),
        );
        self.any_active.store(true, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        drop(active_rooms);
    }

    pub fn unregister_room(&self, room_instance_id: RoomInstanceId) {
        let mut active_rooms = write_unpoisoned(&self.active_rooms);
        active_rooms.remove(&room_instance_id);
        self.any_active
            .store(!active_rooms.is_empty(), Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        drop(active_rooms);
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn write_packet(&self, packet: &ForwardedPacket, transport_media_id: TransportMediaId) {
        let Some(src_key) = packet.stable_src_key() else {
            return;
        };
        let Some(sink) = self.sink_for_room(src_key.room_instance_id()) else {
            return;
        };
        sink.record_packet(
            src_key,
            transport_media_id,
            packet.received_at(),
            packet.payload(),
        );
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn has_active_room(&self, room_instance_id: RoomInstanceId) -> bool {
        read_unpoisoned(&self.active_rooms).contains_key(&room_instance_id)
    }

    fn active_room_count(&self) -> usize {
        read_unpoisoned(&self.active_rooms).len()
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
