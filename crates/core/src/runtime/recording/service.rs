use std::{
    array::from_fn,
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use o_sfu_router::{RouterEvent, SessionId as UserId};

use super::{MediaPacketSink, user::RecordingSession};
use crate::runtime::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::{RtpForwardDestinationKind, RuntimeMetrics},
    packet_sink_registry::RoomPacketSinkRegistry,
    router_events::RoomRouterEventSink,
    sync::{lock_unpoisoned, read_unpoisoned, write_unpoisoned},
};

const RECENT_CAPTURED_STREAM_CACHE_SLOTS: usize = 64;
const EMPTY_CAPTURED_STREAM_CACHE_ENTRY: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordingLifecycleState {
    Idle,
    Recording,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordingTransitionError {
    pub(super) state: RecordingLifecycleState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordingServiceSnapshot {
    pub(crate) lifecycle: RecordingLifecycleState,
    pub(crate) user_count: usize,
    pub(crate) producer_count: usize,
    pub(crate) captured_packet_count: u64,
    pub(crate) captured_stream_count: usize,
}

struct RecordingCaptureState {
    active: AtomicBool,
    captured_packet_count: AtomicU64,
    captured_streams: RwLock<BTreeSet<TransportMediaId>>,
    recent_captured_streams: [AtomicU64; RECENT_CAPTURED_STREAM_CACHE_SLOTS],
    metrics: Arc<RuntimeMetrics>,
}

impl MediaPacketSink for RecordingCaptureState {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }

        self.captured_packet_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_recording_captured_packet();

        if self.recent_stream_cache_contains(transport_media_id) {
            return;
        }

        let is_new_stream = {
            let mut captured_streams = write_unpoisoned(&self.captured_streams);
            captured_streams.insert(transport_media_id)
        };
        if is_new_stream {
            self.metrics.record_recording_captured_stream();
        }
        self.remember_recent_stream(transport_media_id);
    }
}

impl RecordingCaptureState {
    fn new(metrics: Arc<RuntimeMetrics>) -> Self {
        Self {
            active: AtomicBool::new(false),
            captured_packet_count: AtomicU64::new(0),
            captured_streams: RwLock::new(BTreeSet::new()),
            recent_captured_streams: from_fn(|_| AtomicU64::new(EMPTY_CAPTURED_STREAM_CACHE_ENTRY)),
            metrics,
        }
    }

    fn snapshot(&self, user_count: usize, producer_count: usize) -> RecordingServiceSnapshot {
        RecordingServiceSnapshot {
            lifecycle: self.lifecycle_state(),
            user_count,
            producer_count,
            captured_packet_count: self.captured_packet_count.load(Ordering::Acquire),
            captured_stream_count: read_unpoisoned(&self.captured_streams).len(),
        }
    }

    fn start(&self) -> Result<(), RecordingTransitionError> {
        self.active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(Self::transition_error)?;
        Ok(())
    }

    fn stop(&self) -> Result<(), RecordingTransitionError> {
        self.active
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .map_err(Self::transition_error)?;
        Ok(())
    }

    fn transition_error(active: bool) -> RecordingTransitionError {
        RecordingTransitionError {
            state: if active {
                RecordingLifecycleState::Recording
            } else {
                RecordingLifecycleState::Idle
            },
        }
    }

    fn lifecycle_state(&self) -> RecordingLifecycleState {
        if self.active.load(Ordering::Acquire) {
            RecordingLifecycleState::Recording
        } else {
            RecordingLifecycleState::Idle
        }
    }

    fn recent_stream_cache_contains(&self, transport_media_id: TransportMediaId) -> bool {
        let slot = self.recent_stream_cache_slot(transport_media_id);
        self.recent_captured_streams.get(slot).is_some_and(|entry| {
            entry.load(Ordering::Relaxed) == stream_cache_entry(transport_media_id)
        })
    }

    fn remember_recent_stream(&self, transport_media_id: TransportMediaId) {
        let slot = self.recent_stream_cache_slot(transport_media_id);
        if let Some(entry) = self.recent_captured_streams.get(slot) {
            entry.store(stream_cache_entry(transport_media_id), Ordering::Relaxed);
        }
    }

    fn recent_stream_cache_slot(&self, transport_media_id: TransportMediaId) -> usize {
        let slot_count = u64::try_from(self.recent_captured_streams.len()).unwrap_or(1);
        let slot = transport_media_id.as_u64() % slot_count;
        usize::try_from(slot).unwrap_or(0)
    }
}

fn stream_cache_entry(transport_media_id: TransportMediaId) -> u64 {
    transport_media_id.as_u64().saturating_add(1)
}

pub(crate) struct RecordingService {
    room_instance_id: RoomInstanceId,
    packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    capture: Arc<RecordingCaptureState>,
    users: Mutex<BTreeMap<UserId, RecordingSession>>,
}

impl RecordingService {
    pub(crate) fn new(
        room_instance_id: RoomInstanceId,
        packet_sink_registry: Arc<RoomPacketSinkRegistry>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            room_instance_id,
            packet_sink_registry,
            capture: Arc::new(RecordingCaptureState::new(metrics)),
            users: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn start(&self) -> Result<(), RecordingTransitionError> {
        self.capture.start()?;
        self.packet_sink_registry.register_room(
            self.room_instance_id,
            Arc::<RecordingCaptureState>::clone(&self.capture),
            RtpForwardDestinationKind::Recording,
        );
        Ok(())
    }

    pub(crate) fn stop(&self) -> Result<(), RecordingTransitionError> {
        self.capture.stop()?;
        self.packet_sink_registry
            .unregister_room(self.room_instance_id);
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> RecordingServiceSnapshot {
        let users = self.lock_sessions();
        self.capture.snapshot(
            users.len(),
            users.values().map(RecordingSession::producer_count).sum(),
        )
    }

    fn handle_router_event(&self, event: RouterEvent) {
        let mut users = self.lock_sessions();
        match event {
            RouterEvent::SessionJoined {
                session_id: user_id,
            } => {
                users.entry(user_id).or_default();
            }
            RouterEvent::SessionLeft {
                session_id: user_id,
            } => {
                users.remove(&user_id);
            }
            RouterEvent::ProducerAdded {
                session_id: user_id,
                transport_id,
                producer_id,
                media_kind,
            } => {
                users.entry(user_id).or_default().add_producer(
                    producer_id,
                    transport_id,
                    media_kind,
                );
            }
            RouterEvent::ProducerRemoved {
                session_id: user_id,
                producer_id,
                ..
            } => {
                if let Some(user) = users.get_mut(&user_id) {
                    user.remove_producer(producer_id);
                }
            }
        }
    }

    fn lock_sessions(&self) -> MutexGuard<'_, BTreeMap<UserId, RecordingSession>> {
        lock_unpoisoned(&self.users)
    }
}

impl RoomRouterEventSink for RecordingService {
    fn handle_room_router_event(&self, event: RouterEvent) {
        self.handle_router_event(event);
    }
}

impl fmt::Debug for RecordingService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingService")
            .field("room_instance_id", &self.room_instance_id)
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}
