use std::{
    array::from_fn,
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Instant,
};

use o_sfu_router::{ProducerId, RouterEvent, SessionId as UserId, TransportId};

use super::{MediaPacketSink, into_packet_sink, user::RecordingSession};
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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordingLifecycleState {
    Idle,
    Starting,
    Recording,
    Stopping,
    Finalizing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordingTransitionError {
    action: RecordingAction,
    pub(super) state: RecordingLifecycleState,
}

impl RecordingLifecycleState {
    const fn as_u8(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Starting => 1,
            Self::Recording => 2,
            Self::Stopping => 3,
            Self::Finalizing => 4,
        }
    }

    const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Starting,
            2 => Self::Recording,
            3 => Self::Stopping,
            4 => Self::Finalizing,
            _ => Self::Idle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordingAction {
    Start,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordingServiceSnapshot {
    pub(crate) lifecycle: RecordingLifecycleState,
    pub(crate) user_count: usize,
    pub(crate) producer_count: usize,
    pub(crate) captured_packet_count: u64,
    pub(crate) captured_stream_count: usize,
}

struct RecordingPacketCollector {
    lifecycle: Arc<AtomicU8>,
    captured_packet_count: Arc<AtomicU64>,
    captured_streams: Arc<RwLock<BTreeSet<TransportMediaId>>>,
    recent_captured_streams: [AtomicU64; RECENT_CAPTURED_STREAM_CACHE_SLOTS],
    metrics: Arc<RuntimeMetrics>,
}

impl MediaPacketSink for RecordingPacketCollector {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        if RecordingLifecycleState::from_u8(self.lifecycle.load(Ordering::Acquire))
            != RecordingLifecycleState::Recording
        {
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

impl RecordingPacketCollector {
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

struct RecordingServiceState {
    users: BTreeMap<UserId, RecordingSession>,
}

pub(crate) struct RecordingService {
    room_instance_id: RoomInstanceId,
    packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    lifecycle: Arc<AtomicU8>,
    users: Arc<Mutex<RecordingServiceState>>,
    captured_packet_count: Arc<AtomicU64>,
    captured_streams: Arc<RwLock<BTreeSet<TransportMediaId>>>,
    packet_collector: Arc<RecordingPacketCollector>,
}

impl RecordingService {
    pub(crate) fn new(
        room_instance_id: RoomInstanceId,
        packet_sink_registry: Arc<RoomPacketSinkRegistry>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let lifecycle = Arc::new(AtomicU8::new(RecordingLifecycleState::Idle.as_u8()));
        let users = Arc::new(Mutex::new(RecordingServiceState {
            users: BTreeMap::new(),
        }));
        let captured_packet_count = Arc::new(AtomicU64::new(0));
        let captured_streams = Arc::new(RwLock::new(BTreeSet::new()));
        Self {
            room_instance_id,
            packet_sink_registry,
            lifecycle: Arc::clone(&lifecycle),
            users: Arc::clone(&users),
            captured_packet_count: Arc::clone(&captured_packet_count),
            captured_streams: Arc::clone(&captured_streams),
            packet_collector: Arc::new(RecordingPacketCollector {
                lifecycle,
                captured_packet_count,
                captured_streams,
                recent_captured_streams: from_fn(|_| {
                    AtomicU64::new(EMPTY_CAPTURED_STREAM_CACHE_ENTRY)
                }),
                metrics,
            }),
        }
    }

    pub(crate) fn start(&self) -> Result<(), RecordingTransitionError> {
        self.transition_lifecycle(
            RecordingLifecycleState::Idle,
            RecordingLifecycleState::Starting,
            RecordingAction::Start,
        )?;
        let sink = into_packet_sink(Arc::<RecordingPacketCollector>::clone(
            &self.packet_collector,
        ));
        self.packet_sink_registry.register_room(
            self.room_instance_id,
            sink,
            RtpForwardDestinationKind::Recording,
        );
        self.lifecycle.store(
            RecordingLifecycleState::Recording.as_u8(),
            Ordering::Release,
        );
        Ok(())
    }

    pub(crate) fn stop(&self) -> Result<(), RecordingTransitionError> {
        self.transition_lifecycle(
            RecordingLifecycleState::Recording,
            RecordingLifecycleState::Stopping,
            RecordingAction::Stop,
        )?;
        self.packet_sink_registry
            .unregister_room(self.room_instance_id);
        self.lifecycle.store(
            RecordingLifecycleState::Finalizing.as_u8(),
            Ordering::Release,
        );
        self.lifecycle
            .store(RecordingLifecycleState::Idle.as_u8(), Ordering::Release);
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> RecordingServiceSnapshot {
        let lifecycle = RecordingLifecycleState::from_u8(self.lifecycle.load(Ordering::Acquire));
        let state = self.lock_sessions();
        let captured_streams = read_unpoisoned(&self.captured_streams);
        RecordingServiceSnapshot {
            lifecycle,
            user_count: state.users.len(),
            producer_count: state
                .users
                .values()
                .map(RecordingSession::producer_count)
                .sum(),
            captured_packet_count: self.captured_packet_count.load(Ordering::Acquire),
            captured_stream_count: captured_streams.len(),
        }
    }

    fn handle_router_event(&self, event: RouterEvent) {
        let mut state = self.lock_sessions();
        match event {
            RouterEvent::SessionJoined {
                session_id: user_id,
            } => {
                state.users.entry(user_id).or_default();
            }
            RouterEvent::SessionLeft {
                session_id: user_id,
            } => {
                state.users.remove(&user_id);
            }
            RouterEvent::ProducerAdded {
                session_id: user_id,
                transport_id,
                producer_id,
                media_kind,
            } => add_tracked_producer(
                &mut state.users,
                user_id,
                producer_id,
                transport_id,
                media_kind,
            ),
            RouterEvent::ProducerRemoved {
                session_id: user_id,
                producer_id,
                ..
            } => {
                if let Some(user) = state.users.get_mut(&user_id) {
                    user.remove_producer(producer_id);
                }
            }
        }
    }

    fn transition_lifecycle(
        &self,
        expected: RecordingLifecycleState,
        next: RecordingLifecycleState,
        action: RecordingAction,
    ) -> Result<(), RecordingTransitionError> {
        self.lifecycle
            .compare_exchange(
                expected.as_u8(),
                next.as_u8(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|state| RecordingTransitionError {
                action,
                state: RecordingLifecycleState::from_u8(state),
            })?;
        Ok(())
    }

    fn lock_sessions(&self) -> MutexGuard<'_, RecordingServiceState> {
        lock_unpoisoned(&self.users)
    }
}

fn add_tracked_producer(
    users: &mut BTreeMap<UserId, RecordingSession>,
    user_id: UserId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: o_sfu_router::MediaKind,
) {
    users
        .entry(user_id)
        .or_default()
        .add_producer(producer_id, transport_id, media_kind);
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
