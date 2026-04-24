use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, RwLock,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Instant,
};

use o_sfu_router::{ProducerId, RouterEvent, RouterObserver, SessionId, TransportId};

use super::{MediaPacketSink, MediaSource, into_packet_sink, session::RecordingSession};
use crate::runtime::{
    ChannelInstanceId,
    metrics::RuntimeMetrics,
    transport_adapter::{TransportMediaId, TransportSessionKey},
};

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
    pub(crate) session_count: usize,
    pub(crate) producer_count: usize,
    pub(crate) captured_packet_count: u64,
    pub(crate) captured_stream_count: usize,
}

struct RecordingPacketCollector {
    lifecycle: Arc<AtomicU8>,
    captured_packet_count: Arc<AtomicU64>,
    captured_streams: Arc<RwLock<BTreeSet<(TransportSessionKey, TransportMediaId)>>>,
    metrics: Arc<RuntimeMetrics>,
}

impl MediaPacketSink for RecordingPacketCollector {
    fn record_packet(
        &self,
        session_key: &TransportSessionKey,
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

        let key = (session_key.clone(), transport_media_id);
        {
            let captured_streams = self
                .captured_streams
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if captured_streams.contains(&key) {
                return;
            }
        }

        let mut captured_streams = self
            .captured_streams
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if captured_streams.insert(key) {
            self.metrics.record_recording_captured_stream();
        }
    }
}

struct RecordingServiceState {
    sessions: BTreeMap<SessionId, RecordingSession>,
}

// TODO: needs documentation:
pub(crate) struct RecordingService {
    channel_instance_id: ChannelInstanceId,
    media_source: Arc<dyn MediaSource>,
    lifecycle: Arc<AtomicU8>,
    sessions: Arc<Mutex<RecordingServiceState>>,
    captured_packet_count: Arc<AtomicU64>,
    captured_streams: Arc<RwLock<BTreeSet<(TransportSessionKey, TransportMediaId)>>>,
    packet_collector: Arc<RecordingPacketCollector>,
}

impl RecordingService {
    pub(crate) fn new(
        channel_instance_id: ChannelInstanceId,
        media_source: Arc<dyn MediaSource>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let lifecycle = Arc::new(AtomicU8::new(RecordingLifecycleState::Idle.as_u8()));
        let sessions = Arc::new(Mutex::new(RecordingServiceState {
            sessions: BTreeMap::new(),
        }));
        let captured_packet_count = Arc::new(AtomicU64::new(0));
        let captured_streams = Arc::new(RwLock::new(BTreeSet::new()));
        Self {
            channel_instance_id,
            media_source,
            lifecycle: Arc::clone(&lifecycle),
            sessions: Arc::clone(&sessions),
            captured_packet_count: Arc::clone(&captured_packet_count),
            captured_streams: Arc::clone(&captured_streams),
            packet_collector: Arc::new(RecordingPacketCollector {
                lifecycle,
                captured_packet_count,
                captured_streams,
                metrics,
            }),
        }
    }

    // TODO: needs documentation:
    pub(crate) fn start(&self) -> Result<(), RecordingTransitionError> {
        self.transition_lifecycle(
            RecordingLifecycleState::Idle,
            RecordingLifecycleState::Starting,
            RecordingAction::Start,
        )?;
        let sink = into_packet_sink(Arc::<RecordingPacketCollector>::clone(
            &self.packet_collector,
        ));
        self.media_source
            .activate_channel(self.channel_instance_id, sink);
        self.lifecycle.store(
            RecordingLifecycleState::Recording.as_u8(),
            Ordering::Release,
        );
        Ok(())
    }

    // TODO: needs documentation:
    pub(crate) fn stop(&self) -> Result<(), RecordingTransitionError> {
        self.transition_lifecycle(
            RecordingLifecycleState::Recording,
            RecordingLifecycleState::Stopping,
            RecordingAction::Stop,
        )?;
        self.media_source
            .deactivate_channel(self.channel_instance_id);
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
        let captured_streams = self
            .captured_streams
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        RecordingServiceSnapshot {
            lifecycle,
            session_count: state.sessions.len(),
            producer_count: state
                .sessions
                .values()
                .map(RecordingSession::producer_count)
                .sum(),
            captured_packet_count: self.captured_packet_count.load(Ordering::Acquire),
            captured_stream_count: captured_streams.len(),
        }
    }

    // TODO: needs documentation:
    pub(crate) fn handle_router_event(&self, event: RouterEvent) {
        let mut state = self.lock_sessions();
        match event {
            RouterEvent::SessionJoined { session_id } => {
                state
                    .sessions
                    .entry(session_id)
                    .or_insert_with_key(|id| RecordingSession::new(*id));
            }
            RouterEvent::SessionLeft { session_id } => {
                state.sessions.remove(&session_id);
            }
            RouterEvent::ProducerAdded {
                session_id,
                transport_id,
                producer_id,
                media_kind,
                stream_type,
            } => add_tracked_producer(
                &mut state.sessions,
                session_id,
                producer_id,
                transport_id,
                media_kind,
                stream_type,
            ),
            RouterEvent::ProducerRemoved {
                session_id,
                producer_id,
                ..
            } => {
                if let Some(session) = state.sessions.get_mut(&session_id) {
                    session.remove_producer(producer_id);
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
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn add_tracked_producer(
    sessions: &mut BTreeMap<SessionId, RecordingSession>,
    session_id: SessionId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: o_sfu_router::MediaKind,
    stream_type: o_sfu_router::StreamType,
) {
    sessions
        .entry(session_id)
        .or_insert_with_key(|id| RecordingSession::new(*id))
        .add_producer(producer_id, transport_id, media_kind, stream_type);
}

#[derive(Clone)]
pub(crate) struct RecordingRouterObserver {
    service: Arc<RecordingService>,
}

impl RecordingRouterObserver {
    pub(crate) fn new(service: Arc<RecordingService>) -> Self {
        Self { service }
    }
}

impl RouterObserver for RecordingRouterObserver {
    fn on_event(&mut self, event: RouterEvent) {
        self.service.handle_router_event(event);
    }
}

impl fmt::Debug for RecordingRouterObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingRouterObserver")
            .field("channel_instance_id", &self.service.channel_instance_id)
            .finish()
    }
}

impl fmt::Debug for RecordingService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingService")
            .field("channel_instance_id", &self.channel_instance_id)
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}
