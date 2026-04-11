use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Instant,
};

use o_sfu_router::{ProducerId, RouterEvent, RouterObserver, SessionId, TransportId};

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::{MediaFrameSink, MediaSource, into_frame_sink, session::RecordingSession};

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
    state: RecordingLifecycleState,
}

impl RecordingTransitionError {
    #[cfg(test)]
    pub(crate) fn state(self) -> RecordingLifecycleState {
        self.state
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

struct RecordingFrameCollector {
    state: Arc<Mutex<RecordingServiceState>>,
}

impl MediaFrameSink for RecordingFrameCollector {
    fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.lifecycle != RecordingLifecycleState::Recording {
            return;
        }
        state.captured_packet_count = state.captured_packet_count.saturating_add(1);
        let key = (session_key.clone(), transport_media_id);
        let next_count = state
            .captured_packets_by_stream
            .get(&key)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        state.captured_packets_by_stream.insert(key, next_count);
    }
}

struct RecordingServiceState {
    lifecycle: RecordingLifecycleState,
    sessions: BTreeMap<SessionId, RecordingSession>,
    captured_packet_count: u64,
    captured_packets_by_stream: BTreeMap<(TransportSessionKey, TransportMediaId), u64>,
}

pub(crate) struct RecordingService {
    channel_runtime_id: u64,
    media_source: Arc<dyn MediaSource>,
    state: Arc<Mutex<RecordingServiceState>>,
    frame_collector: Arc<RecordingFrameCollector>,
}

impl RecordingService {
    pub(crate) fn new(channel_runtime_id: u64, media_source: Arc<dyn MediaSource>) -> Self {
        let state = Arc::new(Mutex::new(RecordingServiceState {
            lifecycle: RecordingLifecycleState::Idle,
            sessions: BTreeMap::new(),
            captured_packet_count: 0,
            captured_packets_by_stream: BTreeMap::new(),
        }));
        Self {
            channel_runtime_id,
            media_source,
            frame_collector: Arc::new(RecordingFrameCollector {
                state: Arc::clone(&state),
            }),
            state,
        }
    }

    pub(crate) fn start(&self) -> Result<(), RecordingTransitionError> {
        {
            let mut state = self.lock_state();
            if state.lifecycle != RecordingLifecycleState::Idle {
                return Err(RecordingTransitionError {
                    action: RecordingAction::Start,
                    state: state.lifecycle,
                });
            }
            state.lifecycle = RecordingLifecycleState::Starting;
        }
        let sink = into_frame_sink(Arc::<RecordingFrameCollector>::clone(&self.frame_collector));
        self.media_source
            .activate_channel(self.channel_runtime_id, sink);
        {
            let mut state = self.lock_state();
            state.lifecycle = RecordingLifecycleState::Recording;
        }
        Ok(())
    }

    pub(crate) fn stop(&self) -> Result<(), RecordingTransitionError> {
        {
            let mut state = self.lock_state();
            if state.lifecycle != RecordingLifecycleState::Recording {
                return Err(RecordingTransitionError {
                    action: RecordingAction::Stop,
                    state: state.lifecycle,
                });
            }
            state.lifecycle = RecordingLifecycleState::Stopping;
        }
        self.media_source
            .deactivate_channel(self.channel_runtime_id);
        {
            let mut state = self.lock_state();
            state.lifecycle = RecordingLifecycleState::Finalizing;
            state.lifecycle = RecordingLifecycleState::Idle;
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> RecordingServiceSnapshot {
        let state = self.lock_state();
        RecordingServiceSnapshot {
            lifecycle: state.lifecycle,
            session_count: state.sessions.len(),
            producer_count: state
                .sessions
                .values()
                .map(RecordingSession::producer_count)
                .sum(),
            captured_packet_count: state.captured_packet_count,
            captured_stream_count: state.captured_packets_by_stream.len(),
        }
    }

    pub(crate) fn handle_router_event(&self, event: RouterEvent) {
        let mut state = self.lock_state();
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

    fn lock_state(&self) -> MutexGuard<'_, RecordingServiceState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
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
            .field("channel_runtime_id", &self.service.channel_runtime_id)
            .finish()
    }
}

impl fmt::Debug for RecordingService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingService")
            .field("channel_runtime_id", &self.channel_runtime_id)
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}
