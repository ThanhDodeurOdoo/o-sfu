//! Verification-related models for the SFU protocol core.
//!
//! state machines, request trackers and batching helpers
//! that are used by the higher-level connection-lifecycle and request-handling
//! models.

use std::mem::take;

use crate::{
    shared::{
        AvailableFeatures, DownloadStates, RecordingState, SessionId, SessionInfo, StreamType,
    },
    signaling::RequestId,
};

use super::{
    BATCH_FLUSH_DELAY_MS, BATCH_FLUSH_TIMER_ID, Command, Commands, ConnectionState,
    MAX_OUTBOUND_BATCH_LEN, PendingRequestKind,
    connection_lifecycle::{
        LifecycleModel, RuntimeCleanupMode, connect_model, disconnect_model,
        handle_recovery_timer_model, on_transport_ready_model, on_ws_close_model,
    },
    next_recovery_delay,
    request_tracker::RequestTracker,
    sticky_replay::StickyReplayState,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationRegisteredRequest {
    pub request_id: RequestId,
    pub timeout_timer_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationFlushMode {
    Immediate,
    Batched,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationRequestTracker {
    inner: RequestTracker,
}

impl Default for VerificationRequestTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl VerificationRequestTracker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: RequestTracker::new(),
        }
    }

    pub fn register_request(&mut self, kind: PendingRequestKind) -> VerificationRegisteredRequest {
        let registered_request = self.inner.register_request(kind);
        VerificationRegisteredRequest {
            request_id: registered_request.request_id,
            timeout_timer_id: registered_request.timeout_timer_id,
        }
    }

    pub fn resolve_response(
        &mut self,
        response_to: &RequestId,
        expected_kind: PendingRequestKind,
        ok: bool,
    ) -> Commands {
        self.inner.resolve_response(response_to, expected_kind, ok)
    }

    pub fn resolve_timeout(&mut self, timer_id: u32) -> Option<Commands> {
        self.inner.resolve_timeout(timer_id)
    }

    pub fn clear_with_commands(&mut self) -> Commands {
        self.inner.clear_with_commands()
    }

    #[must_use]
    pub fn has_pending_kind(&self, kind: PendingRequestKind) -> bool {
        self.inner.has_pending_kind(kind)
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.inner.pending_count()
    }

    #[must_use]
    pub fn timeout_count(&self) -> usize {
        self.inner.timeout_count()
    }

    #[must_use]
    pub fn contains_pending_request(&self, request_id: &RequestId) -> bool {
        self.inner.contains_pending_request(request_id)
    }

    #[must_use]
    pub fn contains_timeout_timer(&self, timer_id: u32) -> bool {
        self.inner.contains_timeout_timer(timer_id)
    }

    #[must_use]
    pub fn has_bijection_between_requests_and_timers(&self) -> bool {
        self.inner.has_bijection_between_requests_and_timers()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerificationOutboundBatcher {
    pending_tokens: Vec<u8>,
    flush_scheduled: bool,
}

impl VerificationOutboundBatcher {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue_with_batch(
        &mut self,
        token: u8,
        mode: VerificationFlushMode,
    ) -> (Commands, Option<Vec<u8>>) {
        match mode {
            VerificationFlushMode::Immediate => {
                self.pending_tokens.push(token);
                self.flush_with_batch(true)
            }
            VerificationFlushMode::Batched => {
                self.pending_tokens.push(token);
                if self.pending_tokens.len() >= MAX_OUTBOUND_BATCH_LEN {
                    self.flush_with_batch(true)
                } else if self.flush_scheduled {
                    (Vec::new(), None)
                } else {
                    self.flush_scheduled = true;
                    (
                        vec![Command::ScheduleTimer {
                            id: BATCH_FLUSH_TIMER_ID,
                            ms: BATCH_FLUSH_DELAY_MS,
                        }],
                        None,
                    )
                }
            }
        }
    }

    pub fn flush_with_batch(&mut self, cancel_timer: bool) -> (Commands, Option<Vec<u8>>) {
        if self.pending_tokens.is_empty() {
            self.flush_scheduled = false;
            return (Vec::new(), None);
        }
        let batch = take(&mut self.pending_tokens);
        let had_timer = self.flush_scheduled;
        self.flush_scheduled = false;
        let mut commands = Vec::new();
        if cancel_timer && had_timer {
            commands.push(Command::CancelTimer {
                id: BATCH_FLUSH_TIMER_ID,
            });
        }
        commands.push(Command::SendWebSocket(String::from("verification-batch")));
        (commands, Some(batch))
    }

    pub fn clear_with_commands(&mut self) -> Commands {
        let commands = if self.flush_scheduled {
            vec![Command::CancelTimer {
                id: BATCH_FLUSH_TIMER_ID,
            }]
        } else {
            Vec::new()
        };
        self.pending_tokens.clear();
        self.flush_scheduled = false;
        commands
    }

    #[must_use]
    pub fn pending_snapshot(&self) -> Vec<u8> {
        self.pending_tokens.clone()
    }

    #[must_use]
    pub fn flush_scheduled(&self) -> bool {
        self.flush_scheduled
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerificationStickyReplay {
    inner: StickyReplayState,
}

impl VerificationStickyReplay {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: StickyReplayState::new(),
        }
    }

    pub fn set_publish_active(&mut self, stream_type: StreamType, active: bool) {
        self.inner.set_publish_active(stream_type, active);
    }

    pub fn remember_subscription_states(
        &mut self,
        session_id: &SessionId,
        states: &DownloadStates,
    ) {
        self.inner.remember_subscription_states(session_id, states);
    }

    pub fn remember_info(&mut self, info: &SessionInfo) {
        self.inner.remember_info(info);
    }

    #[must_use]
    pub fn active_publications_len(&self) -> usize {
        self.inner.active_publications_len()
    }

    #[must_use]
    pub fn desired_subscriptions_len(&self) -> usize {
        self.inner.desired_subscriptions_len()
    }

    #[must_use]
    pub fn subscription_state(&self, session_id: &SessionId) -> Option<DownloadStates> {
        self.inner.subscription_state(session_id)
    }

    #[must_use]
    pub fn has_desired_info(&self) -> bool {
        self.inner.has_desired_info()
    }

    #[must_use]
    pub fn desired_info(&self) -> Option<SessionInfo> {
        self.inner.desired_info()
    }

    #[must_use]
    pub fn replay_summary(&self) -> VerificationReplaySummary {
        VerificationReplaySummary {
            publish_count: self.active_publications_len(),
            subscribe_count: self.desired_subscriptions_len(),
            info_count: usize::from(self.has_desired_info()),
        }
    }
}

#[must_use]
pub fn verification_next_recovery_delay(current_delay_ms: u32) -> u32 {
    next_recovery_delay(current_delay_ms)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerificationReplaySummary {
    pub publish_count: usize,
    pub subscribe_count: usize,
    pub info_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationConnectionLifecycle {
    model: LifecycleModel,
    sticky_state_present: bool,
    runtime_state_present: bool,
}

impl Default for VerificationConnectionLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl VerificationConnectionLifecycle {
    #[must_use]
    pub fn new() -> Self {
        Self {
            model: LifecycleModel::new(),
            sticky_state_present: false,
            runtime_state_present: false,
        }
    }

    pub fn connect(&mut self, url: String, jwt: String, channel: Option<String>) -> Commands {
        let plan = connect_model(&mut self.model, url, jwt, channel);
        self.apply_plan(plan)
    }

    pub fn on_transport_ready(&mut self) -> Commands {
        let plan = on_transport_ready_model(&mut self.model);
        self.apply_plan(plan)
    }

    pub fn on_welcome(&mut self) -> Commands {
        if !matches!(
            self.model.state,
            ConnectionState::Connecting | ConnectionState::Recovering
        ) {
            return Vec::new();
        }
        self.model.features = AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: true,
            video_recording: false,
        };
        self.model.recording_state = RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        };
        self.model.recovery_delay_ms = super::INITIAL_RECOVERY_DELAY_MS;
        self.model.state = ConnectionState::Authenticated;
        vec![super::Command::EmitStateChange {
            state: self.model.state,
            cause: None,
        }]
    }

    pub fn disconnect(&mut self) -> Commands {
        let plan = disconnect_model(&mut self.model);
        self.apply_plan(plan)
    }

    pub fn on_ws_close(&mut self, close_code: u16) -> Commands {
        let plan = on_ws_close_model(&mut self.model, close_code);
        self.apply_plan(plan)
    }

    pub fn on_timer(&mut self, timer_id: u32) -> Commands {
        if timer_id != super::RECOVERY_TIMER_ID {
            return Vec::new();
        }
        let plan = handle_recovery_timer_model(&mut self.model);
        self.apply_plan(plan)
    }

    pub fn mark_sticky_state_present(&mut self) {
        self.sticky_state_present = true;
    }

    pub fn mark_runtime_state_present(&mut self) {
        self.runtime_state_present = true;
    }

    #[must_use]
    pub const fn state(&self) -> ConnectionState {
        self.model.state
    }

    #[must_use]
    pub fn has_connect_context(&self) -> bool {
        self.model.connect_context.is_some()
    }

    #[must_use]
    pub fn sticky_state_present(&self) -> bool {
        self.sticky_state_present
    }

    #[must_use]
    pub fn runtime_state_present(&self) -> bool {
        self.runtime_state_present
    }

    fn apply_plan(&mut self, plan: super::connection_lifecycle::LifecyclePlan) -> Commands {
        if plan.clear_sticky_state {
            self.sticky_state_present = false;
        }
        if !matches!(plan.runtime_cleanup_mode, RuntimeCleanupMode::None) {
            self.runtime_state_present = false;
        }
        let mut commands = plan.commands_before_cleanup;
        commands.extend(plan.commands_after_cleanup);
        commands
    }
}
