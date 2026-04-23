//! Verification helpers that still run shared production lifecycle logic.
//!
//! The protocol proof surface stays intentionally narrow here. This module only
//! exposes the connection-lifecycle state machine because its transition model
//! is shared with production code.

use crate::shared::{AvailableFeatures, RecordingState};

use super::{
    Command, Commands, ConnectionState, INITIAL_RECOVERY_DELAY_MS, RECOVERY_TIMER_ID,
    connection_lifecycle::{
        LifecycleModel, LifecyclePlan, RuntimeCleanupMode, connect_model, disconnect_model,
        handle_recovery_timer_model, on_transport_ready_model, on_ws_close_model,
    },
};

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
        self.model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        self.model.state = ConnectionState::Authenticated;
        vec![Command::EmitStateChange {
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
        if timer_id != RECOVERY_TIMER_ID {
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

    fn apply_plan(&mut self, plan: LifecyclePlan) -> Commands {
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
