//! Verification helpers that still run shared production lifecycle logic.
//!
//! The protocol proof surface stays narrow here. This module only
//! exposes the connection-lifecycle state machine because its transition model
//! is shared with production code.

use super::{
    ConnectionState, INITIAL_RECOVERY_DELAY_MS, RECOVERY_TIMER_ID,
    connection_lifecycle::{
        ConnectCommandSource, LifecycleCloseCause, LifecycleEffect, LifecycleEffects,
        LifecycleModel, LifecyclePlan, RuntimeCleanupMode, connect_model, disconnect_model,
        handle_recovery_timer_model, on_transport_ready_model, on_ws_close_model,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerificationLifecycleEffects {
    connect_requested: bool,
    recovery_timer_ms: Option<u32>,
    close_websocket_code: Option<u16>,
    state_change: Option<(ConnectionState, Option<LifecycleCloseCause>)>,
    close_peer_connection: bool,
    cancel_recovery_timer: bool,
}

impl VerificationLifecycleEffects {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.connect_requested
            && self.recovery_timer_ms.is_none()
            && self.close_websocket_code.is_none()
            && self.state_change.is_none()
            && !self.close_peer_connection
            && !self.cancel_recovery_timer
    }

    #[must_use]
    pub fn has_connect(&self) -> bool {
        self.connect_requested
    }

    #[must_use]
    pub fn recovery_timer_count(&self, timer_id: u32) -> usize {
        usize::from(timer_id == RECOVERY_TIMER_ID && self.recovery_timer_ms.is_some())
    }

    #[must_use]
    pub fn recovery_timer_delay(&self, timer_id: u32) -> Option<u32> {
        if timer_id == RECOVERY_TIMER_ID {
            self.recovery_timer_ms
        } else {
            None
        }
    }

    #[must_use]
    pub fn has_close_peer_connection(&self) -> bool {
        self.close_peer_connection
    }
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

    pub fn connect(&mut self) -> VerificationLifecycleEffects {
        let plan = connect_model(&mut self.model);
        self.apply_plan(&plan)
    }

    pub fn on_transport_ready(&mut self) -> VerificationLifecycleEffects {
        let plan = on_transport_ready_model(&mut self.model);
        self.apply_plan(&plan)
    }

    pub fn on_welcome(&mut self) -> VerificationLifecycleEffects {
        if !matches!(
            self.model.state,
            ConnectionState::Connecting | ConnectionState::Recovering
        ) {
            return VerificationLifecycleEffects::default();
        }
        self.model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        self.model.state = ConnectionState::Authenticated;
        VerificationLifecycleEffects {
            state_change: Some((self.model.state, None)),
            ..VerificationLifecycleEffects::default()
        }
    }

    pub fn disconnect(&mut self) -> VerificationLifecycleEffects {
        let plan = disconnect_model(&mut self.model);
        self.apply_plan(&plan)
    }

    pub fn on_ws_close(&mut self, close_code: u16) -> VerificationLifecycleEffects {
        let plan = on_ws_close_model(&mut self.model, close_code);
        self.apply_plan(&plan)
    }

    pub fn on_timer(&mut self, timer_id: u32) -> VerificationLifecycleEffects {
        if timer_id != RECOVERY_TIMER_ID {
            return VerificationLifecycleEffects::default();
        }
        let plan = handle_recovery_timer_model(&mut self.model);
        self.apply_plan(&plan)
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
        self.model.has_connect_context
    }

    #[must_use]
    pub fn sticky_state_present(&self) -> bool {
        self.sticky_state_present
    }

    #[must_use]
    pub fn runtime_state_present(&self) -> bool {
        self.runtime_state_present
    }

    fn apply_plan(&mut self, plan: &LifecyclePlan) -> VerificationLifecycleEffects {
        if plan.clear_sticky_state {
            self.sticky_state_present = false;
        }
        if !matches!(plan.runtime_cleanup_mode, RuntimeCleanupMode::None) {
            self.runtime_state_present = false;
        }
        let mut effects = VerificationLifecycleEffects::default();
        summarize_effects(&mut effects, &plan.effects_before_cleanup);
        summarize_effects(&mut effects, &plan.effects_after_cleanup);
        if !matches!(plan.connect_after_cleanup, ConnectCommandSource::None) {
            effects.connect_requested = true;
        }
        effects
    }
}

fn summarize_effects(summary: &mut VerificationLifecycleEffects, effects: &LifecycleEffects) {
    match effects {
        LifecycleEffects::None => {}
        LifecycleEffects::One(first) => summarize_effect(summary, *first),
        LifecycleEffects::Three(first, second, third) => {
            summarize_effect(summary, *first);
            summarize_effect(summary, *second);
            summarize_effect(summary, *third);
        }
    }
}

fn summarize_effect(summary: &mut VerificationLifecycleEffects, effect: LifecycleEffect) {
    match effect {
        LifecycleEffect::EmitStateChange { state, cause } => {
            summary.state_change = Some((state, cause));
        }
        LifecycleEffect::ClosePeerConnection => {
            summary.close_peer_connection = true;
        }
        LifecycleEffect::CloseWebSocket { code } => {
            summary.close_websocket_code = Some(code);
        }
        LifecycleEffect::ScheduleRecoveryTimer { ms } => {
            summary.recovery_timer_ms = Some(ms);
        }
        LifecycleEffect::CancelRecoveryTimer => {
            summary.cancel_recovery_timer = true;
        }
    }
}
