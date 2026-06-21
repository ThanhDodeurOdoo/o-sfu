//! Verification helpers that still run shared production lifecycle logic.
//!
//! The protocol proof surface stays narrow here. This module only
//! exposes the connection-lifecycle state machine because its transition model
//! is shared with production code.

use super::{
    ConnectionState, INITIAL_RECOVERY_DELAY_MS, RECOVERY_TIMER_ID,
    connection_lifecycle::{
        LifecycleAction, LifecycleCloseCause, LifecycleModel, LifecycleTransition, connect_model,
        disconnect_model, handle_recovery_timer_model, on_transport_ready_model, on_ws_close_model,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerificationLifecycleEffects {
    connect_count: usize,
    recovery_timer_ms: Option<u32>,
    recovery_timer_count: usize,
    close_websocket_code: Option<u16>,
    state_change: Option<(ConnectionState, Option<LifecycleCloseCause>)>,
    close_peer_connection_count: usize,
    cancel_recovery_timer: bool,
}

impl VerificationLifecycleEffects {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.connect_count == 0
            && self.recovery_timer_count == 0
            && self.close_websocket_code.is_none()
            && self.state_change.is_none()
            && self.close_peer_connection_count == 0
            && !self.cancel_recovery_timer
    }

    #[must_use]
    pub fn has_connect(&self) -> bool {
        self.connect_count > 0
    }

    #[must_use]
    pub fn connect_count(&self) -> usize {
        self.connect_count
    }

    #[must_use]
    pub fn recovery_timer_count(&self, timer_id: u32) -> usize {
        if timer_id == RECOVERY_TIMER_ID {
            self.recovery_timer_count
        } else {
            0
        }
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
        self.close_peer_connection_count > 0
    }

    #[must_use]
    pub fn close_peer_connection_count(&self) -> usize {
        self.close_peer_connection_count
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
        let transition = connect_model(&mut self.model);
        self.apply_transition(&transition)
    }

    pub fn on_transport_ready(&mut self) -> VerificationLifecycleEffects {
        let transition = on_transport_ready_model(&mut self.model);
        self.apply_transition(&transition)
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
        let transition = disconnect_model(&mut self.model);
        self.apply_transition(&transition)
    }

    pub fn on_ws_close(&mut self, close_code: u16) -> VerificationLifecycleEffects {
        let transition = on_ws_close_model(&mut self.model, close_code);
        self.apply_transition(&transition)
    }

    pub fn on_timer(&mut self, timer_id: u32) -> VerificationLifecycleEffects {
        if timer_id != RECOVERY_TIMER_ID {
            return VerificationLifecycleEffects::default();
        }
        let transition = handle_recovery_timer_model(&mut self.model);
        self.apply_transition(&transition)
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

    fn apply_transition(
        &mut self,
        transition: &LifecycleTransition,
    ) -> VerificationLifecycleEffects {
        let mut effects = VerificationLifecycleEffects::default();
        for action in &transition.actions {
            match *action {
                LifecycleAction::StoreFreshConnectContext
                | LifecycleAction::ClearConnectContext
                | LifecycleAction::ClearWelcomeSnapshot => {}
                LifecycleAction::ClearRuntimeStateSilently
                | LifecycleAction::ClearRuntimeStateWithCommands => {
                    self.runtime_state_present = false;
                }
                LifecycleAction::ClearStickyState => {
                    self.sticky_state_present = false;
                }
                LifecycleAction::EmitStateChange { state, cause } => {
                    effects.state_change = Some((state, cause));
                }
                LifecycleAction::ClosePeerConnection => {
                    effects.close_peer_connection_count += 1;
                }
                LifecycleAction::CloseWebSocket { code } => {
                    effects.close_websocket_code = Some(code);
                }
                LifecycleAction::ScheduleRecoveryTimer { ms } => {
                    effects.recovery_timer_count += 1;
                    effects.recovery_timer_ms = Some(ms);
                }
                LifecycleAction::CancelRecoveryTimer => {
                    effects.cancel_recovery_timer = true;
                }
                LifecycleAction::EmitConnectCommand => {
                    effects.connect_count += 1;
                }
            }
        }
        effects
    }
}
