//! Lifecycle transitions for the protocol core connection state machine
//!
//! This module owns the "outer shell" of the client connection lifecycle:
//! connect, transport-ready, websocket close, explicit disconnect and delayed
//! recovery (It does not handle signaling messages or sticky replay content by
//! itself(
//!
//! it does:
//! - decide whether a transition is legal from the current state
//! - decide what state the core should move to next
//! - decide whether runtime or sticky state must be cleared
//! - emit the command sequence the host/runtime must execute around that move
//!
//! The slightly unusual split between `LifecycleModel`, `LifecyclePlan` and
//! `apply_model` is becaise: The transition helpers stay pure and easy
//! to reason about, while `apply_model` is the only place that mutates the live
//! `ProtocolCore` and triggers cleanup side effects. That keeps lifecycle rules
//! testable and proof-friendly without pushing cleanup policy all over `core.rs`.
//!
//! A few lifecycle rules:
//! (if you are familiar with odoo/sfu's  client its basicly the same rules)
//!
//! - explicit `disconnect()` is terminal for the current session attempt and
//!   clears both runtime and sticky state
//! - terminal websocket close codes (`AuthFailed`, `Kicked`, `ChannelFull`)
//!   also stop recovery, but keep sticky state untouched because the caller did
//!   not explicitly ask to wipe intent
//! - non-terminal websocket closes keep the saved connect context and move into
//!   `Recovering`, so the recovery timer can reconnect later
//! - the recovery timer only reconnects from `Recovering`. stale timer fires in
//!   any other state are no-ops
//!
//! Example flows:
//!
//! ```text
//! Disconnected --> connect()--> Connecting --> on_welcome() --> Authenticated
//! Authenticated --> on_transport_ready() --> Connected
//! ```
//!
//! ```text
//! Connected --> on_ws_close(1011) --> Recovering --> timer --> Connecting
//! ```
//!
//! ```text
//! Connected --> disconnect()--> Disconnected
//! ```
//!
//! The last flow is intentionally different from `on_ws_close(...)`: explicit
//! disconnect wipes replayable intent and suppresses later recovery, while a
//! transient socket loss keeps enough state around to reconnect and
//! rebuild from saved state.

use crate::{
    bundle_api::BundleConnectionState,
    shared::{AvailableFeatures, RecordingState},
    signaling::WebSocketCloseCode,
};

use super::{
    Command, Commands, ConnectContext, INITIAL_RECOVERY_DELAY_MS, ProtocolCore, RECOVERY_TIMER_ID,
    close_cause, empty_features, next_recovery_delay,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LifecycleModel {
    pub(super) state: BundleConnectionState,
    pub(super) features: AvailableFeatures,
    pub(super) recording_state: RecordingState,
    pub(super) connect_context: Option<ConnectContext>,
    pub(super) recovery_delay_ms: u32,
}

impl LifecycleModel {
    #[cfg(feature = "verification-models")]
    #[must_use]
    pub(super) fn new() -> Self {
        Self {
            state: BundleConnectionState::Disconnected,
            features: empty_features(),
            recording_state: RecordingState::default(),
            connect_context: None,
            recovery_delay_ms: INITIAL_RECOVERY_DELAY_MS,
        }
    }
}

/// Cleanup policy that `apply_model` should use after the pure transition logic
/// has decided on the next lifecycle state.
///
/// This exists so the model helpers can say "what kind of cleanup is needed"
/// without directly mutating the live runtime state themselves.
///
/// Example:
///
/// ```text
/// connect() uses `Silent` because a fresh connect should drop old runtime
/// state, but it should not emit teardown commands for an already-dead session.
///
/// disconnect() uses `WithCommands` becuse the caller is ending a live session
/// and the host must see the explicit cleanup commands that fall out of it
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RuntimeCleanupMode {
    None,
    Silent,
    WithCommands,
}

/// Pure result of one lifecycle transition.
///
/// The transition helpers do not mutate the live core directly. They return a
/// plan that says which commands should happen before cleanup, whether sticky
/// state should be dropped, which runtime cleanup mode to use and which
/// commands should happen after cleanup.
///
/// Keeping that plan explicit matters because command ordering is part of the
/// contract. A refactor that clears runtime state too early or schedules a
/// recovery timer too late can change observable behavior even if the final
/// state enum still looks correct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LifecyclePlan {
    pub(super) commands_before_cleanup: Commands,
    pub(super) commands_after_cleanup: Commands,
    pub(super) clear_sticky_state: bool,
    pub(super) runtime_cleanup_mode: RuntimeCleanupMode,
}

impl LifecyclePlan {
    #[must_use]
    fn none() -> Self {
        Self {
            commands_before_cleanup: Vec::new(),
            commands_after_cleanup: Vec::new(),
            clear_sticky_state: false,
            runtime_cleanup_mode: RuntimeCleanupMode::None,
        }
    }
}

pub(super) fn connect_model(
    model: &mut LifecycleModel,
    url: String,
    jwt: String,
    channel: Option<String>,
) -> LifecyclePlan {
    if !matches!(
        model.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return LifecyclePlan::none();
    }
    model.connect_context = Some(ConnectContext {
        url: url.clone(),
        jwt,
        channel,
    });
    model.features = empty_features();
    model.recording_state = RecordingState::default();
    model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    model.state = BundleConnectionState::Connecting;
    LifecyclePlan {
        commands_before_cleanup: Vec::new(),
        commands_after_cleanup: vec![
            Command::EmitStateChange {
                state: model.state,
                cause: None,
            },
            Command::Connect { url },
        ],
        clear_sticky_state: true,
        runtime_cleanup_mode: RuntimeCleanupMode::Silent,
    }
}

pub(super) fn on_transport_ready_model(model: &mut LifecycleModel) -> LifecyclePlan {
    if model.state != BundleConnectionState::Authenticated {
        return LifecyclePlan::none();
    }
    model.state = BundleConnectionState::Connected;
    LifecyclePlan {
        commands_before_cleanup: Vec::new(),
        commands_after_cleanup: vec![Command::EmitStateChange {
            state: model.state,
            cause: None,
        }],
        clear_sticky_state: false,
        runtime_cleanup_mode: RuntimeCleanupMode::None,
    }
}

pub(super) fn disconnect_model(model: &mut LifecycleModel) -> LifecyclePlan {
    if matches!(
        model.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return LifecyclePlan::none();
    }
    model.state = BundleConnectionState::Disconnected;
    model.connect_context = None;
    model.features = empty_features();
    model.recording_state = RecordingState::default();
    model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    LifecyclePlan {
        commands_before_cleanup: vec![Command::CancelTimer {
            id: RECOVERY_TIMER_ID,
        }],
        commands_after_cleanup: vec![
            Command::CloseWebSocket {
                code: u16::from(WebSocketCloseCode::Clean),
            },
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: model.state,
                cause: None,
            },
        ],
        clear_sticky_state: true,
        runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
    }
}

pub(super) fn on_ws_close_model(model: &mut LifecycleModel, close_code: u16) -> LifecyclePlan {
    if matches!(
        model.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return LifecyclePlan::none();
    }

    if let Some(
        WebSocketCloseCode::AuthFailed
        | WebSocketCloseCode::Kicked
        | WebSocketCloseCode::ChannelFull,
    ) = WebSocketCloseCode::from_u16(close_code)
    {
        model.state = BundleConnectionState::Closed;
        model.connect_context = None;
        model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        return LifecyclePlan {
            commands_before_cleanup: Vec::new(),
            commands_after_cleanup: vec![
                Command::CancelTimer {
                    id: RECOVERY_TIMER_ID,
                },
                Command::ClosePeerConnection,
                Command::EmitStateChange {
                    state: model.state,
                    cause: close_cause(close_code).map(str::to_owned),
                },
            ],
            clear_sticky_state: false,
            runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
        };
    }

    let Some(connect_context) = model.connect_context.as_ref() else {
        model.state = BundleConnectionState::Disconnected;
        return LifecyclePlan {
            commands_before_cleanup: Vec::new(),
            commands_after_cleanup: vec![Command::EmitStateChange {
                state: model.state,
                cause: None,
            }],
            clear_sticky_state: false,
            runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
        };
    };

    let _ = connect_context;
    let delay_ms = model.recovery_delay_ms;
    model.recovery_delay_ms = next_recovery_delay(delay_ms);
    model.state = BundleConnectionState::Recovering;
    LifecyclePlan {
        commands_before_cleanup: Vec::new(),
        commands_after_cleanup: vec![
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: model.state,
                cause: None,
            },
            Command::ScheduleTimer {
                id: RECOVERY_TIMER_ID,
                ms: delay_ms,
            },
        ],
        clear_sticky_state: false,
        runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
    }
}

pub(super) fn handle_recovery_timer_model(model: &mut LifecycleModel) -> LifecyclePlan {
    if model.state != BundleConnectionState::Recovering {
        return LifecyclePlan::none();
    }
    let Some(connect_context) = model.connect_context.as_ref() else {
        return LifecyclePlan::none();
    };
    model.state = BundleConnectionState::Connecting;
    LifecyclePlan {
        commands_before_cleanup: Vec::new(),
        commands_after_cleanup: vec![
            Command::EmitStateChange {
                state: model.state,
                cause: None,
            },
            Command::Connect {
                url: connect_context.url.clone(),
            },
        ],
        clear_sticky_state: false,
        runtime_cleanup_mode: RuntimeCleanupMode::None,
    }
}

fn lifecycle_model(core: &ProtocolCore) -> LifecycleModel {
    LifecycleModel {
        state: core.state,
        features: core.features.clone(),
        recording_state: core.recording_state.clone(),
        connect_context: core.connect_context.clone(),
        recovery_delay_ms: core.recovery_delay_ms,
    }
}

fn apply_model(core: &mut ProtocolCore, model: LifecycleModel, plan: LifecyclePlan) -> Commands {
    core.state = model.state;
    core.features = model.features;
    core.recording_state = model.recording_state;
    core.connect_context = model.connect_context;
    core.recovery_delay_ms = model.recovery_delay_ms;

    let mut commands = plan.commands_before_cleanup;
    match plan.runtime_cleanup_mode {
        RuntimeCleanupMode::None => {}
        RuntimeCleanupMode::Silent => core.clear_runtime_state(),
        RuntimeCleanupMode::WithCommands => {
            commands.extend(core.clear_runtime_state_with_commands());
        }
    }
    if plan.clear_sticky_state {
        core.clear_sticky_state();
    }
    commands.extend(plan.commands_after_cleanup);
    commands
}

/// Starts a fresh connection attempt from a disconnected or closed state.
///
/// This is the only lifecycle entry point that intentionally wipes both
/// runtime state and sticky replay state before reconnecting. A brand-new
/// `connect(...)` means "start over with this new endpoint and auth context",
/// not "resume whatever the previous session was trying to do".
///
/// Calls from any other state are ignored so the host cannot accidentally stack
/// overlapping connection attempts on top of an already-live session.
///
/// ```text
/// Disconnected --connect(url, jwt, room)--> Connecting
/// ```
pub(super) fn connect(
    core: &mut ProtocolCore,
    url: String,
    jwt: String,
    channel: Option<String>,
) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = connect_model(&mut model, url, jwt, channel);
    apply_model(core, model, plan)
}

/// Marks the transport side as ready after the websocket/authentication phase.
///
/// This only accepts the `Authenticated -> Connected` step. Earlier states have
/// not completed protocol admission yet and later states have already consumed
/// this transition.
///
/// ```text
/// Connecting --on_welcome()--> Authenticated --on_transport_ready()--> Connected
/// ```
pub(super) fn on_transport_ready(core: &mut ProtocolCore) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = on_transport_ready_model(&mut model);
    apply_model(core, model, plan)
}

/// Ends the current session attempt on purpose.
///
/// Unlike `on_ws_close(...)`, this is not a recovery path. It clears the saved
/// connect context, runtime state and sticky replay state, then closes the
/// websocket and peer connection. Any later recovery-timer delivery becomes a
/// no-op because the caller explicitly asked to stop.
pub(super) fn disconnect(core: &mut ProtocolCore) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = disconnect_model(&mut model);
    apply_model(core, model, plan)
}

/// Handles websocket closure after a session was already in flight.
///
/// There are three different cases here and mixing them up is the main way to
/// break reconnect behavior:
///
/// - terminal close codes move to `Closed`, clear the saved connect context,
///   and suppress recovery
/// - non-terminal closes with saved connect context move to `Recovering` and
///   schedule the recovery timer
/// - non-terminal closes without saved connect context fall back to
///   `Disconnected`, because there is nothing safe to reconnect to
///
/// Example:
///
/// ```text
/// Connected --on_ws_close(AuthFailed)--> Closed
/// Connected --on_ws_close(1011)--> Recovering
/// ```
pub(super) fn on_ws_close(core: &mut ProtocolCore, close_code: u16) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = on_ws_close_model(&mut model, close_code);
    apply_model(core, model, plan)
}

/// Retries the saved websocket connection after a recovery delay.
///
/// This is intentionally narrow: only `Recovering` may consume the recovery
/// timer. A stale timer firing after a successful reconnect or explicit
/// disconnect must do nothing, otherwise dead sessions can get resurrected by
/// old scheduled work.
///
/// Example:
///
/// ```text
/// Connected --on_ws_close(1011)--> Recovering
/// Recovering --handle_recovery_timer()--> Connecting
/// Connected --handle_recovery_timer()--> no-op
/// ```
pub(super) fn handle_recovery_timer(core: &mut ProtocolCore) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = handle_recovery_timer_model(&mut model);
    apply_model(core, model, plan)
}
