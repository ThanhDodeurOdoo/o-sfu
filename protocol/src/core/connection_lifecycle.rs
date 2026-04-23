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
    bundle_api::BundleConnectionState, shared::RecordingState, signaling::WebSocketCloseCode,
};

use super::{
    Command, Commands, ConnectContext, INITIAL_RECOVERY_DELAY_MS, ProtocolCore, RECOVERY_TIMER_ID,
    empty_features, next_recovery_delay,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LifecycleModel {
    pub(super) state: BundleConnectionState,
    pub(super) has_connect_context: bool,
    pub(super) recovery_delay_ms: u32,
}

impl LifecycleModel {
    #[cfg(feature = "verification-models")]
    #[must_use]
    pub(super) fn new() -> Self {
        Self {
            state: BundleConnectionState::Disconnected,
            has_connect_context: false,
            recovery_delay_ms: INITIAL_RECOVERY_DELAY_MS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectContextUpdate {
    Preserve,
    Clear,
    ReplaceFromInput,
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

/// Small typed effect surface for the connection lifecycle state machine.
///
/// The lifecycle logic only needs a narrow slice of the full protocol command
/// space: state changes, websocket connect or close, peer-connection close and
/// recovery timer control. Keeping that surface separate lets production code
/// translate it into real [`Command`] values while proofs stay on the shared
/// lifecycle logic instead of paying for unrelated protocol command variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEffect {
    EmitStateChange {
        state: BundleConnectionState,
        cause: Option<LifecycleCloseCause>,
    },
    ClosePeerConnection,
    CloseWebSocket {
        code: u16,
    },
    ScheduleRecoveryTimer {
        ms: u32,
    },
    CancelRecoveryTimer,
}

/// Small bounded list of lifecycle effects emitted by one transition.
///
/// Most lifecycle transitions in this module emit nothing, one effect, or a
/// short ordered pair like "state change, then start the recovery timer". A
/// `Vec` would work,
/// but it would make the shape looser than the domain really is and it would
/// pull allocation and generic collection machinery into the proof path for no
/// gain.
///
/// This enum keeps the contract explicit:
///
/// - effects stay ordered
/// - transitions can emit at most three effects
/// - there is no invalid partially-filled state like "slot three is set but
///   slot one is not"
///
/// That makes normal code easier to read and it keeps the virification-facing
/// lifecycle surface small without inventing a fake model.
///
/// Example:
///
/// ```rs
/// LifecycleEffects::one(LifecycleEffect::ScheduleRecoveryTimer { ms: 1_000 })
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEffects {
    None,
    One(LifecycleEffect),
    Three(LifecycleEffect, LifecycleEffect, LifecycleEffect),
}

impl LifecycleEffects {
    #[must_use]
    pub const fn new() -> Self {
        Self::None
    }

    #[must_use]
    pub const fn one(first: LifecycleEffect) -> Self {
        Self::One(first)
    }

    #[must_use]
    pub const fn three(
        first: LifecycleEffect,
        second: LifecycleEffect,
        third: LifecycleEffect,
    ) -> Self {
        Self::Three(first, second, third)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleCloseCause {
    AuthFailed,
    Kicked,
    ChannelFull,
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
/// recovery timer too latecan change observable behavior even if the final
/// state enum still looks correct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LifecyclePlan {
    pub(super) effects_before_cleanup: LifecycleEffects,
    pub(super) effects_after_cleanup: LifecycleEffects,
    pub(super) connect_after_cleanup: bool,
    pub(super) clear_sticky_state: bool,
    pub(super) runtime_cleanup_mode: RuntimeCleanupMode,
    pub(super) connect_context_update: ConnectContextUpdate,
    pub(super) reset_session_state: bool,
}

impl LifecyclePlan {
    #[must_use]
    fn none() -> Self {
        Self {
            effects_before_cleanup: LifecycleEffects::new(),
            effects_after_cleanup: LifecycleEffects::new(),
            connect_after_cleanup: false,
            clear_sticky_state: false,
            runtime_cleanup_mode: RuntimeCleanupMode::None,
            connect_context_update: ConnectContextUpdate::Preserve,
            reset_session_state: false,
        }
    }
}

pub(super) fn connect_model(model: &mut LifecycleModel) -> LifecyclePlan {
    if !matches!(
        model.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return LifecyclePlan::none();
    }
    model.has_connect_context = true;
    model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    model.state = BundleConnectionState::Connecting;
    LifecyclePlan {
        effects_before_cleanup: LifecycleEffects::new(),
        effects_after_cleanup: LifecycleEffects::one(LifecycleEffect::EmitStateChange {
            state: model.state,
            cause: None,
        }),
        connect_after_cleanup: true,
        clear_sticky_state: true,
        runtime_cleanup_mode: RuntimeCleanupMode::Silent,
        connect_context_update: ConnectContextUpdate::ReplaceFromInput,
        reset_session_state: true,
    }
}

pub(super) fn on_transport_ready_model(model: &mut LifecycleModel) -> LifecyclePlan {
    if model.state != BundleConnectionState::Authenticated {
        return LifecyclePlan::none();
    }
    model.state = BundleConnectionState::Connected;
    LifecyclePlan {
        effects_before_cleanup: LifecycleEffects::new(),
        effects_after_cleanup: LifecycleEffects::one(LifecycleEffect::EmitStateChange {
            state: model.state,
            cause: None,
        }),
        connect_after_cleanup: false,
        clear_sticky_state: false,
        runtime_cleanup_mode: RuntimeCleanupMode::None,
        connect_context_update: ConnectContextUpdate::Preserve,
        reset_session_state: false,
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
    model.has_connect_context = false;
    model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    LifecyclePlan {
        effects_before_cleanup: LifecycleEffects::one(LifecycleEffect::CancelRecoveryTimer),
        effects_after_cleanup: LifecycleEffects::three(
            LifecycleEffect::CloseWebSocket {
                code: u16::from(WebSocketCloseCode::Clean),
            },
            LifecycleEffect::ClosePeerConnection,
            LifecycleEffect::EmitStateChange {
                state: model.state,
                cause: None,
            },
        ),
        connect_after_cleanup: false,
        clear_sticky_state: true,
        runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
        connect_context_update: ConnectContextUpdate::Clear,
        reset_session_state: true,
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
        model.has_connect_context = false;
        model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        return LifecyclePlan {
            effects_before_cleanup: LifecycleEffects::new(),
            effects_after_cleanup: LifecycleEffects::three(
                LifecycleEffect::CancelRecoveryTimer,
                LifecycleEffect::ClosePeerConnection,
                LifecycleEffect::EmitStateChange {
                    state: model.state,
                    cause: terminal_close_cause(close_code),
                },
            ),
            connect_after_cleanup: false,
            clear_sticky_state: false,
            runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
            connect_context_update: ConnectContextUpdate::Clear,
            reset_session_state: true,
        };
    }

    if !model.has_connect_context {
        model.state = BundleConnectionState::Disconnected;
        return LifecyclePlan {
            effects_before_cleanup: LifecycleEffects::new(),
            effects_after_cleanup: LifecycleEffects::one(LifecycleEffect::EmitStateChange {
                state: model.state,
                cause: None,
            }),
            connect_after_cleanup: false,
            clear_sticky_state: false,
            runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
            connect_context_update: ConnectContextUpdate::Preserve,
            reset_session_state: false,
        };
    }
    let delay_ms = model.recovery_delay_ms;
    model.recovery_delay_ms = next_recovery_delay(delay_ms);
    model.state = BundleConnectionState::Recovering;
    LifecyclePlan {
        effects_before_cleanup: LifecycleEffects::new(),
        effects_after_cleanup: LifecycleEffects::three(
            LifecycleEffect::ClosePeerConnection,
            LifecycleEffect::EmitStateChange {
                state: model.state,
                cause: None,
            },
            LifecycleEffect::ScheduleRecoveryTimer { ms: delay_ms },
        ),
        connect_after_cleanup: false,
        clear_sticky_state: false,
        runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
        connect_context_update: ConnectContextUpdate::Preserve,
        reset_session_state: false,
    }
}

pub(super) fn handle_recovery_timer_model(model: &mut LifecycleModel) -> LifecyclePlan {
    if model.state != BundleConnectionState::Recovering || !model.has_connect_context {
        return LifecyclePlan::none();
    }
    model.state = BundleConnectionState::Connecting;
    LifecyclePlan {
        effects_before_cleanup: LifecycleEffects::new(),
        effects_after_cleanup: LifecycleEffects::one(LifecycleEffect::EmitStateChange {
            state: model.state,
            cause: None,
        }),
        connect_after_cleanup: true,
        clear_sticky_state: false,
        runtime_cleanup_mode: RuntimeCleanupMode::None,
        connect_context_update: ConnectContextUpdate::Preserve,
        reset_session_state: false,
    }
}

fn lifecycle_model(core: &ProtocolCore) -> LifecycleModel {
    LifecycleModel {
        state: core.state,
        has_connect_context: core.connect_context.is_some(),
        recovery_delay_ms: core.recovery_delay_ms,
    }
}

fn apply_model(
    core: &mut ProtocolCore,
    model: LifecycleModel,
    plan: LifecyclePlan,
    fresh_connect_context: Option<ConnectContext>,
) -> Commands {
    core.state = model.state;
    core.recovery_delay_ms = model.recovery_delay_ms;
    if plan.reset_session_state {
        core.features = empty_features();
        core.recording_state = RecordingState::default();
    }
    match plan.connect_context_update {
        ConnectContextUpdate::Preserve => {}
        ConnectContextUpdate::Clear => {
            core.connect_context = None;
        }
        ConnectContextUpdate::ReplaceFromInput => {
            core.connect_context = fresh_connect_context;
        }
    }

    let mut commands = lifecycle_effects_to_commands(core, plan.effects_before_cleanup);
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
    commands.extend(lifecycle_effects_to_commands(
        core,
        plan.effects_after_cleanup,
    ));
    if plan.connect_after_cleanup {
        commands.push(connect_command(core));
    }
    commands
}

fn lifecycle_effects_to_commands(core: &ProtocolCore, effects: LifecycleEffects) -> Commands {
    match effects {
        LifecycleEffects::None => Vec::new(),
        LifecycleEffects::One(first) => vec![lifecycle_effect_to_command(core, first)],
        LifecycleEffects::Three(first, second, third) => vec![
            lifecycle_effect_to_command(core, first),
            lifecycle_effect_to_command(core, second),
            lifecycle_effect_to_command(core, third),
        ],
    }
}

fn lifecycle_effect_to_command(_core: &ProtocolCore, effect: LifecycleEffect) -> Command {
    match effect {
        LifecycleEffect::EmitStateChange { state, cause } => Command::EmitStateChange {
            state,
            cause: cause.map(lifecycle_close_cause_label).map(str::to_owned),
        },
        LifecycleEffect::ClosePeerConnection => Command::ClosePeerConnection,
        LifecycleEffect::CloseWebSocket { code } => Command::CloseWebSocket { code },
        LifecycleEffect::ScheduleRecoveryTimer { ms } => Command::ScheduleTimer {
            id: RECOVERY_TIMER_ID,
            ms,
        },
        LifecycleEffect::CancelRecoveryTimer => Command::CancelTimer {
            id: RECOVERY_TIMER_ID,
        },
    }
}

fn connect_command(core: &ProtocolCore) -> Command {
    let url = core
        .connect_context
        .as_ref()
        .map(|connect_context| connect_context.url.clone());
    assert!(
        url.is_some(),
        "connect command requires saved connect context"
    );
    Command::Connect {
        url: url.unwrap_or_default(),
    }
}

fn terminal_close_cause(close_code: u16) -> Option<LifecycleCloseCause> {
    match WebSocketCloseCode::from_u16(close_code) {
        Some(WebSocketCloseCode::AuthFailed) => Some(LifecycleCloseCause::AuthFailed),
        Some(WebSocketCloseCode::Kicked) => Some(LifecycleCloseCause::Kicked),
        Some(WebSocketCloseCode::ChannelFull) => Some(LifecycleCloseCause::ChannelFull),
        _ => None,
    }
}

fn lifecycle_close_cause_label(cause: LifecycleCloseCause) -> &'static str {
    match cause {
        LifecycleCloseCause::AuthFailed => "auth_failed",
        LifecycleCloseCause::Kicked => "kicked",
        LifecycleCloseCause::ChannelFull => "full",
    }
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
    let plan = connect_model(&mut model);
    apply_model(
        core,
        model,
        plan,
        Some(ConnectContext { url, jwt, channel }),
    )
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
    apply_model(core, model, plan, None)
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
    apply_model(core, model, plan, None)
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
    apply_model(core, model, plan, None)
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
    apply_model(core, model, plan, None)
}
