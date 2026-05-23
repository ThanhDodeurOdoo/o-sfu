//! lifecycle transitions for the protocol core connection state machine
//!
//! this module owns the outer client connection lifecycle for [`ProtocolCore`]:
//! connect requests, transport readiness, websocket close events, explicit
//! disconnects and recovery timer callbacks
//!
//! it is a control-plane module
//! it never opens sockets or advances transport work directly
//! each transition returns ordered [`Command`] values the host must execute
//! through the `CommandBatch` contract
//!
//! the lifecycle split has three layers:
//!
//! ```text
//! ProtocolCore snapshot -> LifecycleModel -> LifecyclePlan -> Commands
//! ```
//!
//! the [`LifecycleModel`] is the small state slice shared by production and
//! verification
//! transition helpers mutate only that model and return a [`LifecyclePlan`]
//! `apply_plan` is the bridge back to [`ProtocolCore`],
//! where runtime state, sticky replay state and host-visible commands are
//! updated in the prescribed order
//!
//! the main contract is:
//!
//! - a fresh `connect` starts a new user attempt and wipes replayable intent
//! - explicit `disconnect` is terminal for that user attempt and clears saved context
//! - terminal close codes move to `Closed` without scheduling recovery
//! - transient close events keep the saved connect context and enter `Recovering`
//! - recovery timers only reconnect while the model is still `Recovering`
//!
//! example flows:
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
//! the last flow is intentionally different from `on_ws_close`: explicit
//! disconnect wipes replayable intent and suppresses later recovery, while a
//! transient socket loss keeps enough state around to reconnect and rebuild
//! from saved state

use super::{
    Command, Commands, ConnectContext, INITIAL_RECOVERY_DELAY_MS, ProtocolCore, RECOVERY_TIMER_ID,
    empty_features, next_recovery_delay,
};
use crate::{
    bundle_api::BundleConnectionState, shared::RecordingState, signaling::WebSocketCloseCode,
};

/// proof-friendly snapshot of the connection lifecycle state
///
/// this keeps only the state needed to decide whether a lifecycle event is
/// legal and what retry delay should be used next
/// runtime queues, track bindings and sticky replay stay on [`ProtocolCore`]
/// so verification can share production transition logic without modeling
/// unrelated state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LifecycleModel {
    /// current host-visible lifecycle state used to accept or reject events
    pub(super) state: BundleConnectionState,
    /// whether authentication data is still available for websocket retry
    pub(super) has_connect_context: bool,
    /// next delay that will be used for a transient close recovery attempt
    pub(super) recovery_delay_ms: u32,
}

impl LifecycleModel {
    /// creates the empty lifecycle model used by verification harnesses
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

/// policy for updating the saved connect context after a lifecycle transition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectContextUpdate {
    /// keep the saved context unchanged
    Preserve,
    /// drop the saved context so later recovery callbacks cannot reconnect
    Clear,
    /// replace the saved context with the input passed to `connect`
    ReplaceFromInput,
}

/// cleanup policy that `apply_plan` should use after the pure transition logic
/// has chosen the next lifecycle state
///
/// the pure model can say which cleanup class is required without touching
/// [`ProtocolCore`] state
/// production code uses this to choose silent teardown
/// or host-visible teardown commands, while verification only needs to know
/// whether runtime state survives
///
/// example:
///
/// ```text
/// connect() uses `Silent` because a fresh connect should drop old runtime
/// state without emitting teardown commands for an already-dead user
///
/// disconnect() uses `WithCommands` because the caller is ending a live user
/// and the host must see the explicit cleanup commands that fall out of it
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RuntimeCleanupMode {
    /// leave outbound batches, pending requests and track bindings untouched
    None,
    /// clear runtime-only state without asking the host to close live resources
    Silent,
    /// clear runtime-only state and emit the host cleanup commands still owed
    WithCommands,
}

/// source for a websocket connect command emitted after lifecycle cleanup
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectCommandSource {
    /// do not emit a websocket connect command
    None,
    /// use the URL from the current `connect` input
    FreshInput,
    /// use the URL from the saved connect context
    SavedContext,
}

/// lifecycle side effect shared by production command translation and verification
///
/// this is intentionally narrower than [`Command`]
/// lifecycle transitions only need state projection, socket teardown, peer
/// teardown and recovery timer control
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEffect {
    /// emit a host-visible lifecycle state update
    EmitStateChange {
        /// new lifecycle state to publish
        state: BundleConnectionState,
        /// optional terminal reason to expose through the bundle edge
        cause: Option<LifecycleCloseCause>,
    },
    /// close the current peer connection before the next transport attempt
    ClosePeerConnection,
    /// close the websocket with the given close code
    CloseWebSocket {
        /// websocket close code to pass to the host
        code: u16,
    },
    /// schedule the recovery timer after peer cleanup
    ScheduleRecoveryTimer {
        /// delay in milliseconds before the host calls the timer entry point
        ms: u32,
    },
    /// cancel a previously scheduled recovery timer
    CancelRecoveryTimer,
}

/// bounded ordered list of lifecycle effects emitted by one transition
///
/// most lifecycle transitions emit nothing, one effect or the fixed triplet
/// used by teardown and recovery
/// a `Vec` would make the domain looser and pull allocation into the proof path
/// for no gain
///
/// this enum keeps the contract explicit:
///
/// - effects stay ordered
/// - transitions can emit at most three effects
/// - there is no invalid partially-filled state like "slot three is set but
///   slot one is not"
///
/// that keeps normal code direct and the verification-facing lifecycle surface
/// small without inventing a fake collection model
///
/// example:
///
/// ```rs
/// LifecycleEffects::one(LifecycleEffect::ScheduleRecoveryTimer { ms: 1_000 })
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEffects {
    /// no lifecycle effects were produced
    None,
    /// one lifecycle effect was produced
    One(LifecycleEffect),
    /// three lifecycle effects were produced in execution order
    Three(LifecycleEffect, LifecycleEffect, LifecycleEffect),
}

impl LifecycleEffects {
    /// creates an empty effect list
    #[must_use]
    pub const fn new() -> Self {
        Self::None
    }

    /// creates an effect list with one ordered effect
    #[must_use]
    pub const fn one(first: LifecycleEffect) -> Self {
        Self::One(first)
    }

    /// creates an effect list with three ordered effects
    #[must_use]
    pub const fn three(
        first: LifecycleEffect,
        second: LifecycleEffect,
        third: LifecycleEffect,
    ) -> Self {
        Self::Three(first, second, third)
    }
}

/// host-visible reason attached to a terminal lifecycle state change
///
/// values are derived from terminal websocket close codes and rendered as
/// compatibility labels by production command translation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleCloseCause {
    /// authentication was rejected by the server
    AuthFailed,
    /// the server removed the user from the room
    Kicked,
    /// the room refused the connection because capacity was exhausted
    RoomFull,
}

/// pure result of one lifecycle transition
///
/// the plan is the contract between proof-only model logic and live
/// [`ProtocolCore`] mutation
/// it records what must happen before cleanup, what state may be dropped, how
/// the saved connect context changes and which host
/// effects must happen after cleanup
///
/// ordering matters because `CommandBatch` validates lifecycle side effects at
/// the host boundary
/// keep websocket close before peer close and schedule recovery only after peer
/// close
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LifecyclePlan {
    /// host effects that must be emitted before runtime cleanup
    pub(super) effects_before_cleanup: LifecycleEffects,
    /// host effects that must be emitted after cleanup has reached core state
    pub(super) effects_after_cleanup: LifecycleEffects,
    /// source for a websocket connect command appended after cleanup
    pub(super) connect_after_cleanup: ConnectCommandSource,
    /// whether sticky replay intent must be dropped for the new lifecycle
    pub(super) clear_sticky_state: bool,
    /// how runtime-only state should be cleared for this transition
    pub(super) runtime_cleanup_mode: RuntimeCleanupMode,
    /// how the saved connect context changes after this transition
    pub(super) connect_context_update: ConnectContextUpdate,
    /// whether server-owned session snapshots should be reset
    pub(super) reset_session_state: bool,
}

impl LifecyclePlan {
    #[must_use]
    fn none() -> Self {
        Self {
            effects_before_cleanup: LifecycleEffects::new(),
            effects_after_cleanup: LifecycleEffects::new(),
            connect_after_cleanup: ConnectCommandSource::None,
            clear_sticky_state: false,
            runtime_cleanup_mode: RuntimeCleanupMode::None,
            connect_context_update: ConnectContextUpdate::Preserve,
            reset_session_state: false,
        }
    }

    #[must_use]
    fn fresh_connect(state: BundleConnectionState) -> Self {
        Self {
            effects_after_cleanup: LifecycleEffects::one(LifecycleEffect::EmitStateChange {
                state,
                cause: None,
            }),
            connect_after_cleanup: ConnectCommandSource::FreshInput,
            clear_sticky_state: true,
            runtime_cleanup_mode: RuntimeCleanupMode::Silent,
            connect_context_update: ConnectContextUpdate::ReplaceFromInput,
            reset_session_state: true,
            ..Self::none()
        }
    }

    #[must_use]
    fn state_change(state: BundleConnectionState) -> Self {
        Self {
            effects_after_cleanup: LifecycleEffects::one(LifecycleEffect::EmitStateChange {
                state,
                cause: None,
            }),
            ..Self::none()
        }
    }

    #[must_use]
    fn explicit_disconnect(state: BundleConnectionState) -> Self {
        Self {
            effects_before_cleanup: LifecycleEffects::one(LifecycleEffect::CancelRecoveryTimer),
            effects_after_cleanup: LifecycleEffects::three(
                LifecycleEffect::CloseWebSocket {
                    code: u16::from(WebSocketCloseCode::Clean),
                },
                LifecycleEffect::ClosePeerConnection,
                LifecycleEffect::EmitStateChange { state, cause: None },
            ),
            clear_sticky_state: true,
            runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
            connect_context_update: ConnectContextUpdate::Clear,
            reset_session_state: true,
            ..Self::none()
        }
    }

    #[must_use]
    fn terminal_close(state: BundleConnectionState, cause: Option<LifecycleCloseCause>) -> Self {
        Self {
            effects_after_cleanup: LifecycleEffects::three(
                LifecycleEffect::CancelRecoveryTimer,
                LifecycleEffect::ClosePeerConnection,
                LifecycleEffect::EmitStateChange { state, cause },
            ),
            runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
            connect_context_update: ConnectContextUpdate::Clear,
            reset_session_state: true,
            ..Self::none()
        }
    }

    #[must_use]
    fn socket_closed_without_context(state: BundleConnectionState) -> Self {
        Self {
            effects_after_cleanup: LifecycleEffects::one(LifecycleEffect::EmitStateChange {
                state,
                cause: None,
            }),
            runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
            ..Self::none()
        }
    }

    #[must_use]
    fn recover(state: BundleConnectionState, delay_ms: u32) -> Self {
        Self {
            effects_after_cleanup: LifecycleEffects::three(
                LifecycleEffect::ClosePeerConnection,
                LifecycleEffect::EmitStateChange { state, cause: None },
                LifecycleEffect::ScheduleRecoveryTimer { ms: delay_ms },
            ),
            runtime_cleanup_mode: RuntimeCleanupMode::WithCommands,
            ..Self::none()
        }
    }

    #[must_use]
    fn retry_connect(state: BundleConnectionState) -> Self {
        Self {
            effects_after_cleanup: LifecycleEffects::one(LifecycleEffect::EmitStateChange {
                state,
                cause: None,
            }),
            connect_after_cleanup: ConnectCommandSource::SavedContext,
            ..Self::none()
        }
    }
}

/// plans a fresh connection attempt in the pure lifecycle model
///
/// accepted only from `Disconnected` or `Closed`
/// accepted attempts reset recovery backoff, mark the saved connect context as
/// present and move the lifecycle to `Connecting`
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
    LifecyclePlan::fresh_connect(model.state)
}

/// plans the protocol admission to media-ready transition
///
/// only `Authenticated` can become `Connected`
/// all other states are stale or premature host events and produce no effects
pub(super) fn on_transport_ready_model(model: &mut LifecycleModel) -> LifecyclePlan {
    if model.state != BundleConnectionState::Authenticated {
        return LifecyclePlan::none();
    }
    model.state = BundleConnectionState::Connected;
    LifecyclePlan::state_change(model.state)
}

/// plans an explicit user disconnect
///
/// this is a terminal action for the current user attempt
/// it clears the saved connect context, resets recovery backoff and requests
/// host-visible runtime cleanup
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
    LifecyclePlan::explicit_disconnect(model.state)
}

/// plans websocket close handling for terminal and recoverable socket loss
///
/// terminal close codes move to `Closed` and suppress recovery
/// recoverable closes with saved context enter `Recovering`
/// recoverable closes without saved context fall back to `Disconnected`
pub(super) fn on_ws_close_model(model: &mut LifecycleModel, close_code: u16) -> LifecyclePlan {
    if matches!(
        model.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return LifecyclePlan::none();
    }

    if let Some(
        WebSocketCloseCode::AuthFailed | WebSocketCloseCode::Kicked | WebSocketCloseCode::RoomFull,
    ) = WebSocketCloseCode::from_u16(close_code)
    {
        model.state = BundleConnectionState::Closed;
        model.has_connect_context = false;
        model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        return LifecyclePlan::terminal_close(model.state, terminal_close_cause(close_code));
    }

    if !model.has_connect_context {
        model.state = BundleConnectionState::Disconnected;
        return LifecyclePlan::socket_closed_without_context(model.state);
    }
    let delay_ms = model.recovery_delay_ms;
    model.recovery_delay_ms = next_recovery_delay(delay_ms);
    model.state = BundleConnectionState::Recovering;
    LifecyclePlan::recover(model.state, delay_ms)
}

/// plans the retry attempt for the recovery timer
///
/// only `Recovering` with a saved connect context may retry
/// stale timers in any other state are ignored
pub(super) fn handle_recovery_timer_model(model: &mut LifecycleModel) -> LifecyclePlan {
    if model.state != BundleConnectionState::Recovering || !model.has_connect_context {
        return LifecyclePlan::none();
    }
    model.state = BundleConnectionState::Connecting;
    LifecyclePlan::retry_connect(model.state)
}

/// extracts the proof-friendly lifecycle model from the live core
fn lifecycle_model(core: &ProtocolCore) -> LifecycleModel {
    LifecycleModel {
        state: core.state,
        has_connect_context: core.connect_context.is_some(),
        recovery_delay_ms: core.recovery_delay_ms,
    }
}

/// commits a pure lifecycle plan into the live [`ProtocolCore`]
///
/// this is the only function in this module that mutates runtime state, sticky
/// replay state and saved connect context
/// it captures any websocket URL before context replacement so fresh connects
/// and saved-context retries both emit the right [`Command::Connect`]
fn apply_plan(
    core: &mut ProtocolCore,
    model: LifecycleModel,
    plan: LifecyclePlan,
    fresh_connect_context: Option<ConnectContext>,
) -> Commands {
    let connect_url = connect_url(core, &plan, fresh_connect_context.as_ref());
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
    if let Some(url) = connect_url {
        commands.push(Command::Connect { url });
    }
    commands
}

/// selects the URL for a post-cleanup websocket connect command
///
/// fresh connect paths use the caller input
/// recovery paths use the context still stored on the core
fn connect_url(
    core: &ProtocolCore,
    plan: &LifecyclePlan,
    fresh_connect_context: Option<&ConnectContext>,
) -> Option<String> {
    match plan.connect_after_cleanup {
        ConnectCommandSource::None => None,
        ConnectCommandSource::FreshInput => {
            fresh_connect_context.map(|connect_context| connect_context.url.clone())
        }
        ConnectCommandSource::SavedContext => core
            .connect_context
            .as_ref()
            .map(|connect_context| connect_context.url.clone()),
    }
}

/// translates an ordered lifecycle effect list into host commands
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

/// projects one lifecycle effect into the protocol command boundary
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

/// maps terminal websocket close codes to host-visible lifecycle causes
fn terminal_close_cause(close_code: u16) -> Option<LifecycleCloseCause> {
    match WebSocketCloseCode::from_u16(close_code) {
        Some(WebSocketCloseCode::AuthFailed) => Some(LifecycleCloseCause::AuthFailed),
        Some(WebSocketCloseCode::Kicked) => Some(LifecycleCloseCause::Kicked),
        Some(WebSocketCloseCode::RoomFull) => Some(LifecycleCloseCause::RoomFull),
        _ => None,
    }
}

/// returns the compatibility label exposed through `EmitStateChange`
fn lifecycle_close_cause_label(cause: LifecycleCloseCause) -> &'static str {
    match cause {
        LifecycleCloseCause::AuthFailed => "auth_failed",
        LifecycleCloseCause::Kicked => "kicked",
        LifecycleCloseCause::RoomFull => "full",
    }
}

/// starts a fresh connection attempt from a disconnected or closed state
///
/// this is the only lifecycle entry point that intentionally wipes both
/// runtime state and sticky replay state before reconnecting
/// a brand-new `connect` means "start over with this endpoint and auth context",
/// not "resume whatever the previous user was trying to do"
///
/// calls from any other state are ignored so the host cannot accidentally stack
/// overlapping connection attempts on top of an already-live user
///
/// ```text
/// Disconnected --connect(url, jwt, room)--> Connecting
/// ```
pub(super) fn connect(
    core: &mut ProtocolCore,
    url: String,
    jwt: String,
    room: Option<String>,
) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = connect_model(&mut model);
    apply_plan(core, model, plan, Some(ConnectContext { url, jwt, room }))
}

/// marks the transport side as ready after websocket authentication
///
/// this only accepts the `Authenticated -> Connected` step
/// earlier states have not completed protocol admission yet and later states
/// have already consumed this transition
///
/// ```text
/// Connecting --on_welcome()--> Authenticated --on_transport_ready()--> Connected
/// ```
pub(super) fn on_transport_ready(core: &mut ProtocolCore) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = on_transport_ready_model(&mut model);
    apply_plan(core, model, plan, None)
}

/// ends the current user attempt on purpose
///
/// unlike `on_ws_close`, this is not a recovery path
/// it clears the saved connect context, runtime state and sticky replay state,
/// then closes the websocket and peer connection
/// any later recovery-timer delivery becomes a no-op because the caller
/// explicitly asked to stop
pub(super) fn disconnect(core: &mut ProtocolCore) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = disconnect_model(&mut model);
    apply_plan(core, model, plan, None)
}

/// handles websocket closure after a user was already in flight
///
/// there are three different cases here and mixing them up is the main way to
/// break reconnect behavior:
///
/// - terminal close codes move to `Closed`, clear the saved connect context,
///   and suppress recovery
/// - non-terminal closes with saved connect context move to `Recovering` and
///   schedule the recovery timer
/// - non-terminal closes without saved connect context fall back to
///   `Disconnected`, because there is nothing safe to reconnect to
///
/// example:
///
/// ```text
/// Connected --on_ws_close(AuthFailed)--> Closed
/// Connected --on_ws_close(1011)--> Recovering
/// ```
pub(super) fn on_ws_close(core: &mut ProtocolCore, close_code: u16) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = on_ws_close_model(&mut model, close_code);
    apply_plan(core, model, plan, None)
}

/// retries the saved websocket connection after a recovery delay
///
/// this is intentionally narrow
/// only `Recovering` may consume the recovery timer
/// a stale timer firing after a successful reconnect or explicit
/// disconnect must do nothing, otherwise old scheduled work can restart an
/// inactive attempt
///
/// example:
///
/// ```text
/// Connected --on_ws_close(1011)--> Recovering
/// Recovering --handle_recovery_timer()--> Connecting
/// Connected --handle_recovery_timer()--> no-op
/// ```
pub(super) fn handle_recovery_timer(core: &mut ProtocolCore) -> Commands {
    let mut model = lifecycle_model(core);
    let plan = handle_recovery_timer_model(&mut model);
    apply_plan(core, model, plan, None)
}
