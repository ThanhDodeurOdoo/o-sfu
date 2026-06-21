//! lifecycle transitions for the protocol core connection state machine
//!
//! this module owns the outer client connection lifecycle for [`ProtocolCore`]:
//! connect requests, transport readiness, websocket close events, explicit
//! disconnects and recovery timer callbacks
//!
//! it is a control-plane module
//! it never opens sockets or advances transport work directly
//! each transition returns ordered [`Command`] values the host must execute
//! through the [`super::CommandBatch`] contract
//!
//! the lifecycle split has two layers:
//!
//! ```text
//! ProtocolCore snapshot -> LifecycleModel -> LifecycleTransition
//! ```
//!
//! the [`LifecycleModel`] is the small state slice shared by production and
//! verification
//! transition helpers mutate only that model and return one ordered
//! [`LifecycleTransition`]
//! [`apply_transition`] is the bridge back to [`ProtocolCore`],
//! where runtime state, sticky replay state and host-visible commands are
//! updated in the prescribed order
//!
//! the main contract is:
//!
//! - a fresh [`connect`] starts a new user attempt and wipes replayable intent
//! - explicit [`disconnect`] is terminal for that user attempt and clears saved context
//! - terminal close codes move to [`BundleConnectionState::Closed`] without scheduling recovery
//! - transient close events keep the saved connect context and enter
//!   [`BundleConnectionState::Recovering`]
//! - recovery timers only reconnect while the model is still [`BundleConnectionState::Recovering`]
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
//! the last flow is different from [`on_ws_close`]: explicit
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

pub(super) struct LifecycleTransition {
    pub(super) actions: Vec<LifecycleAction>,
}

impl LifecycleTransition {
    #[must_use]
    fn none() -> Self {
        Self::new(Vec::new())
    }

    #[must_use]
    fn new(actions: Vec<LifecycleAction>) -> Self {
        Self { actions }
    }
}

#[derive(Clone, Copy)]
pub(super) enum LifecycleAction {
    StoreFreshConnectContext,
    ClearConnectContext,
    ClearWelcomeSnapshot,
    ClearRuntimeStateSilently,
    ClearRuntimeStateWithCommands,
    ClearStickyState,
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
    EmitConnectCommand,
}

fn state_change(state: BundleConnectionState) -> LifecycleAction {
    LifecycleAction::EmitStateChange { state, cause: None }
}

/// builds a fresh connection attempt in the pure lifecycle model
///
/// accepted only from [`BundleConnectionState::Disconnected`],
/// [`BundleConnectionState::Closed`] or [`BundleConnectionState::Recovering`]
/// accepted attempts reset recovery backoff, mark the saved connect context as
/// present and move the lifecycle to [`BundleConnectionState::Connecting`]
pub(super) fn connect_model(model: &mut LifecycleModel) -> LifecycleTransition {
    let mut actions = match model.state {
        BundleConnectionState::Disconnected | BundleConnectionState::Closed => Vec::new(),
        BundleConnectionState::Recovering => vec![LifecycleAction::CancelRecoveryTimer],
        _ => return LifecycleTransition::none(),
    };
    model.has_connect_context = true;
    model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    model.state = BundleConnectionState::Connecting;
    actions.extend([
        LifecycleAction::StoreFreshConnectContext,
        LifecycleAction::ClearWelcomeSnapshot,
        LifecycleAction::ClearRuntimeStateSilently,
        LifecycleAction::ClearStickyState,
        state_change(model.state),
        LifecycleAction::EmitConnectCommand,
    ]);
    LifecycleTransition::new(actions)
}

/// builds the protocol admission to media-ready transition
///
/// only [`BundleConnectionState::Authenticated`] can become [`BundleConnectionState::Connected`]
/// all other states are stale or premature host events and produce no effects
pub(super) fn on_transport_ready_model(model: &mut LifecycleModel) -> LifecycleTransition {
    if model.state != BundleConnectionState::Authenticated {
        return LifecycleTransition::none();
    }
    model.state = BundleConnectionState::Connected;
    LifecycleTransition::new(vec![state_change(model.state)])
}

/// builds an explicit user disconnect transition
///
/// this is a terminal action for the current user attempt
/// it clears the saved connect context, resets recovery backoff and requests
/// host-visible runtime cleanup
pub(super) fn disconnect_model(model: &mut LifecycleModel) -> LifecycleTransition {
    if matches!(
        model.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return LifecycleTransition::none();
    }
    model.state = BundleConnectionState::Disconnected;
    model.has_connect_context = false;
    model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    LifecycleTransition::new(vec![
        LifecycleAction::CancelRecoveryTimer,
        LifecycleAction::ClearConnectContext,
        LifecycleAction::ClearWelcomeSnapshot,
        LifecycleAction::ClearRuntimeStateWithCommands,
        LifecycleAction::ClearStickyState,
        LifecycleAction::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::Clean),
        },
        LifecycleAction::ClosePeerConnection,
        state_change(model.state),
    ])
}

/// builds websocket close handling for terminal and recoverable socket loss
///
/// terminal close codes move to [`BundleConnectionState::Closed`] and suppress recovery
/// recoverable closes with saved context enter [`BundleConnectionState::Recovering`]
/// recoverable closes without saved context fall back to [`BundleConnectionState::Disconnected`]
pub(super) fn on_ws_close_model(
    model: &mut LifecycleModel,
    close_code: u16,
) -> LifecycleTransition {
    if matches!(
        model.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return LifecycleTransition::none();
    }

    if let Some(
        terminal_code @ (WebSocketCloseCode::ProtocolError
        | WebSocketCloseCode::AuthFailed
        | WebSocketCloseCode::Kicked
        | WebSocketCloseCode::RoomFull),
    ) = WebSocketCloseCode::from_u16(close_code)
    {
        model.state = BundleConnectionState::Closed;
        model.has_connect_context = false;
        model.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        return LifecycleTransition::new(vec![
            LifecycleAction::ClearConnectContext,
            LifecycleAction::ClearWelcomeSnapshot,
            LifecycleAction::ClearRuntimeStateWithCommands,
            LifecycleAction::CancelRecoveryTimer,
            LifecycleAction::ClosePeerConnection,
            LifecycleAction::EmitStateChange {
                state: model.state,
                cause: terminal_close_cause(terminal_code),
            },
        ]);
    }

    if !model.has_connect_context {
        model.state = BundleConnectionState::Disconnected;
        return LifecycleTransition::new(vec![
            LifecycleAction::ClearRuntimeStateWithCommands,
            state_change(model.state),
        ]);
    }
    let delay_ms = model.recovery_delay_ms;
    model.recovery_delay_ms = next_recovery_delay(delay_ms);
    model.state = BundleConnectionState::Recovering;
    LifecycleTransition::new(vec![
        LifecycleAction::ClearRuntimeStateWithCommands,
        LifecycleAction::ClosePeerConnection,
        state_change(model.state),
        LifecycleAction::ScheduleRecoveryTimer { ms: delay_ms },
    ])
}

/// builds the retry attempt for the recovery timer
///
/// only [`BundleConnectionState::Recovering`] with a saved connect context may retry
/// stale timers in any other state are ignored
pub(super) fn handle_recovery_timer_model(model: &mut LifecycleModel) -> LifecycleTransition {
    if model.state != BundleConnectionState::Recovering || !model.has_connect_context {
        return LifecycleTransition::none();
    }
    model.state = BundleConnectionState::Connecting;
    LifecycleTransition::new(vec![
        state_change(model.state),
        LifecycleAction::EmitConnectCommand,
    ])
}

/// extracts the proof-friendly lifecycle model from the live core
fn lifecycle_model(core: &ProtocolCore) -> LifecycleModel {
    LifecycleModel {
        state: core.state(),
        has_connect_context: core.connect_context.is_some(),
        recovery_delay_ms: core.recovery_delay_ms,
    }
}

/// commits a pure lifecycle transition into the live [`ProtocolCore`]
///
/// this is the only function in this module that mutates runtime state, sticky
/// replay state and saved connect context
fn apply_transition(
    core: &mut ProtocolCore,
    model: LifecycleModel,
    transition: LifecycleTransition,
    mut fresh_connect_context: Option<ConnectContext>,
) -> Commands {
    core.phase.apply_lifecycle_state(model.state);
    core.recovery_delay_ms = model.recovery_delay_ms;

    let mut commands = Vec::new();
    for action in transition.actions {
        match action {
            LifecycleAction::StoreFreshConnectContext => {
                core.connect_context = fresh_connect_context.take();
            }
            LifecycleAction::ClearConnectContext => core.connect_context = None,
            LifecycleAction::ClearWelcomeSnapshot => {
                core.features = empty_features();
                core.recording_state = RecordingState::default();
            }
            LifecycleAction::ClearRuntimeStateSilently => core.clear_runtime_state(),
            LifecycleAction::ClearRuntimeStateWithCommands => {
                commands.extend(core.clear_runtime_state_with_commands());
            }
            LifecycleAction::ClearStickyState => core.clear_sticky_state(),
            LifecycleAction::EmitStateChange { state, cause } => {
                commands.push(Command::EmitStateChange {
                    state,
                    cause: cause.map(lifecycle_close_cause_label).map(str::to_owned),
                });
            }
            LifecycleAction::ClosePeerConnection => commands.push(Command::ClosePeerConnection),
            LifecycleAction::CloseWebSocket { code } => {
                commands.push(Command::CloseWebSocket { code });
            }
            LifecycleAction::ScheduleRecoveryTimer { ms } => {
                commands.push(Command::ScheduleTimer {
                    id: RECOVERY_TIMER_ID,
                    ms,
                });
            }
            LifecycleAction::CancelRecoveryTimer => {
                commands.push(Command::CancelTimer {
                    id: RECOVERY_TIMER_ID,
                });
            }
            LifecycleAction::EmitConnectCommand => {
                if let Some(connect_context) = core.connect_context.as_ref() {
                    commands.push(Command::Connect {
                        url: connect_context.url.clone(),
                    });
                }
            }
        }
    }
    commands
}

/// maps terminal websocket close codes to host-visible lifecycle causes
fn terminal_close_cause(close_code: WebSocketCloseCode) -> Option<LifecycleCloseCause> {
    match close_code {
        WebSocketCloseCode::AuthFailed => Some(LifecycleCloseCause::AuthFailed),
        WebSocketCloseCode::Kicked => Some(LifecycleCloseCause::Kicked),
        WebSocketCloseCode::RoomFull => Some(LifecycleCloseCause::RoomFull),
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

/// starts a fresh connection attempt from an inactive or recovering state
///
/// this is the only lifecycle entry point that wipes both
/// runtime state and sticky replay state before reconnecting
/// a brand-new [`connect`] means "start over with this endpoint and auth context",
/// not "resume whatever the previous user was trying to do"
///
/// calls from live admission states are ignored so the host cannot accidentally
/// stack overlapping connection attempts on top of an already-live user
/// a call from [`BundleConnectionState::Recovering`] also cancels the stale recovery timer before the
/// new socket attempt starts
///
/// ```text
/// Disconnected --connect(url, jwt, room)--> Connecting
/// Closed       --connect(url, jwt, room)--> Connecting
/// Recovering  --connect(url, jwt, room)--> Connecting
/// ```
pub(super) fn connect(
    core: &mut ProtocolCore,
    url: String,
    jwt: String,
    room: Option<String>,
) -> Commands {
    let mut model = lifecycle_model(core);
    let transition = connect_model(&mut model);
    apply_transition(
        core,
        model,
        transition,
        Some(ConnectContext { url, jwt, room }),
    )
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
    let transition = on_transport_ready_model(&mut model);
    apply_transition(core, model, transition, None)
}

/// ends the current user attempt on purpose
///
/// unlike [`on_ws_close`], this is not a recovery path
/// it clears the saved connect context, runtime state and sticky replay state,
/// then closes the websocket and peer connection
/// any later recovery-timer delivery becomes a no-op because the caller
/// explicitly asked to stop
pub(super) fn disconnect(core: &mut ProtocolCore) -> Commands {
    let mut model = lifecycle_model(core);
    let transition = disconnect_model(&mut model);
    apply_transition(core, model, transition, None)
}

/// handles websocket closure after a user was already in flight
///
/// there are three different cases here and mixing them up is the main way to
/// break reconnect behavior:
///
/// - terminal close codes move to [`BundleConnectionState::Closed`], clear the saved connect context,
///   and suppress recovery
/// - non-terminal closes with saved connect context move to [`BundleConnectionState::Recovering`] and
///   schedule the recovery timer
/// - non-terminal closes without saved connect context fall back to
///   [`BundleConnectionState::Disconnected`], because there is nothing safe to reconnect to
///
/// example:
///
/// ```text
/// Connected --on_ws_close(AuthFailed)--> Closed
/// Connected --on_ws_close(1011)--> Recovering
/// ```
pub(super) fn on_ws_close(core: &mut ProtocolCore, close_code: u16) -> Commands {
    let mut model = lifecycle_model(core);
    let transition = on_ws_close_model(&mut model, close_code);
    apply_transition(core, model, transition, None)
}

/// retries the saved websocket connection after a recovery delay
///
/// this is narrow
/// only [`BundleConnectionState::Recovering`] may consume the recovery timer
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
    let transition = handle_recovery_timer_model(&mut model);
    apply_transition(core, model, transition, None)
}
