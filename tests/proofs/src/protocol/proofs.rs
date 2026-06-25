use o_sfu_protocol::{
    host::{ConnectionState, RECOVERY_TIMER_ID, verification::VerificationConnectionLifecycle},
    wire::WebSocketCloseCode,
};

const NON_TERMINAL_CLOSE_CODE: u16 = 1011;

// Proves terminal close codes cut off recovery completely: they move the client
// to `Closed`, clear reconnect context and never leave a recovery timer path
// behind. useful to check because these close codes are the public
// contract for "do not try again" failures.
#[kani::proof]
#[kani::unwind(9)]
fn protocol_core_terminal_close_codes_never_schedule_recovery() {
    let stage = kani::any::<u8>() % 3;
    let close_code = terminal_close_code(kani::any::<u8>() % 3);
    let mut core = lifecycle_at_stage(stage);

    let commands = core.on_ws_close(close_code);
    assert_eq!(core.state(), ConnectionState::Closed);
    assert_eq!(commands.recovery_timer_count(), 0);
    assert!(core.on_timer(RECOVERY_TIMER_ID).is_empty());
    assert!(!core.has_connect_context());

    let reconnect_commands = core.connect();
    assert_eq!(reconnect_commands.connect_count(), 1);

    std::mem::forget(commands);
    std::mem::forget(reconnect_commands);
    std::mem::forget(core);
}

// Proves a non-terminal close with saved connect context always enters exactly
// one recovery path: the peer connection is closed and one recovery timer is
// scheduled. That gives a "legality" check on the reconnect entry point
// instead of trusting several sampled recovery tests to cover the combinations.
#[kani::proof]
#[kani::unwind(9)]
fn protocol_core_non_terminal_close_with_context_recovers_once() {
    let stage = kani::any::<u8>() % 3;
    let mut core = lifecycle_at_stage(stage);

    let commands = core.on_ws_close(NON_TERMINAL_CLOSE_CODE);
    assert_eq!(core.state(), ConnectionState::Recovering);
    assert_eq!(commands.recovery_timer_count(), 1);
    assert_eq!(commands.close_peer_connection_count(), 1);
    assert!(core.has_connect_context());

    std::mem::forget(commands);
    std::mem::forget(core);
}

// Proves the recovery timer is only live in the recovering state. Any other
// lifecycle state must ignore that timer id, while recovering must emit one
// reconnect back to `Connecting`. This keeps timer delivery idempotent and
// stops stale timers from resurrecting dead users.
#[kani::proof]
#[kani::unwind(9)]
fn protocol_core_recovery_timer_reconnects_only_from_recovering() {
    let selector = kani::any::<u8>() % 6;
    let mut core = lifecycle_at_state_selector(selector);
    let expect_reconnect = core.state() == ConnectionState::Recovering;

    let commands = core.on_timer(RECOVERY_TIMER_ID);
    assert_eq!(commands.connect_count(), usize::from(expect_reconnect));
    if expect_reconnect {
        assert_eq!(core.state(), ConnectionState::Connecting);
    }

    std::mem::forget(commands);
    std::mem::forget(core);
}

// Proves a successful welcome resets recovery backoff so the next disconnect
// starts again from the initial retry delay. That matters because a healthy
// reconnect should not carry over an old penalty and leave the client stuck
// waiting longer after a later unrelated drop.
#[kani::proof]
#[kani::unwind(9)]
fn protocol_core_welcome_resets_recovery_backoff() {
    let mut core = lifecycle_at_stage(2);

    let first_close = core.on_ws_close(NON_TERMINAL_CLOSE_CODE);
    let first_delay = first_close.recovery_timer_delay();
    assert!(first_delay.is_some());
    let _ = core.on_timer(RECOVERY_TIMER_ID);
    let _ = core.on_welcome();
    let second_close = core.on_ws_close(NON_TERMINAL_CLOSE_CODE);
    let second_delay = second_close.recovery_timer_delay();

    assert_eq!(first_delay, second_delay);

    std::mem::forget(first_close);
    std::mem::forget(second_close);
    std::mem::forget(core);
}

// Proves explicit disconnect is a real terminal cleanup step for the current
// user: it clears reconnect context, suppresses any later recovery timer and
// allows a fresh connect attempt to start from a clean state. This is valuable
// because disconnect and involuntary close share a lot of machinery but must
// have very different recovery semantics.
#[kani::proof]
#[kani::unwind(9)]
fn protocol_core_connecting_disconnect_suppresses_recovery() {
    assert_disconnect_suppresses_recovery(0);
}

#[kani::proof]
#[kani::unwind(9)]
fn protocol_core_authenticated_disconnect_suppresses_recovery() {
    assert_disconnect_suppresses_recovery(1);
}

#[kani::proof]
#[kani::unwind(9)]
fn protocol_core_connected_disconnect_suppresses_recovery() {
    assert_disconnect_suppresses_recovery(2);
}

fn assert_disconnect_suppresses_recovery(stage: u8) {
    let mut core = lifecycle_at_stage(stage);
    core.seed_sticky_replay();
    core.seed_source_snapshot();

    let disconnect_commands = core.disconnect();
    assert_eq!(core.state(), ConnectionState::Disconnected);
    assert!(!core.has_connect_context());
    assert!(!core.has_sticky_replay());
    assert!(!core.has_source_snapshot());
    assert_eq!(disconnect_commands.close_peer_connection_count(), 1);
    assert!(core.on_timer(RECOVERY_TIMER_ID).is_empty());
    assert!(core.on_ws_close(NON_TERMINAL_CLOSE_CODE).is_empty());

    let reconnect_commands = core.connect();
    assert_eq!(reconnect_commands.connect_count(), 1);

    std::mem::forget(disconnect_commands);
    std::mem::forget(reconnect_commands);
    std::mem::forget(core);
}

fn lifecycle_at_stage(stage: u8) -> VerificationConnectionLifecycle {
    let mut core = VerificationConnectionLifecycle::new();
    let _ = core.connect();
    if stage >= 1 {
        let _ = core.on_welcome();
    }
    if stage >= 2 {
        let _ = core.on_transport_ready();
    }
    core
}

fn lifecycle_at_state_selector(selector: u8) -> VerificationConnectionLifecycle {
    match selector {
        0 => VerificationConnectionLifecycle::new(),
        1 => lifecycle_at_stage(0),
        2 => lifecycle_at_stage(1),
        3 => lifecycle_at_stage(2),
        4 => {
            let mut core = lifecycle_at_stage(2);
            let _ = core.on_ws_close(NON_TERMINAL_CLOSE_CODE);
            core
        }
        _ => {
            let mut core = lifecycle_at_stage(1);
            let _ = core.on_ws_close(u16::from(WebSocketCloseCode::RoomFull));
            core
        }
    }
}

fn terminal_close_code(selector: u8) -> u16 {
    match selector {
        0 => u16::from(WebSocketCloseCode::AuthFailed),
        1 => u16::from(WebSocketCloseCode::Kicked),
        _ => u16::from(WebSocketCloseCode::RoomFull),
    }
}
