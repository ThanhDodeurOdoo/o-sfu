use o_sfu_protocol::{
    core::{
        Command, ConnectionState, RECOVERY_TIMER_ID, verification::VerificationConnectionLifecycle,
    },
    signaling::WebSocketCloseCode,
};

const NON_TERMINAL_CLOSE_CODE: u16 = 1011;

// Proves terminal close codes cut off recovery completely: they move the client
// to `Closed`, clear reconnect context and never leave a recovery timer path
// behind. usefull to check because these close codes are the public
// contract for "do not try again" failures.
#[kani::proof]
fn protocol_core_terminal_close_codes_never_schedule_recovery() {
    let stage = kani::any::<u8>() % 3;
    let close_code = terminal_close_code(kani::any::<u8>() % 3);
    let mut core = lifecycle_at_stage(stage);

    let commands = core.on_ws_close(close_code);
    assert_eq!(core.state(), ConnectionState::Closed);
    assert_eq!(scheduled_timer_count(&commands, RECOVERY_TIMER_ID), 0);
    assert!(core.on_timer(RECOVERY_TIMER_ID).is_empty());
    assert!(!core.has_connect_context());

    let reconnect_commands = core.connect(
        String::from("wss://next.example/socket"),
        String::from("fresh-jwt"),
        None,
    );
    assert!(has_connect_command(
        &reconnect_commands,
        "wss://next.example/socket"
    ));

    std::mem::forget(commands);
    std::mem::forget(reconnect_commands);
    std::mem::forget(core);
}

// Proves a non-terminal close with saved connect context always enters exactly
// one recovery path: the peer connection is closed and one recovery timer is
// scheduled. That gives a "legality" check on the reconnect entry point
// instead of trusting several sampled recovery tests to cover the combinations.
#[kani::proof]
fn protocol_core_non_terminal_close_with_context_recovers_once() {
    let stage = kani::any::<u8>() % 3;
    let mut core = lifecycle_at_stage(stage);

    let commands = core.on_ws_close(NON_TERMINAL_CLOSE_CODE);
    assert_eq!(core.state(), ConnectionState::Recovering);
    assert_eq!(scheduled_timer_count(&commands, RECOVERY_TIMER_ID), 1);
    assert!(has_close_peer_connection(&commands));
    assert!(core.has_connect_context());

    std::mem::forget(commands);
    std::mem::forget(core);
}

// Proves the recovery timer is only live in the recovering state. Any other
// lifecycle state must ignore that timer id, while recovering must emit one
// reconnect back to `Connecting`. This keeps timer delivery idempotent and
// stops stale timers from resurrecting dead sessions.
#[kani::proof]
fn protocol_core_recovery_timer_reconnects_only_from_recovering() {
    let stage = kani::any::<u8>() % 6;
    let mut core = lifecycle_at_lifecycle_state(stage);

    let commands = core.on_timer(RECOVERY_TIMER_ID);
    let expect_reconnect = stage == 4;
    assert_eq!(
        has_connect_command(&commands, "wss://proof.example/socket"),
        expect_reconnect
    );
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
fn protocol_core_welcome_resets_recovery_backoff() {
    let mut core = lifecycle_at_stage(2);

    let first_close = core.on_ws_close(NON_TERMINAL_CLOSE_CODE);
    let first_delay = scheduled_delay(&first_close, RECOVERY_TIMER_ID).unwrap_or_default();
    let _ = core.on_timer(RECOVERY_TIMER_ID);
    let _ = core.on_welcome();
    let second_close = core.on_ws_close(NON_TERMINAL_CLOSE_CODE);
    let second_delay = scheduled_delay(&second_close, RECOVERY_TIMER_ID).unwrap_or_default();

    assert_eq!(first_delay, second_delay);

    std::mem::forget(first_close);
    std::mem::forget(second_close);
    std::mem::forget(core);
}

// Proves explicit disconnect is a real terminal cleanup step for the current
// session: it clears reconnect context, suppresses any later recovery timer and
// allows a fresh connect attempt to start from a clean state. This is valuable
// because disconnect and involuntary close share a lot of machinery but must
// have very different recovery semantics.
#[kani::proof]
fn protocol_core_disconnect_suppresses_recovery_and_allows_fresh_connect() {
    let stage = kani::any::<u8>() % 3;
    let mut core = lifecycle_at_stage(stage);
    core.mark_sticky_state_present();
    core.mark_runtime_state_present();

    let disconnect_commands = core.disconnect();
    assert_eq!(core.state(), ConnectionState::Disconnected);
    assert!(!core.has_connect_context());
    assert!(!core.sticky_state_present());
    assert!(!core.runtime_state_present());
    assert!(has_close_peer_connection(&disconnect_commands));
    assert!(core.on_timer(RECOVERY_TIMER_ID).is_empty());
    assert!(core.on_ws_close(NON_TERMINAL_CLOSE_CODE).is_empty());

    let reconnect_commands = core.connect(
        String::from("wss://fresh.example/socket"),
        String::from("fresh-jwt"),
        Some(String::from("fresh-room")),
    );
    assert!(has_connect_command(
        &reconnect_commands,
        "wss://fresh.example/socket"
    ));

    std::mem::forget(disconnect_commands);
    std::mem::forget(reconnect_commands);
    std::mem::forget(core);
}

fn lifecycle_at_stage(stage: u8) -> VerificationConnectionLifecycle {
    let mut core = VerificationConnectionLifecycle::new();
    let _ = core.connect(
        String::from("wss://proof.example/socket"),
        String::from("jwt-proof"),
        Some(String::from("room")),
    );
    if stage >= 1 {
        let _ = core.on_welcome();
    }
    if stage >= 2 {
        let _ = core.on_transport_ready();
    }
    core
}

fn lifecycle_at_lifecycle_state(stage: u8) -> VerificationConnectionLifecycle {
    let mut core = VerificationConnectionLifecycle::new();
    match stage {
        0 => {}
        1 => {
            let _ = core.connect(
                String::from("wss://proof.example/socket"),
                String::from("jwt-proof"),
                Some(String::from("room")),
            );
        }
        2 => {
            core = lifecycle_at_stage(1);
        }
        3 => {
            core = lifecycle_at_stage(2);
        }
        4 => {
            core = lifecycle_at_stage(2);
            let _ = core.on_ws_close(NON_TERMINAL_CLOSE_CODE);
        }
        _ => {
            core = lifecycle_at_stage(1);
            let _ = core.on_ws_close(u16::from(WebSocketCloseCode::ChannelFull));
        }
    }
    core
}

fn terminal_close_code(selector: u8) -> u16 {
    match selector {
        0 => u16::from(WebSocketCloseCode::AuthFailed),
        1 => u16::from(WebSocketCloseCode::Kicked),
        _ => u16::from(WebSocketCloseCode::ChannelFull),
    }
}

fn scheduled_timer_count(commands: &[Command], timer_id: u32) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, Command::ScheduleTimer { id, .. } if *id == timer_id))
        .count()
}

fn scheduled_delay(commands: &[Command], timer_id: u32) -> Option<u32> {
    commands.iter().find_map(|command| match command {
        Command::ScheduleTimer { id, ms } if *id == timer_id => Some(*ms),
        _ => None,
    })
}

fn has_connect_command(commands: &[Command], url: &str) -> bool {
    commands.iter().any(|command| match command {
        Command::Connect { url: actual } => actual == url,
        _ => false,
    })
}

fn has_close_peer_connection(commands: &[Command]) -> bool {
    commands
        .iter()
        .any(|command| matches!(command, Command::ClosePeerConnection))
}
