use o_sfu_protocol::{
    core::{
        Command, ConnectionState, PendingRequestKind, RECOVERY_TIMER_ID,
        verification::{
            VerificationConnectionLifecycle, VerificationFlushMode, VerificationOutboundBatcher,
            VerificationRequestTracker, VerificationStickyReplay,
        },
    },
    shared::{DownloadStates, SessionId, SessionInfo, StreamType},
    signaling::{RequestId, WebSocketCloseCode},
};

const NON_TERMINAL_CLOSE_CODE: u16 = 1011;

// Proves that invalid responses are pure no-ops: an unknown request id or wrong
// request kind must not disturb the live request/timer pairing. This is usefull
// because request completion bugs tend to look fine in sampled tests until a
// stale or crossed response silently drops the wrong pending request.
#[kani::proof]
fn request_tracker_rejects_mismatched_resolution_without_state_loss() {
    let mut tracker = VerificationRequestTracker::new();
    let registered = tracker.register_request(PendingRequestKind::StartRecording);
    let mismatch_uses_unknown_request = kani::any::<bool>();

    assert!(tracker.has_bijection_between_requests_and_timers());
    let mismatch_commands = if mismatch_uses_unknown_request {
        tracker.resolve_response(
            &RequestId::new("missing"),
            PendingRequestKind::StartRecording,
            true,
        )
    } else {
        tracker.resolve_response(
            &registered.request_id,
            PendingRequestKind::StopRecording,
            true,
        )
    };

    assert!(mismatch_commands.is_empty());
    assert!(tracker.has_bijection_between_requests_and_timers());
    assert_eq!(tracker.pending_count(), 1);
    assert_eq!(tracker.timeout_count(), 1);
    assert!(tracker.contains_pending_request(&registered.request_id));
    assert!(tracker.contains_timeout_timer(registered.timeout_timer_id));

    let resolved = tracker.resolve_response(
        &registered.request_id,
        PendingRequestKind::StartRecording,
        true,
    );

    assert_eq!(
        resolved,
        vec![
            Command::CancelTimer {
                id: registered.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id: registered.request_id,
                ok: true,
            },
        ]
    );
    assert!(tracker.has_bijection_between_requests_and_timers());
    assert_eq!(tracker.pending_count(), 0);
    assert_eq!(tracker.timeout_count(), 0);
}

// Proves the two destructive paths on the tracker stay exact: a timeout removes
// only its own live request and `clear_with_commands` emits one cancel plus one
// failed resolution for every request still pending.
//
// That gives the no-lost and no-duplicated completion guarantee the host code relies on.
#[kani::proof]
fn request_tracker_timeout_and_clear_resolve_each_live_request_once() {
    let mut tracker = VerificationRequestTracker::new();
    let first = tracker.register_request(PendingRequestKind::StartRecording);
    let second = tracker.register_request(PendingRequestKind::StopRecording);

    let timeout_commands = tracker.resolve_timeout(first.timeout_timer_id);
    assert_eq!(
        timeout_commands,
        Some(vec![Command::ResolvePendingRequest {
            request_id: first.request_id.clone(),
            ok: false,
        }])
    );
    assert!(tracker.has_bijection_between_requests_and_timers());
    assert_eq!(tracker.pending_count(), 1);
    assert_eq!(tracker.timeout_count(), 1);

    let clear_commands = tracker.clear_with_commands();
    assert_eq!(
        clear_commands,
        vec![
            Command::CancelTimer {
                id: second.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id: second.request_id,
                ok: false,
            },
        ]
    );
    assert!(tracker.has_bijection_between_requests_and_timers());
    assert_eq!(tracker.pending_count(), 0);
    assert_eq!(tracker.timeout_count(), 0);
}

// Proves the batched send path preserves queue order until the flush actually
// happens and that one scheduled flush timer is enough to cover the whole batch.
#[kani::proof]
fn outbound_batcher_preserves_token_order_until_flush() {
    let mut batcher = VerificationOutboundBatcher::new();
    let first = 1_u8;
    let second = 2_u8;

    let (first_commands, first_batch) =
        batcher.enqueue_with_batch(first, VerificationFlushMode::Batched);
    let (second_commands, second_batch) =
        batcher.enqueue_with_batch(second, VerificationFlushMode::Batched);

    assert_eq!(
        first_commands,
        vec![Command::ScheduleTimer { id: 2, ms: 100 }]
    );
    assert!(first_batch.is_none());
    assert!(second_commands.is_empty());
    assert!(second_batch.is_none());
    assert!(batcher.flush_scheduled());
    assert_eq!(batcher.pending_snapshot(), vec![first, second]);

    let (flush_commands, flushed_batch) = batcher.flush_with_batch(false);
    assert!(!batcher.flush_scheduled());
    assert!(batcher.pending_snapshot().is_empty());
    assert_eq!(send_websocket_count(&flush_commands), 1);
    assert_eq!(flushed_batch, Some(vec![first, second]));

    std::mem::forget(flush_commands);
    std::mem::forget(flushed_batch);
    std::mem::forget(batcher);
}

// Proves the two tricky batcher edges: a capacity flush drains exactly the full
// pending batch once and clearing a scheduled batch cancels its timer without
// letting the cleared items leak out later. That prevent batching from losing or
// duplicating control-plane messages under queue pressure.
#[kani::proof]
fn outbound_batcher_capacity_and_clear_do_not_duplicate_tokens() {
    let mut batcher = VerificationOutboundBatcher::new();
    let mut index = 0_u8;
    while index < 15 {
        let (commands, flushed_batch) =
            batcher.enqueue_with_batch(index, VerificationFlushMode::Batched);
        if index == 0 {
            assert_eq!(commands, vec![Command::ScheduleTimer { id: 2, ms: 100 }]);
        } else {
            assert!(commands.is_empty());
        }
        assert!(flushed_batch.is_none());
        index += 1;
    }
    assert!(batcher.flush_scheduled());
    assert_eq!(batcher.pending_snapshot().len(), 15);

    let (capacity_commands, flushed_batch) =
        batcher.enqueue_with_batch(15, VerificationFlushMode::Batched);
    let Some(flushed_batch) = flushed_batch else {
        panic!("capacity flush must emit the pending batch");
    };
    assert_eq!(send_websocket_count(&capacity_commands), 1);
    assert_eq!(flushed_batch.len(), 16);
    assert_eq!(flushed_batch[0], 0);
    assert_eq!(flushed_batch[15], 15);
    assert!(!batcher.flush_scheduled());
    assert!(batcher.pending_snapshot().is_empty());

    let mut clear_batcher = VerificationOutboundBatcher::new();
    let _ = clear_batcher.enqueue_with_batch(42, VerificationFlushMode::Batched);
    let clear_commands = clear_batcher.clear_with_commands();
    assert_eq!(clear_commands, vec![Command::CancelTimer { id: 2 }]);
    assert!(!clear_batcher.flush_scheduled());
    assert!(clear_batcher.pending_snapshot().is_empty());
    let (flush_commands, flushed_batch) = clear_batcher.flush_with_batch(false);
    assert!(flush_commands.is_empty());
    assert!(flushed_batch.is_none());

    std::mem::forget(capacity_commands);
    std::mem::forget(clear_commands);
    std::mem::forget(flush_commands);
    std::mem::forget(flushed_batch);
    std::mem::forget(clear_batcher);
    std::mem::forget(batcher);
}

// Proves sticky publish intent is set-like and replay-stable: repeated writes
// for the same stream type collapse to one remembered state and replay stays
// unchanged until the remembered publish state actually changes. This matter
// because reconnect replay should converge on intent, not on the history of UI
// toggles that produced it.
#[kani::proof]
fn sticky_replay_deduplicates_publish_intent_and_stays_stable() {
    let mut replay = VerificationStickyReplay::new();
    let camera_active = kani::any::<bool>();
    let audio_active = kani::any::<bool>();

    replay.set_publish_active(StreamType::Camera, true);
    replay.set_publish_active(StreamType::Camera, camera_active);
    replay.set_publish_active(StreamType::Audio, audio_active);
    replay.set_publish_active(StreamType::Audio, audio_active);

    let first_summary = replay.replay_summary();
    let second_summary = replay.replay_summary();
    assert_eq!(first_summary, second_summary);
    assert_eq!(
        replay.active_publications_len(),
        usize::from(camera_active) + usize::from(audio_active)
    );
    assert_eq!(
        first_summary.publish_count,
        usize::from(camera_active) + usize::from(audio_active)
    );
    std::mem::forget(replay);
}

// Proves sticky subscribe/info replay is latest-field-wins instead of append
// based: per-peer subscription fields merge in place and session info patches
// collapse into one replayable snapshot. useful to check because reconnect
// correctness depends on replaying the final desired state rather than a stale
// sequence of partial updates.
#[kani::proof]
fn sticky_replay_merges_subscription_and_info_updates_by_latest_field() {
    let mut replay = VerificationStickyReplay::new();
    let first_audio = kani::any::<bool>();
    let second_audio = kani::any::<bool>();
    let camera_state = kani::any::<bool>();
    let talking_state = kani::any::<bool>();
    let hand_state = kani::any::<bool>();

    replay.remember_subscription_states(
        &SessionId::Integer(7),
        &DownloadStates {
            audio: Some(first_audio),
            camera: None,
            screen: None,
        },
    );
    replay.remember_subscription_states(
        &SessionId::Integer(7),
        &DownloadStates {
            audio: Some(second_audio),
            camera: Some(camera_state),
            screen: None,
        },
    );
    replay.remember_subscription_states(
        &SessionId::Integer(8),
        &DownloadStates {
            audio: None,
            camera: None,
            screen: Some(true),
        },
    );
    replay.remember_info(&SessionInfo {
        is_talking: Some(talking_state),
        ..SessionInfo::default()
    });
    replay.remember_info(&SessionInfo {
        is_raising_hand: Some(hand_state),
        ..SessionInfo::default()
    });

    assert_eq!(replay.desired_subscriptions_len(), 2);
    assert!(replay.has_desired_info());

    let replay_summary = replay.replay_summary();
    assert_eq!(replay_summary.subscribe_count, 2);
    assert_eq!(replay_summary.info_count, 1);
    assert_eq!(
        replay.subscription_state(&SessionId::Integer(7)),
        Some(DownloadStates {
            audio: Some(second_audio),
            camera: Some(camera_state),
            screen: None,
        })
    );
    assert_eq!(
        replay.subscription_state(&SessionId::Integer(8)),
        Some(DownloadStates {
            audio: None,
            camera: None,
            screen: Some(true),
        })
    );
    assert_eq!(
        replay.desired_info(),
        Some(SessionInfo {
            is_talking: Some(talking_state),
            is_raising_hand: Some(hand_state),
            ..SessionInfo::default()
        })
    );
    std::mem::forget(replay);
}

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

fn send_websocket_count(commands: &[Command]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, Command::SendWebSocket(_)))
        .count()
}
