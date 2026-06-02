use std::time::{Duration, Instant};

use str0m::media::{KeyframeRequestKind, Rid};

use super::super::{
    keyframe_tracker::{KeyframeRequestDecision, KeyframeRequestTracker},
    relay_registry::{RelayPacketMailbox, RelayTargetId},
    route_control::{PacketLayerGate, PacketLayerMetadata, PacketRouteDecision},
    route_table::RouteTable,
};
use crate::engine::media_transport::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource, TransportMediaId,
};

fn assert_active_speaker_ids(state: &RouteTable, now: Instant, expected: &[TransportMediaId]) {
    let ids = state
        .active_speaker_sources(now)
        .into_iter()
        .map(ActiveSpeakerSource::transport_media_id)
        .collect::<Vec<_>>();
    assert_eq!(ids.as_slice(), expected);
}

fn assert_single_active_speaker(
    state: &RouteTable,
    now: Instant,
    source_transport_media_id: TransportMediaId,
    last_audio_level_dbov: Option<i8>,
) {
    let snapshot = state.active_speaker_sources(now);
    assert_eq!(snapshot.len(), 1);
    let source = &snapshot[0];
    assert_eq!(source.transport_media_id(), source_transport_media_id);
    assert_eq!(source.last_audio_level_dbov(), last_audio_level_dbov);
}

fn assert_single_active_speaker_diagnostic(
    state: &RouteTable,
    now: Instant,
    activity_state: ActiveSpeakerActivityState,
    reason: ActiveSpeakerActivityReason,
) {
    let diagnostics = state.active_speaker_diagnostics(now);
    assert_eq!(diagnostics.len(), 1);
    let diagnostic = &diagnostics[0];
    assert_eq!(diagnostic.state(), activity_state);
    assert_eq!(diagnostic.reason(), reason);
}

fn track_source_wide(
    state: &mut KeyframeRequestTracker,
    source_transport_media_id: TransportMediaId,
    now: Instant,
) -> KeyframeRequestDecision {
    state.track(
        source_transport_media_id,
        None,
        KeyframeRequestKind::Pli,
        now,
    )
}

#[test]
fn keyframe_tracker_absorbs_repeated_requests_while_pending() {
    let mut state = KeyframeRequestTracker::default();
    let source_transport_media_id = TransportMediaId::new(17);
    let now = Instant::now();

    assert_eq!(
        track_source_wide(&mut state, source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(state.next_deadline(), Some(now + Duration::from_secs(1)));
    assert_eq!(
        track_source_wide(&mut state, source_transport_media_id, now),
        KeyframeRequestDecision::Absorb
    );
}

#[test]
fn keyframe_tracker_expires_pending_request_without_duplicate_feedback() {
    let mut state = KeyframeRequestTracker::default();
    let source_transport_media_id = TransportMediaId::new(18);
    let now = Instant::now();
    let mut retries = Vec::new();

    assert_eq!(
        track_source_wide(&mut state, source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );
    state.drain_due(now + Duration::from_secs(1), &mut retries);

    assert!(retries.is_empty());
    assert_eq!(state.next_deadline(), None);
    assert_eq!(
        track_source_wide(
            &mut state,
            source_transport_media_id,
            now + Duration::from_secs(1)
        ),
        KeyframeRequestDecision::Forward
    );
}

#[test]
fn keyframe_tracker_retries_after_duplicate_feedback() {
    let mut state = KeyframeRequestTracker::default();
    let source_transport_media_id = TransportMediaId::new(181);
    let now = Instant::now();
    let mut retries = Vec::new();

    assert_eq!(
        track_source_wide(&mut state, source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            source_transport_media_id,
            None,
            KeyframeRequestKind::Fir,
            now + Duration::from_millis(100)
        ),
        KeyframeRequestDecision::Absorb
    );

    state.drain_due(now + Duration::from_secs(1), &mut retries);

    assert_eq!(retries.len(), 1);
    let retry = retries[0];
    assert_eq!(retry.source_transport_media_id, source_transport_media_id);
    assert_eq!(retry.rid, None);
    assert_eq!(retry.kind, KeyframeRequestKind::Fir);
}

#[test]
fn keyframe_tracker_tracks_explicit_rids_independently() {
    let mut state = KeyframeRequestTracker::default();
    let source_transport_media_id = TransportMediaId::new(118);
    let now = Instant::now();

    assert_eq!(
        track_source_wide(&mut state, source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            source_transport_media_id,
            Some(Rid::from("hi")),
            KeyframeRequestKind::Pli,
            now
        ),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            source_transport_media_id,
            Some(Rid::from("hi")),
            KeyframeRequestKind::Pli,
            now
        ),
        KeyframeRequestDecision::Absorb
    );
    assert_eq!(
        state.track(
            source_transport_media_id,
            Some(Rid::from("lo")),
            KeyframeRequestKind::Pli,
            now
        ),
        KeyframeRequestDecision::Forward
    );
}

#[test]
fn keyframe_tracker_decoder_refresh_clears_matching_pending_request() {
    let mut state = KeyframeRequestTracker::default();
    let source_transport_media_id = TransportMediaId::new(182);
    let now = Instant::now();
    let hi = Rid::from("hi");
    let lo = Rid::from("lo");
    let mut retries = Vec::new();

    assert_eq!(
        state.track(
            source_transport_media_id,
            Some(hi),
            KeyframeRequestKind::Pli,
            now
        ),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            source_transport_media_id,
            Some(lo),
            KeyframeRequestKind::Pli,
            now
        ),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.track(
            source_transport_media_id,
            Some(hi),
            KeyframeRequestKind::Pli,
            now + Duration::from_millis(100)
        ),
        KeyframeRequestDecision::Absorb
    );

    assert_eq!(
        state.observe_refresh(source_transport_media_id, Some(lo)),
        1
    );
    state.drain_due(now + Duration::from_secs(1), &mut retries);

    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].rid, Some(hi));
}

#[test]
fn keyframe_tracker_decoder_refresh_clears_source_wide_pending_request() {
    let mut state = KeyframeRequestTracker::default();
    let source_transport_media_id = TransportMediaId::new(183);
    let now = Instant::now();
    let mut retries = Vec::new();

    assert_eq!(
        track_source_wide(&mut state, source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        track_source_wide(
            &mut state,
            source_transport_media_id,
            now + Duration::from_millis(100)
        ),
        KeyframeRequestDecision::Absorb
    );
    assert_eq!(
        state.observe_refresh(source_transport_media_id, Some(Rid::from("hi"))),
        1
    );

    state.drain_due(now + Duration::from_secs(1), &mut retries);

    assert!(retries.is_empty());
}

#[test]
fn route_control_drops_packets_when_the_is_source_blocked() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(19);
    state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Block));

    assert_eq!(
        state.decide_packet_route(source_transport_media_id, PacketLayerMetadata::default()),
        PacketRouteDecision::Drop
    );
}

#[test]
fn route_control_combines_local_and_remote_target_gates() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(21);

    state.set_local_packet_gate(
        source_transport_media_id,
        Some(PacketLayerGate::Rid("hi".into())),
    );
    state.set_relay_packet_gate(
        source_transport_media_id,
        RelayTargetId::new(1),
        PacketLayerGate::Rid("hi".into()),
    );

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.set_relay_packet_gate(
        source_transport_media_id,
        RelayTargetId::new(2),
        PacketLayerGate::Rid("lo".into()),
    );

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Open)
    );
}

#[test]
fn route_control_refreshes_source_gate_after_relay_gate_removal() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(121);
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let relay_target = RelayTargetId::new(1);
    state.set_local_packet_gate(
        source_transport_media_id,
        Some(PacketLayerGate::Rid("hi".into())),
    );
    state.add_relay_target(source_transport_media_id, relay_target, relay_mailbox);
    state.set_relay_packet_gate(
        source_transport_media_id,
        relay_target,
        PacketLayerGate::Rid("lo".into()),
    );

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Open)
    );

    state.remove_relay_target(source_transport_media_id, relay_target);

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Rid("hi".into()))
    );
    assert_eq!(
        state.decide_packet_route(
            source_transport_media_id,
            PacketLayerMetadata::new(Some("lo".into()), None)
        ),
        PacketRouteDecision::Drop
    );
}

#[test]
fn route_control_refreshes_source_gate_after_local_gate_clear() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(122);
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let relay_target = RelayTargetId::new(1);
    state.set_local_packet_gate(
        source_transport_media_id,
        Some(PacketLayerGate::Rid("hi".into())),
    );
    state.add_relay_target(source_transport_media_id, relay_target, relay_mailbox);
    state.set_relay_packet_gate(
        source_transport_media_id,
        relay_target,
        PacketLayerGate::Rid("hi".into()),
    );

    state.set_local_packet_gate(source_transport_media_id, None);

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.remove_relay_target(source_transport_media_id, relay_target);

    assert_eq!(state.effective_packet_gate(source_transport_media_id), None);
    assert_eq!(
        state.decide_packet_route(
            source_transport_media_id,
            PacketLayerMetadata::new(Some("lo".into()), None)
        ),
        PacketRouteDecision::Forward
    );
}

#[test]
fn route_control_transport_audio_policy_blocks_silent_sources() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(22);
    let now = Instant::now();

    state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
    state.observe_audio_activity(source_transport_media_id, Some(false), None, now);

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );
}

#[test]
fn route_control_vad_true_promotes_active_speaker_immediately() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(28);
    let now = Instant::now();

    assert!(state.observe_audio_activity(source_transport_media_id, Some(true), Some(-90), now));

    assert_single_active_speaker(&state, now, source_transport_media_id, Some(-90));
    assert_single_active_speaker_diagnostic(
        &state,
        now,
        ActiveSpeakerActivityState::Active,
        ActiveSpeakerActivityReason::Vad,
    );
}

#[test]
fn route_control_vad_true_refresh_extends_deadline_without_dirtying_room_policy() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(33);
    let now = Instant::now();

    assert!(state.observe_audio_activity(source_transport_media_id, Some(true), None, now));
    let first_deadline = state.next_active_speaker_deadline(now).unwrap();
    let refresh_at = now + Duration::from_millis(20);

    assert!(!state.observe_audio_activity(source_transport_media_id, Some(true), None, refresh_at));
    assert_eq!(
        state.next_active_speaker_deadline(refresh_at),
        Some(first_deadline + Duration::from_millis(20))
    );
    assert_active_speaker_ids(&state, refresh_at, &[source_transport_media_id]);
}

#[test]
fn route_control_vad_false_inside_hold_window_does_not_dirty_room_policy() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(34);
    let now = Instant::now();

    assert!(state.observe_audio_activity(source_transport_media_id, Some(true), None, now));

    assert!(!state.observe_audio_activity(
        source_transport_media_id,
        Some(false),
        None,
        now + Duration::from_millis(100)
    ));
    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Open)
    );
    assert_active_speaker_ids(
        &state,
        now + Duration::from_millis(100),
        &[source_transport_media_id],
    );
}

#[test]
fn route_control_newer_speaker_order_change_dirties_room_policy() {
    let mut state = RouteTable::default();
    let first_source_transport_media_id = TransportMediaId::new(35);
    let second_source_transport_media_id = TransportMediaId::new(36);
    let now = Instant::now();

    assert!(state.observe_audio_activity(first_source_transport_media_id, Some(true), None, now));
    assert!(state.observe_audio_activity(
        second_source_transport_media_id,
        Some(true),
        None,
        now + Duration::from_millis(10)
    ));
    assert_active_speaker_ids(
        &state,
        now + Duration::from_millis(10),
        &[
            second_source_transport_media_id,
            first_source_transport_media_id,
        ],
    );

    assert!(state.observe_audio_activity(
        first_source_transport_media_id,
        Some(true),
        None,
        now + Duration::from_millis(20)
    ));
    assert_active_speaker_ids(
        &state,
        now + Duration::from_millis(20),
        &[
            first_source_transport_media_id,
            second_source_transport_media_id,
        ],
    );
}

#[test]
fn route_control_same_timestamp_audio_level_rank_change_dirties_room_policy() {
    let mut state = RouteTable::default();
    let first_source_transport_media_id = TransportMediaId::new(37);
    let second_source_transport_media_id = TransportMediaId::new(38);
    let now = Instant::now();

    state.observe_audio_activity(first_source_transport_media_id, Some(true), Some(-30), now);
    state.observe_audio_activity(second_source_transport_media_id, Some(true), Some(-10), now);
    assert!(state.observe_audio_activity(
        first_source_transport_media_id,
        Some(true),
        Some(-1),
        now
    ));
    assert!(!state.observe_audio_activity(
        first_source_transport_media_id,
        Some(true),
        Some(-1),
        now
    ));
}

#[test]
fn route_control_vad_false_overrides_loud_audio_level() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(29);
    let now = Instant::now();

    state.observe_audio_activity(source_transport_media_id, Some(false), Some(-12), now);

    assert!(state.active_speaker_sources(now).is_empty());
    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );
    assert_single_active_speaker_diagnostic(
        &state,
        now,
        ActiveSpeakerActivityState::Blocked,
        ActiveSpeakerActivityReason::VadFalse,
    );
}

#[test]
fn route_control_transport_audio_policy_holds_recent_speech_open() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(23);
    let now = Instant::now();

    state.set_local_packet_gate(
        source_transport_media_id,
        Some(PacketLayerGate::Rid("hi".into())),
    );
    state.observe_audio_activity(source_transport_media_id, Some(true), None, now);
    state.observe_audio_activity(
        source_transport_media_id,
        Some(false),
        None,
        now + Duration::from_millis(100),
    );

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Rid("hi".into()))
    );
}

#[test]
fn route_control_transport_audio_policy_reblocks_after_the_hold_window() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(24);
    let now = Instant::now();

    state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
    state.observe_audio_activity(source_transport_media_id, Some(true), None, now);
    state.observe_audio_activity(
        source_transport_media_id,
        Some(false),
        None,
        now + Duration::from_millis(300),
    );

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );
}

#[test]
fn route_control_transport_audio_policy_uses_repeated_audio_level_fallback() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(25);
    let now = Instant::now();

    state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
    state.observe_audio_activity(source_transport_media_id, None, Some(-24), now);

    assert!(state.active_speaker_sources(now).is_empty());
    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Open)
    );

    state.observe_audio_activity(
        source_transport_media_id,
        None,
        Some(-24),
        now + Duration::from_millis(20),
    );

    let observed_at = now + Duration::from_millis(20);
    assert_single_active_speaker(&state, observed_at, source_transport_media_id, Some(-24));
    assert_single_active_speaker_diagnostic(
        &state,
        observed_at,
        ActiveSpeakerActivityState::Active,
        ActiveSpeakerActivityReason::AudioLevel,
    );
}

#[test]
fn route_control_transport_audio_policy_rejects_persistent_low_noise() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(26);
    let now = Instant::now();

    for offset in [0, 20, 40] {
        state.observe_audio_activity(
            source_transport_media_id,
            None,
            Some(-80),
            now + Duration::from_millis(offset),
        );
    }

    assert!(
        state
            .active_speaker_sources(now + Duration::from_millis(40))
            .is_empty()
    );
    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );
    assert_single_active_speaker_diagnostic(
        &state,
        now + Duration::from_millis(40),
        ActiveSpeakerActivityState::Blocked,
        ActiveSpeakerActivityReason::LowNoise,
    );
}

#[test]
fn route_control_active_speaker_expiry_is_observable() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(30);
    let now = Instant::now();

    state.observe_audio_activity(source_transport_media_id, Some(true), None, now);

    let expired_at = now + Duration::from_millis(300);
    assert!(state.active_speaker_sources(expired_at).is_empty());
    assert_single_active_speaker_diagnostic(
        &state,
        expired_at,
        ActiveSpeakerActivityState::RecentlyExpired,
        ActiveSpeakerActivityReason::Expired,
    );
}

#[test]
fn route_control_active_speaker_order_is_deterministic_for_equal_observations() {
    let mut state = RouteTable::default();
    let first_source_transport_media_id = TransportMediaId::new(31);
    let second_source_transport_media_id = TransportMediaId::new(32);
    let now = Instant::now();

    state.observe_audio_activity(second_source_transport_media_id, Some(true), None, now);
    state.observe_audio_activity(first_source_transport_media_id, Some(true), None, now);

    assert_active_speaker_ids(
        &state,
        now,
        &[
            first_source_transport_media_id,
            second_source_transport_media_id,
        ],
    );
}

#[test]
fn route_control_local_packet_gate_composes_with_transport_audio_policy() {
    let mut state = RouteTable::default();
    let source_transport_media_id = TransportMediaId::new(27);
    let now = Instant::now();

    state.set_local_packet_gate(
        source_transport_media_id,
        Some(PacketLayerGate::Rid("hi".into())),
    );
    state.observe_audio_activity(source_transport_media_id, Some(true), None, now);

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.observe_audio_activity(
        source_transport_media_id,
        Some(false),
        None,
        now + Duration::from_millis(300),
    );

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );
}
