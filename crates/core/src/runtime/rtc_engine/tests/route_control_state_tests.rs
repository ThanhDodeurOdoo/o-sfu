use std::time::{Duration, Instant};

use str0m::media::Rid;

use super::super::{
    relay_registry::RelayTargetId,
    route_control::{
        KeyframeRequestDecision, PacketLayerGate, PacketLayerMetadata, PacketRouteDecision,
        RouteControlState,
    },
};
use crate::runtime::media_transport::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource, TransportMediaId,
};

fn active_speaker_ids(state: &RouteControlState, now: Instant) -> Vec<TransportMediaId> {
    state
        .active_speaker_sources(now)
        .into_iter()
        .map(ActiveSpeakerSource::transport_media_id)
        .collect()
}

#[test]
fn route_control_absorbs_repeated_keyframe_requests_within_the_window() {
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(17);
    let now = Instant::now();

    assert_eq!(
        state.decide_keyframe_request(source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.decide_keyframe_request(source_transport_media_id, now),
        KeyframeRequestDecision::Absorb
    );
}

#[test]
fn route_control_reopens_after_the_coalesce_window() {
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(18);
    let now = Instant::now();

    assert_eq!(
        state.decide_keyframe_request(source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.decide_keyframe_request(source_transport_media_id, now + Duration::from_secs(1)),
        KeyframeRequestDecision::Forward
    );
}

#[test]
fn route_control_coalesces_explicit_rids_independently() {
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(118);
    let now = Instant::now();

    assert_eq!(
        state.decide_keyframe_request(source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.decide_keyframe_request_for_rid(
            source_transport_media_id,
            Some(Rid::from("hi")),
            now
        ),
        KeyframeRequestDecision::Forward
    );
    assert_eq!(
        state.decide_keyframe_request_for_rid(
            source_transport_media_id,
            Some(Rid::from("hi")),
            now
        ),
        KeyframeRequestDecision::Absorb
    );
    assert_eq!(
        state.decide_keyframe_request_for_rid(
            source_transport_media_id,
            Some(Rid::from("lo")),
            now
        ),
        KeyframeRequestDecision::Forward
    );
}

#[test]
fn route_control_drops_packets_when_the_source_is_blocked() {
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(19);
    state.set_packet_gate(source_transport_media_id, PacketLayerGate::Block);

    assert_eq!(
        state.decide_packet_route(source_transport_media_id, PacketLayerMetadata::default()),
        PacketRouteDecision::Drop
    );
}

#[test]
fn route_control_combines_local_and_remote_target_gates() {
    let mut state = RouteControlState::default();
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
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(121);
    state.set_local_packet_gate(
        source_transport_media_id,
        Some(PacketLayerGate::Rid("hi".into())),
    );
    state.set_relay_packet_gate(
        source_transport_media_id,
        RelayTargetId::new(1),
        PacketLayerGate::Rid("lo".into()),
    );

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Open)
    );

    state.forget_relay_packet_gate(source_transport_media_id, RelayTargetId::new(1));

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
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(122);
    state.set_local_packet_gate(
        source_transport_media_id,
        Some(PacketLayerGate::Rid("hi".into())),
    );
    state.set_relay_packet_gate(
        source_transport_media_id,
        RelayTargetId::new(1),
        PacketLayerGate::Rid("hi".into()),
    );

    state.set_local_packet_gate(source_transport_media_id, None);

    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.forget_relay_packet_gate(source_transport_media_id, RelayTargetId::new(1));

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
    let mut state = RouteControlState::default();
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
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(28);
    let now = Instant::now();

    assert!(state.observe_audio_activity(source_transport_media_id, Some(true), Some(-90), now));

    let snapshot = state.active_speaker_sources(now);
    assert_eq!(snapshot.len(), 1);
    assert_eq!(
        snapshot.first().map(|source| source.transport_media_id()),
        Some(source_transport_media_id)
    );
    assert_eq!(
        snapshot
            .first()
            .and_then(|source| source.last_audio_level_dbov()),
        Some(-90)
    );

    let diagnostics = state.active_speaker_diagnostics(now);
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.state()),
        Some(ActiveSpeakerActivityState::Active)
    );
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.reason()),
        Some(ActiveSpeakerActivityReason::Vad)
    );
}

#[test]
fn route_control_vad_true_refresh_extends_deadline_without_dirtying_room_policy() {
    let mut state = RouteControlState::default();
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
    assert_eq!(
        active_speaker_ids(&state, refresh_at),
        vec![source_transport_media_id]
    );
}

#[test]
fn route_control_vad_false_inside_hold_window_does_not_dirty_room_policy() {
    let mut state = RouteControlState::default();
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
    assert_eq!(
        active_speaker_ids(&state, now + Duration::from_millis(100)),
        vec![source_transport_media_id]
    );
}

#[test]
fn route_control_newer_speaker_order_change_dirties_room_policy() {
    let mut state = RouteControlState::default();
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
    assert_eq!(
        active_speaker_ids(&state, now + Duration::from_millis(10)),
        vec![
            second_source_transport_media_id,
            first_source_transport_media_id
        ]
    );

    assert!(state.observe_audio_activity(
        first_source_transport_media_id,
        Some(true),
        None,
        now + Duration::from_millis(20)
    ));
    assert_eq!(
        active_speaker_ids(&state, now + Duration::from_millis(20)),
        vec![
            first_source_transport_media_id,
            second_source_transport_media_id
        ]
    );
}

#[test]
fn route_control_same_timestamp_audio_level_rank_change_dirties_room_policy() {
    let mut state = RouteControlState::default();
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
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(29);
    let now = Instant::now();

    state.observe_audio_activity(source_transport_media_id, Some(false), Some(-12), now);

    assert!(state.active_speaker_sources(now).is_empty());
    assert_eq!(
        state.effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );
    let diagnostics = state.active_speaker_diagnostics(now);
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.state()),
        Some(ActiveSpeakerActivityState::Blocked)
    );
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.reason()),
        Some(ActiveSpeakerActivityReason::VadFalse)
    );
}

#[test]
fn route_control_transport_audio_policy_holds_recent_speech_open() {
    let mut state = RouteControlState::default();
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
    let mut state = RouteControlState::default();
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
    let mut state = RouteControlState::default();
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

    let snapshot = state.active_speaker_sources(now + Duration::from_millis(20));
    assert_eq!(snapshot.len(), 1);
    assert_eq!(
        snapshot.first().map(|source| source.transport_media_id()),
        Some(source_transport_media_id)
    );
    let diagnostics = state.active_speaker_diagnostics(now + Duration::from_millis(20));
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.state()),
        Some(ActiveSpeakerActivityState::Active)
    );
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.reason()),
        Some(ActiveSpeakerActivityReason::AudioLevel)
    );
}

#[test]
fn route_control_transport_audio_policy_rejects_persistent_low_noise() {
    let mut state = RouteControlState::default();
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
    let diagnostics = state.active_speaker_diagnostics(now + Duration::from_millis(40));
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.state()),
        Some(ActiveSpeakerActivityState::Blocked)
    );
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.reason()),
        Some(ActiveSpeakerActivityReason::LowNoise)
    );
}

#[test]
fn route_control_active_speaker_expiry_is_observable() {
    let mut state = RouteControlState::default();
    let source_transport_media_id = TransportMediaId::new(30);
    let now = Instant::now();

    state.observe_audio_activity(source_transport_media_id, Some(true), None, now);

    let expired_at = now + Duration::from_millis(300);
    assert!(state.active_speaker_sources(expired_at).is_empty());
    let diagnostics = state.active_speaker_diagnostics(expired_at);
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.state()),
        Some(ActiveSpeakerActivityState::RecentlyExpired)
    );
    assert_eq!(
        diagnostics.first().map(|diagnostic| diagnostic.reason()),
        Some(ActiveSpeakerActivityReason::Expired)
    );
}

#[test]
fn route_control_active_speaker_order_is_deterministic_for_equal_observations() {
    let mut state = RouteControlState::default();
    let first_source_transport_media_id = TransportMediaId::new(31);
    let second_source_transport_media_id = TransportMediaId::new(32);
    let now = Instant::now();

    state.observe_audio_activity(second_source_transport_media_id, Some(true), None, now);
    state.observe_audio_activity(first_source_transport_media_id, Some(true), None, now);

    assert_eq!(
        active_speaker_ids(&state, now),
        vec![
            first_source_transport_media_id,
            second_source_transport_media_id
        ]
    );
}

#[test]
fn route_control_local_packet_gate_composes_with_transport_audio_policy() {
    let mut state = RouteControlState::default();
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
