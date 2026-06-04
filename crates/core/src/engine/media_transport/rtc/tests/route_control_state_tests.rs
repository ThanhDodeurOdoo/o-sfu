use std::time::{Duration, Instant};

use str0m::media::{
    KeyframeRequestKind::{Fir, Pli},
    Rid,
};
use tokio::sync::mpsc;

use super::super::{
    commands::RemoteSourceControl,
    demux::MediaRouteDestination,
    keyframe_tracker::KEYFRAME_RETRY_DRAIN_LIMIT,
    relay_registry::{RelayPacketMailbox, RelayTargetId},
    route_control::{PacketLayerGate, PacketLayerMetadata, PacketRouteDecision},
    route_table::RouteTable,
    slots::ConsumerStreamHandle,
    test_support::test_transport_session_key,
};
use crate::engine::{
    UserId,
    media_transport::{
        ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
        TransportMediaId, TransportSourceKey,
    },
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
    src_media: TransportMediaId,
    last_audio_level_dbov: Option<i8>,
) {
    let snapshot = state.active_speaker_sources(now);
    assert_eq!(snapshot.len(), 1);
    let source = &snapshot[0];
    assert_eq!(source.transport_media_id(), src_media);
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

#[test]
fn route_control_source_teardown_clears_source_owned_state() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(184);
    let dst_media = TransportMediaId::new(185);
    let now = Instant::now();
    let hi = Rid::from("hi");
    let target_id = RelayTargetId::new(3);
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let (control_tx, _control_rx) = mpsc::channel(1);
    let source = TransportSourceKey::new(
        test_transport_session_key(18, 0, 19, UserId::Integer(20)),
        src_media,
    );

    state.add_consumer_route(
        src_media,
        MediaRouteDestination {
            dest_session: test_transport_session_key(18, 0, 21, UserId::Integer(22)),
            dest_transport_media_id: dst_media,
            dest_stream: ConsumerStreamHandle::default(),
            dest_mid: "cam-down".into(),
            dest_payload_type: None,
            nackable: true,
            active: true,
            packet_gate: PacketLayerGate::Open,
            pending_gate: None,
        },
    );
    state
        .register_remote_source(&source, RemoteSourceControl::new(control_tx, target_id))
        .unwrap();
    state.add_relay_target(src_media, target_id, relay_mailbox);
    state.set_relay_pkt_gate(src_media, target_id, PacketLayerGate::Rid(hi));
    state.observe_audio_activity(src_media, Some(true), Some(-20), now);
    state.observe_producer_packet(src_media, Some(hi), true, now);
    state.schedule_rid_refresh(src_media, hi, now + Duration::from_millis(10));
    state.track_kf_req(src_media, Some(hi), Pli, now);
    state.track_kf_req(src_media, Some(hi), Fir, now + Duration::from_millis(100));

    assert!(state.take_route(src_media).is_some());

    assert!(state.local_route(src_media).is_none());
    assert!(state.remote_source(src_media).is_none());
    assert!(state.relay_packet_gate(src_media, target_id).is_none());
    assert!(state.active_speaker_sources(now).is_empty());
    assert!(!state.producer_rid_is_ready(src_media, hi, now, Duration::from_secs(1)));
    assert_eq!(state.next_rid_refresh_deadline(), None);
    assert_eq!(state.next_kf_deadline(), None);

    let mut retries = Vec::new();
    state.drain_due_kf_reqs(now + Duration::from_secs(1), &mut retries);
    assert!(retries.is_empty());

    state.remove_relay_target(src_media, target_id);
    assert!(!state.has_sources());
}

#[test]
fn route_control_stale_keyframe_deadlines_are_pruned_incrementally() {
    let mut state = RouteTable::default();
    let now = Instant::now();
    let retry_at = now + Duration::from_secs(1);
    let stale_deadlines = KEYFRAME_RETRY_DRAIN_LIMIT * 2;

    for source_index in (1_000_u64..).take(stale_deadlines) {
        let source_id = TransportMediaId::new(source_index);
        state.track_kf_req(source_id, None, Pli, now);
        assert_eq!(state.observe_decoder_refresh(source_id, None), 1);
    }

    let live_source_id = TransportMediaId::new(2_000);
    state.track_kf_req(live_source_id, None, Pli, now);
    state.track_kf_req(live_source_id, None, Pli, now + Duration::from_millis(1));

    assert_eq!(state.next_kf_deadline(), Some(retry_at));

    let mut retries = Vec::new();
    state.drain_due_kf_reqs(retry_at, &mut retries);
    assert!(retries.is_empty());

    state.drain_due_kf_reqs(retry_at, &mut retries);
    assert_eq!(retries.len(), 1);
}

#[test]
fn route_control_drops_packets_when_the_is_source_blocked() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(19);
    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Block));

    assert_eq!(
        state.decide_packet_route(src_media, PacketLayerMetadata::default()),
        PacketRouteDecision::Drop
    );
}

#[test]
fn route_control_combines_local_and_remote_target_gates() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(21);

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.set_relay_pkt_gate(
        src_media,
        RelayTargetId::new(1),
        PacketLayerGate::Rid("hi".into()),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.set_relay_pkt_gate(
        src_media,
        RelayTargetId::new(2),
        PacketLayerGate::Rid("lo".into()),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );
}

#[test]
fn route_control_refreshes_source_gate_after_relay_gate_removal() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(121);
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let relay_target = RelayTargetId::new(1);
    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.add_relay_target(src_media, relay_target, relay_mailbox);
    state.set_relay_pkt_gate(src_media, relay_target, PacketLayerGate::Rid("lo".into()));

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );

    state.remove_relay_target(src_media, relay_target);

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );
    assert_eq!(
        state.decide_packet_route(src_media, PacketLayerMetadata::new(Some("lo".into()), None)),
        PacketRouteDecision::Drop
    );
}

#[test]
fn route_control_refreshes_source_gate_after_local_gate_clear() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(122);
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let relay_target = RelayTargetId::new(1);
    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.add_relay_target(src_media, relay_target, relay_mailbox);
    state.set_relay_pkt_gate(src_media, relay_target, PacketLayerGate::Rid("hi".into()));

    state.set_local_pkt_gate(src_media, None);

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.remove_relay_target(src_media, relay_target);

    assert_eq!(state.effective_packet_gate(src_media), None);
    assert_eq!(
        state.decide_packet_route(src_media, PacketLayerMetadata::new(Some("lo".into()), None)),
        PacketRouteDecision::Forward
    );
}

#[test]
fn route_control_transport_audio_policy_blocks_silent_sources() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(22);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Open));
    state.observe_audio_activity(src_media, Some(false), None, now);

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Block)
    );
}

#[test]
fn route_control_vad_true_promotes_active_speaker_immediately() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(28);
    let now = Instant::now();

    assert!(state.observe_audio_activity(src_media, Some(true), Some(-90), now));

    assert_single_active_speaker(&state, now, src_media, Some(-90));
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
    let src_media = TransportMediaId::new(33);
    let now = Instant::now();

    assert!(state.observe_audio_activity(src_media, Some(true), None, now));
    let first_deadline = state.next_active_speaker_deadline(now).unwrap();
    let refresh_at = now + Duration::from_millis(20);

    assert!(!state.observe_audio_activity(src_media, Some(true), None, refresh_at));
    assert_eq!(
        state.next_active_speaker_deadline(refresh_at),
        Some(first_deadline + Duration::from_millis(20))
    );
    assert_active_speaker_ids(&state, refresh_at, &[src_media]);
}

#[test]
fn route_control_vad_false_inside_hold_window_does_not_dirty_room_policy() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(34);
    let now = Instant::now();

    assert!(state.observe_audio_activity(src_media, Some(true), None, now));

    assert!(!state.observe_audio_activity(
        src_media,
        Some(false),
        None,
        now + Duration::from_millis(100)
    ));
    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );
    assert_active_speaker_ids(&state, now + Duration::from_millis(100), &[src_media]);
}

#[test]
fn route_control_newer_speaker_order_change_dirties_room_policy() {
    let mut state = RouteTable::default();
    let first_src_media = TransportMediaId::new(35);
    let second_src_media = TransportMediaId::new(36);
    let now = Instant::now();

    assert!(state.observe_audio_activity(first_src_media, Some(true), None, now));
    assert!(state.observe_audio_activity(
        second_src_media,
        Some(true),
        None,
        now + Duration::from_millis(10)
    ));
    assert_active_speaker_ids(
        &state,
        now + Duration::from_millis(10),
        &[second_src_media, first_src_media],
    );

    assert!(state.observe_audio_activity(
        first_src_media,
        Some(true),
        None,
        now + Duration::from_millis(20)
    ));
    assert_active_speaker_ids(
        &state,
        now + Duration::from_millis(20),
        &[first_src_media, second_src_media],
    );
}

#[test]
fn route_control_same_timestamp_audio_level_rank_change_dirties_room_policy() {
    let mut state = RouteTable::default();
    let first_src_media = TransportMediaId::new(37);
    let second_src_media = TransportMediaId::new(38);
    let now = Instant::now();

    state.observe_audio_activity(first_src_media, Some(true), Some(-30), now);
    state.observe_audio_activity(second_src_media, Some(true), Some(-10), now);
    assert!(state.observe_audio_activity(first_src_media, Some(true), Some(-1), now));
    assert!(!state.observe_audio_activity(first_src_media, Some(true), Some(-1), now));
}

#[test]
fn route_control_vad_false_overrides_loud_audio_level() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(29);
    let now = Instant::now();

    state.observe_audio_activity(src_media, Some(false), Some(-12), now);

    assert!(state.active_speaker_sources(now).is_empty());
    assert_eq!(
        state.effective_packet_gate(src_media),
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
    let src_media = TransportMediaId::new(23);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.observe_audio_activity(src_media, Some(true), None, now);
    state.observe_audio_activity(
        src_media,
        Some(false),
        None,
        now + Duration::from_millis(100),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );
}

#[test]
fn route_control_transport_audio_policy_reblocks_after_the_hold_window() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(24);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Open));
    state.observe_audio_activity(src_media, Some(true), None, now);
    state.observe_audio_activity(
        src_media,
        Some(false),
        None,
        now + Duration::from_millis(300),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Block)
    );
}

#[test]
fn route_control_transport_audio_policy_uses_repeated_audio_level_fallback() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(25);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Open));
    state.observe_audio_activity(src_media, None, Some(-24), now);

    assert!(state.active_speaker_sources(now).is_empty());
    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );

    state.observe_audio_activity(src_media, None, Some(-24), now + Duration::from_millis(20));

    let observed_at = now + Duration::from_millis(20);
    assert_single_active_speaker(&state, observed_at, src_media, Some(-24));
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
    let src_media = TransportMediaId::new(26);
    let now = Instant::now();

    for offset in [0, 20, 40] {
        state.observe_audio_activity(
            src_media,
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
        state.effective_packet_gate(src_media),
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
    let src_media = TransportMediaId::new(30);
    let now = Instant::now();

    state.observe_audio_activity(src_media, Some(true), None, now);

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
    let first_src_media = TransportMediaId::new(31);
    let second_src_media = TransportMediaId::new(32);
    let now = Instant::now();

    state.observe_audio_activity(second_src_media, Some(true), None, now);
    state.observe_audio_activity(first_src_media, Some(true), None, now);

    assert_active_speaker_ids(&state, now, &[first_src_media, second_src_media]);
}

#[test]
fn route_control_local_packet_gate_composes_with_transport_audio_policy() {
    let mut state = RouteTable::default();
    let src_media = TransportMediaId::new(27);
    let now = Instant::now();

    state.set_local_pkt_gate(src_media, Some(PacketLayerGate::Rid("hi".into())));
    state.observe_audio_activity(src_media, Some(true), None, now);

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    state.observe_audio_activity(
        src_media,
        Some(false),
        None,
        now + Duration::from_millis(300),
    );

    assert_eq!(
        state.effective_packet_gate(src_media),
        Some(PacketLayerGate::Block)
    );
}
