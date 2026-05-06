use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use o_sfu_router::{MediaStream as RouterRtpParameters, StreamBinding};
use str0m::{
    media::{KeyframeRequestKind, MediaKind, Mid, Pt, Rid},
    rtp::Ssrc,
};
use tokio::sync::{mpsc, oneshot};

use super::{
    AddSendMediaRequest, ConsumerPacketGateRequest, RemoteKeyframeRequest,
    control::consumer_packet_gate, drain_due_rid_keyframe_refreshes, observe_source_rid_readiness,
    refresh_source_packet_gate, request_keyframe_for_source, respond_add_send_media,
    respond_remove_media, respond_request_consumer_keyframe, respond_request_remote_keyframe,
    respond_set_consumer_packet_gate, respond_set_consumer_packet_gates,
    respond_set_remote_source_packet_gate,
};
use crate::{
    MediaCodecFlags,
    runtime::{
        UserId,
        media_transport::{TransportAdapterError, TransportMediaId, TransportSessionKey},
        metrics::RuntimeMetrics,
        rtc_engine::{
            bitrate::RtcBitrateState,
            bootstrap,
            commands::{ConsumerPacketGateCommand, RemoteSourceControl, RtcWorkerCommand},
            demux::{MediaRouteDestination, MediaRouteEntry},
            media_registry::RegisteredMediaHandle,
            relay_registry::{RelayPacketMailbox, RelayTargetId},
            route_control::{KeyframeRequestDecision, PacketLayerGate},
            state::RtcBootstrapState,
            test_support::test_transport_session_key,
        },
    },
};

fn prepare_source_session(
    state: &mut RtcBootstrapState,
    source_session: &TransportSessionKey,
    source_mid: Mid,
    ssrc: u32,
) -> TransportMediaId {
    prepare_source_session_with_rid(state, source_session, source_mid, ssrc, None)
}

fn prepare_source_session_with_rid(
    state: &mut RtcBootstrapState,
    source_session: &TransportSessionKey,
    source_mid: Mid,
    ssrc: u32,
    rid: Option<Rid>,
) -> TransportMediaId {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 47_000));
    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            source_session,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.users.get_mut(source_session) else {
        return TransportMediaId::default();
    };
    let mut direct_api = source_session_state.rtc.direct_api();
    direct_api.declare_media(source_mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(ssrc), None, source_mid, rid);
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: source_session.clone(),
        mid: source_mid,
    })
}

fn add_source_rid_stream(
    state: &mut RtcBootstrapState,
    source_session: &TransportSessionKey,
    source_mid: Mid,
    ssrc: u32,
    rid: Rid,
) {
    assert!(state.users.contains_key(source_session));
    if let Some(source_session_state) = state.users.get_mut(source_session) {
        source_session_state.rtc.direct_api().expect_stream_rx(
            Ssrc::from(ssrc),
            None,
            source_mid,
            Some(rid),
        );
    }
}

fn assert_consumer_packet_gate(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    consumer_session: &TransportSessionKey,
    packet_gate: &PacketLayerGate,
    pending_packet_gate: Option<&PacketLayerGate>,
) {
    assert!(
        state
            .media_route_index
            .get(&source_transport_media_id)
            .is_some_and(
                |route_entry| route_entry.destinations.iter().any(|destination| {
                    destination.dest_session == *consumer_session
                        && &destination.packet_gate == packet_gate
                        && destination.pending_packet_gate.as_ref() == pending_packet_gate
                })
            )
    );
}

#[test]
fn consumer_packet_gate_selects_lowest_bitrate_simulcast_rid() {
    let parameters = RouterRtpParameters::new(
        vec![],
        vec![],
        vec![
            StreamBinding::new()
                .with_rid("hi")
                .with_max_bitrate(4_000_000),
            StreamBinding::new()
                .with_rid("lo")
                .with_max_bitrate(150_000),
        ],
    );

    assert_eq!(
        consumer_packet_gate(&parameters),
        PacketLayerGate::Rid("lo".into())
    );
}

#[test]
fn consumer_packet_gate_uses_declared_order_for_rid_only_simulcast() {
    let parameters = RouterRtpParameters::new(
        vec![],
        vec![],
        vec![
            StreamBinding::new().with_rid("lo"),
            StreamBinding::new().with_rid("hi"),
        ],
    );

    assert_eq!(
        consumer_packet_gate(&parameters),
        PacketLayerGate::Rid("lo".into())
    );
}

#[test]
fn consumer_packet_gate_keeps_ridless_or_mixed_routes_open() {
    let ridless = RouterRtpParameters::new(vec![], vec![], vec![StreamBinding::new()]);
    let mixed = RouterRtpParameters::new(
        vec![],
        vec![],
        vec![
            StreamBinding::new().with_rid("lo"),
            StreamBinding::new().with_ssrc(72_002),
        ],
    );

    assert_eq!(consumer_packet_gate(&ridless), PacketLayerGate::Open);
    assert_eq!(consumer_packet_gate(&mixed), PacketLayerGate::Open);
}

struct PendingSelectedRidRoute {
    state: RtcBootstrapState,
    metrics: RuntimeMetrics,
    source_session: TransportSessionKey,
    consumer_session: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    selected_rid: Rid,
    fallback_rid: Rid,
}

fn prepare_pending_selected_rid_route() -> PendingSelectedRidRoute {
    let source_session = test_transport_session_key(231, 0, 232, UserId::Integer(233));
    let consumer_session = test_transport_session_key(231, 0, 234, UserId::Integer(235));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("hi");
    let fallback_rid = Rid::from("lo");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_301,
        Some(selected_rid),
    );
    add_source_rid_stream(
        &mut state,
        &source_session,
        source_mid,
        88_302,
        fallback_rid,
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
            }],
        },
    );
    let command_now = Instant::now();
    let (response_tx, response_rx) = oneshot::channel();
    respond_set_consumer_packet_gate(
        &mut state,
        ConsumerPacketGateRequest {
            consumer_session_key: &consumer_session,
            consumer_transport_media_id,
            source_session_key: &source_session,
            source_transport_media_id,
            packet_gate: PacketLayerGate::Rid(selected_rid),
        },
        command_now,
        response_tx,
    );
    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    PendingSelectedRidRoute {
        state,
        metrics,
        source_session,
        consumer_session,
        source_transport_media_id,
        selected_rid,
        fallback_rid,
    }
}

#[test]
fn remote_keyframe_requests_drop_when_the_relay_target_is_inactive() {
    let source_session = test_transport_session_key(101, 0, 102, UserId::Integer(103));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let (_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 66_666);

    respond_request_remote_keyframe(
        &mut state,
        &metrics,
        &RemoteKeyframeRequest {
            source_session_key: &source_session,
            source_transport_media_id,
            target_id: RelayTargetId::new(7),
            rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    );

    assert!(!state.dirty_sessions.contains(&source_session));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded, 0);
    assert_eq!(snapshot.rtc_route_control_absorbed, 0);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops, 1);
}

#[test]
fn remote_keyframe_requests_forward_once_and_then_absorb_within_the_window() {
    let source_session = test_transport_session_key(111, 0, 112, UserId::Integer(113));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let (mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 77_777);
    let relay_target_id = RelayTargetId::new(8);

    state.add_relay_target(source_transport_media_id, relay_target_id, mailbox.into());
    state.set_relay_target_active(source_transport_media_id, relay_target_id, true);

    respond_request_remote_keyframe(
        &mut state,
        &metrics,
        &RemoteKeyframeRequest {
            source_session_key: &source_session,
            source_transport_media_id,
            target_id: relay_target_id,
            rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    );
    respond_request_remote_keyframe(
        &mut state,
        &metrics,
        &RemoteKeyframeRequest {
            source_session_key: &source_session,
            source_transport_media_id,
            target_id: relay_target_id,
            rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    );

    assert!(state.dirty_sessions.contains(&source_session));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded, 1);
    assert_eq!(snapshot.rtc_route_control_absorbed, 1);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops, 0);
}

#[test]
fn consumer_keyframe_request_marks_local_video_source_dirty() {
    let source_session = test_transport_session_key(115, 0, 116, UserId::Integer(117));
    let consumer_session = test_transport_session_key(115, 0, 118, UserId::Integer(119));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 88_001);
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
            }],
        },
    );

    let (response_tx, response_rx) = oneshot::channel();
    respond_request_consumer_keyframe(
        &mut state,
        &metrics,
        &consumer_session,
        consumer_transport_media_id,
        &source_session,
        source_transport_media_id,
        response_tx,
    );

    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    assert!(state.dirty_sessions.contains(&source_session));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);
}

#[test]
fn consumer_keyframe_request_uses_rid_scoped_local_video_source() {
    let source_session = test_transport_session_key(215, 0, 216, UserId::Integer(217));
    let consumer_session = test_transport_session_key(215, 0, 218, UserId::Integer(219));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("hi");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_101,
        Some(selected_rid),
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Rid(selected_rid),
                pending_packet_gate: None,
            }],
        },
    );

    let (response_tx, response_rx) = oneshot::channel();
    respond_request_consumer_keyframe(
        &mut state,
        &metrics,
        &consumer_session,
        consumer_transport_media_id,
        &source_session,
        source_transport_media_id,
        response_tx,
    );

    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    assert!(state.dirty_sessions.contains(&source_session));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);
}

#[test]
fn open_consumer_keyframe_request_refreshes_simulcast_video_source() {
    let source_session = test_transport_session_key(225, 0, 226, UserId::Integer(227));
    let consumer_session = test_transport_session_key(225, 0, 228, UserId::Integer(229));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_201,
        Some(Rid::from("lo")),
    );
    let Some(source_session_state) = state.users.get_mut(&source_session) else {
        assert!(state.users.contains_key(&source_session));
        return;
    };
    source_session_state.rtc.direct_api().expect_stream_rx(
        Ssrc::from(88_202),
        None,
        source_mid,
        Some(Rid::from("hi")),
    );
    source_session_state
        .sdp_negotiation
        .negotiated_producer_parameters
        .insert(
            source_mid,
            RouterRtpParameters::new(
                vec![],
                vec![],
                vec![
                    StreamBinding::new().with_ssrc(88_201).with_rid("lo"),
                    StreamBinding::new().with_ssrc(88_202).with_rid("hi"),
                ],
            )
            .with_mid(source_mid.to_string()),
        );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
            }],
        },
    );

    let (response_tx, response_rx) = oneshot::channel();
    respond_request_consumer_keyframe(
        &mut state,
        &metrics,
        &consumer_session,
        consumer_transport_media_id,
        &source_session,
        source_transport_media_id,
        response_tx,
    );

    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    assert!(state.dirty_sessions.contains(&source_session));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);
}

#[test]
fn consumer_keyframe_request_forwards_remote_video_refresh() {
    let source_session = test_transport_session_key(125, 0, 126, UserId::Integer(127));
    let consumer_session = test_transport_session_key(125, 1, 128, UserId::Integer(129));
    let consumer_mid = Mid::from("cam-down");
    let source_transport_media_id = TransportMediaId::new(131);
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let (control_tx, mut control_rx) = mpsc::channel(1);

    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(control_tx, RelayTargetId::new(11)),
            )
            .is_ok()
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
            }],
        },
    );

    let (response_tx, response_rx) = oneshot::channel();
    respond_request_consumer_keyframe(
        &mut state,
        &metrics,
        &consumer_session,
        consumer_transport_media_id,
        &source_session,
        source_transport_media_id,
        response_tx,
    );

    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    assert!(matches!(
        control_rx.try_recv().ok(),
        Some(RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id: forwarded_transport_media_id,
            target_id,
            rid: None,
            kind: KeyframeRequestKind::Pli,
        }) if source_session_key == source_session
            && forwarded_transport_media_id == source_transport_media_id
            && target_id == RelayTargetId::new(11)
    ));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);
}

#[test]
fn consumer_keyframe_request_forwards_remote_video_refresh_with_selected_rid() {
    let source_session = test_transport_session_key(225, 0, 226, UserId::Integer(227));
    let consumer_session = test_transport_session_key(225, 1, 228, UserId::Integer(229));
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("hi");
    let source_transport_media_id = TransportMediaId::new(231);
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let (control_tx, mut control_rx) = mpsc::channel(1);

    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(control_tx, RelayTargetId::new(12)),
            )
            .is_ok()
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Rid(selected_rid),
                pending_packet_gate: None,
            }],
        },
    );

    let (response_tx, response_rx) = oneshot::channel();
    respond_request_consumer_keyframe(
        &mut state,
        &metrics,
        &consumer_session,
        consumer_transport_media_id,
        &source_session,
        source_transport_media_id,
        response_tx,
    );

    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    assert!(matches!(
        control_rx.try_recv().ok(),
        Some(RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id: forwarded_transport_media_id,
            target_id,
            rid: Some(rid),
            kind: KeyframeRequestKind::Pli,
        }) if source_session_key == source_session
            && forwarded_transport_media_id == source_transport_media_id
            && target_id == RelayTargetId::new(12)
            && rid == selected_rid
    ));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);
}

#[test]
fn set_consumer_packet_gate_updates_one_route_without_rewriting_the_source_gate() {
    let source_session = test_transport_session_key(131, 0, 132, UserId::Integer(133));
    let first_consumer_session = test_transport_session_key(131, 0, 134, UserId::Integer(135));
    let second_consumer_session = test_transport_session_key(131, 0, 136, UserId::Integer(137));
    let source_mid = Mid::from("cam-up");
    let first_consumer_mid = Mid::from("cam-down-a");
    let second_consumer_mid = Mid::from("cam-down-b");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 88_889);
    let first_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: first_consumer_session.clone(),
            mid: first_consumer_mid,
            source_transport_media_id,
        });
    let second_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: second_consumer_session.clone(),
            mid: second_consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![
                MediaRouteDestination {
                    dest_session: first_consumer_session.clone(),
                    dest_transport_media_id: first_consumer_transport_media_id,
                    dest_mid: first_consumer_mid,
                    dest_payload_type: None,
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                    pending_packet_gate: None,
                },
                MediaRouteDestination {
                    dest_session: second_consumer_session.clone(),
                    dest_transport_media_id: second_consumer_transport_media_id,
                    dest_mid: second_consumer_mid,
                    dest_payload_type: None,
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                    pending_packet_gate: None,
                },
            ],
        },
    );
    let observed_at = Instant::now();
    state.observe_producer_rid_packet(source_transport_media_id, Rid::from("lo"), observed_at);

    let (response_tx, response_rx) = oneshot::channel();
    respond_set_consumer_packet_gate(
        &mut state,
        ConsumerPacketGateRequest {
            consumer_session_key: &first_consumer_session,
            consumer_transport_media_id: first_consumer_transport_media_id,
            source_session_key: &source_session,
            source_transport_media_id,
            packet_gate: PacketLayerGate::Rid("lo".into()),
        },
        observed_at + Duration::from_millis(20),
        response_tx,
    );

    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    assert!(matches!(
        state.media_route_index.get(&source_transport_media_id),
        Some(route_entry) if route_entry.destinations.iter().any(|destination| {
            destination.dest_session == first_consumer_session
                && destination.packet_gate == PacketLayerGate::Rid("lo".into())
        })
    ));
    assert!(matches!(
        state.media_route_index.get(&source_transport_media_id),
        Some(route_entry) if route_entry.destinations.iter().any(|destination| {
            destination.dest_session == second_consumer_session
                && destination.packet_gate == PacketLayerGate::Open
        })
    ));
    assert_eq!(
        state
            .route_control
            .effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Open)
    );
}

#[test]
fn selected_rid_gate_uses_supplied_time_for_live_and_stale_updates() {
    let source_session = test_transport_session_key(531, 0, 532, UserId::Integer(533));
    let consumer_session = test_transport_session_key(531, 0, 534, UserId::Integer(535));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("hi");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_921,
        Some(selected_rid),
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
            }],
        },
    );
    let observed_at = Instant::now();
    state.observe_producer_rid_packet(source_transport_media_id, selected_rid, observed_at);

    let (live_response_tx, live_response_rx) = oneshot::channel();
    respond_set_consumer_packet_gate(
        &mut state,
        ConsumerPacketGateRequest {
            consumer_session_key: &consumer_session,
            consumer_transport_media_id,
            source_session_key: &source_session,
            source_transport_media_id,
            packet_gate: PacketLayerGate::Rid(selected_rid),
        },
        observed_at + Duration::from_millis(500),
        live_response_tx,
    );

    assert_eq!(live_response_rx.blocking_recv(), Ok(Ok(())));
    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Rid(selected_rid),
        None,
    );

    let (stale_response_tx, stale_response_rx) = oneshot::channel();
    respond_set_consumer_packet_gate(
        &mut state,
        ConsumerPacketGateRequest {
            consumer_session_key: &consumer_session,
            consumer_transport_media_id,
            source_session_key: &source_session,
            source_transport_media_id,
            packet_gate: PacketLayerGate::Rid(selected_rid),
        },
        observed_at + Duration::from_secs(3),
        stale_response_tx,
    );

    assert_eq!(stale_response_rx.blocking_recv(), Ok(Ok(())));
    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid(selected_rid)),
    );
}

#[test]
fn selected_rid_packet_gate_uses_bootstrap_fallback_before_becoming_strict() {
    let PendingSelectedRidRoute {
        mut state,
        metrics,
        source_session,
        consumer_session,
        source_transport_media_id,
        selected_rid,
        fallback_rid,
    } = prepare_pending_selected_rid_route();
    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid(selected_rid)),
    );
    assert_eq!(
        state
            .route_control
            .effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );

    let now = Instant::now();
    assert_eq!(
        state
            .route_control
            .decide_keyframe_request(source_transport_media_id, now),
        KeyframeRequestDecision::Forward
    );

    assert!(!observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        fallback_rid,
        false,
        now,
    ));

    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid(selected_rid)),
    );
    assert_eq!(
        state
            .route_control
            .effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );

    assert!(!state.dirty_sessions.contains(&source_session));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 0);

    assert!(observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        fallback_rid,
        true,
        now + Duration::from_millis(10),
    ));

    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Rid(fallback_rid),
        Some(&PacketLayerGate::Rid(selected_rid)),
    );
    assert_eq!(
        state
            .route_control
            .effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Rid(fallback_rid))
    );
    assert!(state.dirty_sessions.contains(&source_session));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);
}

#[test]
fn selected_rid_packet_gate_switches_from_bootstrap_fallback_on_selected_keyframe() {
    let PendingSelectedRidRoute {
        mut state,
        metrics,
        source_session,
        consumer_session,
        source_transport_media_id,
        selected_rid,
        fallback_rid,
    } = prepare_pending_selected_rid_route();
    let now = Instant::now();

    assert!(observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        fallback_rid,
        true,
        now,
    ));

    assert!(!observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        selected_rid,
        false,
        now + Duration::from_millis(10),
    ));
    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Rid(fallback_rid),
        Some(&PacketLayerGate::Rid(selected_rid)),
    );

    assert!(observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        selected_rid,
        true,
        now + Duration::from_millis(20),
    ));
    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Rid(selected_rid),
        None,
    );
}

#[test]
fn selected_rid_activation_sends_bounded_follow_up_keyframe_refreshes() {
    let source_session = test_transport_session_key(431, 0, 432, UserId::Integer(433));
    let consumer_session = test_transport_session_key(431, 0, 434, UserId::Integer(435));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("hi");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_351,
        Some(selected_rid),
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session,
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: Some(PacketLayerGate::Rid(selected_rid)),
            }],
        },
    );

    let now = Instant::now();
    assert!(observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        selected_rid,
        true,
        now,
    ));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);

    for (elapsed_ms, expected_forwarded) in [
        (700, 1),
        (1_200, 2),
        (2_700, 3),
        (5_200, 4),
        (8_200, 5),
        (13_200, 6),
    ] {
        assert!(!observe_source_rid_readiness(
            &mut state,
            &metrics,
            &source_session,
            source_transport_media_id,
            selected_rid,
            true,
            now + Duration::from_millis(elapsed_ms),
        ));
        assert_eq!(
            metrics.snapshot().rtc_route_control_forwarded,
            expected_forwarded
        );
    }
}

#[test]
fn selected_rid_keyframe_refreshes_are_timer_driven_after_activation() {
    let source_session = test_transport_session_key(531, 0, 532, UserId::Integer(533));
    let consumer_session = test_transport_session_key(531, 0, 534, UserId::Integer(535));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("hi");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_451,
        Some(selected_rid),
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session,
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: test_transport_session_key(531, 0, 534, UserId::Integer(535)),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: Some(PacketLayerGate::Rid(selected_rid)),
            }],
        },
    );

    let now = Instant::now();
    assert!(observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        selected_rid,
        true,
        now,
    ));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);

    drain_due_rid_keyframe_refreshes(&mut state, &metrics, now + Duration::from_millis(1_200));

    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 2);
    assert!(state.dirty_sessions.contains(&source_session));
}

#[test]
fn selected_rid_packet_gate_blocks_when_selected_rid_goes_stale() {
    let source_session = test_transport_session_key(331, 0, 332, UserId::Integer(333));
    let consumer_session = test_transport_session_key(331, 0, 334, UserId::Integer(335));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("hi");
    let fallback_rid = Rid::from("lo");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_401,
        Some(selected_rid),
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Rid(selected_rid),
                pending_packet_gate: None,
            }],
        },
    );
    refresh_source_packet_gate(&mut state, source_transport_media_id);

    let now = Instant::now();
    let stale_observed_at = now
        .checked_sub(Duration::from_secs(3))
        .map_or(now, |observed_at| observed_at);
    state.observe_producer_rid_packet(source_transport_media_id, selected_rid, stale_observed_at);

    assert!(observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        fallback_rid,
        false,
        now,
    ));

    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid(selected_rid)),
    );
    assert_eq!(
        state
            .route_control
            .effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Block)
    );
    assert!(state.dirty_sessions.contains(&source_session));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);

    assert!(!observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        selected_rid,
        false,
        now + Duration::from_millis(10),
    ));
    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid(selected_rid)),
    );

    assert!(observe_source_rid_readiness(
        &mut state,
        &metrics,
        &source_session,
        source_transport_media_id,
        selected_rid,
        true,
        now + Duration::from_millis(20),
    ));

    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Rid(selected_rid),
        None,
    );
}

#[test]
fn batched_consumer_packet_gates_keep_remote_relay_open_during_rid_bootstrap() {
    let source_session = test_transport_session_key(141, 0, 142, UserId::Integer(143));
    let first_consumer_session = test_transport_session_key(141, 1, 144, UserId::Integer(145));
    let second_consumer_session = test_transport_session_key(141, 1, 146, UserId::Integer(147));
    let first_consumer_mid = Mid::from("cam-down-a");
    let second_consumer_mid = Mid::from("cam-down-b");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id = TransportMediaId::new(41);
    let (command_tx, mut command_rx) = mpsc::channel(4);
    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(command_tx, RelayTargetId::new(16)),
            )
            .is_ok()
    );
    let first_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: first_consumer_session.clone(),
            mid: first_consumer_mid,
            source_transport_media_id,
        });
    let second_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: second_consumer_session.clone(),
            mid: second_consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![
                MediaRouteDestination {
                    dest_session: first_consumer_session.clone(),
                    dest_transport_media_id: first_consumer_transport_media_id,
                    dest_mid: first_consumer_mid,
                    dest_payload_type: None,
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                    pending_packet_gate: None,
                },
                MediaRouteDestination {
                    dest_session: second_consumer_session.clone(),
                    dest_transport_media_id: second_consumer_transport_media_id,
                    dest_mid: second_consumer_mid,
                    dest_payload_type: None,
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                    pending_packet_gate: None,
                },
            ],
        },
    );

    let (response_tx, response_rx) = oneshot::channel();
    respond_set_consumer_packet_gates(
        &mut state,
        &source_session,
        source_transport_media_id,
        vec![
            ConsumerPacketGateCommand::new(
                first_consumer_session,
                first_consumer_transport_media_id,
                PacketLayerGate::Rid("lo".into()),
            ),
            ConsumerPacketGateCommand::new(
                second_consumer_session,
                second_consumer_transport_media_id,
                PacketLayerGate::Rid("hi".into()),
            ),
        ],
        Instant::now(),
        response_tx,
    );

    assert_eq!(response_rx.blocking_recv(), Ok(Ok(vec![Ok(()), Ok(())])));
    assert!(matches!(
        command_rx.try_recv().ok(),
        Some(RtcWorkerCommand::SetRemoteSourcePacketGate {
            source_session_key,
            source_transport_media_id: forwarded_source_transport_media_id,
            target_id,
            packet_gate: PacketLayerGate::Open,
        }) if source_session_key == source_session
            && forwarded_source_transport_media_id == source_transport_media_id
            && target_id == RelayTargetId::new(16)
    ));
    assert!(command_rx.try_recv().is_err());
}

#[test]
fn explicit_consumer_block_still_blocks_remote_relay() {
    let source_session = test_transport_session_key(141, 0, 152, UserId::Integer(153));
    let consumer_session = test_transport_session_key(141, 1, 154, UserId::Integer(155));
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id = TransportMediaId::new(51);
    let (command_tx, mut command_rx) = mpsc::channel(4);
    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(command_tx, RelayTargetId::new(17)),
            )
            .is_ok()
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session,
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Block,
                pending_packet_gate: None,
            }],
        },
    );

    refresh_source_packet_gate(&mut state, source_transport_media_id);

    assert!(matches!(
        command_rx.try_recv().ok(),
        Some(RtcWorkerCommand::SetRemoteSourcePacketGate {
            source_session_key,
            source_transport_media_id: forwarded_source_transport_media_id,
            target_id,
            packet_gate: PacketLayerGate::Block,
        }) if source_session_key == source_session
            && forwarded_source_transport_media_id == source_transport_media_id
            && target_id == RelayTargetId::new(17)
    ));
    assert!(command_rx.try_recv().is_err());
}

#[test]
fn selected_consumer_rid_keeps_remote_relay_open() {
    let source_session = test_transport_session_key(141, 0, 162, UserId::Integer(163));
    let consumer_session = test_transport_session_key(141, 1, 164, UserId::Integer(165));
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id = TransportMediaId::new(61);
    let (command_tx, mut command_rx) = mpsc::channel(4);
    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(command_tx, RelayTargetId::new(18)),
            )
            .is_ok()
    );
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session,
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Rid("lo".into()),
                pending_packet_gate: None,
            }],
        },
    );

    refresh_source_packet_gate(&mut state, source_transport_media_id);

    assert!(matches!(
        command_rx.try_recv().ok(),
        Some(RtcWorkerCommand::SetRemoteSourcePacketGate {
            source_session_key,
            source_transport_media_id: forwarded_source_transport_media_id,
            target_id,
            packet_gate: PacketLayerGate::Open,
        }) if source_session_key == source_session
            && forwarded_source_transport_media_id == source_transport_media_id
            && target_id == RelayTargetId::new(18)
    ));
    assert!(command_rx.try_recv().is_err());
}

#[test]
fn add_send_media_rolls_back_remote_source_registration_when_consumer_session_is_missing() {
    let source_session = test_transport_session_key(151, 0, 152, UserId::Integer(153));
    let consumer_session = test_transport_session_key(151, 1, 154, UserId::Integer(155));
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id = TransportMediaId::new(33);
    let (command_tx, _command_rx) = mpsc::channel(1);
    let remote_source_control = RemoteSourceControl::new(command_tx, RelayTargetId::new(10));
    let consumer_rtp_parameters = RouterRtpParameters::new(vec![], vec![], vec![]);
    let (response_tx, response_rx) = oneshot::channel();

    respond_add_send_media(
        &mut state,
        AddSendMediaRequest {
            consumer_session_key: &consumer_session,
            media_kind: MediaKind::Video,
            source_session_key: &source_session,
            source_transport_media_id,
            remote_source_control: Some(remote_source_control),
            consumer_rtp_parameters: &consumer_rtp_parameters,
        },
        Instant::now(),
        response_tx,
    );

    assert_eq!(
        response_rx.blocking_recv(),
        Ok(Err(TransportAdapterError::TransportUnavailable))
    );
    assert!(
        state
            .remote_source_registration(source_transport_media_id)
            .is_none()
    );
}

#[test]
fn add_send_media_declares_one_ridless_downstream_stream_for_simulcast_source() {
    let source_session = test_transport_session_key(151, 0, 156, UserId::Integer(157));
    let consumer_session = test_transport_session_key(151, 0, 158, UserId::Integer(159));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 71_001);
    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &consumer_session,
            SocketAddr::from(([127, 0, 0, 1], 47_101)),
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let consumer_rtp_parameters = RouterRtpParameters::new(
        vec![],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(72_001)
                .with_rid("lo")
                .with_payload_type(96),
            StreamBinding::new()
                .with_ssrc(72_002)
                .with_rid("hi")
                .with_payload_type(96),
        ],
    )
    .with_mid(consumer_mid.to_string());
    let (response_tx, response_rx) = oneshot::channel();

    respond_add_send_media(
        &mut state,
        AddSendMediaRequest {
            consumer_session_key: &consumer_session,
            media_kind: MediaKind::Video,
            source_session_key: &source_session,
            source_transport_media_id,
            remote_source_control: None,
            consumer_rtp_parameters: &consumer_rtp_parameters,
        },
        Instant::now(),
        response_tx,
    );

    assert!(matches!(response_rx.blocking_recv(), Ok(Ok(_))));
    if !state.users.contains_key(&consumer_session) {
        assert!(state.users.contains_key(&consumer_session));
        return;
    }
    let Some(consumer_session_state) = state.users.get_mut(&consumer_session) else {
        return;
    };
    let mut direct_api = consumer_session_state.rtc.direct_api();
    assert!(direct_api.stream_tx_by_mid(consumer_mid, None).is_some());
    assert!(
        direct_api
            .stream_tx_by_mid(consumer_mid, Some(Rid::from("lo")))
            .is_none()
    );
    assert!(
        direct_api
            .stream_tx_by_mid(consumer_mid, Some(Rid::from("hi")))
            .is_none()
    );
    assert!(
        state
            .media_route_index
            .get(&source_transport_media_id)
            .is_some_and(
                |route_entry| route_entry.destinations.iter().any(|destination| {
                    destination.dest_transport_media_id == TransportMediaId::new(1)
                        && destination.dest_payload_type == Some(Pt::from(96))
                })
            )
    );
    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid("lo".into())),
    );
}

#[test]
fn add_send_media_uses_supplied_time_for_initial_selected_rid_gate() {
    let source_session = test_transport_session_key(751, 0, 752, UserId::Integer(753));
    let consumer_session = test_transport_session_key(751, 0, 754, UserId::Integer(755));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("lo");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        71_101,
        Some(selected_rid),
    );
    let observed_at = Instant::now();
    state.observe_producer_rid_packet(source_transport_media_id, selected_rid, observed_at);
    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &consumer_session,
            SocketAddr::from(([127, 0, 0, 1], 47_102)),
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let consumer_rtp_parameters = RouterRtpParameters::new(
        vec![],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(72_101)
                .with_rid("lo")
                .with_payload_type(96),
        ],
    )
    .with_mid(consumer_mid.to_string());
    let (response_tx, response_rx) = oneshot::channel();

    respond_add_send_media(
        &mut state,
        AddSendMediaRequest {
            consumer_session_key: &consumer_session,
            media_kind: MediaKind::Video,
            source_session_key: &source_session,
            source_transport_media_id,
            remote_source_control: None,
            consumer_rtp_parameters: &consumer_rtp_parameters,
        },
        observed_at + Duration::from_millis(250),
        response_tx,
    );

    assert!(matches!(response_rx.blocking_recv(), Ok(Ok(_))));
    assert_consumer_packet_gate(
        &state,
        source_transport_media_id,
        &consumer_session,
        &PacketLayerGate::Rid(selected_rid),
        None,
    );
}

#[test]
fn remove_media_keeps_registered_handle_when_negotiated_removal_cannot_stage() {
    let session_key = test_transport_session_key(161, 0, 162, UserId::Integer(163));
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 47_100));
    let producer_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &session_key,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let session_state = state.users.get_mut(&session_key);
    assert!(session_state.is_some());
    let Some(session_state) = session_state else {
        return;
    };
    {
        let mut direct_api = session_state.rtc.direct_api();
        direct_api.declare_media(producer_mid, MediaKind::Video);
        direct_api.remove_media(producer_mid);
    }
    session_state.sdp_negotiation.initial_offer_applied = true;

    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    let bitrate_state = Arc::new(Mutex::new(RtcBitrateState::default()));
    let (response_tx, response_rx) = oneshot::channel();

    respond_remove_media(
        &mut state,
        &bitrate_state,
        &session_key,
        transport_media_id,
        response_tx,
    );

    assert_eq!(
        response_rx.blocking_recv(),
        Ok(Err(TransportAdapterError::InvalidInput))
    );
    assert!(matches!(
        state.media_handle(transport_media_id),
        Some(RegisteredMediaHandle::Producer { session_key: stored_session_key, mid })
            if stored_session_key == &session_key && *mid == producer_mid
    ));
}

#[test]
fn request_keyframe_ignores_wrong_source_owner() {
    let source_session = test_transport_session_key(131, 0, 132, UserId::Integer(133));
    let wrong_session = test_transport_session_key(131, 0, 134, UserId::Integer(135));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 99_999);

    request_keyframe_for_source(
        &mut state,
        &metrics,
        &wrong_session,
        source_transport_media_id,
        None,
        KeyframeRequestKind::Pli,
        Instant::now(),
    );

    assert!(!state.dirty_sessions.contains(&source_session));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded, 0);
    assert_eq!(snapshot.rtc_route_control_absorbed, 0);
}

#[test]
fn remote_source_packet_gate_ignores_wrong_source_owner() {
    let source_session = test_transport_session_key(141, 0, 142, UserId::Integer(143));
    let wrong_session = test_transport_session_key(141, 0, 144, UserId::Integer(145));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 101_010);

    respond_set_remote_source_packet_gate(
        &mut state,
        &wrong_session,
        source_transport_media_id,
        RelayTargetId::new(9),
        PacketLayerGate::Rid("hi".into()),
    );

    assert_eq!(
        state
            .route_control
            .effective_packet_gate(source_transport_media_id),
        None
    );
}
