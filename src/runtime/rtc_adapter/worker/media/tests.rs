use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use o_sfu_protocol::shared::SessionId;
use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::{
    media::{KeyframeRequestKind, MediaKind, Mid, Rid},
    rtp::Ssrc,
};
use tokio::sync::{mpsc, oneshot};

use super::{
    AddSendMediaRequest, RemoteKeyframeRequest, request_keyframe_for_source,
    respond_add_send_media, respond_remove_media, respond_request_consumer_keyframe,
    respond_request_remote_keyframe, respond_set_consumer_packet_gate,
    respond_set_consumer_packet_gates, respond_set_remote_source_packet_gate,
};
use crate::{
    config::MediaCodecFlags,
    runtime::{
        metrics::RuntimeMetrics,
        rtc_adapter::{
            bitrate::RtcBitrateState,
            bootstrap,
            commands::{ConsumerPacketGateCommand, RemoteSourceControl, RtcWorkerCommand},
            demux::{MediaRouteDestination, MediaRouteEntry},
            media_registry::RegisteredMediaHandle,
            relay_registry::{RelayPacketMailbox, RelayRegistry, RelayTargetId},
            route_control::PacketLayerGate,
            state::RtcBootstrapState,
            test_support::test_transport_session_key,
        },
        transport_adapter::{TransportAdapterError, TransportMediaId, TransportSessionKey},
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
            &mut state.sessions,
            source_session,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.sessions.get_mut(source_session) else {
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

#[test]
fn remote_keyframe_requests_drop_when_the_relay_target_is_inactive() {
    let source_session = test_transport_session_key(101, 0, 102, SessionId::Integer(103));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let relay_registry = RelayRegistry::default();
    let (_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 66_666);

    respond_request_remote_keyframe(
        &mut state,
        &metrics,
        &relay_registry,
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
    let source_session = test_transport_session_key(111, 0, 112, SessionId::Integer(113));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let relay_registry = RelayRegistry::default();
    let (mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 77_777);
    let relay_target_id = RelayTargetId::new(8);

    relay_registry.activate_source_target(
        source_transport_media_id,
        relay_target_id,
        mailbox.into(),
    );
    relay_registry.set_source_target_active(source_transport_media_id, relay_target_id, true);

    respond_request_remote_keyframe(
        &mut state,
        &metrics,
        &relay_registry,
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
        &relay_registry,
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
    let source_session = test_transport_session_key(115, 0, 116, SessionId::Integer(117));
    let consumer_session = test_transport_session_key(115, 0, 118, SessionId::Integer(119));
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
                active: true,
                packet_gate: PacketLayerGate::Open,
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
    let source_session = test_transport_session_key(215, 0, 216, SessionId::Integer(217));
    let consumer_session = test_transport_session_key(215, 0, 218, SessionId::Integer(219));
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
                active: true,
                packet_gate: PacketLayerGate::Rid(selected_rid),
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
    let source_session = test_transport_session_key(125, 0, 126, SessionId::Integer(127));
    let consumer_session = test_transport_session_key(125, 1, 128, SessionId::Integer(129));
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
                active: true,
                packet_gate: PacketLayerGate::Open,
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
    let source_session = test_transport_session_key(225, 0, 226, SessionId::Integer(227));
    let consumer_session = test_transport_session_key(225, 1, 228, SessionId::Integer(229));
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
                active: true,
                packet_gate: PacketLayerGate::Rid(selected_rid),
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
    let source_session = test_transport_session_key(131, 0, 132, SessionId::Integer(133));
    let first_consumer_session = test_transport_session_key(131, 0, 134, SessionId::Integer(135));
    let second_consumer_session = test_transport_session_key(131, 0, 136, SessionId::Integer(137));
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
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                },
                MediaRouteDestination {
                    dest_session: second_consumer_session.clone(),
                    dest_transport_media_id: second_consumer_transport_media_id,
                    dest_mid: second_consumer_mid,
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                },
            ],
        },
    );

    let (response_tx, response_rx) = oneshot::channel();
    respond_set_consumer_packet_gate(
        &mut state,
        &first_consumer_session,
        first_consumer_transport_media_id,
        &source_session,
        source_transport_media_id,
        PacketLayerGate::Rid("lo".into()),
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
fn batched_consumer_packet_gates_refresh_remote_source_once() {
    let source_session = test_transport_session_key(141, 0, 142, SessionId::Integer(143));
    let first_consumer_session = test_transport_session_key(141, 1, 144, SessionId::Integer(145));
    let second_consumer_session = test_transport_session_key(141, 1, 146, SessionId::Integer(147));
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
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                },
                MediaRouteDestination {
                    dest_session: second_consumer_session.clone(),
                    dest_transport_media_id: second_consumer_transport_media_id,
                    dest_mid: second_consumer_mid,
                    active: true,
                    packet_gate: PacketLayerGate::Open,
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
fn add_send_media_rolls_back_remote_source_registration_when_consumer_session_is_missing() {
    let source_session = test_transport_session_key(151, 0, 152, SessionId::Integer(153));
    let consumer_session = test_transport_session_key(151, 1, 154, SessionId::Integer(155));
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
fn remove_media_keeps_registered_handle_when_negotiated_removal_cannot_stage() {
    let session_key = test_transport_session_key(161, 0, 162, SessionId::Integer(163));
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 47_100));
    let producer_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.sessions,
            &session_key,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let session_state = state.sessions.get_mut(&session_key);
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
    let source_session = test_transport_session_key(131, 0, 132, SessionId::Integer(133));
    let wrong_session = test_transport_session_key(131, 0, 134, SessionId::Integer(135));
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
    let source_session = test_transport_session_key(141, 0, 142, SessionId::Integer(143));
    let wrong_session = test_transport_session_key(141, 0, 144, SessionId::Integer(145));
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
