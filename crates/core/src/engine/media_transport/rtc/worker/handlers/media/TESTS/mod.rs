#![allow(
    clippy::panic,
    reason = "media worker tests use panic only for mandatory fixture setup failures"
)]

#[path = "fixtures.rs"]
mod fixtures;

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use fixtures::{
    LocalVideoRoute, RemoteVideoRoute, prepare_pending_selected_rid_route,
    set_consumer_packet_gate_at,
};
use o_sfu_rfc::rtp::CodecName;
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, MediaStream as RouterRtpParameters, PayloadType, StreamBinding},
};
use str0m::{
    media::{KeyframeRequestKind, MediaKind, Mid, Pt, Rid},
    rtp::Ssrc,
};
use tokio::sync::mpsc;

use super::{
    AddSendMediaRequest, KeyframeRequestMode, KeyframeRequestTarget, apply_media_control_batch,
    apply_route_control_request, control::remove_consumer_route, request_kf_for_target,
    worker_add_send_media, worker_remove_media,
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::{
            ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome, ProducerActivity,
            ProducerRouteControl, SourceActivityRevision, SourceActivityUpdate,
            TransportAdapterError, TransportConsumerRoute, TransportMediaId, TransportSourceKey,
            rtc::{
                bootstrap,
                commands::{
                    RemoteSourceControl, RouteControlRequest, WorkerMediaControlBatch,
                    WorkerMediaControlBatchOutcome,
                },
                keyframe_tracker::{KeyframeRequestDecision, KeyframeRequestOrigin},
                media_registry::{ConsumerKeyframeTarget, RegisteredMediaHandle},
                relay_registry::{RelayPacketMailbox, RelayTargetId},
                route_control::PacketLayerGate,
                source_route::RemoteSourceRegistration,
                state::PacketLoopState,
                test_support::{
                    MediaWorkerScenario, add_source_rid_stream, assert_consumer_packet_gate,
                    assert_remote_keyframe_command, assert_remote_packet_gate_command,
                    drain_ready_sessions, prepare_source_session, prepare_source_session_with_rid,
                    register_remote_source_control, register_saturated_remote_source,
                    saturated_remote_control, test_consumer_session_key,
                    test_consumer_session_key_on_worker, test_source_session_key,
                    test_transport_session_key,
                },
                worker::RtcWorkerCommand,
            },
        },
        metrics::{
            RtcMetricsRecorder, RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt,
        },
    },
};

fn apply_source_activity(
    state: &mut PacketLoopState,
    rtc_metrics: &RtcMetricsRecorder,
    source: TransportSourceKey,
    update: SourceActivityUpdate,
    now: Instant,
) {
    let outcome = apply_media_control_batch(
        state,
        rtc_metrics,
        Bitrate::from_mbps(10),
        now,
        WorkerMediaControlBatch::ProducerActivity(vec![(
            0,
            ProducerRouteControl { source, update },
        )]),
    );
    let WorkerMediaControlBatchOutcome::Applied(results) = outcome else {
        panic!("producer activity should return applied results");
    };
    assert_eq!(results, vec![Ok(())]);
}

#[test]
fn media_removal_rejects_a_registered_handle_without_its_owner_session() {
    let source_session = test_source_session_key(91);
    let source_mid = Mid::from("cam-up");
    let mut state = PacketLoopState::default();
    let source_media = prepare_source_session(&mut state, &source_session, source_mid, 91_000);
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: source_session.clone(),
        mid: source_mid,
    });
    assert!(state.users.remove(&source_session).is_some());

    assert_eq!(
        worker_remove_media(&mut state, &Arc::default(), &source_session, source_media),
        Err(TransportAdapterError::InvalidInput)
    );
    assert!(state.media_handle(source_media).is_some());
}

#[test]
fn remote_keyframe_requests_drop_when_the_relay_target_is_inactive() {
    let source_session = test_source_session_key(101);
    let source_mid = Mid::from("cam-up");
    let mut state = PacketLoopState::default();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();
    let (_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let src_media = prepare_source_session(&mut state, &source_session, source_mid, 66_666);

    apply_route_control_request(
        &mut state,
        &rtc_metrics,
        RouteControlRequest::RequestRemoteKeyframe {
            source: TransportSourceKey::new(source_session.clone(), src_media),
            target_id: RelayTargetId::new(7),
            rid: None,
            kind: KeyframeRequestKind::Pli,
        },
        None,
    );

    assert!(drain_ready_sessions(&mut state).is_empty());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 0);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops(), 1);
}

#[test]
fn remote_source_activity_gates_feedback_and_rejects_stale_reconciliation() {
    let now = Instant::now();
    let mut route = RemoteVideoRoute::new(106, 66_668, 10);
    let source = TransportSourceKey::new(route.source_session.clone(), route.src_media);
    let first_revision = SourceActivityRevision::default().next();
    let second_revision = first_revision.next();
    let third_revision = second_revision.next();

    assert_remote_packet_gate_command(
        &mut route.control_rx,
        &route.source_session,
        route.src_media,
        route.target_id,
        PacketLayerGate::Open,
    );
    route.request_kf();
    route.request_kf();
    assert_remote_keyframe_command(
        &mut route.control_rx,
        &route.source_session,
        route.src_media,
        route.target_id,
        None,
    );
    assert!(route.state.routes.next_kf_deadline().is_some());
    apply_route_control_request(
        &mut route.state,
        &route.rtc_metrics,
        RouteControlRequest::SetRemoteSourceActivity {
            source: source.clone(),
            update: SourceActivityUpdate::new(ProducerActivity::Inactive, second_revision),
        },
        None,
    );

    assert!(!route.state.routes.source_is_active(route.src_media));
    let mut retries = Vec::new();
    route
        .state
        .routes
        .drain_due_kf_reqs(now + Duration::from_secs(2), &mut retries);
    assert!(retries.is_empty());
    let WorkerMediaControlBatchOutcome::Consumers(results) = route.request_kf() else {
        panic!("inactive source feedback should return a consumer result");
    };
    assert_eq!(results, [ConsumerRouteControlOutcome::default()]);
    assert!(route.control_rx.try_recv().is_err());

    apply_route_control_request(
        &mut route.state,
        &route.rtc_metrics,
        RouteControlRequest::SetRemoteSourceActivity {
            source: source.clone(),
            update: SourceActivityUpdate::new(ProducerActivity::Active, first_revision),
        },
        None,
    );
    assert!(!route.state.routes.source_is_active(route.src_media));

    apply_route_control_request(
        &mut route.state,
        &route.rtc_metrics,
        RouteControlRequest::SetRemoteSourceActivity {
            source,
            update: SourceActivityUpdate::new(ProducerActivity::Active, third_revision),
        },
        None,
    );
    assert!(route.state.routes.source_is_active(route.src_media));
    assert!(route.control_rx.try_recv().is_err());
}

#[test]
fn source_worker_forwards_each_coalesced_remote_keyframe_request() {
    let source_session = test_source_session_key(111);
    let source_mid = Mid::from("cam-up");
    let mut state = PacketLoopState::default();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();
    let (mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let src_media = prepare_source_session(&mut state, &source_session, source_mid, 77_777);
    let relay_target_id = RelayTargetId::new(8);

    state
        .routes
        .add_relay_target(src_media, relay_target_id, mailbox);
    state
        .routes
        .set_relay_target_active(src_media, relay_target_id, true);

    apply_route_control_request(
        &mut state,
        &rtc_metrics,
        RouteControlRequest::RequestRemoteKeyframe {
            source: TransportSourceKey::new(source_session.clone(), src_media),
            target_id: relay_target_id,
            rid: None,
            kind: KeyframeRequestKind::Pli,
        },
        None,
    );
    apply_route_control_request(
        &mut state,
        &rtc_metrics,
        RouteControlRequest::RequestRemoteKeyframe {
            source: TransportSourceKey::new(source_session.clone(), src_media),
            target_id: relay_target_id,
            rid: None,
            kind: KeyframeRequestKind::Fir,
        },
        None,
    );

    assert_eq!(
        drain_ready_sessions(&mut state),
        vec![source_session.clone()]
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 2);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops(), 0);
}

#[test]
fn source_only_activity_respects_revision_and_media_kind() {
    for (seed, ssrc, kind, expected_kf) in [
        (112, 77_778, MediaKind::Video, 1),
        (113, 77_779, MediaKind::Audio, 0),
    ] {
        let now = Instant::now();
        let source_session = test_source_session_key(seed);
        let mut state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        assert!(
            bootstrap::ensure_session_rtc_state(
                &mut state.users,
                &source_session,
                SocketAddr::from(([127, 0, 0, 1], 47_001)),
                Bitrate::from_mbps(10),
            )
            .is_ok()
        );
        let source_mid = Mid::from(if kind == MediaKind::Video {
            "cam-up"
        } else {
            "aud-up"
        });
        let Some(session) = state.users.get_mut(&source_session) else {
            panic!("source session should exist after RTC state bootstrap");
        };
        let mut direct_api = session.rtc.direct_api();
        direct_api.declare_media(source_mid, kind);
        direct_api.expect_stream_rx(Ssrc::from(ssrc), None, source_mid, None);
        let src_media = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: source_session.clone(),
            mid: source_mid,
        });
        let source = TransportSourceKey::new(source_session.clone(), src_media);
        let pause_revision = SourceActivityRevision::default().next();
        let resume_revision = pause_revision.next();
        assert!(state.routes.local_route(src_media).is_none());

        apply_source_activity(
            &mut state,
            &rtc_metrics,
            source.clone(),
            SourceActivityUpdate::new(ProducerActivity::Inactive, pause_revision),
            now,
        );
        apply_source_activity(
            &mut state,
            &rtc_metrics,
            source.clone(),
            SourceActivityUpdate::new(ProducerActivity::Active, resume_revision),
            now + Duration::from_millis(1),
        );

        assert!(state.routes.source_is_active(src_media));
        assert_eq!(
            drain_ready_sessions(&mut state),
            if expected_kf == 1 {
                vec![source_session]
            } else {
                vec![]
            }
        );
        assert_eq!(
            metrics.snapshot().rtc_keyframe_requests_forwarded(),
            expected_kf
        );

        apply_source_activity(
            &mut state,
            &rtc_metrics,
            source,
            SourceActivityUpdate::new(ProducerActivity::Inactive, pause_revision),
            now + Duration::from_millis(2),
        );
        assert!(state.routes.source_is_active(src_media));
        assert!(drain_ready_sessions(&mut state).is_empty());
    }
}

#[test]
fn consumer_follow_up_failures_are_isolated_and_classified() {
    let mut route = LocalVideoRoute::new(115, 88_001);
    let sibling_session = test_transport_session_key(115, 0, 116, UserId::Integer(116));
    let sibling_media = MediaWorkerScenario::new(&mut route.state).destination(
        route.src_media,
        sibling_session.clone(),
        Mid::from("cam-down-sibling"),
    );
    let failed = route.consumer_route();
    let sibling =
        TransportConsumerRoute::new(sibling_session, sibling_media, failed.source().clone());
    let missing = TransportConsumerRoute::new(
        failed.consumer_session_key().clone(),
        TransportMediaId::new(u64::MAX),
        failed.source().clone(),
    );
    route.state.set_consumer_dst_idx(
        failed.consumer_session_key(),
        Mid::from("cam-down"),
        failed.consumer_transport_media_id(),
        route.src_media,
        None,
    );
    let _ = drain_ready_sessions(&mut route.state);

    let controls = [
        ConsumerRouteControl::new(failed)
            .activity(ConsumerActivity::Inactive)
            .request_keyframe(true),
        ConsumerRouteControl::new(missing).request_keyframe(true),
        ConsumerRouteControl::new(sibling)
            .activity(ConsumerActivity::Active)
            .request_keyframe(true),
    ]
    .into_iter()
    .enumerate()
    .collect();
    let WorkerMediaControlBatchOutcome::Consumers(results) = apply_media_control_batch(
        &mut route.state,
        &route.rtc_metrics,
        Bitrate::from_mbps(10),
        Instant::now(),
        WorkerMediaControlBatch::ConsumerFollowUp(controls),
    ) else {
        panic!("consumer follow-up should return consumer outcomes");
    };
    let [activity_failed, keyframe_failed, sibling] = results.as_slice() else {
        panic!("consumer follow-up should preserve every result");
    };
    assert!(activity_failed.activity_failed());
    assert!(keyframe_failed.keyframe_failed());
    assert_eq!(
        [
            activity_failed.error(),
            keyframe_failed.error(),
            sibling.error(),
        ],
        [
            Some(TransportAdapterError::TransportUnavailable),
            Some(TransportAdapterError::TransportUnavailable),
            None,
        ]
    );
    route.assert_source_ready();
    let snapshot = route.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn open_consumer_keyframe_request_refreshes_simulcast_video_source() {
    let source_mid = Mid::from("cam-up");
    let mut route = LocalVideoRoute::with_rid(225, 88_201, Rid::from("lo"));
    add_source_rid_stream(
        &mut route.state,
        &route.source_session,
        source_mid,
        88_202,
        Rid::from("hi"),
    );
    let Some(source_session_state) = route.state.users.get_mut(&route.source_session) else {
        panic!("source session should exist after adding the high RID stream");
    };
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

    route.request_kf();
    route.assert_source_ready();
    assert_eq!(route.metrics.snapshot().rtc_route_control_forwarded(), 1);
}

#[test]
fn consumer_keyframe_requests_forward_remote_video_refreshes() {
    let selected_rid = Rid::from("hi");
    let routes = [
        (RemoteVideoRoute::new(125, 131, 11), None),
        (
            RemoteVideoRoute::with_gate(225, 231, 12, PacketLayerGate::Rid(selected_rid)),
            Some(selected_rid),
        ),
    ];
    for (mut route, expected_rid) in routes {
        assert_remote_packet_gate_command(
            &mut route.control_rx,
            &route.source_session,
            route.src_media,
            route.target_id,
            PacketLayerGate::Open,
        );
        route.request_kf();
        assert_remote_keyframe_command(
            &mut route.control_rx,
            &route.source_session,
            route.src_media,
            route.target_id,
            expected_rid,
        );
        assert_eq!(route.metrics.snapshot().rtc_route_control_forwarded(), 1);
    }
}

#[test]
fn set_consumer_pkt_gate_updates_one_route_without_rewriting_the_source_gate() {
    let source_session = test_source_session_key(131);
    let first_consumer_session = test_consumer_session_key(131);
    let second_consumer_session = test_transport_session_key(131, 0, 136, UserId::Integer(137));
    let source_mid = Mid::from("cam-up");
    let first_consumer_mid = Mid::from("cam-down-a");
    let second_consumer_mid = Mid::from("cam-down-b");
    let mut state = PacketLoopState::default();
    let src_media = prepare_source_session(&mut state, &source_session, source_mid, 88_889);
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let first_consumer_media = scenario.destination(
        src_media,
        first_consumer_session.clone(),
        first_consumer_mid,
    );
    scenario.destination(
        src_media,
        second_consumer_session.clone(),
        second_consumer_mid,
    );
    let observed_at = Instant::now();
    state
        .routes
        .observe_producer_packet(src_media, Some(Rid::from("lo")), false, observed_at);

    let route = TransportConsumerRoute::new(
        first_consumer_session.clone(),
        first_consumer_media,
        TransportSourceKey::new(source_session.clone(), src_media),
    );
    set_consumer_packet_gate_at(
        &mut state,
        &route,
        PacketLayerGate::Rid("lo".into()),
        observed_at + Duration::from_millis(20),
    );
    assert!(matches!(
        state.routes.local_route(src_media),
        Some(route_entry) if route_entry.destinations.iter().any(|destination| {
            destination.dest_session == first_consumer_session
                && destination.packet_gate == PacketLayerGate::Block
                && destination.pending_gate == Some(PacketLayerGate::Rid("lo".into()))
        })
    ));
    assert_eq!(
        state.active_consumer_kf_target(&first_consumer_session, first_consumer_mid, None,),
        Some(ConsumerKeyframeTarget {
            src_media,
            rid: Some(Rid::from("lo")),
        })
    );
    assert!(matches!(
        state.routes.local_route(src_media),
        Some(route_entry) if route_entry.destinations.iter().any(|destination| {
            destination.dest_session == second_consumer_session
                && destination.packet_gate == PacketLayerGate::Open
        })
    ));
    assert_eq!(
        state.routes.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );
}

#[test]
fn removing_consumer_route_clears_feedback_index_and_repairs_moved_route() {
    let first_consumer_session = test_transport_session_key(64, 1, 65, UserId::Integer(66));
    let second_consumer_session = test_transport_session_key(64, 1, 67, UserId::Integer(68));
    let first_consumer_mid = Mid::from("cam-down-first");
    let second_consumer_mid = Mid::from("cam-down-second");
    let src_media = TransportMediaId::new(64_100);
    let rid = Rid::from("hi");
    let mut state = PacketLoopState::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let first_consumer_media = scenario.destination(
        src_media,
        first_consumer_session.clone(),
        first_consumer_mid,
    );
    scenario.destination_with_gate(
        src_media,
        second_consumer_session.clone(),
        second_consumer_mid,
        PacketLayerGate::Rid(rid),
    );

    remove_consumer_route(
        &mut state,
        &first_consumer_session,
        first_consumer_media,
        src_media,
    );

    assert_eq!(
        state.active_consumer_kf_target(&first_consumer_session, first_consumer_mid, None,),
        None
    );
    assert_eq!(
        state.active_consumer_kf_target(&second_consumer_session, second_consumer_mid, None,),
        Some(ConsumerKeyframeTarget {
            src_media,
            rid: Some(rid),
        })
    );
}

#[test]
fn selected_rid_gate_waits_for_a_keyframe_despite_recent_packet_liveness() {
    let selected_rid = Rid::from("hi");
    let mut route = LocalVideoRoute::with_rid(531, 88_921, selected_rid);
    let observed_at = Instant::now();
    route.state.routes.observe_producer_packet(
        route.src_media,
        Some(selected_rid),
        false,
        observed_at,
    );

    let consumer_route = route.consumer_route();
    set_consumer_packet_gate_at(
        &mut route.state,
        &consumer_route,
        PacketLayerGate::Rid(selected_rid),
        observed_at + Duration::from_millis(500),
    );

    assert_consumer_packet_gate(
        &route.state,
        route.src_media,
        &route.consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid(selected_rid)),
    );

    set_consumer_packet_gate_at(
        &mut route.state,
        &consumer_route,
        PacketLayerGate::Rid(selected_rid),
        observed_at + Duration::from_secs(3),
    );

    assert_consumer_packet_gate(
        &route.state,
        route.src_media,
        &route.consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid(selected_rid)),
    );
}

#[test]
fn selected_rid_packet_gate_uses_bootstrap_fallback_before_becoming_strict() {
    let mut route = prepare_pending_selected_rid_route();
    route.assert_packet_gate(
        PacketLayerGate::Block,
        Some(PacketLayerGate::Rid(route.selected_rid)),
    );
    assert_eq!(
        route.state.routes.effective_packet_gate(route.src_media),
        Some(PacketLayerGate::Block)
    );

    let now = Instant::now();
    assert_eq!(
        route.state.routes.track_kf_req(
            route.src_media,
            None,
            KeyframeRequestKind::Pli,
            KeyframeRequestOrigin::ConsumerFeedback,
            now,
        ),
        KeyframeRequestDecision::Forward
    );

    assert!(!route.observe_rid_ready(route.fallback_rid, false, now));

    route.assert_packet_gate(
        PacketLayerGate::Block,
        Some(PacketLayerGate::Rid(route.selected_rid)),
    );
    assert_eq!(
        route.state.routes.effective_packet_gate(route.src_media),
        Some(PacketLayerGate::Block)
    );

    assert!(drain_ready_sessions(&mut route.state).is_empty());
    assert_eq!(route.metrics.snapshot().rtc_route_control_forwarded(), 0);

    assert!(route.observe_rid_ready(route.fallback_rid, true, now + Duration::from_millis(10)));

    route.assert_packet_gate(
        PacketLayerGate::Rid(route.fallback_rid),
        Some(PacketLayerGate::Rid(route.selected_rid)),
    );
    assert_eq!(
        route.state.routes.effective_packet_gate(route.src_media),
        Some(PacketLayerGate::Rid(route.fallback_rid))
    );
    assert_eq!(
        drain_ready_sessions(&mut route.state),
        vec![route.source_session.clone()]
    );
    assert_eq!(route.metrics.snapshot().rtc_route_control_forwarded(), 1);
}

#[test]
fn selected_rid_packet_gate_switches_from_bootstrap_fallback_on_selected_keyframe() {
    let mut route = prepare_pending_selected_rid_route();
    let now = Instant::now();

    assert!(route.observe_rid_ready(route.fallback_rid, true, now));

    assert!(!route.observe_rid_ready(route.selected_rid, false, now + Duration::from_millis(10)));
    route.assert_packet_gate(
        PacketLayerGate::Rid(route.fallback_rid),
        Some(PacketLayerGate::Rid(route.selected_rid)),
    );

    assert!(route.observe_rid_ready(route.selected_rid, true, now + Duration::from_millis(20)));
    route.assert_packet_gate(PacketLayerGate::Rid(route.selected_rid), None);
}

#[test]
fn selected_rid_packet_gate_blocks_when_selected_rid_goes_stale() {
    let selected_rid = Rid::from("hi");
    let fallback_rid = Rid::from("lo");
    let mut route = LocalVideoRoute::with_rid_gate(
        331,
        88_401,
        selected_rid,
        PacketLayerGate::Rid(selected_rid),
    );
    route.state.routes.refresh_src_pkt_gate(route.src_media);

    let now = Instant::now();
    let stale_observed_at = now
        .checked_sub(Duration::from_secs(3))
        .map_or(now, |observed_at| observed_at);
    route.state.routes.observe_producer_packet(
        route.src_media,
        Some(selected_rid),
        false,
        stale_observed_at,
    );

    assert!(route.observe_rid_ready(fallback_rid, false, now));

    route.assert_packet_gate(
        PacketLayerGate::Block,
        Some(PacketLayerGate::Rid(selected_rid)),
    );
    assert_eq!(
        route
            .state
            .active_consumer_kf_target(&route.consumer_session, Mid::from("cam-down"), None,),
        Some(ConsumerKeyframeTarget {
            src_media: route.src_media,
            rid: Some(selected_rid),
        })
    );
    assert_eq!(
        route.state.routes.effective_packet_gate(route.src_media),
        Some(PacketLayerGate::Block)
    );
    route.assert_source_ready();
    assert_eq!(route.metrics.snapshot().rtc_route_control_forwarded(), 1);

    assert!(!route.observe_rid_ready(selected_rid, false, now + Duration::from_millis(10)));
    route.assert_packet_gate(
        PacketLayerGate::Block,
        Some(PacketLayerGate::Rid(selected_rid)),
    );

    assert!(route.observe_rid_ready(selected_rid, true, now + Duration::from_millis(20)));

    route.assert_packet_gate(PacketLayerGate::Rid(selected_rid), None);
}

#[test]
fn batched_consumer_packet_gates_keep_remote_relay_open_during_rid_bootstrap() {
    let source_session = test_source_session_key(141);
    let first_consumer_session = test_consumer_session_key_on_worker(141, 1);
    let second_consumer_session = test_transport_session_key(141, 1, 146, UserId::Integer(147));
    let first_consumer_mid = Mid::from("cam-down-a");
    let second_consumer_mid = Mid::from("cam-down-b");
    let mut state = PacketLoopState::default();
    let rtc_metrics = RuntimeMetrics::default().register_rtc_worker();
    let src_media = TransportMediaId::new(41);
    let (command_tx, mut command_rx) = mpsc::channel(4);
    let source = TransportSourceKey::new(source_session.clone(), src_media);
    assert!(
        state
            .routes
            .register_remote_source(
                &source,
                RemoteSourceControl::new(
                    command_tx,
                    RelayTargetId::new(16),
                    Arc::clone(&rtc_metrics),
                ),
            )
            .is_ok()
    );
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let first_consumer_media = scenario.destination(
        src_media,
        first_consumer_session.clone(),
        first_consumer_mid,
    );
    let second_consumer_media = scenario.destination(
        src_media,
        second_consumer_session.clone(),
        second_consumer_mid,
    );
    let update = |index, session, media, rid: &str| {
        (
            index,
            TransportConsumerRoute::new(session, media, source.clone()),
            PacketLayerGate::Rid(rid.into()),
        )
    };

    let outcome = apply_media_control_batch(
        &mut state,
        &rtc_metrics,
        Bitrate::from_mbps(10),
        Instant::now(),
        WorkerMediaControlBatch::ConsumerGates {
            source: source.clone(),
            updates: vec![
                update(0, first_consumer_session, first_consumer_media, "lo"),
                update(1, second_consumer_session, second_consumer_media, "hi"),
            ],
        },
    );
    let WorkerMediaControlBatchOutcome::Applied(results) = outcome else {
        panic!("consumer gate batch should return applied results");
    };
    assert_eq!(results, vec![Ok(()), Ok(())]);
    let mut open_gate_commands = 0;
    loop {
        match command_rx.try_recv() {
            Ok(RtcWorkerCommand::RouteControl {
                request:
                    RouteControlRequest::SetRemoteSourcePacketGate {
                        source: forwarded_source,
                        target_id,
                        packet_gate,
                    },
                response: None,
            }) => {
                assert_eq!(forwarded_source.session_key(), &source_session);
                assert_eq!(forwarded_source.transport_media_id(), src_media);
                assert_eq!(target_id, RelayTargetId::new(16));
                assert_eq!(packet_gate, PacketLayerGate::Open);
                open_gate_commands += 1;
            }
            unexpected => {
                assert!(
                    matches!(
                        unexpected,
                        Err(mpsc::error::TryRecvError::Empty
                            | mpsc::error::TryRecvError::Disconnected)
                    ),
                    "expected only remote packet-gate commands"
                );
                break;
            }
        }
    }
    assert!(open_gate_commands > 0);
}

#[test]
fn explicit_consumer_block_still_blocks_remote_relay() {
    let mut route = RemoteVideoRoute::with_gate(141, 51, 17, PacketLayerGate::Block);

    route.state.routes.refresh_src_pkt_gate(route.src_media);

    assert_remote_packet_gate_command(
        &mut route.control_rx,
        &route.source_session,
        route.src_media,
        route.target_id,
        PacketLayerGate::Block,
    );
    assert!(route.control_rx.try_recv().is_err());
}

#[test]
fn selected_rid_relay_retries_saturated_readiness_keyframe() {
    let mut route = RemoteVideoRoute::with_gate(141, 61, 18, PacketLayerGate::Rid("lo".into()));

    route.state.routes.refresh_src_pkt_gate(route.src_media);

    assert_remote_packet_gate_command(
        &mut route.control_rx,
        &route.source_session,
        route.src_media,
        route.target_id,
        PacketLayerGate::Open,
    );
    let routes = &mut route.state.routes;
    routes.publish_remote_pkt_gate(route.src_media, PacketLayerGate::Block);
    route.request_kf();
    let snapshot = route.metrics.snapshot();
    assert_eq!(snapshot.rtc_remote_control_keyframe_drops(), 1);
    assert!(route.control_rx.try_recv().is_ok());
    let Some(deadline) = route.state.routes.next_kf_deadline() else {
        panic!("saturated keyframe request should remain scheduled");
    };
    let Some((source, control)) = route
        .state
        .routes
        .remote_source(route.src_media)
        .map(RemoteSourceRegistration::cloned_control_path)
    else {
        panic!("remote source should remain registered");
    };
    let mut retries = Vec::new();
    route.state.routes.drain_due_kf_reqs(deadline, &mut retries);
    let Some(retry) = retries.pop() else {
        panic!("saturated keyframe request should become due");
    };
    request_kf_for_target(
        &mut route.state,
        &route.rtc_metrics,
        KeyframeRequestTarget::Remote(&source, &control),
        retry.rid,
        retry.kind,
        KeyframeRequestMode::Retry,
    );
    assert_remote_keyframe_command(
        &mut route.control_rx,
        &route.source_session,
        route.src_media,
        route.target_id,
        Some("lo".into()),
    );
}

#[test]
fn mutual_remote_packet_gates_converge_after_mailbox_pressure_clears() {
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();
    let first_source = TransportSourceKey::new(
        test_transport_session_key(141, 0, 166, UserId::Integer(167)),
        TransportMediaId::new(62),
    );
    let second_source = TransportSourceKey::new(
        test_transport_session_key(141, 1, 168, UserId::Integer(169)),
        TransportMediaId::new(63),
    );
    let first_target = RelayTargetId::new(19);
    let second_target = RelayTargetId::new(20);
    let (first_tx, mut first_rx) = saturated_remote_control(&first_source, first_target);
    let (second_tx, mut second_rx) = saturated_remote_control(&second_source, second_target);

    let mut first_state = PacketLoopState::default();
    let mut second_state = PacketLoopState::default();
    register_remote_source_control(
        &mut first_state,
        &second_source,
        second_tx,
        second_target,
        Arc::clone(&rtc_metrics),
    );
    register_remote_source_control(
        &mut second_state,
        &first_source,
        first_tx,
        first_target,
        Arc::clone(&rtc_metrics),
    );
    let pending_gate = |state: &PacketLoopState, source: &TransportSourceKey| {
        state
            .routes
            .remote_source(source.transport_media_id())
            .and_then(RemoteSourceRegistration::pending_gate)
    };

    for (state, source) in [
        (&mut first_state, &second_source),
        (&mut second_state, &first_source),
    ] {
        state
            .routes
            .publish_remote_pkt_gate(source.transport_media_id(), PacketLayerGate::Block);
        state
            .routes
            .publish_remote_pkt_gate(source.transport_media_id(), PacketLayerGate::Open);
        assert_eq!(pending_gate(state, source), Some(PacketLayerGate::Open));
    }
    assert!(first_rx.try_recv().is_ok());
    assert!(second_rx.try_recv().is_ok());
    first_state.routes.flush_remote_pkt_gates();
    second_state.routes.flush_remote_pkt_gates();
    assert_remote_packet_gate_command(
        &mut first_rx,
        first_source.session_key(),
        first_source.transport_media_id(),
        first_target,
        PacketLayerGate::Open,
    );
    assert_remote_packet_gate_command(
        &mut second_rx,
        second_source.session_key(),
        second_source.transport_media_id(),
        second_target,
        PacketLayerGate::Open,
    );
    assert_eq!(pending_gate(&first_state, &second_source), None);
    assert_eq!(pending_gate(&second_state, &first_source), None);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_remote_control_packet_gate_drops(), 4);
    assert_eq!(snapshot.rtc_remote_packet_gate_retries(), 2);
    assert_eq!(snapshot.rtc_remote_packet_gate_flushes(), 2);
}

#[test]
fn flushed_remote_packet_gate_can_queue_again_under_later_pressure() {
    let source_session = test_transport_session_key(141, 0, 172, UserId::Integer(173));
    let mut state = PacketLoopState::default();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();
    let src_media = TransportMediaId::new(65);
    let target_id = RelayTargetId::new(22);
    let mut command_rx = register_saturated_remote_source(
        &mut state,
        src_media,
        &source_session,
        target_id,
        Arc::clone(&rtc_metrics),
    );

    state
        .routes
        .publish_remote_pkt_gate(src_media, PacketLayerGate::Block);
    assert!(command_rx.try_recv().is_ok());
    state.routes.flush_remote_pkt_gates();

    state
        .routes
        .publish_remote_pkt_gate(src_media, PacketLayerGate::Open);
    assert_remote_packet_gate_command(
        &mut command_rx,
        &source_session,
        src_media,
        target_id,
        PacketLayerGate::Block,
    );
    state.routes.flush_remote_pkt_gates();

    assert_remote_packet_gate_command(
        &mut command_rx,
        &source_session,
        src_media,
        target_id,
        PacketLayerGate::Open,
    );
    assert_eq!(
        state
            .routes
            .remote_source(src_media)
            .and_then(RemoteSourceRegistration::pending_gate),
        None
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_remote_control_packet_gate_drops(), 2);
    assert_eq!(snapshot.rtc_remote_packet_gate_retries(), 2);
    assert_eq!(snapshot.rtc_remote_packet_gate_flushes(), 2);
}

#[test]
fn remote_source_teardown_drops_pending_gate_state() {
    let source_session = test_transport_session_key(141, 0, 170, UserId::Integer(171));
    let mut state = PacketLoopState::default();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();
    let src_media = TransportMediaId::new(64);
    let _command_rx = register_saturated_remote_source(
        &mut state,
        src_media,
        &source_session,
        RelayTargetId::new(21),
        Arc::clone(&rtc_metrics),
    );

    state
        .routes
        .publish_remote_pkt_gate(src_media, PacketLayerGate::Block);
    assert!(
        state
            .routes
            .remote_source(src_media)
            .is_some_and(|registration| registration.pending_gate().is_some())
    );

    state.routes.prune_unrouted_remote_src(src_media);

    assert!(state.routes.remote_source(src_media).is_none());
    assert_eq!(metrics.snapshot().rtc_remote_control_packet_gate_drops(), 1);
}

#[test]
fn add_send_media_rolls_back_remote_source_registration_when_consumer_session_is_missing() {
    let source_session = test_source_session_key(151);
    let consumer_session = test_consumer_session_key_on_worker(151, 1);
    let mut state = PacketLoopState::default();
    let rtc_metrics = RuntimeMetrics::default().register_rtc_worker();
    let src_media = TransportMediaId::new(33);
    let (command_tx, _command_rx) = mpsc::channel(1);
    let remote_source_control =
        RemoteSourceControl::new(command_tx, RelayTargetId::new(10), rtc_metrics);
    let consumer_rtp_parameters = RouterRtpParameters::new(vec![], vec![], vec![]);
    let source = TransportSourceKey::new(source_session, src_media);

    let result = worker_add_send_media(
        &mut state,
        AddSendMediaRequest {
            consumer_key: &consumer_session,
            media_kind: MediaKind::Video,
            source: &source,
            remote_source_control: Some(remote_source_control),
            consumer_rtp_parameters: &consumer_rtp_parameters,
            active: true,
        },
    );

    assert_eq!(result, Err(TransportAdapterError::TransportUnavailable));
    assert!(state.routes.remote_source(src_media).is_none());
}

#[test]
fn add_send_media_declares_one_ridless_downstream_stream_for_simulcast_source() {
    let source_session = test_transport_session_key(151, 0, 156, UserId::Integer(157));
    let consumer_session = test_transport_session_key(151, 0, 158, UserId::Integer(159));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let mut state = PacketLoopState::default();
    let src_media = prepare_source_session(&mut state, &source_session, source_mid, 71_001);
    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &consumer_session,
            SocketAddr::from(([127, 0, 0, 1], 47_101)),
            Bitrate::from_mbps(10),
        )
        .is_ok()
    );
    let consumer_rtp_parameters = RouterRtpParameters::new(
        vec![MediaFormat::new(
            RouterMediaKind::Video,
            CodecName::Vp8,
            PayloadType::new(96),
            90_000,
        )],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(72_001)
                .with_rid("lo")
                .with_payload_type(PayloadType::new(96)),
            StreamBinding::new()
                .with_ssrc(72_002)
                .with_rid("hi")
                .with_payload_type(PayloadType::new(96)),
        ],
    )
    .with_mid(consumer_mid.to_string());
    let source = TransportSourceKey::new(source_session.clone(), src_media);

    assert!(
        worker_add_send_media(
            &mut state,
            AddSendMediaRequest {
                consumer_key: &consumer_session,
                media_kind: MediaKind::Video,
                source: &source,
                remote_source_control: None,
                consumer_rtp_parameters: &consumer_rtp_parameters,
                active: true,
            },
        )
        .is_ok()
    );

    let Some(consumer_session_state) = state.users.get_mut(&consumer_session) else {
        panic!("consumer session should exist after RTC state bootstrap");
    };
    let mut direct_api = consumer_session_state.rtc.direct_api();
    assert!(
        direct_api
            .stream_tx_by_mid(consumer_mid, None)
            .is_some_and(|stream| stream.rtx().is_none())
    );
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
            .routes
            .local_route(src_media)
            .is_some_and(
                |route_entry| route_entry.destinations.iter().any(|destination| {
                    destination.dest_transport_media_id == TransportMediaId::new(1)
                        && destination.dest_payload_type == Some(Pt::from(96))
                })
            )
    );
    assert_consumer_packet_gate(
        &state,
        src_media,
        &consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid("lo".into())),
    );
}

#[test]
fn add_send_media_blocks_initial_video_until_a_decoder_refresh() {
    let source_session = test_source_session_key(751);
    let consumer_session = test_consumer_session_key(751);
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let selected_rid = Rid::from("lo");
    let mut state = PacketLoopState::default();
    let src_media = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        71_101,
        Some(selected_rid),
    );
    let observed_at = Instant::now();
    state
        .routes
        .observe_producer_packet(src_media, Some(selected_rid), false, observed_at);
    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &consumer_session,
            SocketAddr::from(([127, 0, 0, 1], 47_102)),
            Bitrate::from_mbps(10),
        )
        .is_ok()
    );
    let consumer_rtp_parameters = RouterRtpParameters::new(
        vec![MediaFormat::new(
            RouterMediaKind::Video,
            CodecName::Vp8,
            PayloadType::new(96),
            90_000,
        )],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(72_101)
                .with_rid("lo")
                .with_payload_type(PayloadType::new(96)),
        ],
    )
    .with_mid(consumer_mid.to_string());
    let source = TransportSourceKey::new(source_session.clone(), src_media);

    assert!(
        worker_add_send_media(
            &mut state,
            AddSendMediaRequest {
                consumer_key: &consumer_session,
                media_kind: MediaKind::Video,
                source: &source,
                remote_source_control: None,
                consumer_rtp_parameters: &consumer_rtp_parameters,
                active: true,
            },
        )
        .is_ok()
    );

    assert_consumer_packet_gate(
        &state,
        src_media,
        &consumer_session,
        &PacketLayerGate::Block,
        Some(&PacketLayerGate::Rid(selected_rid)),
    );
}

#[test]
fn request_keyframe_ignores_wrong_source_owner() {
    let source_session = test_source_session_key(131);
    let wrong_session = test_consumer_session_key(131);
    let source_mid = Mid::from("cam-up");
    let mut state = PacketLoopState::default();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();
    let src_media = prepare_source_session(&mut state, &source_session, source_mid, 99_999);

    request_kf_for_target(
        &mut state,
        &rtc_metrics,
        KeyframeRequestTarget::Local(&wrong_session, src_media),
        None,
        KeyframeRequestKind::Pli,
        KeyframeRequestMode::Track {
            now: Instant::now(),
            origin: KeyframeRequestOrigin::ConsumerFeedback,
        },
    );

    assert!(drain_ready_sessions(&mut state).is_empty());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 0);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn remote_source_packet_gate_ignores_wrong_source_owner() {
    let source_session = test_source_session_key(141);
    let wrong_session = test_consumer_session_key(141);
    let source_mid = Mid::from("cam-up");
    let mut state = PacketLoopState::default();
    let rtc_metrics = RuntimeMetrics::default().register_rtc_worker();
    let src_media = prepare_source_session(&mut state, &source_session, source_mid, 101_010);

    apply_route_control_request(
        &mut state,
        &rtc_metrics,
        RouteControlRequest::SetRemoteSourcePacketGate {
            source: TransportSourceKey::new(wrong_session, src_media),
            target_id: RelayTargetId::new(9),
            packet_gate: PacketLayerGate::Rid("hi".into()),
        },
        None,
    );

    assert_eq!(state.routes.effective_packet_gate(src_media), None);
}
