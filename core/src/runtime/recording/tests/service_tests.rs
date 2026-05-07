use std::sync::Arc;

use o_sfu_router::{MediaKind, ProducerId, RouterEvent, SessionId as RouterSessionId, TransportId};

use crate::runtime::{
    RoomInstanceId, UserId as SignalingSessionId,
    media_transport::TransportMediaId,
    metrics::RuntimeMetrics,
    recording::{
        MediaSource, MediaTap, RecordingService,
        test_support::{RecordingLifecycleState, is_room_active, transition_error_state},
    },
    router_events::RoomRouterEventSink,
    rtc_engine::test_support::{sample_forwarded_packet, test_transport_session_key},
};

#[test]
fn recording_service_counts_packets_without_recounting_streams() {
    let media_tap = Arc::new(MediaTap::default());
    let media_source = Arc::<MediaTap>::clone(&media_tap);
    let media_source: Arc<dyn MediaSource> = media_source;
    let metrics = Arc::new(RuntimeMetrics::default());
    let service = RecordingService::new(
        RoomInstanceId::from_raw(30),
        media_source,
        Arc::clone(&metrics),
    );
    let session_key = test_transport_session_key(30, 0, 1, SignalingSessionId::Integer(9));
    let first_packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"first");
    let second_packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"second");
    let third_packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"third");
    let ignored_packet = sample_forwarded_packet(session_key, "aud-up", b"ignored");

    assert!(service.start().is_ok());
    media_tap.write_packet(&first_packet, TransportMediaId::new(1));
    media_tap.write_packet(&second_packet, TransportMediaId::new(1));
    media_tap.write_packet(&third_packet, TransportMediaId::new(2));

    let snapshot = service.snapshot();
    assert_eq!(snapshot.captured_packet_count, 3);
    assert_eq!(snapshot.captured_stream_count, 2);
    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_captured_packets(), 3);
    assert_eq!(metrics_snapshot.recording_captured_streams(), 2);

    assert!(service.stop().is_ok());
    media_tap.write_packet(&ignored_packet, TransportMediaId::new(3));
    let snapshot = service.snapshot();
    assert_eq!(snapshot.captured_packet_count, 3);
    assert_eq!(snapshot.captured_stream_count, 2);
}

#[test]
fn recording_service_allows_only_legal_state_machine_transitions() {
    let media_tap = Arc::new(MediaTap::default());
    let media_source = Arc::<MediaTap>::clone(&media_tap);
    let media_source: Arc<dyn MediaSource> = media_source;
    let service = RecordingService::new(
        RoomInstanceId::from_raw(17),
        media_source,
        Arc::new(RuntimeMetrics::default()),
    );

    assert_eq!(service.snapshot().lifecycle, RecordingLifecycleState::Idle);
    assert!(service.start().is_ok());
    assert_eq!(
        service.snapshot().lifecycle,
        RecordingLifecycleState::Recording
    );
    assert!(is_room_active(&media_tap, RoomInstanceId::from_raw(17)));

    let invalid_start = service.start();
    assert!(invalid_start.is_err());
    assert_eq!(
        invalid_start.err().map(transition_error_state),
        Some(RecordingLifecycleState::Recording)
    );

    assert!(service.stop().is_ok());
    assert_eq!(service.snapshot().lifecycle, RecordingLifecycleState::Idle);
    assert!(!is_room_active(&media_tap, RoomInstanceId::from_raw(17)));
}

#[test]
fn recording_service_tracks_router_event_sink_inventory() {
    let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
    let service = RecordingService::new(
        RoomInstanceId::from_raw(22),
        media_source,
        Arc::new(RuntimeMetrics::default()),
    );
    let user_id = RouterSessionId(9);

    RoomRouterEventSink::handle_room_router_event(
        &service,
        RouterEvent::SessionJoined {
            session_id: user_id,
        },
    );
    RoomRouterEventSink::handle_room_router_event(
        &service,
        RouterEvent::ProducerAdded {
            session_id: user_id,
            transport_id: TransportId(3),
            producer_id: ProducerId(4),
            media_kind: MediaKind::Video,
        },
    );

    let snapshot = service.snapshot();
    assert_eq!(snapshot.user_count, 1);
    assert_eq!(snapshot.producer_count, 1);

    RoomRouterEventSink::handle_room_router_event(
        &service,
        RouterEvent::ProducerRemoved {
            session_id: user_id,
            transport_id: TransportId(3),
            producer_id: ProducerId(4),
            media_kind: MediaKind::Video,
        },
    );
    RoomRouterEventSink::handle_room_router_event(
        &service,
        RouterEvent::SessionLeft {
            session_id: user_id,
        },
    );

    let snapshot = service.snapshot();
    assert_eq!(snapshot.user_count, 0);
    assert_eq!(snapshot.producer_count, 0);
}
