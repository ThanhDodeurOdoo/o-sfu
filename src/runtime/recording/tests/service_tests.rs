use std::sync::Arc;

use o_sfu_router::{
    MediaKind, ProducerId, RouterEvent, SessionId as RouterSessionId, StreamType, TransportId,
};

use crate::runtime::recording::{
    MediaSource, MediaTap, RecordingLifecycleState, RecordingService, into_media_source,
};

#[test]
fn recording_service_allows_only_legal_state_machine_transitions() {
    let media_tap = Arc::new(MediaTap::default());
    let media_source = into_media_source(Arc::<MediaTap>::clone(&media_tap));
    let service = RecordingService::new(17, media_source);

    assert_eq!(service.snapshot().lifecycle, RecordingLifecycleState::Idle);
    assert!(service.start().is_ok());
    assert_eq!(
        service.snapshot().lifecycle,
        RecordingLifecycleState::Recording
    );
    assert!(media_tap.is_channel_active(17));

    let invalid_start = service.start();
    assert!(invalid_start.is_err());
    assert_eq!(
        invalid_start
            .err()
            .map(super::super::service::RecordingTransitionError::state),
        Some(RecordingLifecycleState::Recording)
    );

    assert!(service.stop().is_ok());
    assert_eq!(service.snapshot().lifecycle, RecordingLifecycleState::Idle);
    assert!(!media_tap.is_channel_active(17));
}

#[test]
fn recording_service_tracks_router_observer_inventory() {
    let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
    let service = RecordingService::new(22, media_source);
    let session_id = RouterSessionId(9);

    service.handle_router_event(RouterEvent::SessionJoined { session_id });
    service.handle_router_event(RouterEvent::ProducerAdded {
        session_id,
        transport_id: TransportId(3),
        producer_id: ProducerId(4),
        media_kind: MediaKind::Video,
        stream_type: StreamType::Camera,
    });

    let snapshot = service.snapshot();
    assert_eq!(snapshot.session_count, 1);
    assert_eq!(snapshot.producer_count, 1);

    service.handle_router_event(RouterEvent::ProducerRemoved {
        session_id,
        transport_id: TransportId(3),
        producer_id: ProducerId(4),
        media_kind: MediaKind::Video,
        stream_type: StreamType::Camera,
    });
    service.handle_router_event(RouterEvent::SessionLeft { session_id });

    let snapshot = service.snapshot();
    assert_eq!(snapshot.session_count, 0);
    assert_eq!(snapshot.producer_count, 0);
}
