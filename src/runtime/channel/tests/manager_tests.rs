use std::{collections::BTreeSet, time::Instant};

use super::fixtures::*;
use crate::{
    config::{MediaCodecFlags, RuntimeFeatureFlags},
    runtime::{
        diagnostics::DiagnosticsStore,
        metrics::RuntimeMetrics,
        recording::MediaTap,
        transport_adapter::{SourcePacketGate, TransportMediaId},
    },
};

async fn publish_audio_and_camera(
    channel: &Arc<super::super::Channel>,
    session_id: &SessionId,
    transport_adapter: &RuntimeTransportAdapter,
) {
    assert!(
        channel
            .test_api()
            .media()
            .publish_track(
                session_id,
                StreamType::Audio,
                MediaKind::Audio,
                test_audio_rtp_parameters(),
                transport_adapter,
            )
            .await
            .is_some()
    );
    assert!(
        channel
            .test_api()
            .media()
            .publish_track(
                session_id,
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                transport_adapter,
            )
            .await
            .is_some()
    );
}

async fn source_media_ids(
    channel: &Arc<super::super::Channel>,
    session_id: &SessionId,
) -> (TransportMediaId, TransportMediaId) {
    let Some(connection_id) = channel
        .test_api()
        .inspect()
        .session_connection_id(session_id)
        .await
    else {
        panic!("session should exist");
    };
    let Some(audio_media_id) = channel
        .test_api()
        .inspect()
        .producer_transport_media_id(session_id, connection_id, StreamType::Audio)
        .await
    else {
        panic!("audio producer should expose a transport media id");
    };
    let Some(camera_media_id) = channel
        .test_api()
        .inspect()
        .producer_transport_media_id(session_id, connection_id, StreamType::Camera)
        .await
    else {
        panic!("camera producer should expose a transport media id");
    };
    (audio_media_id, camera_media_id)
}

fn assert_source_packet_selection_update(
    events: &[FakeWebRtcEvent],
    session_id: &SessionId,
    transport_media_id: TransportMediaId,
    selection: Option<&str>,
) {
    assert!(events.iter().any(|event| match (event, selection) {
        (
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id: updated_session_id,
                transport_media_id: updated_media_id,
                packet_gate: SourcePacketGate::Open,
            },
            None,
        ) => updated_session_id == session_id && *updated_media_id == transport_media_id,
        (
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id: updated_session_id,
                transport_media_id: updated_media_id,
                packet_gate: SourcePacketGate::Rid(rid),
            },
            Some(expected_rid),
        ) => {
            updated_session_id == session_id
                && *updated_media_id == transport_media_id
                && rid == expected_rid
        }
        _ => false,
    }));
}

fn assert_featured_snapshot_update(
    messages: &[SessionOutbound],
    session_id: &SessionId,
    is_featured: bool,
) {
    assert!(messages.iter().any(|message| {
        matches!(
            message,
            SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(snapshot))
                if snapshot.get(session_id).is_some_and(|info| info.is_featured == Some(is_featured))
        )
    }));
}

#[tokio::test]
async fn channel_manager_is_idempotent_by_issuer() {
    let manager = ChannelManager::for_test();
    let config = ChannelConfig::default();
    let first = manager.serve_channel("issuer-a", None, &config, None).await;
    let second = manager
        .serve_channel("issuer-a", Some("ignored"), &config, None)
        .await;
    let third = manager.serve_channel("issuer-b", None, &config, None).await;
    assert_eq!(first.uuid(), second.uuid());
    assert_ne!(first.uuid(), third.uuid());
}

#[tokio::test]
async fn channel_manager_concurrent_create_attempts_publish_one_live_channel() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = Arc::new(ChannelManager::new(
        super::super::ChannelManagerConfig::new(
            1,
            super::super::ChannelRuntimePolicy::new(
                ChannelAdmissionPolicy::new(2),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        Arc::new(MediaTap::default()),
        Arc::new(DiagnosticsStore::default()),
        Arc::clone(&metrics),
    ));
    let config = ChannelConfig::default();

    let (first, second) = tokio::join!(
        manager.serve_channel("issuer-a", None, &config, None),
        manager.serve_channel("issuer-a", None, &config, None),
    );

    assert_eq!(first.uuid(), second.uuid());
    assert_eq!(metrics.snapshot().active_channels, 1);
}

#[tokio::test]
async fn channel_manager_assigns_media_workers_explicitly() {
    let manager = ChannelManager::for_test_with_media_workers(2);
    let first = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let second = manager
        .serve_channel("issuer-b", None, &ChannelConfig::default(), None)
        .await;
    let third = manager
        .serve_channel("issuer-c", None, &ChannelConfig::default(), None)
        .await;

    assert_eq!(first.test_api().inspect().media_worker_id(), 0);
    assert_eq!(second.test_api().inspect().media_worker_id(), 1);
    assert_eq!(third.test_api().inspect().media_worker_id(), 0);
}

#[tokio::test]
async fn channel_manager_lookup_by_uuid() {
    let manager = ChannelManager::for_test();
    let channel = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let fetched = manager.get_by_uuid(channel.uuid()).await;
    assert!(fetched.is_some());
    assert_eq!(
        fetched.map(|channel| channel.uuid().to_owned()),
        Some(channel.uuid().to_owned())
    );
    assert!(manager.get_by_uuid("nonexistent").await.is_none());
}

#[tokio::test]
async fn channel_manager_join_session_reports_missing_channel() {
    let manager = ChannelManager::for_test_with_admission_policy(ChannelAdmissionPolicy::new(1));
    let transport_adapter = RuntimeTransportAdapter::fake_for_testing();
    let (tx, _rx) = test_sender();
    let result = manager
        .join_session_for_test(
            "missing-channel",
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: tx,
            },
            &transport_adapter,
        )
        .await;
    assert!(matches!(
        result,
        Err(ChannelManagerJoinError::MissingChannel)
    ));
}

#[tokio::test]
async fn manager_leave_session_removes_empty_channel() {
    let manager = ChannelManager::for_test_with_admission_policy(ChannelAdmissionPolicy::new(1));
    let transport_adapter = RuntimeTransportAdapter::fake_for_testing();
    let first_channel = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let channel_uuid = first_channel.uuid().to_owned();
    let (tx, _rx) = test_sender();
    let joined = manager
        .join_session_for_test(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: tx,
            },
            &transport_adapter,
        )
        .await;
    assert!(joined.is_ok());
    let Some((_channel, connection_id)) = joined.ok() else {
        return;
    };

    manager
        .leave_session_for_test(
            &channel_uuid,
            &SessionId::Integer(1),
            connection_id,
            &transport_adapter,
        )
        .await;

    assert!(manager.get_by_uuid(&channel_uuid).await.is_none());
    let replacement = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), channel_uuid);
}

#[tokio::test]
async fn manager_disconnect_sessions_removes_empty_channel() {
    let manager = ChannelManager::for_test_with_admission_policy(ChannelAdmissionPolicy::new(1));
    let transport_adapter = RuntimeTransportAdapter::fake_for_testing();
    let first_channel = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let channel_uuid = first_channel.uuid().to_owned();
    let (tx, _rx) = test_sender();
    let joined = manager
        .join_session_for_test(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: tx,
            },
            &transport_adapter,
        )
        .await;
    assert!(joined.is_ok());

    manager
        .disconnect_sessions_for_test(&channel_uuid, &[SessionId::Integer(1)], &transport_adapter)
        .await;

    assert!(manager.get_by_uuid(&channel_uuid).await.is_none());
    let replacement = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), channel_uuid);
}

#[tokio::test]
async fn manager_concurrent_empty_room_cleanup_decrements_metrics_once() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = Arc::new(ChannelManager::new(
        super::super::ChannelManagerConfig::new(
            1,
            super::super::ChannelRuntimePolicy::new(
                ChannelAdmissionPolicy::new(1),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        Arc::new(MediaTap::default()),
        Arc::new(DiagnosticsStore::default()),
        Arc::clone(&metrics),
    ));
    let transport_adapter = RuntimeTransportAdapter::fake_for_testing();
    let channel = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let channel_uuid = channel.uuid().to_owned();
    let (tx, _rx) = test_sender();
    let joined = manager
        .join_session_for_test(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: tx,
            },
            &transport_adapter,
        )
        .await;
    assert!(joined.is_ok());
    assert_eq!(metrics.snapshot().active_channels, 1);
    assert_eq!(metrics.snapshot().active_sessions, 1);

    let first_session_ids = [SessionId::Integer(1)];
    let second_session_ids = [SessionId::Integer(1)];
    let manager_ref = Arc::clone(&manager);
    let transport_ref = transport_adapter.clone();
    let first_cleanup = async {
        manager_ref
            .disconnect_sessions_for_test(&channel_uuid, &first_session_ids, &transport_ref)
            .await;
    };
    let second_cleanup = async {
        manager
            .disconnect_sessions_for_test(&channel_uuid, &second_session_ids, &transport_adapter)
            .await;
    };

    tokio::join!(first_cleanup, second_cleanup);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_channels, 0);
    assert_eq!(snapshot.active_sessions, 0);
    assert!(manager.get_by_uuid(&channel_uuid).await.is_none());
}

#[tokio::test]
async fn manager_metrics_track_live_channels_and_sessions_without_replacement_drift() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = ChannelManager::new(
        super::super::ChannelManagerConfig::new(
            1,
            super::super::ChannelRuntimePolicy::new(
                ChannelAdmissionPolicy::new(2),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        Arc::new(MediaTap::default()),
        Arc::new(DiagnosticsStore::default()),
        Arc::clone(&metrics),
    );
    let transport_adapter = RuntimeTransportAdapter::fake_for_testing();
    let channel = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let channel_uuid = channel.uuid().to_owned();
    assert_eq!(metrics.snapshot().active_channels, 1);

    let (first_tx, _first_rx) = test_sender();
    let first_join = manager
        .join_session_for_test(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: first_tx,
            },
            &transport_adapter,
        )
        .await;
    assert!(first_join.is_ok());
    assert_eq!(metrics.snapshot().active_sessions, 1);

    let (replacement_tx, _replacement_rx) = test_sender();
    let replacement_join = manager
        .join_session_for_test(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: Some(String::from("replacement")),
                permissions: SessionPermissions::default(),
                sender: replacement_tx,
            },
            &transport_adapter,
        )
        .await;
    assert!(replacement_join.is_ok());
    assert_eq!(metrics.snapshot().active_sessions, 1);

    manager
        .disconnect_sessions_for_test(&channel_uuid, &[SessionId::Integer(1)], &transport_adapter)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_channels, 0);
    assert_eq!(snapshot.active_sessions, 0);
}

#[tokio::test]
async fn manager_metrics_track_live_media_totals_across_publish_and_disconnect() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = ChannelManager::new(
        super::super::ChannelManagerConfig::new(
            1,
            super::super::ChannelRuntimePolicy::new(
                ChannelAdmissionPolicy::new(2),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        Arc::new(MediaTap::default()),
        Arc::new(DiagnosticsStore::default()),
        Arc::clone(&metrics),
    );
    let transport_adapter = RuntimeTransportAdapter::fake_for_testing();
    let channel = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let channel_uuid = channel.uuid().to_owned();

    for raw_session_id in [1_i64, 2_i64] {
        let (sender, _receiver) = test_sender();
        let joined = manager
            .join_session_for_test(
                &channel_uuid,
                JoinSessionRequest {
                    session_id: SessionId::Integer(raw_session_id),
                    label: None,
                    permissions: SessionPermissions::default(),
                    sender,
                },
                &transport_adapter,
            )
            .await;
        assert!(joined.is_ok(), "session {raw_session_id} should join");
        make_session_ready(&channel, &SessionId::Integer(raw_session_id)).await;
    }

    assert!(
        channel
            .test_api()
            .media()
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &transport_adapter,
            )
            .await
            .is_some()
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_channels, 1);
    assert_eq!(snapshot.active_sessions, 2);
    assert_eq!(snapshot.active_publications, 1);
    assert_eq!(snapshot.active_subscriptions, 1);

    manager
        .disconnect_sessions_for_test(&channel_uuid, &[SessionId::Integer(1)], &transport_adapter)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_channels, 1);
    assert_eq!(snapshot.active_sessions, 1);
    assert_eq!(snapshot.active_publications, 0);
    assert_eq!(snapshot.active_subscriptions, 0);

    manager
        .disconnect_sessions_for_test(&channel_uuid, &[SessionId::Integer(2)], &transport_adapter)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_channels, 0);
    assert_eq!(snapshot.active_sessions, 0);
    assert_eq!(snapshot.active_publications, 0);
    assert_eq!(snapshot.active_subscriptions, 0);
}

#[tokio::test]
async fn manager_syncs_active_speaker_camera_policy_without_room_mutations() {
    let manager = ChannelManager::for_test();
    let transport_adapter = RuntimeTransportAdapter::fake_for_testing();
    let fake = transport_adapter
        .as_fake_adapter()
        .expect("test expects the fake transport adapter");
    let channel = manager
        .serve_channel("issuer-a", None, &ChannelConfig::default(), None)
        .await;

    let mut receivers = Vec::new();
    for raw_session_id in [1_i64, 2_i64, 3_i64] {
        let (sender, receiver) = test_sender();
        receivers.push(receiver);
        channel
            .test_api()
            .lifecycle()
            .join_session(
                SessionId::Integer(raw_session_id),
                None,
                SessionPermissions::default(),
                sender,
            )
            .await
            .expect("session join should succeed");
        make_session_ready(&channel, &SessionId::Integer(raw_session_id)).await;
    }
    for raw_session_id in [1_i64, 2_i64] {
        publish_audio_and_camera(
            &channel,
            &SessionId::Integer(raw_session_id),
            &transport_adapter,
        )
        .await;
    }
    for receiver in &mut receivers {
        let _ = drain_outbound(receiver);
    }

    let (_first_audio_media_id, first_camera_media_id) =
        source_media_ids(&channel, &SessionId::Integer(1)).await;
    let (second_audio_media_id, second_camera_media_id) =
        source_media_ids(&channel, &SessionId::Integer(2)).await;

    let baseline_event_count = fake.snapshot_events().len();
    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        second_audio_media_id,
        Instant::now(),
    )]);

    manager
        .sync_source_packet_selection_policies_for_runtime_ids(
            &BTreeSet::from([channel.instance_id()]),
            &transport_adapter,
            &transport_adapter,
        )
        .await;

    let events = fake.snapshot_events();
    let policy_events = &events[baseline_event_count..];
    let featured_messages = drain_outbound(&mut receivers[0]);
    assert_source_packet_selection_update(
        policy_events,
        &SessionId::Integer(2),
        second_camera_media_id,
        None,
    );
    assert!(!policy_events.iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id,
                transport_media_id,
                packet_gate: SourcePacketGate::Open,
            } if *session_id == SessionId::Integer(1)
                && *transport_media_id == first_camera_media_id
        )
    }));
    assert_featured_snapshot_update(&featured_messages, &SessionId::Integer(1), false);
    assert_featured_snapshot_update(&featured_messages, &SessionId::Integer(2), true);
}
