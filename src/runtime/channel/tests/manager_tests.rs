use super::fixtures::*;
use crate::config::{MediaCodecFlags, RuntimeFeatureFlags};
use crate::runtime::{metrics::RuntimeMetrics, recording::MediaTap};

#[tokio::test]
async fn channel_manager_is_idempotent_by_issuer() {
    let manager = ChannelManager::for_test();
    let config = ChannelConfig::default();
    let first = manager.create_or_get("issuer-a", None, &config, None).await;
    let second = manager
        .create_or_get("issuer-a", Some("ignored"), &config, None)
        .await;
    let third = manager.create_or_get("issuer-b", None, &config, None).await;
    assert_eq!(first.uuid(), second.uuid());
    assert_ne!(first.uuid(), third.uuid());
}

#[tokio::test]
async fn channel_manager_assigns_media_workers_explicitly() {
    let manager = ChannelManager::for_test_with_media_workers(2);
    let first = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let second = manager
        .create_or_get("issuer-b", None, &ChannelConfig::default(), None)
        .await;
    let third = manager
        .create_or_get("issuer-c", None, &ChannelConfig::default(), None)
        .await;

    assert_eq!(first.media_worker_id(), 0);
    assert_eq!(second.media_worker_id(), 1);
    assert_eq!(third.media_worker_id(), 0);
}

#[tokio::test]
async fn channel_manager_lookup_by_uuid() {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
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
    let transport_adapter = RuntimeTransportAdapter::builder().stub().build();
    let (tx, _rx) = test_sender();
    let result = manager
        .join_session(
            "missing-channel",
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: tx,
            },
            &transport_adapter,
            super::super::SessionCleanupPolicy::StateOnly,
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
    let transport_adapter = RuntimeTransportAdapter::builder().stub().build();
    let first_channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let channel_uuid = first_channel.uuid().to_owned();
    let (tx, _rx) = test_sender();
    let joined = manager
        .join_session(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: tx,
            },
            &transport_adapter,
            super::super::SessionCleanupPolicy::StateOnly,
        )
        .await;
    assert!(joined.is_ok());
    let Some((_channel, connection_id)) = joined.ok() else {
        return;
    };

    manager
        .leave_session(
            &channel_uuid,
            &SessionId::Integer(1),
            connection_id,
            &transport_adapter,
            super::super::SessionCleanupPolicy::StateOnly,
        )
        .await;

    assert!(manager.get_by_uuid(&channel_uuid).await.is_none());
    let replacement = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), channel_uuid);
}

#[tokio::test]
async fn manager_disconnect_sessions_removes_empty_channel() {
    let manager = ChannelManager::for_test_with_admission_policy(ChannelAdmissionPolicy::new(1));
    let transport_adapter = RuntimeTransportAdapter::builder().stub().build();
    let first_channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let channel_uuid = first_channel.uuid().to_owned();
    let (tx, _rx) = test_sender();
    let joined = manager
        .join_session(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: tx,
            },
            &transport_adapter,
            super::super::SessionCleanupPolicy::StateOnly,
        )
        .await;
    assert!(joined.is_ok());

    manager
        .disconnect_sessions(
            &channel_uuid,
            &[SessionId::Integer(1)],
            &transport_adapter,
            super::super::SessionCleanupPolicy::StateOnly,
        )
        .await;

    assert!(manager.get_by_uuid(&channel_uuid).await.is_none());
    let replacement = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), channel_uuid);
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
        Arc::clone(&metrics),
    );
    let transport_adapter = RuntimeTransportAdapter::builder().stub().build();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let channel_uuid = channel.uuid().to_owned();
    assert_eq!(metrics.snapshot().active_channels, 1);

    let (first_tx, _first_rx) = test_sender();
    let first_join = manager
        .join_session(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: None,
                permissions: SessionPermissions::default(),
                sender: first_tx,
            },
            &transport_adapter,
            super::super::SessionCleanupPolicy::StateOnly,
        )
        .await;
    assert!(first_join.is_ok());
    assert_eq!(metrics.snapshot().active_sessions, 1);

    let (replacement_tx, _replacement_rx) = test_sender();
    let replacement_join = manager
        .join_session(
            &channel_uuid,
            JoinSessionRequest {
                session_id: SessionId::Integer(1),
                label: Some(String::from("replacement")),
                permissions: SessionPermissions::default(),
                sender: replacement_tx,
            },
            &transport_adapter,
            super::super::SessionCleanupPolicy::StateOnly,
        )
        .await;
    assert!(replacement_join.is_ok());
    assert_eq!(metrics.snapshot().active_sessions, 1);

    manager
        .disconnect_sessions(
            &channel_uuid,
            &[SessionId::Integer(1)],
            &transport_adapter,
            super::super::SessionCleanupPolicy::StateOnly,
        )
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_channels, 0);
    assert_eq!(snapshot.active_sessions, 0);
}
