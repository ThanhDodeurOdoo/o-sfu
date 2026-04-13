use super::fixtures::*;

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
    let transport_adapter = RuntimeTransportAdapter::stub();
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
    let transport_adapter = RuntimeTransportAdapter::stub();
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
    let transport_adapter = RuntimeTransportAdapter::stub();
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
        )
        .await;
    assert!(joined.is_ok());

    manager
        .disconnect_sessions(&channel_uuid, &[SessionId::Integer(1)], &transport_adapter)
        .await;

    assert!(manager.get_by_uuid(&channel_uuid).await.is_none());
    let replacement = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), channel_uuid);
}
