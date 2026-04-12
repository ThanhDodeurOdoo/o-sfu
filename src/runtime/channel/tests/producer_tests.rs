use super::fixtures::*;

#[tokio::test]
async fn production_change_pauses_producer_and_broadcasts_info() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

    // Session 1 publishes a camera track.
    let producer_id = channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    assert!(producer_id.is_some());

    // Drain the INIT_CONSUMER bootstrap that went to session 2.
    let bootstrap_msgs = drain_outbound(&mut rx2);
    assert!(
        bootstrap_msgs
            .iter()
            .any(|m| matches!(m, SessionOutbound::Request(..))),
        "session 2 should have received a bootstrap remote track request"
    );
    // Session 1 shouldn't get its own consumer.
    assert!(drain_outbound(&mut rx1).is_empty());

    // Now session 1 sends PRODUCTION_CHANGE: camera off (pause).
    channel
        .update_upload_state(&SessionId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    // Both sessions should receive a session info broadcast with isCameraOn = false.
    let msgs1 = drain_outbound(&mut rx1);
    let msgs2 = drain_outbound(&mut rx2);
    assert_eq!(msgs1.len(), 1, "session 1 should get info broadcast");
    assert_eq!(msgs2.len(), 1, "session 2 should get info broadcast");

    // Verify the broadcast contains isCameraOn = false.
    let info_msg = &msgs1[0];
    if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) = info_msg {
        let info = snapshot
            .values()
            .next()
            .expect("snapshot should have one entry");
        assert_eq!(info.is_camera_on, Some(false));
    } else {
        panic!("expected SessionInfoChanged, got {info_msg:?}");
    }

    // Resume: session 1 sends PRODUCTION_CHANGE: camera on.
    channel
        .update_upload_state(&SessionId::Integer(1), StreamType::Camera, true, &adapter)
        .await;

    let msgs1 = drain_outbound(&mut rx1);
    if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) = &msgs1[0]
    {
        let info = snapshot.values().next().unwrap();
        assert_eq!(info.is_camera_on, Some(true));
    } else {
        panic!("expected SessionInfoChanged after resume");
    }
}

#[tokio::test]
async fn publish_track_uses_negotiated_consumer_rtp_parameters() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;
    assert!(
        channel
            .set_client_rtp_capabilities(
                &SessionId::Integer(2),
                test_client_rtp_capabilities_without_video_rtx(),
            )
            .await
            .session_present
    );

    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    let request = drain_outbound(&mut rx2)
        .into_iter()
        .find_map(|message| match message {
            SessionOutbound::Request(request) => Some(*request),
            SessionOutbound::Message(_) | SessionOutbound::Close(_) => None,
        })
        .expect("subscriber should receive INIT_CONSUMER");
    let CurrentServerRequest::BootstrapRemoteTrack(payload) = request else {
        panic!("expected INIT_CONSUMER request");
    };
    let codecs = payload
        .rtp_parameters
        .0
        .get("codecs")
        .and_then(serde_json::Value::as_array)
        .expect("consumer bootstrap should include codecs");
    assert_eq!(codecs.len(), 1);
    assert_eq!(codecs[0].get("mimeType"), Some(&json!("video/VP8")));
    assert_eq!(codecs[0].get("payloadType"), Some(&json!(96)));
}

#[tokio::test]
async fn session_replacement_purges_stale_published_media_state() {
    let (channel, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_sessions().await;

    let producer_id = channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    assert!(producer_id.is_some());
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, SessionOutbound::Request(_)))
    );

    {
        let state = channel.state.read().await;
        assert_eq!(state.producers.len(), 1);
        assert_eq!(state.consumer_index.len(), 1);
        drop(state);
    }

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                replacement_tx,
                10,
            )
            .await
            .is_ok()
    );

    {
        let state = channel.state.read().await;
        assert!(state.producers.is_empty());
        assert!(state.consumer_index.is_empty());
        drop(state);
    }
}

#[tokio::test]
async fn publish_track_releases_channel_lock_while_waiting_on_transport_adapter() {
    let (channel, _adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;
    let (stub_adapter, _) = stub_adapter();
    let RuntimeTransportAdapter::Stub(stub) = &stub_adapter else {
        panic!("expected stub transport adapter");
    };
    stub.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = stub_adapter.clone();
        async move {
            channel
                .publish_track(
                    &SessionId::Integer(1),
                    StreamType::Camera,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_stub_event(stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::PublishMediaRequested {
                session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        channel
            .update_session_info(
                &SessionId::Integer(2),
                SessionInfo {
                    is_talking: Some(true),
                    ..SessionInfo::default()
                },
                false,
            )
            .await;
    })
    .await;
    assert!(
        update_result.is_ok(),
        "session info update should not wait for publish transport declaration"
    );

    assert!(publish_task.await.unwrap().is_some());
    assert!(
        drain_outbound(&mut rx1).iter().any(|msg| matches!(
            msg,
            SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut rx2).iter().any(|msg| matches!(
            msg,
            SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
        )),
        "peer should still receive the concurrent info broadcast"
    );
}

#[tokio::test]
async fn publish_track_defers_producer_commit_until_transport_publish_succeeds() {
    let (channel, adapter, stub, _rx1, _rx2) = setup_two_ready_sessions_with_stub().await;
    stub.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = adapter.clone();
        async move {
            channel
                .publish_track(
                    &SessionId::Integer(1),
                    StreamType::Camera,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::PublishMediaRequested {
                session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    {
        let state = channel.state.read().await;
        assert!(state.producers.is_empty());
        drop(state);
    }

    assert!(publish_task.await.unwrap().is_some());

    {
        let state = channel.state.read().await;
        assert_eq!(state.producers.len(), 1);
        assert!(
            state
                .producers
                .values()
                .all(|producer| producer.transport_media_id.is_some())
        );
        drop(state);
    }
}

#[tokio::test]
async fn publish_track_cleans_up_transport_media_when_session_leaves_mid_publish() {
    let (channel, adapter, stub, _rx1, _rx2) = setup_two_ready_sessions_with_stub().await;
    stub.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = adapter.clone();
        async move {
            channel
                .publish_track(
                    &SessionId::Integer(1),
                    StreamType::Camera,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::PublishMediaRequested {
                session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(channel.leave_session(&SessionId::Integer(1), 0).await);
    assert!(publish_task.await.unwrap().is_none());

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::MediaRemoved {
                session_id: SessionId::Integer(1),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_updates_screen_sharing_info() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Screen,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;

    // Drain bootstrap messages.
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // Pause screen sharing.
    channel
        .update_upload_state(&SessionId::Integer(1), StreamType::Screen, false, &adapter)
        .await;

    let msgs = drain_outbound(&mut rx1);
    if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) = &msgs[0] {
        let info = snapshot.values().next().unwrap();
        assert_eq!(info.is_screen_sharing_on, Some(false));
    } else {
        panic!("expected SessionInfoChanged for screen sharing");
    }
}

#[tokio::test]
async fn production_change_updates_transport_route_activity() {
    let (channel, adapter, stub, mut rx1, mut rx2) = setup_two_ready_sessions_with_stub().await;

    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    channel
        .update_upload_state(&SessionId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ProducerActivityUpdated {
                session_id: SessionId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn late_join_bootstrap_releases_channel_lock_while_waiting_on_transport_adapter() {
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    stub.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_late_join_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        channel
            .update_session_info(
                &SessionId::Integer(1),
                SessionInfo {
                    is_talking: Some(true),
                    ..SessionInfo::default()
                },
                false,
            )
            .await;
    })
    .await;
    assert!(
        update_result.is_ok(),
        "session info update should not wait for late-join consumer declaration"
    );

    bootstrap_task.await.unwrap();
    assert!(
        drain_outbound(&mut publisher_rx).iter().any(|msg| matches!(
            msg,
            SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|msg| matches!(
                msg,
                SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
                    | SessionOutbound::Request(_)
            )),
        "late joiner should receive outbound traffic while bootstrap is running"
    );
}

#[tokio::test]
async fn late_join_bootstrap_defers_consumer_commit_until_transport_consume_succeeds() {
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    stub.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_late_join_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    {
        let state = channel.state.read().await;
        assert!(state.consumer_index.is_empty());
        drop(state);
    }

    bootstrap_task.await.unwrap();

    {
        let state = channel.state.read().await;
        assert_eq!(state.consumer_index.len(), 1);
        drop(state);
    }
}

#[tokio::test]
async fn late_join_bootstrap_cleans_up_transport_media_when_session_leaves_mid_consume() {
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    stub.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_late_join_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(channel.leave_session(&SessionId::Integer(2), 1).await);
    bootstrap_task.await.unwrap();

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::MediaRemoved {
                session_id: SessionId::Integer(2),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_ignores_unknown_stream_type() {
    let (channel, adapter, mut rx1, mut _rx2) = setup_two_ready_sessions().await;

    // No producer published for audio. PRODUCTION_CHANGE should be a no-op.
    channel
        .update_upload_state(&SessionId::Integer(1), StreamType::Audio, false, &adapter)
        .await;

    assert!(
        drain_outbound(&mut rx1).is_empty(),
        "no broadcast expected when no producer exists for the stream type"
    );
}

#[tokio::test]
async fn client_capabilities_bootstrap_late_join_when_download_connected_first() {
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let download_update = channel
        .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
        .await;
    assert!(download_update.session_present);
    assert!(!download_update.became_consumer_ready);

    assert!(
        channel
            .apply_client_rtp_capabilities(
                &SessionId::Integer(2),
                test_client_rtp_capabilities(),
                &transport_adapter,
            )
            .await
    );
    assert!(
        channel
            .session_has_parsed_client_rtp_capabilities(&SessionId::Integer(2))
            .await
    );

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, SessionOutbound::Request(_))),
        "subscriber should receive a consumer bootstrap after capabilities make it ready"
    );
}

#[tokio::test]
async fn transport_connect_bootstrap_late_join_when_capabilities_arrive_first() {
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let capabilities_update = channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    assert!(capabilities_update.session_present);
    assert!(!capabilities_update.became_consumer_ready);
    assert!(
        channel
            .session_has_parsed_client_rtp_capabilities(&SessionId::Integer(2))
            .await
    );

    assert!(
        channel
            .apply_transport_connected(
                &SessionId::Integer(2),
                TransportConnectDirection::Download,
                &transport_adapter,
            )
            .await
    );

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, SessionOutbound::Request(_))),
        "subscriber should receive a consumer bootstrap after download connect makes it ready"
    );
}
