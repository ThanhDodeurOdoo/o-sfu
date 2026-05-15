use super::support::*;

#[tokio::test]
async fn explicit_unpublish_removes_state_when_transport_cleanup_fails() {
    let mut scenario = setup_real_rtc_refresh_scenario().await;

    publish_track(
        &scenario.room,
        &scenario.publisher_user_id,
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        &scenario.media_transport,
    )
    .await;
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut scenario.subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_)))
    );

    let Some(connection_id) = scenario
        .room
        .test_api()
        .inspect()
        .user_connection_id(&scenario.publisher_user_id)
        .await
    else {
        panic!("publisher connection should exist");
    };
    let Some(transport_media_id) = scenario
        .room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &scenario.publisher_user_id,
            connection_id,
            TestSourceKind::AudioDetector,
        )
        .await
    else {
        panic!("published audio should expose a transport media id");
    };
    let transport_user_key = scenario
        .room
        .transport_user_key(&scenario.publisher_user_id, connection_id);
    scenario
        .media_transport
        .close_session(&transport_user_key)
        .await
        .expect("closing the publisher transport should succeed");

    assert_eq!(
        scenario
            .room
            .unpublish_track(
                &scenario.publisher_user_id,
                connection_id,
                &stream_id_for_source(TestSourceKind::AudioDetector),
                &scenario.media_transport,
            )
            .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Failed
        },
        "unpublish should commit room state and queue failed transport cleanup"
    );

    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 0);
    assert_user_has_no_producer_route_target(
        &scenario.room,
        &scenario.publisher_user_id,
        connection_id,
        TestSourceKind::AudioDetector,
    )
    .await;
    assert_transport_media_mapping_is_missing(&scenario.room, transport_media_id).await;
    assert!(
        scenario
            .room
            .test_api()
            .lifecycle()
            .pending_cleanup_retry_count()
            > 0
    );
    assert!(
        drain_outbound(&mut scenario.publisher_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::TrackBindingUpdate(_)))
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::TrackBindingUpdate(_)))
    );
}

#[tokio::test]
async fn publish_track_releases_room_lock_while_waiting_on_media_transport() {
    let (room, _adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let (fake_media_transport, _) = fake_adapter();
    let fake = fake_media_transport
        .as_fake_transport()
        .expect("expected fake media transport");
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = fake_media_transport.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    TestSourceKind::ScalableVideo,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_fake_event(fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        let user_id = UserId::Integer(2);
        room.update_user_info(
            &user_id,
            user_connection_id(&room, &user_id).await,
            UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            },
            UserInfoRefresh::NotNeeded,
            &fake_media_transport,
        )
        .await;
    })
    .await;
    assert!(
        update_result.is_ok(),
        "user info update should not wait for publish transport declaration"
    );

    assert!(publish_task.await.unwrap().is_some());
    assert!(
        drain_outbound(&mut rx1).iter().any(|msg| matches!(
            msg,
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut rx2).iter().any(|msg| matches!(
            msg,
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(_))
        )),
        "user should still receive the concurrent info broadcast"
    );
}

#[tokio::test]
async fn publish_track_defers_producer_commit_until_transport_publish_succeeds() {
    let (room, adapter, fake, _rx1, _rx2) = setup_two_ready_users_with_fake().await;
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = adapter.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    TestSourceKind::ScalableVideo,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert_eq!(room.test_api().inspect().producer_count().await, 0);

    assert!(publish_task.await.unwrap().is_some());

    assert_eq!(room.test_api().inspect().producer_count().await, 1);
    let transport_media_id = room
        .test_api()
        .inspect()
        .first_published_transport_media_id()
        .await;
    assert!(transport_media_id.is_some());
    assert_eq!(
        room.test_api()
            .inspect()
            .producer_stream_type_for_transport_media_id(
                transport_media_id.expect("published track should have a transport id")
            )
            .await,
        Some(TestSourceKind::ScalableVideo)
    );
    assert!(
        room.test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                TestSourceKind::ScalableVideo,
            )
            .await
    );
}

#[tokio::test]
async fn publish_track_cleans_up_transport_media_when_user_leaves_mid_publish() {
    let (room, adapter, fake, _rx1, _rx2) = setup_two_ready_users_with_fake().await;
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = adapter.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    TestSourceKind::ScalableVideo,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(1),
            test_connection_id(0),
            UserCleanup::state_only(None),
        )
        .await
    );
    assert!(publish_task.await.unwrap().is_none());

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved {
                user_id: UserId::Integer(1),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn late_join_bootstrap_releases_room_lock_while_waiting_on_media_transport() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        let user_id = UserId::Integer(1);
        room.update_user_info(
            &user_id,
            user_connection_id(&room, &user_id).await,
            UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            },
            UserInfoRefresh::NotNeeded,
            &media_transport,
        )
        .await;
    })
    .await;
    assert!(
        update_result.is_ok(),
        "user info update should not wait for late-join consumer declaration"
    );

    bootstrap_task.await.unwrap();
    assert!(
        drain_outbound(&mut publisher_rx).iter().any(|msg| matches!(
            msg,
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|msg| matches!(
                msg,
                UserOutbound::Message(RoomEventMessage::UserInfoChanged(_))
                    | UserOutbound::Request(_)
            )),
        "late joiner should receive outbound traffic while bootstrap is running"
    );
}

#[tokio::test]
async fn late_join_bootstrap_defers_consumer_commit_until_transport_consume_succeeds() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert_eq!(room.test_api().inspect().consumer_count().await, 0);

    bootstrap_task.await.unwrap();

    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
}

#[tokio::test]
async fn late_join_bootstrap_cleans_up_transport_media_when_user_leaves_mid_consume() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(2),
            test_connection_id(1),
            UserCleanup::state_only(None),
        )
        .await
    );
    bootstrap_task.await.unwrap();

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved {
                user_id: UserId::Integer(2),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn late_join_bootstrap_queues_transport_cleanup_retry_when_commit_cleanup_fails() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let consumer_transport_media_id = fake.next_transport_media_id();
    fake.fail_remove_media_until_allowed(consumer_transport_media_id);
    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(2),
            test_connection_id(1),
            UserCleanup::state_only(None),
        )
        .await
    );
    bootstrap_task.await.unwrap();

    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 1);
    assert!(!fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::MediaRemoved {
            user_id: UserId::Integer(2),
            transport_media_id,
        } if *transport_media_id == consumer_transport_media_id
    )));

    fake.allow_remove_media(consumer_transport_media_id);
    room.test_api()
        .lifecycle()
        .force_cleanup_retry_cycle(&media_transport)
        .await;

    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 0);
    assert!(fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::MediaRemoved {
            user_id: UserId::Integer(2),
            transport_media_id,
        } if *transport_media_id == consumer_transport_media_id
    )));
}

#[tokio::test]
async fn in_flight_bootstrap_retry_does_not_duplicate_consumer_or_unpublish_cleanup() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    TestSourceKind::ScalableVideo,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let _ = refresh_session_consumers(&room, &UserId::Integer(2), &media_transport).await;

    assert!(
        publish_task
            .await
            .unwrap_or_else(|error| panic!("publish task should finish: {error}"))
            .is_some()
    );

    let consume_requests = fake
        .snapshot_events()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                FakeMediaTransportEvent::ConsumeMediaRequested {
                    consumer_user_id: UserId::Integer(2),
                    source_user_id: UserId::Integer(1),
                    media_kind: MediaKind::Video,
                }
            )
        })
        .count();
    assert_eq!(
        consume_requests, 1,
        "late-join retry must not schedule a second consumer consume while publish bootstrap is in flight"
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert_eq!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .filter(|message| matches!(message, UserOutbound::Request(_)))
            .count(),
        1,
        "subscriber should receive exactly one bootstrap request for the published track"
    );

    assert_eq!(
        room.unpublish_track(
            &UserId::Integer(1),
            test_connection_id(0),
            &stream_id_for_source(TestSourceKind::ScalableVideo),
            &media_transport
        )
        .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );
    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);

    let removed_media = fake
        .snapshot_events()
        .into_iter()
        .filter(|event| matches!(event, FakeMediaTransportEvent::MediaRemoved { .. }))
        .count();
    assert_eq!(
        removed_media, 2,
        "unpublish should remove exactly the publisher and subscriber transport media after a retried bootstrap"
    );
}
