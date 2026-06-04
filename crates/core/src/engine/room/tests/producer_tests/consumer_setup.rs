use super::support::*;

#[tokio::test]
async fn client_capabilities_setup_late_join_when_download_connected_first() {
    let (room, media_transport, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_consumer_setup_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let download_update = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    assert!(download_update.session_present);
    assert!(!download_update.became_consumer_ready);

    assert!(
        apply_client_rtp_capabilities(
            &room,
            &UserId::Integer(2),
            user_connection_id(&room, &UserId::Integer(2)).await,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await
    );
    assert!(
        room.test_api()
            .inspect()
            .session_has_parsed_client_rtp_capabilities(&UserId::Integer(2))
            .await
    );

    assert_remote_track_setup_for_stream(
        &drain_outbound(&mut subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert!(refresh_session_consumers(&room, &UserId::Integer(2), &media_transport).await);
}

#[tokio::test]
async fn transport_connect_setup_late_join_when_capabilities_arrive_first() {
    let (room, media_transport, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_consumer_setup_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let capabilities_update =
        set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
            .await;
    assert!(capabilities_update.session_present);
    assert!(!capabilities_update.became_consumer_ready);
    assert!(
        room.test_api()
            .inspect()
            .session_has_parsed_client_rtp_capabilities(&UserId::Integer(2))
            .await
    );

    assert!(
        apply_consume_transport_ready(
            &room,
            &UserId::Integer(2),
            user_connection_id(&room, &UserId::Integer(2)).await,
            &media_transport,
        )
        .await
    );

    assert_remote_track_setup_for_stream(
        &drain_outbound(&mut subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert!(refresh_session_consumers(&room, &UserId::Integer(2), &media_transport).await);
}

#[tokio::test]
async fn refresh_retry_sets_up_only_missing_consumers_on_real_rtc() {
    let mut scenario = Box::pin(setup_real_rtc_refresh_scenario()).await;

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .publish_track(
                &scenario.publisher_user_id,
                TestSourceKind::ScalableVideo,
                MediaKind::Video,
                video_rtp_parameters_with_mid("cam-refresh-retry", 22_222),
                &scenario.media_transport,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert_remote_track_setup_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 1);

    let first_refresh_offer = scenario
        .media_transport
        .create_session_renegotiation_offer(&scenario.subscriber_session_key)
        .await
        .expect("first subscriber refresh should stage an rtc offer");

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .publish_track(
                &scenario.publisher_user_id,
                TestSourceKind::ReadableVideo,
                MediaKind::Video,
                video_rtp_parameters_with_mid("screen-refresh-retry", 33_333),
                &scenario.media_transport,
            )
            .await
            .is_some()
    );
    assert_eq!(
        scenario.room.test_api().inspect().consumer_count().await,
        1,
        "second consumer must stay pending while the first rtc offer awaits an answer"
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx).is_empty(),
        "no second setup should be emitted before the first refresh answer lands"
    );

    settle_refresh_offer(&mut scenario, first_refresh_offer).await;

    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 2);
    assert_remote_track_setup_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        TestSourceKind::ReadableVideo,
    );

    let second_refresh_offer = scenario
        .media_transport
        .create_session_renegotiation_offer(&scenario.subscriber_session_key)
        .await
        .expect("retry should stage the deferred rtc offer");
    settle_refresh_offer(&mut scenario, second_refresh_offer).await;

    assert_eq!(
        scenario.room.test_api().inspect().consumer_count().await,
        2,
        "retry pass must not duplicate already-committed consumers"
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx).is_empty(),
        "no new setup should be emitted once every consumer already exists"
    );
}

#[tokio::test]
async fn negotiated_publish_commit_sets_up_consumers_on_real_rtc() {
    let mut scenario = Box::pin(setup_real_rtc_refresh_scenario()).await;
    let publisher_connection_id =
        user_connection_id(&scenario.room, &scenario.publisher_user_id).await;
    let publisher_session_key = scenario
        .room
        .transport_user_key(&scenario.publisher_user_id, publisher_connection_id)
        .await;
    let mut publisher_remote = build_remote_rtc(55_101);
    apply_offer_answer(
        &scenario.media_transport,
        &publisher_session_key,
        &mut publisher_remote,
        scenario.publisher_initial_offer.into_sdp(),
    )
    .await;

    let transport_media_id = scenario
        .media_transport
        .publish_media(
            &publisher_session_key,
            MediaKind::Video,
            &o_sfu_router::MediaStream::new(vec![], vec![], vec![]),
        )
        .await
        .expect("protocol publish intent should stage a recv-only media line");
    let publish_offer = scenario
        .media_transport
        .create_session_renegotiation_offer(&publisher_session_key)
        .await
        .expect("protocol publish should stage a follow-up offer");
    apply_offer_answer(
        &scenario.media_transport,
        &publisher_session_key,
        &mut publisher_remote,
        publish_offer.into_sdp(),
    )
    .await;
    let negotiated_parameters = scenario
        .media_transport
        .test_api()
        .negotiated_producer_parameters(&publisher_session_key, transport_media_id)
        .await
        .expect("answered protocol publish should expose negotiated producer parameters");

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .publish_negotiated_track(
                &scenario.publisher_user_id,
                NegotiatedPublish {
                    connection_id: publisher_connection_id,
                    stream_type: TestSourceKind::ScalableVideo,
                    media_kind: MediaKind::Video,
                    transport_media_id,
                    consumable_rtp_parameters: negotiated_parameters,
                },
                &scenario.media_transport,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert_remote_track_setup_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 1);
}
