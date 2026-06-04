use super::support::*;
use crate::prelude::SfuCore;

#[tokio::test]
async fn sfu_core_facade_drives_join_publish_renegotiate_and_subscribe() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room(
            "issuer-sfu-core",
            TEST_ROOM_KEY,
            &RoomConfig::default(),
            None,
        )
        .await;
    let media_transport = build_real_rtc_media_transport();
    let core = SfuCore::new(media_transport);
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);
    let (publisher_tx, _publisher_rx) = test_sender();
    let (subscriber_tx, _subscriber_rx) = test_sender();
    let publisher_connection_id =
        join_user_with_sender(&room, publisher_user_id.clone(), publisher_tx).await;
    let subscriber_connection_id =
        join_user_with_sender(&room, subscriber_user_id.clone(), subscriber_tx).await;
    let mut publisher_remote = build_remote_rtc(55_200);
    let mut subscriber_remote = build_remote_rtc(55_201);

    apply_initial_core_answer(
        &core,
        &room,
        &publisher_user_id,
        publisher_connection_id,
        &mut publisher_remote,
    )
    .await;
    apply_initial_core_answer(
        &core,
        &room,
        &subscriber_user_id,
        subscriber_connection_id,
        &mut subscriber_remote,
    )
    .await;

    let publish_intent = source_publish_intent_for_source(TestSourceKind::ScalableVideo);
    assert_eq!(
        core.session(&room, &publisher_user_id, publisher_connection_id)
            .await
            .publication()
            .stage(&publish_intent)
            .await
            .expect("publish should stage through core facade"),
        PublishStageOutcome::Staged
    );
    assert!(
        core.session(&room, &publisher_user_id, publisher_connection_id)
            .await
            .publication()
            .has_staged(&stream_id_for_source(TestSourceKind::ScalableVideo))
    );
    commit_staged_scalable_video(
        &core,
        &room,
        &publisher_user_id,
        publisher_connection_id,
        &mut publisher_remote,
    )
    .await;

    let subscription_intents = subscription_intents_from_test_states(&TestSubscriptionStates {
        scalable_video: Some(true),
        ..TestSubscriptionStates::default()
    });
    assert_eq!(
        core.session(&room, &subscriber_user_id, subscriber_connection_id)
            .await
            .subscription()
            .update(&publisher_user_id, &subscription_intents)
            .await,
        SubscriptionUpdateOutcome::Applied
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
}

async fn apply_initial_core_answer(
    core: &SfuCore,
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    remote: &mut Rtc,
) {
    let initial_offer = core
        .session(room, user_id, connection_id)
        .await
        .negotiation()
        .create_initial_offer()
        .await
        .expect("initial offer should be created");
    let answer_sdp = remote_answer_sdp(remote, &initial_offer.offer().sdp);
    assert!(
        core.session(room, user_id, connection_id)
            .await
            .negotiation()
            .apply_initial_answer(&answer_sdp, initial_offer)
            .await
            .expect("initial answer should apply")
            .is_empty()
    );
}

async fn commit_staged_scalable_video(
    core: &SfuCore,
    room: &Room,
    publisher_user_id: &UserId,
    publisher_connection_id: ConnectionId,
    publisher_remote: &mut Rtc,
) {
    let renegotiation_offer = core
        .session(room, publisher_user_id, publisher_connection_id)
        .await
        .negotiation()
        .create_renegotiation_offer()
        .await
        .expect("publisher renegotiation request should succeed")
        .expect("staged publish should create a renegotiation offer");
    let answer_sdp = remote_answer_sdp(publisher_remote, &renegotiation_offer.sdp);
    let committed_streams = core
        .session(room, publisher_user_id, publisher_connection_id)
        .await
        .negotiation()
        .apply_renegotiation_answer(&answer_sdp)
        .await
        .expect("publisher renegotiation answer should commit staged publish");
    assert_eq!(
        committed_streams,
        vec![stream_id_for_source(TestSourceKind::ScalableVideo)]
    );
    assert!(
        core.session(room, publisher_user_id, publisher_connection_id)
            .await
            .publication()
            .is_published(&stream_id_for_source(TestSourceKind::ScalableVideo))
            .await
    );
}
