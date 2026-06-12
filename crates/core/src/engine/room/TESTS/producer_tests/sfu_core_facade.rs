use super::support::*;
use crate::prelude::{MediaSession, NegotiationOffer, SessionEvent, SfuCore};

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

    let mut publisher_session = core
        .session(&room, &publisher_user_id, publisher_connection_id)
        .await;
    let mut subscriber_session = core
        .session(&room, &subscriber_user_id, subscriber_connection_id)
        .await;
    establish_session(&mut publisher_session, &mut publisher_remote).await;
    establish_session(&mut subscriber_session, &mut subscriber_remote).await;

    publish_scalable_video(&mut publisher_session, &mut publisher_remote).await;
    assert_eq!(room.test_api().inspect().producer_count().await, 1);

    let subscription_intents = subscription_intents_from_test_states(&TestSubscriptionStates {
        scalable_video: Some(true),
        ..TestSubscriptionStates::default()
    });
    assert!(
        subscriber_session
            .subscribe(&publisher_user_id, &subscription_intents)
            .await
            .is_ok()
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
}

#[tokio::test]
async fn media_session_initial_offer_is_one_shot() {
    let (mut session, mut remote) = build_publisher_session(55_202).await;
    let offer = session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");

    assert!(
        session
            .establish()
            .await
            .expect("second initial offer should not fail")
            .is_none()
    );

    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("initial answer should apply");
    assert!(events.is_empty());
    assert!(
        session
            .establish()
            .await
            .expect("stable session should reject a new initial offer without failing")
            .is_none()
    );
}

#[tokio::test]
async fn media_session_keeps_pending_offer_after_invalid_answer() {
    let (mut session, mut remote) = build_publisher_session(55_205).await;
    let offer = session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");

    let error = session
        .answer("not an SDP answer")
        .await
        .expect_err("invalid answer should be rejected");
    assert!(error.is_client_error());

    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("pending offer should accept a later valid answer");
    assert!(events.is_empty());
}

#[tokio::test]
async fn media_session_queues_publish_until_initial_answer_is_accepted() {
    let (mut session, mut remote) = build_publisher_session(55_203).await;
    let offer = session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");

    let events = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ScalableVideo,
        ))
        .await
        .expect("publish while awaiting answer should queue");
    assert!(events.is_empty());

    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("initial answer should apply queued publish");
    let offer = single_renegotiation(&events);
    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("queued publish renegotiation answer should apply");
    assert_eq!(
        events,
        vec![SessionEvent::Publication {
            stream_id: stream_id_for_source(TestSourceKind::ScalableVideo),
            active: true
        }]
    );
}

#[tokio::test]
async fn media_session_queues_renegotiation_requested_while_waiting_for_answer() {
    let (mut session, mut remote) = build_publisher_session(55_204).await;
    establish_session(&mut session, &mut remote).await;
    let events = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ScalableVideo,
        ))
        .await
        .expect("publish should stage through the media session");
    let offer = single_renegotiation(&events);

    let events = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ReadableVideo,
        ))
        .await
        .expect("publish while awaiting an answer should queue");
    assert!(events.is_empty());
    assert!(
        session
            .renegotiate()
            .await
            .expect("renegotiation while awaiting an answer should not fail")
            .is_none()
    );

    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("publisher renegotiation answer should commit staged publish");
    let [
        SessionEvent::Publication { stream_id, active },
        SessionEvent::Renegotiation(follow_up),
    ] = events.as_slice()
    else {
        panic!("answer should commit the publication then emit the queued renegotiation");
    };
    assert_eq!(
        stream_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo)
    );
    assert!(*active);

    let answer_sdp = remote_answer_sdp(&mut remote, &follow_up.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("queued publish renegotiation answer should apply");
    assert_eq!(
        events,
        vec![SessionEvent::Publication {
            stream_id: stream_id_for_source(TestSourceKind::ReadableVideo),
            active: true,
        }]
    );
}

async fn build_publisher_session(port: u16) -> (MediaSession, Rtc) {
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
    let (publisher_tx, _publisher_rx) = test_sender();
    let publisher_connection_id =
        join_user_with_sender(&room, publisher_user_id.clone(), publisher_tx).await;
    let session = core
        .session(&room, &publisher_user_id, publisher_connection_id)
        .await;
    (session, build_remote_rtc(port))
}

async fn establish_session(session: &mut MediaSession, remote: &mut Rtc) {
    let offer = session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");
    let answer_sdp = remote_answer_sdp(remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("initial answer should apply");
    assert!(events.is_empty());
}

async fn publish_scalable_video(session: &mut MediaSession, remote: &mut Rtc) {
    let events = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ScalableVideo,
        ))
        .await
        .expect("publish should stage through the media session");
    let offer = single_renegotiation(&events);
    let answer_sdp = remote_answer_sdp(remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("publisher renegotiation answer should commit staged publish");
    assert_eq!(
        events,
        vec![SessionEvent::Publication {
            stream_id: stream_id_for_source(TestSourceKind::ScalableVideo),
            active: true
        }]
    );
}

fn single_renegotiation(events: &[SessionEvent]) -> &NegotiationOffer {
    let [SessionEvent::Renegotiation(offer)] = events else {
        panic!("media session should produce one renegotiation event");
    };
    offer
}
