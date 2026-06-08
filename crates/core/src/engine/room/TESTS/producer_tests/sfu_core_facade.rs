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
