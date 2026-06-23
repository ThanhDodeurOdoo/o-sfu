use super::support::*;
use crate::{
    engine::room::RoomUserAdmission,
    prelude::{MediaSession, NegotiationOffer, SessionEvent, SfuCore},
};

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

    assert_invalid_answer_is_rejected(&mut session).await;

    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("pending offer should accept a later valid answer");
    assert!(events.is_empty());
}

#[tokio::test]
async fn media_session_keeps_staged_publish_after_invalid_answer() {
    let (mut session, mut remote) = build_publisher_session(55_225).await;
    establish_session(&mut session, &mut remote).await;
    let events = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ScalableVideo,
        ))
        .await
        .expect("publish should stage through the media session");
    let offer = single_renegotiation(&events);

    assert_invalid_answer_is_rejected(&mut session).await;

    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let events = session
        .answer(&answer_sdp)
        .await
        .expect("staged publish should accept a later valid answer");
    assert_active_publish(&events, TestSourceKind::ScalableVideo);
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
    assert_active_publish(&events, TestSourceKind::ScalableVideo);
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
    assert_active_publish(&events, TestSourceKind::ReadableVideo);
}

#[tokio::test]
async fn media_session_close_rolls_back_staged_publish_and_removes_room_session() {
    let PublisherFixture {
        manager,
        room,
        user_id,
        connection_id,
        mut session,
        mut remote,
        ..
    } = build_publisher_fixture(55_206).await;
    establish_session(&mut session, &mut remote).await;
    stage_scalable_video(&mut session).await;
    assert!(has_staged_scalable_video(&room, &user_id, connection_id).await);

    assert!(session.close(&manager).await);

    assert!(!has_staged_scalable_video(&room, &user_id, connection_id).await);
    assert!(!room.test_api().inspect().has_session(&user_id).await);
}

#[tokio::test]
async fn media_session_close_is_idempotent_after_completed_cleanup() {
    let PublisherFixture {
        manager,
        room,
        user_id,
        mut session,
        mut remote,
        ..
    } = build_publisher_fixture(55_207).await;
    establish_session(&mut session, &mut remote).await;

    assert!(session.close(&manager).await);
    assert!(!session.close(&manager).await);

    assert!(!room.test_api().inspect().has_session(&user_id).await);
}

#[tokio::test]
async fn replacement_drains_staged_publish_before_stale_close() {
    let PublisherFixture {
        manager,
        room,
        media_transport,
        user_id,
        connection_id,
        mut session,
        mut remote,
    } = build_publisher_fixture(55_208).await;
    establish_session(&mut session, &mut remote).await;
    stage_scalable_video(&mut session).await;
    let staged_media_id = room
        .staged_media_id(&user_id, connection_id, TestSourceKind::ScalableVideo)
        .await
        .expect("test publish should be staged");
    let replacement = admit_user(&manager, &room, user_id.clone(), &media_transport).await;

    assert!(!has_staged_scalable_video(&room, &user_id, connection_id).await);
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(staged_media_id)
            .await
            .is_none()
    );

    assert!(!session.close(&manager).await);

    assert_eq!(
        room.test_api().inspect().user_connection_id(&user_id).await,
        Some(replacement.connection_id)
    );
}

async fn build_publisher_session(port: u16) -> (MediaSession, Rtc) {
    let fixture = build_publisher_fixture(port).await;
    (fixture.session, fixture.remote)
}

struct PublisherFixture {
    manager: RoomManager,
    room: Arc<Room>,
    media_transport: MediaTransport,
    user_id: UserId,
    connection_id: ConnectionId,
    session: MediaSession,
    remote: Rtc,
}

async fn build_publisher_fixture(port: u16) -> PublisherFixture {
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
    let core = SfuCore::new(media_transport.clone());
    let publisher_user_id = UserId::Integer(1);
    let admission = admit_user(&manager, &room, publisher_user_id.clone(), &media_transport).await;
    let RoomUserAdmission {
        connection_id,
        transport_session_key,
        ..
    } = admission;
    let session = core.session_with_transport_key(
        &room,
        &publisher_user_id,
        connection_id,
        transport_session_key,
    );
    PublisherFixture {
        manager,
        room,
        media_transport,
        user_id: publisher_user_id,
        connection_id,
        session,
        remote: build_remote_rtc(port),
    }
}

async fn admit_user(
    manager: &RoomManager,
    room: &Arc<Room>,
    user_id: UserId,
    media_transport: &MediaTransport,
) -> RoomUserAdmission {
    let (sender, _receiver) = test_sender();
    manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id,
                label: None,
                permissions: UserPermissions::default(),
                sender,
            },
            media_transport,
        )
        .await
        .expect("user should join through manager")
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

async fn stage_scalable_video(session: &mut MediaSession) {
    let events = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ScalableVideo,
        ))
        .await
        .expect("publish should stage through the media session");
    single_renegotiation(&events);
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
    assert_active_publish(&events, TestSourceKind::ScalableVideo);
}

async fn assert_invalid_answer_is_rejected(session: &mut MediaSession) {
    let error = session
        .answer("not an SDP answer")
        .await
        .expect_err("invalid answer should be rejected");
    assert!(error.is_client_error());
}

fn assert_active_publish(events: &[SessionEvent], stream_type: TestSourceKind) {
    assert_eq!(
        events,
        &[SessionEvent::Publication {
            stream_id: stream_id_for_source(stream_type),
            active: true,
        }]
    );
}

async fn has_staged_scalable_video(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
) -> bool {
    room.has_staged_publish(
        user_id,
        connection_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
    )
    .await
}

fn single_renegotiation(events: &[SessionEvent]) -> &NegotiationOffer {
    let [SessionEvent::Renegotiation(offer)] = events else {
        panic!("media session should produce one renegotiation event");
    };
    offer
}
