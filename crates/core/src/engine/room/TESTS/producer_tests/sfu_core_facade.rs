use super::support::*;
use crate::{
    engine::room::RoomUserAdmission,
    prelude::{MediaSession, NegotiationOffer, SfuCore},
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
    let offer = session
        .answer(&answer_sdp)
        .await
        .expect("initial answer should apply");
    assert!(offer.is_none());
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
    let offer = session
        .answer(&answer_sdp)
        .await
        .expect("pending offer should accept a later valid answer");
    assert!(offer.is_none());
}

#[tokio::test]
async fn media_session_keeps_staged_publish_after_invalid_answer() {
    let mut fixture = build_publisher_fixture(55_225).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    let video = TestSourceKind::ScalableVideo;
    let offer = fixture
        .session
        .publish(source_publish_intent_for_source(video))
        .await
        .expect("publish should stage through the media session");
    let offer = expect_renegotiation_offer(offer);

    assert_invalid_answer_is_rejected(&mut fixture.session).await;
    fixture.assert_staged(video, true).await;

    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &offer.sdp);
    let offer = fixture
        .session
        .answer(&answer_sdp)
        .await
        .expect("staged publish should accept a later valid answer");
    assert!(offer.is_none());
    fixture.assert_committed(video, 1).await;
}

#[tokio::test]
async fn media_session_queues_publish_until_initial_answer_is_accepted() {
    let mut fixture = build_publisher_fixture(55_203).await;
    let video = TestSourceKind::ScalableVideo;
    let initial_offer = fixture
        .session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");

    let queued_offer = fixture
        .session
        .publish(source_publish_intent_for_source(video))
        .await
        .expect("publish while awaiting answer should queue");
    assert!(queued_offer.is_none());

    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &initial_offer.sdp);
    let offer = fixture
        .session
        .answer(&answer_sdp)
        .await
        .expect("initial answer should apply queued publish");
    let offer = expect_renegotiation_offer(offer);
    fixture.assert_staged(video, true).await;
    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &offer.sdp);
    let offer = fixture
        .session
        .answer(&answer_sdp)
        .await
        .expect("queued publish renegotiation answer should apply");
    assert!(offer.is_none());
    fixture.assert_committed(video, 1).await;
}

#[tokio::test]
async fn media_session_queues_renegotiation_requested_while_waiting_for_answer() {
    let mut fixture = build_publisher_fixture(55_204).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    let video = TestSourceKind::ScalableVideo;
    let readable = TestSourceKind::ReadableVideo;
    let publish_offer = fixture
        .session
        .publish(source_publish_intent_for_source(video))
        .await
        .expect("publish should stage through the media session");
    let publish_offer = expect_renegotiation_offer(publish_offer);

    let queued_offer = fixture
        .session
        .publish(source_publish_intent_for_source(readable))
        .await
        .expect("publish while awaiting an answer should queue");
    assert!(queued_offer.is_none());
    assert!(
        fixture
            .session
            .renegotiate()
            .await
            .expect("renegotiation while awaiting an answer should not fail")
            .is_none()
    );

    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &publish_offer.sdp);
    let follow_up = fixture
        .session
        .answer(&answer_sdp)
        .await
        .expect("publisher renegotiation answer should commit staged publish");
    let follow_up =
        follow_up.expect("answer should emit the queued follow-up renegotiation after commit");
    fixture.assert_committed(video, 1).await;
    fixture.assert_staged(readable, true).await;

    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &follow_up.sdp);
    let offer = fixture
        .session
        .answer(&answer_sdp)
        .await
        .expect("queued publish renegotiation answer should apply");
    assert!(offer.is_none());
    fixture.assert_committed(readable, 2).await;
}

#[tokio::test]
async fn media_session_close_rolls_back_staged_publish_and_removes_room_session() {
    let mut fixture = build_publisher_fixture(55_206).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    stage_scalable_video(&mut fixture.session).await;
    let video = TestSourceKind::ScalableVideo;
    fixture.assert_staged(video, true).await;

    assert!(fixture.session.close(&fixture.manager).await);

    fixture.assert_staged(video, false).await;
    assert!(
        !fixture
            .room
            .test_api()
            .inspect()
            .has_session(&fixture.user_id)
            .await
    );
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
    let mut fixture = build_publisher_fixture(55_208).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    stage_scalable_video(&mut fixture.session).await;
    let video = TestSourceKind::ScalableVideo;
    let staged_media_id = fixture
        .room
        .staged_media_id(&fixture.user_id, fixture.connection_id, video)
        .await
        .expect("test publish should be staged");
    let replacement = admit_user(
        &fixture.manager,
        &fixture.room,
        fixture.user_id.clone(),
        &fixture.media_transport,
    )
    .await;

    fixture.assert_staged(video, false).await;
    assert!(
        fixture
            .media_transport
            .test_api()
            .route_entry_by_media_id(staged_media_id)
            .await
            .is_none()
    );

    assert!(!fixture.session.close(&fixture.manager).await);

    assert_eq!(
        fixture
            .room
            .test_api()
            .inspect()
            .user_connection_id(&fixture.user_id)
            .await,
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

impl PublisherFixture {
    async fn assert_staged(&self, source: TestSourceKind, expected: bool) {
        assert_eq!(
            self.room
                .has_staged_publish(
                    &self.user_id,
                    self.connection_id,
                    &stream_id_for_source(source),
                )
                .await,
            expected
        );
    }

    async fn assert_committed(&self, source: TestSourceKind, producer_count: usize) {
        let stream_id = stream_id_for_source(source);
        let inspect = self.room.test_api().inspect();
        assert!(inspect.is_stream_published(&self.user_id, &stream_id).await);
        assert_eq!(inspect.producer_count().await, producer_count);
        self.assert_staged(source, false).await;
    }
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
    let connection_id = admission.connection_id;
    let session = core.session(&room, &publisher_user_id, connection_id).await;
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
    let offer = session
        .answer(&answer_sdp)
        .await
        .expect("initial answer should apply");
    assert!(offer.is_none());
}

async fn stage_scalable_video(session: &mut MediaSession) {
    let offer = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ScalableVideo,
        ))
        .await
        .expect("publish should stage through the media session");
    expect_renegotiation_offer(offer);
}

async fn publish_scalable_video(session: &mut MediaSession, remote: &mut Rtc) {
    let offer = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ScalableVideo,
        ))
        .await
        .expect("publish should stage through the media session");
    let offer = expect_renegotiation_offer(offer);
    let answer_sdp = remote_answer_sdp(remote, &offer.sdp);
    let offer = session
        .answer(&answer_sdp)
        .await
        .expect("publisher renegotiation answer should commit staged publish");
    assert!(offer.is_none());
}

async fn assert_invalid_answer_is_rejected(session: &mut MediaSession) {
    let error = session
        .answer("not an SDP answer")
        .await
        .expect_err("invalid answer should be rejected");
    assert!(error.is_client_error());
}

fn expect_renegotiation_offer(offer: Option<NegotiationOffer>) -> NegotiationOffer {
    let Some(offer) = offer else {
        panic!("media session should return one renegotiation offer");
    };
    offer
}
