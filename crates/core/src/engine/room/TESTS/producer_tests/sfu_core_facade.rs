use super::support::*;
use crate::prelude::{MediaSession, NegotiationOffer, SessionError, SfuCore};

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
    assert!(matches!(
        session.answer(&answer_sdp).await,
        Err(SessionError::NoPendingRequest)
    ));
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
async fn media_session_close_rolls_back_staged_publish_and_removes_room_session() {
    let mut fixture = build_publisher_fixture(55_206).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    stage_scalable_video(&mut fixture.session).await;
    let video = TestSourceKind::ScalableVideo;
    fixture.assert_staged(video, true).await;

    assert!(fixture.session.close().await);

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
        room,
        user_id,
        mut session,
        mut remote,
        ..
    } = build_publisher_fixture(55_207).await;
    establish_session(&mut session, &mut remote).await;

    assert!(session.close().await);
    assert!(!session.close().await);

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
        .staged_media_id(&fixture.user_id, fixture.session.connection_id(), video)
        .await
        .expect("test publish should be staged");
    let replacement = fixture
        .core
        .admit_user(fixture.room.uuid(), join_request(fixture.user_id.clone()))
        .await
        .expect("replacement user should join through core facade");

    fixture.assert_staged(video, false).await;
    assert!(
        fixture
            .media_transport
            .test_api()
            .route_entry_by_media_id(staged_media_id)
            .await
            .is_none()
    );

    assert!(!fixture.session.close().await);

    assert_eq!(
        fixture
            .room
            .test_api()
            .inspect()
            .user_connection_id(&fixture.user_id)
            .await,
        Some(replacement.connection_id())
    );
}

async fn build_publisher_session(port: u16) -> (MediaSession, Rtc) {
    let fixture = build_publisher_fixture(port).await;
    (fixture.session, fixture.remote)
}

struct PublisherFixture {
    core: SfuCore,
    room: Arc<Room>,
    media_transport: MediaTransport,
    user_id: UserId,
    session: MediaSession,
    remote: Rtc,
}

impl PublisherFixture {
    async fn assert_staged(&self, source: TestSourceKind, expected: bool) {
        assert_eq!(
            self.room
                .has_staged_publish(
                    &self.user_id,
                    self.session.connection_id(),
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
    let manager = Arc::new(RoomManager::for_test());
    let room = manager
        .serve_room(
            "issuer-sfu-core",
            TEST_ROOM_KEY,
            &RoomConfig::default(),
            None,
        )
        .await;
    let media_transport = build_real_rtc_media_transport();
    let core = SfuCore::new(media_transport.clone(), Arc::clone(&manager));
    let publisher_user_id = UserId::Integer(1);
    let session = core
        .admit_user(room.uuid(), join_request(publisher_user_id.clone()))
        .await
        .expect("publisher should join through core facade");
    PublisherFixture {
        core,
        room,
        media_transport,
        user_id: publisher_user_id,
        session,
        remote: build_remote_rtc(port),
    }
}

fn join_request(user_id: UserId) -> JoinUserRequest {
    let (sender, _receiver) = test_sender();
    JoinUserRequest {
        user_id,
        label: None,
        permissions: UserPermissions::default(),
        sender,
    }
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
