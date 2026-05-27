use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU16, Ordering},
};
pub(super) use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_router::test_support::rtp_samples::{
    sample_audio_rtp_parameters, sample_client_rtp_capabilities,
    sample_client_rtp_capabilities_without_video_rtx, sample_simulcast_video_rtp_parameters,
    sample_video_rtp_parameters,
};
pub(super) use o_sfu_router::{
    ConsumerCapability, MediaCapabilities, MediaKind, MediaKind as RouterMediaKind, MediaStream,
    RouterId,
};
pub(super) use tokio::time::timeout;

pub(super) use super::super::{
    JoinUserRequest, RoomAdmissionPolicy, RoomConfig, RoomEffectContext, RoomEventMessage,
    RoomEventRequest, RoomJoinError, RoomManager, RoomManagerJoinError, UserCloseReason,
    UserOutbound, UserOutboundReceiver, UserOutboundSender, topology::RoomTopology,
};
use crate::runtime::room::user_negotiation::{UserNegotiationUpdate, UserTransportReady};
pub(super) use crate::{
    PublicationActivity, PublicationActivityOutcome, PublishStageOutcome,
    RollbackStagedPublishOutcome, RoomMediaLimits, RtcPortRange, SessionNegotiationOutcome,
    SubscriptionUpdateOutcome, UnpublishOutcome,
    runtime::{
        ConnectionId, TestSourceKind, UserId, UserPermissions, VideoLayoutIntent,
        media_transport::{
            AppliedSessionAnswer, MediaTransport, TransportMediaId,
            test_support::test_media_transport_builder,
        },
        metrics::{RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt},
        source_model::{
            SourceSubscriptionIntent, UserStreamId,
            test_support::{
                TestSubscriptionStates, source_publish_intent_for_source, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
    },
};

pub(super) const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";
static NEXT_RTC_TEST_PORT: AtomicU16 = AtomicU16::new(47_000);

/// Realistic client RTP capabilities (default codecs)
pub(super) fn test_client_rtp_capabilities() -> MediaCapabilities {
    sample_client_rtp_capabilities()
}

pub(super) fn test_audio_rtp_parameters() -> MediaStream {
    sample_audio_rtp_parameters(11_111)
}

pub(super) fn test_client_rtp_capabilities_without_video_rtx() -> MediaCapabilities {
    sample_client_rtp_capabilities_without_video_rtx()
}

pub(super) fn test_video_rtp_parameters() -> MediaStream {
    sample_video_rtp_parameters(None, 22_222)
}

pub(super) fn test_simulcast_video_rtp_parameters() -> MediaStream {
    sample_simulcast_video_rtp_parameters(None)
}

pub(super) fn test_sender() -> (UserOutboundSender, UserOutboundReceiver) {
    UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default()))
}

#[allow(
    clippy::panic,
    reason = "the room test fixture uses a fixed-valid RTC config and should fail loudly if it stops being valid"
)]
pub(super) fn real_adapter() -> MediaTransport {
    let port_start = NEXT_RTC_TEST_PORT.fetch_add(100, Ordering::Relaxed);
    let port_end = port_start.saturating_add(99);
    match test_media_transport_builder(RtcPortRange::new(port_start, port_end))
        .worker_count(4)
        .build()
    {
        Ok(transport) => transport,
        Err(error) => panic!("constant RTC room test transport config should be valid: {error}"),
    }
}

pub(super) fn test_connection_id(raw: u64) -> ConnectionId {
    ConnectionId::from_raw(raw)
}

pub(super) async fn join_user_with_sender(
    room: &Arc<super::super::Room>,
    user_id: UserId,
    sender: UserOutboundSender,
) -> ConnectionId {
    room.test_api()
        .lifecycle()
        .join_user(user_id, None, UserPermissions::default(), sender)
        .await
        .expect("user should join")
}

pub(super) async fn join_user_without_transport_cleanup(
    room: &Arc<super::super::Room>,
    adapter: &MediaTransport,
    user_id: UserId,
    sender: UserOutboundSender,
) -> ConnectionId {
    room.test_api()
        .lifecycle()
        .join_session_without_transport_cleanup(
            user_id,
            None,
            UserPermissions::default(),
            sender,
            adapter,
        )
        .await
        .expect("user should join")
}

pub(super) async fn user_connection_id(
    room: &super::super::Room,
    user_id: &UserId,
) -> ConnectionId {
    room.test_api()
        .inspect()
        .user_connection_id(user_id)
        .await
        .expect("test fixture requires a live user connection")
}

pub(super) async fn set_consume_transport_ready(
    room: &super::super::Room,
    user_id: &UserId,
) -> UserNegotiationUpdate {
    set_transport_ready(room, user_id, UserTransportReady::Consume).await
}

async fn set_transport_ready(
    room: &super::super::Room,
    user_id: &UserId,
    readiness: UserTransportReady,
) -> UserNegotiationUpdate {
    let connection_id = user_connection_id(room, user_id).await;
    let mut state = room.state.write().await;
    state.set_transport_ready_for_test(user_id, connection_id, readiness)
}

pub(super) async fn set_client_rtp_capabilities(
    room: &super::super::Room,
    user_id: &UserId,
    capabilities: MediaCapabilities,
) -> UserNegotiationUpdate {
    let connection_id = user_connection_id(room, user_id).await;
    let mut state = room.state.write().await;
    state.set_client_rtp_capabilities_for_test(user_id, connection_id, &capabilities)
}

pub(super) async fn apply_consume_transport_ready(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    media_transport: &MediaTransport,
) -> bool {
    apply_transport_ready(
        room,
        user_id,
        connection_id,
        UserTransportReady::Consume,
        media_transport,
    )
    .await
}

async fn apply_transport_ready(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    readiness: UserTransportReady,
    media_transport: &MediaTransport,
) -> bool {
    let update = {
        let mut state = room.state.write().await;
        state.set_transport_ready_for_test(user_id, connection_id, readiness)
    };
    apply_negotiation_update(room, user_id, connection_id, update, media_transport).await
}

pub(super) async fn apply_client_rtp_capabilities(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    capabilities: MediaCapabilities,
    media_transport: &MediaTransport,
) -> bool {
    let update = {
        let mut state = room.state.write().await;
        state.set_client_rtp_capabilities_for_test(user_id, connection_id, &capabilities)
    };
    apply_negotiation_update(room, user_id, connection_id, update, media_transport).await
}

async fn apply_negotiation_update(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    update: UserNegotiationUpdate,
    media_transport: &MediaTransport,
) -> bool {
    if !update.session_present {
        return false;
    }
    if update.became_consumer_ready {
        return room
            .user_operation(user_id, connection_id, media_transport)
            .bootstrap_missing_consumers()
            .await;
    }
    true
}

pub(super) async fn make_session_ready_with_transport(
    room: &super::super::Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) {
    let connection_id = user_connection_id(room, user_id).await;
    create_transport_session_offer(room, user_id, media_transport).await;
    assert_eq!(
        room.apply_session_negotiated(
            user_id,
            connection_id,
            test_client_rtp_capabilities(),
            media_transport,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );
}

pub(super) async fn create_transport_session_offer(
    room: &super::super::Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) {
    let connection_id = user_connection_id(room, user_id).await;
    let session_key = room.transport_user_key(user_id, connection_id);
    media_transport
        .create_initial_session_offer(&session_key)
        .await
        .expect("real RTC test user should create an initial offer");
}

pub(super) async fn refresh_session_consumers(
    room: &super::super::Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> bool {
    room.user_operation(
        user_id,
        user_connection_id(room, user_id).await,
        media_transport,
    )
    .apply_session_refreshed()
    .await
        == SessionNegotiationOutcome::Applied
}

pub(super) struct StagedPublishScenario {
    pub(super) room: Arc<super::super::Room>,
    pub(super) adapter: MediaTransport,
    pub(super) user_id: UserId,
    pub(super) connection_id: ConnectionId,
    publisher_rx: UserOutboundReceiver,
    subscriber_rx: UserOutboundReceiver,
}

impl StagedPublishScenario {
    fn operation(&self) -> super::super::RoomUserOperation<'_> {
        self.room
            .user_operation(&self.user_id, self.connection_id, &self.adapter)
    }

    pub(super) async fn new() -> Self {
        let (room, adapter, publisher_rx, subscriber_rx) = setup_two_ready_users().await;
        let user_id = UserId::Integer(1);
        let connection_id = user_connection_id(&room, &user_id).await;
        Self {
            room,
            adapter,
            user_id,
            connection_id,
            publisher_rx,
            subscriber_rx,
        }
    }

    pub(super) async fn stage_source(&self, stream_type: TestSourceKind) -> PublishStageOutcome {
        self.operation()
            .stage_negotiated_publish(&source_publish_intent_for_source(stream_type))
            .await
            .expect("stage publish should not hit transport failure")
    }

    pub(super) async fn stage_scalable_video(&self) -> PublishStageOutcome {
        self.stage_source(TestSourceKind::ScalableVideo).await
    }

    pub(super) async fn rollback_scalable_video(&self) -> RollbackStagedPublishOutcome {
        self.operation()
            .rollback_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
            .await
    }

    pub(super) async fn unpublish_scalable_video(&self) -> UnpublishOutcome {
        self.operation()
            .unpublish(&stream_id_for_source(TestSourceKind::ScalableVideo))
            .await
    }

    pub(super) async fn commit(&self) {
        let staged_transport_media_id = self
            .staged_transport_media_id(TestSourceKind::ScalableVideo)
            .await;
        let applied_answer = AppliedSessionAnswer::from_negotiated_producers([(
            staged_transport_media_id,
            test_simulcast_video_rtp_parameters(),
        )]);
        self.operation()
            .commit_staged_publishes(&applied_answer)
            .await;
    }

    pub(super) async fn rollback_connection(&self) {
        self.operation()
            .rollback_staged_publishes_for_connection()
            .await;
    }

    pub(super) async fn staged_count(&self) -> usize {
        self.room
            .staged_publish_count_for_connection(&self.user_id, self.connection_id)
            .await
    }

    pub(super) async fn scalable_video_is_published(&self) -> bool {
        let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
        self.room
            .is_stream_published(&self.user_id, &stream_id)
            .await
    }

    pub(super) async fn staged_transport_media_id(
        &self,
        stream_type: TestSourceKind,
    ) -> TransportMediaId {
        self.room
            .staged_publish_transport_media_id(&self.user_id, self.connection_id, stream_type)
            .await
            .expect("staged publish should expose its transport media id")
    }

    pub(super) fn drain_publisher(&mut self) -> Vec<UserOutbound> {
        drain_outbound(&mut self.publisher_rx)
    }

    pub(super) fn drain_subscriber(&mut self) -> Vec<UserOutbound> {
        drain_outbound(&mut self.subscriber_rx)
    }

    pub(super) fn assert_no_outbound(&mut self) {
        assert!(self.drain_publisher().is_empty());
        assert!(self.drain_subscriber().is_empty());
    }

    pub(super) async fn route_for_staged_media_exists(
        &self,
        transport_media_id: TransportMediaId,
    ) -> bool {
        self.adapter
            .debug_route_entry_by_media_id(transport_media_id)
            .await
            .is_some()
    }
}

#[derive(Clone, Copy)]
struct ReadyRoomFixtureOptions {
    publish_camera_before_subscriber_ready: bool,
}

pub(super) struct ReadyRoomFixture {
    room: Arc<super::super::Room>,
    adapter: MediaTransport,
    first_rx: UserOutboundReceiver,
    second_rx: UserOutboundReceiver,
}

impl ReadyRoomFixtureOptions {
    const fn two_ready_users() -> Self {
        Self {
            publish_camera_before_subscriber_ready: false,
        }
    }

    const fn late_join_bootstrap() -> Self {
        Self {
            publish_camera_before_subscriber_ready: true,
        }
    }
}

async fn setup_ready_room_fixture(options: ReadyRoomFixtureOptions) -> ReadyRoomFixture {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let (first_tx, first_rx) = test_sender();
    let (second_tx, second_rx) = test_sender();
    join_user_with_sender(&room, UserId::Integer(1), first_tx).await;
    join_user_with_sender(&room, UserId::Integer(2), second_tx).await;

    let adapter = real_adapter();

    make_session_ready_with_transport(&room, &UserId::Integer(1), &adapter).await;

    if options.publish_camera_before_subscriber_ready {
        try_publish_camera(&room, &UserId::Integer(1), &adapter).await;
        create_transport_session_offer(&room, &UserId::Integer(2), &adapter).await;
    }

    if !options.publish_camera_before_subscriber_ready {
        make_session_ready_with_transport(&room, &UserId::Integer(2), &adapter).await;
    }

    ReadyRoomFixture {
        room,
        adapter,
        first_rx,
        second_rx,
    }
}

/// Set up a room with two joined users that both have upload and download
/// transports connected plus client RTP capabilities, ready for publish/consume tests.
pub(super) async fn setup_two_ready_users() -> (
    Arc<super::super::Room>,
    MediaTransport,
    UserOutboundReceiver,
    UserOutboundReceiver,
) {
    let fixture = setup_ready_room_fixture(ReadyRoomFixtureOptions::two_ready_users()).await;
    (
        fixture.room,
        fixture.adapter,
        fixture.first_rx,
        fixture.second_rx,
    )
}

pub(super) async fn setup_late_join_bootstrap_scenario() -> (
    Arc<super::super::Room>,
    MediaTransport,
    UserOutboundReceiver,
    UserOutboundReceiver,
) {
    let fixture = setup_ready_room_fixture(ReadyRoomFixtureOptions::late_join_bootstrap()).await;
    (
        fixture.room,
        fixture.adapter,
        fixture.first_rx,
        fixture.second_rx,
    )
}

pub(super) async fn setup_ready_users_with_transport(
    user_ids: &[i64],
) -> (Arc<super::super::Room>, MediaTransport) {
    let (room, adapter, _receivers) = setup_ready_users_with_transport_receivers(user_ids).await;
    (room, adapter)
}

pub(super) async fn setup_ready_users_with_transport_and_media_limits(
    user_ids: &[i64],
    media_limits: RoomMediaLimits,
) -> (Arc<super::super::Room>, MediaTransport) {
    let manager = RoomManager::for_test_with_media_limits(media_limits);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let adapter = real_adapter();
    for &raw_user_id in user_ids {
        let (sender, _receiver) = test_sender();
        let user_id = UserId::Integer(raw_user_id);
        join_user_without_transport_cleanup(&room, &adapter, user_id.clone(), sender).await;
        make_session_ready_with_transport(&room, &user_id, &adapter).await;
    }
    (room, adapter)
}

pub(super) async fn setup_ready_users_with_transport_receivers(
    user_ids: &[i64],
) -> (
    Arc<super::super::Room>,
    MediaTransport,
    Vec<UserOutboundReceiver>,
) {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let adapter = real_adapter();
    let mut receivers = Vec::with_capacity(user_ids.len());
    for &raw_user_id in user_ids {
        let (sender, receiver) = test_sender();
        receivers.push(receiver);
        let user_id = UserId::Integer(raw_user_id);
        join_user_without_transport_cleanup(&room, &adapter, user_id.clone(), sender).await;
        make_session_ready_with_transport(&room, &user_id, &adapter).await;
    }
    (room, adapter, receivers)
}

pub(super) async fn setup_three_ready_users_with_transport()
-> (Arc<super::super::Room>, MediaTransport) {
    setup_ready_users_with_transport(&[1, 2, 3]).await
}

pub(super) struct SourcePolicyScenario {
    pub(super) room: Arc<super::super::Room>,
    pub(super) adapter: MediaTransport,
}

impl SourcePolicyScenario {
    pub(super) async fn with_ready_users(user_ids: &[i64]) -> Self {
        let (room, adapter) = setup_ready_users_with_transport(user_ids).await;
        Self { room, adapter }
    }

    pub(super) async fn with_ready_users_and_media_limits(
        user_ids: &[i64],
        media_limits: RoomMediaLimits,
    ) -> Self {
        let (room, adapter) =
            setup_ready_users_with_transport_and_media_limits(user_ids, media_limits).await;
        Self { room, adapter }
    }

    pub(super) async fn three_ready_users() -> Self {
        Self::with_ready_users(&[1, 2, 3]).await
    }

    pub(super) async fn publish_audio_and_camera(&self, raw_user_id: i64) {
        publish_audio_and_camera(&self.room, &UserId::Integer(raw_user_id), &self.adapter).await;
    }

    pub(super) async fn publish_audio_and_camera_for_users(&self, user_ids: &[i64]) {
        for raw_user_id in user_ids {
            self.publish_audio_and_camera(*raw_user_id).await;
        }
    }

    pub(super) async fn audio_media_id(&self, raw_user_id: i64) -> TransportMediaId {
        let (audio_media_id, _camera_media_id) =
            source_media_ids(&self.room, &UserId::Integer(raw_user_id)).await;
        audio_media_id
    }

    pub(super) async fn mark_active_speaker(&self, transport_media_id: TransportMediaId) {
        self.mark_active_speakers([transport_media_id]).await;
    }

    pub(super) async fn mark_active_speakers(
        &self,
        transport_media_ids: impl IntoIterator<Item = TransportMediaId>,
    ) {
        let observed_at = Instant::now();
        for transport_media_id in transport_media_ids {
            self.adapter
                .debug_observe_audio_activity(transport_media_id, observed_at)
                .await;
        }
    }

    pub(super) async fn mark_active_speakers_with_levels(
        &self,
        speakers: impl IntoIterator<Item = (TransportMediaId, i8)>,
    ) {
        let observed_at = Instant::now();
        for (transport_media_id, audio_level_dbov) in speakers {
            self.adapter
                .debug_observe_audio_activity_with_level(
                    transport_media_id,
                    audio_level_dbov,
                    observed_at,
                )
                .await;
        }
    }

    pub(super) async fn refresh_policy(&self) {
        self.room
            .sync_source_packet_selection_policy(&self.adapter)
            .await;
    }

    pub(super) async fn refresh_policy_until_upgrades_settle(&self) {
        for _ in 0..3 {
            self.refresh_policy().await;
        }
    }

    pub(super) async fn set_scalable_video_layout(
        &self,
        receiver_user_id: i64,
        source_user_id: i64,
        layout: VideoLayoutIntent,
    ) {
        let receiver_user_id = UserId::Integer(receiver_user_id);
        let source_user_id = UserId::Integer(source_user_id);
        let intents = subscription_intents_from_test_states(&TestSubscriptionStates {
            scalable_video_layout: Some(layout),
            ..TestSubscriptionStates::default()
        });
        assert_eq!(
            self.room
                .user_operation(
                    &receiver_user_id,
                    user_connection_id(&self.room, &receiver_user_id).await,
                    &self.adapter,
                )
                .update_subscription(&source_user_id, &intents)
                .await,
            SubscriptionUpdateOutcome::Applied
        );
    }
}

pub(super) async fn try_publish_camera(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> Option<UserStreamId> {
    room.test_api()
        .media()
        .publish_track(
            user_id,
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            media_transport,
        )
        .await
}

pub(super) async fn publish_simulcast_camera(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> UserStreamId {
    publish_track(
        room,
        user_id,
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_simulcast_video_rtp_parameters(),
        media_transport,
    )
    .await
}

pub(super) async fn publish_audio_and_camera(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) {
    publish_track(
        room,
        user_id,
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        media_transport,
    )
    .await;
    publish_simulcast_camera(room, user_id, media_transport).await;
}

pub(super) async fn publish_track(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    stream_type: TestSourceKind,
    media_kind: MediaKind,
    rtp_parameters: MediaStream,
    media_transport: &MediaTransport,
) -> UserStreamId {
    room.test_api()
        .media()
        .publish_track(
            user_id,
            stream_type,
            media_kind,
            rtp_parameters,
            media_transport,
        )
        .await
        .expect("publication should succeed")
}

pub(super) async fn source_media_ids(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
) -> (TransportMediaId, TransportMediaId) {
    let audio_media_id = source_media_id(room, user_id, TestSourceKind::AudioDetector).await;
    let camera_media_id = source_media_id(room, user_id, TestSourceKind::ScalableVideo).await;
    (audio_media_id, camera_media_id)
}

pub(super) fn pause_scalable_video_intents() -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    scalable_video_intents(false)
}

pub(super) fn resume_scalable_video_intents() -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    scalable_video_intents(true)
}

pub(super) fn pause_audio_and_scalable_video_intents()
-> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    subscription_intents_from_test_states(&TestSubscriptionStates {
        audio_detector: Some(false),
        scalable_video: Some(false),
        ..TestSubscriptionStates::default()
    })
}

fn scalable_video_intents(active: bool) -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    subscription_intents_from_test_states(&TestSubscriptionStates {
        scalable_video: Some(active),
        ..TestSubscriptionStates::default()
    })
}

pub(super) async fn source_media_id(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    stream_type: TestSourceKind,
) -> TransportMediaId {
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should exist");
    };
    let Some(transport_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, stream_type)
        .await
    else {
        panic!("{stream_type:?} producer should expose a transport media id");
    };
    transport_media_id
}

pub(super) fn drain_outbound(rx: &mut UserOutboundReceiver) -> Vec<UserOutbound> {
    let mut msgs = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        msgs.push(msg);
    }
    msgs
}
