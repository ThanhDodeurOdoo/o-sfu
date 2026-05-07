pub(super) use std::{sync::Arc, time::Duration};

use o_sfu_router::test_support::rtp_samples::{
    sample_audio_rtp_parameters, sample_client_rtp_capabilities,
    sample_client_rtp_capabilities_without_video_rtx, sample_simulcast_video_rtp_parameters,
    sample_video_rtp_parameters,
};
pub(super) use o_sfu_router::{
    ConsumerCapability, MediaCapabilities, MediaKind, MediaKind as RouterMediaKind, MediaStream,
    RouterId,
};
pub(super) use tokio::{sync::mpsc, task::yield_now, time::timeout};

pub(super) use super::super::{
    JoinUserRequest, RoomAdmissionPolicy, RoomConfig, RoomEventMessage, RoomEventRequest,
    RoomJoinError, RoomManager, RoomManagerJoinError, UserCloseReason, UserOutbound,
    topology::RoomTopology,
};
use crate::runtime::room::user_negotiation::{UserNegotiationUpdate, UserTransportReady};
pub(super) use crate::{
    PublishStageOutcome, RollbackStagedPublishOutcome, SessionNegotiationOutcome, UnpublishOutcome,
    runtime::{
        ConnectionId, TestSourceKind, TestSubscriptionStates, UserId, UserInfo, UserPermissions,
        VideoLayoutIntent,
        media_transport::{
            ActiveSpeakerSource, MediaTransport, SourcePacketGate, TransportMediaId,
            test_support::{FakeMediaTransport, FakeMediaTransportEvent},
        },
        metrics::{MetricName, RuntimeMetricsSnapshot, test_support::RuntimeMetricsSnapshotLookup},
        source_model::{
            UserStreamId,
            test_support::{source_publish_intent_for_source, stream_id_for_source},
        },
    },
    transport::NegotiationPort,
};

pub(super) trait RuntimeMetricsSnapshotTestExt: RuntimeMetricsSnapshotLookup {
    fn active_rooms(&self) -> i64 {
        self.gauge_value(MetricName::RoomsActive, &[])
    }

    fn active_users(&self) -> i64 {
        self.gauge_value(MetricName::UsersActive, &[])
    }

    fn active_publications(&self) -> i64 {
        self.gauge_value(MetricName::PublicationsActive, &[])
    }

    fn active_subscriptions(&self) -> i64 {
        self.gauge_value(MetricName::SubscriptionsActive, &[])
    }

    fn active_recording_rooms(&self) -> i64 {
        self.gauge_value(MetricName::RecordingRoomsActive, &[])
    }

    fn recording_start_accepted(&self) -> u64 {
        self.counter_value(
            MetricName::RecordingActionsTotal,
            &[("action", "start"), ("outcome", "accepted")],
        )
    }

    fn recording_start_rejected(&self) -> u64 {
        self.counter_value(
            MetricName::RecordingActionsTotal,
            &[("action", "start"), ("outcome", "rejected")],
        )
    }

    fn recording_stop_accepted(&self) -> u64 {
        self.counter_value(
            MetricName::RecordingActionsTotal,
            &[("action", "stop"), ("outcome", "accepted")],
        )
    }

    fn recording_stop_rejected(&self) -> u64 {
        self.counter_value(
            MetricName::RecordingActionsTotal,
            &[("action", "stop"), ("outcome", "rejected")],
        )
    }

    fn transport_cleanup_failures_shutdown(&self) -> u64 {
        self.counter_value(
            MetricName::TransportCleanupFailuresTotal,
            &[("kind", "shutdown")],
        )
    }

    fn source_selection_updates_encoding(&self) -> u64 {
        self.counter_value(
            MetricName::SourceSelectionUpdatesTotal,
            &[("selector", "encoding")],
        )
    }
}

impl RuntimeMetricsSnapshotTestExt for RuntimeMetricsSnapshot {}

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

pub(super) fn test_sender() -> (
    mpsc::UnboundedSender<UserOutbound>,
    mpsc::UnboundedReceiver<UserOutbound>,
) {
    mpsc::unbounded_channel()
}

pub(super) fn fake_adapter() -> (MediaTransport, Arc<FakeMediaTransport>) {
    let adapter = Arc::new(FakeMediaTransport::default());
    (
        MediaTransport::from_fake_transport(Arc::clone(&adapter)),
        adapter,
    )
}

pub(super) fn test_connection_id(raw: u64) -> ConnectionId {
    ConnectionId::from_raw(raw)
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

pub(super) async fn set_publish_transport_ready(
    room: &super::super::Room,
    user_id: &UserId,
) -> UserNegotiationUpdate {
    set_transport_ready(room, user_id, UserTransportReady::Publish).await
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

pub(super) async fn apply_publish_transport_ready(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    media_transport: &MediaTransport,
) -> bool {
    apply_transport_ready(
        room,
        user_id,
        connection_id,
        UserTransportReady::Publish,
        media_transport,
    )
    .await
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
            .bootstrap_missing_consumers_for_connection(user_id, connection_id, media_transport)
            .await;
    }
    true
}

pub(super) async fn make_session_ready(room: &super::super::Room, user_id: &UserId) {
    let _ = set_publish_transport_ready(room, user_id).await;
    let _ = set_consume_transport_ready(room, user_id).await;
    let _ = set_client_rtp_capabilities(room, user_id, test_client_rtp_capabilities()).await;
}

pub(super) async fn refresh_session_consumers(
    room: &super::super::Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> bool {
    room.apply_session_refreshed(
        user_id,
        user_connection_id(room, user_id).await,
        media_transport,
    )
    .await
        == SessionNegotiationOutcome::Applied
}

pub(super) struct StagedPublishScenario {
    pub(super) room: Arc<super::super::Room>,
    pub(super) adapter: MediaTransport,
    pub(super) fake: Arc<FakeMediaTransport>,
    pub(super) user_id: UserId,
    pub(super) connection_id: ConnectionId,
    publisher_rx: mpsc::UnboundedReceiver<UserOutbound>,
    subscriber_rx: mpsc::UnboundedReceiver<UserOutbound>,
}

impl StagedPublishScenario {
    pub(super) async fn new() -> Self {
        let (room, adapter, fake, publisher_rx, subscriber_rx) =
            setup_two_ready_users_with_fake().await;
        let user_id = UserId::Integer(1);
        let connection_id = user_connection_id(&room, &user_id).await;
        Self {
            room,
            adapter,
            fake,
            user_id,
            connection_id,
            publisher_rx,
            subscriber_rx,
        }
    }

    pub(super) async fn stage_source(&self, stream_type: TestSourceKind) -> PublishStageOutcome {
        self.room
            .stage_negotiated_publish(
                &self.user_id,
                self.connection_id,
                &source_publish_intent_for_source(stream_type),
                &self.adapter,
            )
            .await
            .expect("stage publish should not hit transport failure")
    }

    pub(super) async fn stage_scalable_video(&self) -> PublishStageOutcome {
        self.stage_source(TestSourceKind::ScalableVideo).await
    }

    pub(super) async fn rollback_scalable_video(&self) -> RollbackStagedPublishOutcome {
        self.room
            .rollback_staged_publish(
                &self.user_id,
                self.connection_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &self.adapter,
            )
            .await
    }

    pub(super) async fn unpublish_scalable_video(&self) -> UnpublishOutcome {
        self.room
            .unpublish_track(
                &self.user_id,
                self.connection_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &self.adapter,
            )
            .await
    }

    pub(super) async fn commit(&self) {
        let session_key = self
            .room
            .transport_user_key(&self.user_id, self.connection_id);
        let applied_answer = self
            .adapter
            .apply_session_answer(&session_key, "")
            .await
            .unwrap_or_default();
        self.room
            .commit_staged_publishes(
                &self.user_id,
                self.connection_id,
                &applied_answer,
                &self.adapter,
                &self.adapter,
            )
            .await;
    }

    pub(super) async fn rollback_connection(&self) {
        self.room
            .rollback_staged_publishes_for_connection(
                &self.user_id,
                self.connection_id,
                &self.adapter,
            )
            .await;
    }

    pub(super) async fn staged_count(&self) -> usize {
        self.room
            .staged_publish_count_for_connection(&self.user_id, self.connection_id)
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

    pub(super) fn publish_media_requested_count(&self) -> usize {
        self.fake
            .snapshot_events()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    FakeMediaTransportEvent::PublishMediaRequested { user_id, .. }
                        if user_id == &self.user_id
                )
            })
            .count()
    }

    pub(super) fn removed_media_count(&self) -> usize {
        self.fake
            .snapshot_events()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    FakeMediaTransportEvent::MediaRemoved { user_id, .. }
                        if user_id == &self.user_id
                )
            })
            .count()
    }

    pub(super) fn has_removed_media(&self, transport_media_id: TransportMediaId) -> bool {
        self.fake.snapshot_events().iter().any(|event| {
            matches!(
                event,
                FakeMediaTransportEvent::MediaRemoved {
                    user_id,
                    transport_media_id: removed_media_id,
                } if user_id == &self.user_id && *removed_media_id == transport_media_id
            )
        })
    }
}

#[derive(Clone, Copy)]
struct ReadySessionScenarioOptions {
    include_fake_adapter: bool,
    publish_camera_before_subscriber_ready: bool,
}

struct ReadySessionScenario {
    room: Arc<super::super::Room>,
    adapter: MediaTransport,
    fake: Option<Arc<FakeMediaTransport>>,
    first_rx: mpsc::UnboundedReceiver<UserOutbound>,
    second_rx: mpsc::UnboundedReceiver<UserOutbound>,
}

impl ReadySessionScenarioOptions {
    const fn two_ready_users() -> Self {
        Self {
            include_fake_adapter: false,
            publish_camera_before_subscriber_ready: false,
        }
    }

    const fn two_ready_users_with_fake() -> Self {
        Self {
            include_fake_adapter: true,
            publish_camera_before_subscriber_ready: false,
        }
    }

    const fn late_join_bootstrap() -> Self {
        Self {
            include_fake_adapter: true,
            publish_camera_before_subscriber_ready: true,
        }
    }
}

async fn setup_ready_user_scenario(options: ReadySessionScenarioOptions) -> ReadySessionScenario {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (first_tx, first_rx) = test_sender();
    let (second_tx, second_rx) = test_sender();
    room.test_api()
        .lifecycle()
        .join_user(
            UserId::Integer(1),
            None,
            UserPermissions::default(),
            first_tx,
        )
        .await
        .unwrap();
    room.test_api()
        .lifecycle()
        .join_user(
            UserId::Integer(2),
            None,
            UserPermissions::default(),
            second_tx,
        )
        .await
        .unwrap();

    let (adapter, fake) = if options.include_fake_adapter {
        let (adapter, fake) = fake_adapter();
        (adapter, Some(fake))
    } else {
        (MediaTransport::fake_for_testing(), None)
    };

    make_session_ready(&room, &UserId::Integer(1)).await;

    if options.publish_camera_before_subscriber_ready {
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                TestSourceKind::ScalableVideo,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await;
    }

    if !options.publish_camera_before_subscriber_ready {
        make_session_ready(&room, &UserId::Integer(2)).await;
    }

    ReadySessionScenario {
        room,
        adapter,
        fake,
        first_rx,
        second_rx,
    }
}

/// Set up a room with two joined users that both have upload and download
/// transports connected plus client RTP capabilities, ready for publish/consume tests.
pub(super) async fn setup_two_ready_users() -> (
    Arc<super::super::Room>,
    MediaTransport,
    mpsc::UnboundedReceiver<UserOutbound>,
    mpsc::UnboundedReceiver<UserOutbound>,
) {
    let scenario = setup_ready_user_scenario(ReadySessionScenarioOptions::two_ready_users()).await;
    (
        scenario.room,
        scenario.adapter,
        scenario.first_rx,
        scenario.second_rx,
    )
}

pub(super) async fn setup_two_ready_users_with_fake() -> (
    Arc<super::super::Room>,
    MediaTransport,
    Arc<FakeMediaTransport>,
    mpsc::UnboundedReceiver<UserOutbound>,
    mpsc::UnboundedReceiver<UserOutbound>,
) {
    let scenario =
        setup_ready_user_scenario(ReadySessionScenarioOptions::two_ready_users_with_fake()).await;
    (
        scenario.room,
        scenario.adapter,
        scenario.fake.unwrap(),
        scenario.first_rx,
        scenario.second_rx,
    )
}

pub(super) async fn setup_late_join_bootstrap_scenario() -> (
    Arc<super::super::Room>,
    MediaTransport,
    Arc<FakeMediaTransport>,
    mpsc::UnboundedReceiver<UserOutbound>,
    mpsc::UnboundedReceiver<UserOutbound>,
) {
    let scenario =
        setup_ready_user_scenario(ReadySessionScenarioOptions::late_join_bootstrap()).await;
    (
        scenario.room,
        scenario.adapter,
        scenario.fake.unwrap(),
        scenario.first_rx,
        scenario.second_rx,
    )
}

pub(super) async fn setup_ready_users_with_fake(
    user_ids: &[i64],
) -> (
    Arc<super::super::Room>,
    MediaTransport,
    Arc<FakeMediaTransport>,
) {
    let (room, adapter, fake, _receivers) = setup_ready_users_with_fake_receivers(user_ids).await;
    (room, adapter, fake)
}

pub(super) async fn setup_ready_users_with_fake_receivers(
    user_ids: &[i64],
) -> (
    Arc<super::super::Room>,
    MediaTransport,
    Arc<FakeMediaTransport>,
    Vec<mpsc::UnboundedReceiver<UserOutbound>>,
) {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    let mut receivers = Vec::with_capacity(user_ids.len());
    for &raw_user_id in user_ids {
        let (sender, receiver) = test_sender();
        receivers.push(receiver);
        let user_id = UserId::Integer(raw_user_id);
        room.test_api()
            .lifecycle()
            .join_session_without_transport_cleanup(
                user_id.clone(),
                None,
                UserPermissions::default(),
                sender,
                &adapter,
            )
            .await
            .expect("user should join");
        make_session_ready(&room, &user_id).await;
    }
    (room, adapter, fake, receivers)
}

pub(super) async fn setup_three_ready_users_with_fake() -> (
    Arc<super::super::Room>,
    MediaTransport,
    Arc<FakeMediaTransport>,
) {
    setup_ready_users_with_fake(&[1, 2, 3]).await
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
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should exist");
    };
    let Some(audio_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, TestSourceKind::AudioDetector)
        .await
    else {
        panic!("audio producer should expose a transport media id");
    };
    let Some(camera_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, TestSourceKind::ScalableVideo)
        .await
    else {
        panic!("camera producer should expose a transport media id");
    };
    (audio_media_id, camera_media_id)
}

pub(super) fn assert_consumer_packet_selection_update(
    events: &[FakeMediaTransportEvent],
    consumer_user_id: &UserId,
    source_user_id: &UserId,
    expected_rid: &str,
) {
    assert!(events.iter().any(|event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumerPacketGateUpdated {
                consumer_user_id: updated_consumer_user_id,
                source_user_id: updated_source_user_id,
                packet_gate: SourcePacketGate::Rid(rid),
            } if updated_consumer_user_id == consumer_user_id
                && updated_source_user_id == source_user_id
                && rid == expected_rid
        )
    }));
}

pub(super) fn assert_consumer_keyframe_request(
    events: &[FakeMediaTransportEvent],
    expected_consumer_user_id: &UserId,
    expected_source_user_id: &UserId,
) {
    assert!(events.iter().any(|event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumerKeyframeRequested {
                consumer_user_id,
                source_user_id,
            } if consumer_user_id == expected_consumer_user_id
                && source_user_id == expected_source_user_id
        )
    }));
}

pub(super) fn assert_consumer_activity_update(
    events: &[FakeMediaTransportEvent],
    expected_consumer_user_id: &UserId,
    expected_source_user_id: &UserId,
    expected_active: bool,
) {
    assert!(events.iter().any(|event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumerActivityUpdated {
                consumer_user_id,
                source_user_id,
                active,
            } if consumer_user_id == expected_consumer_user_id
                && source_user_id == expected_source_user_id
                && *active == expected_active
        )
    }));
}

pub(super) fn assert_featured_snapshot_update(
    messages: &[UserOutbound],
    user_id: &UserId,
    is_featured: bool,
) {
    assert!(messages.iter().any(|message| {
        matches!(
            message,
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(snapshot))
                if snapshot.get(user_id).is_some_and(|info| info.is_featured == Some(is_featured))
        )
    }));
}

pub(super) fn drain_outbound(rx: &mut mpsc::UnboundedReceiver<UserOutbound>) -> Vec<UserOutbound> {
    let mut msgs = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        msgs.push(msg);
    }
    msgs
}

pub(super) async fn wait_for_fake_event(
    adapter: &FakeMediaTransport,
    predicate: impl Fn(&FakeMediaTransportEvent) -> bool,
) {
    let wait_result = timeout(Duration::from_secs(1), async {
        loop {
            if adapter.snapshot_events().iter().any(&predicate) {
                break;
            }
            yield_now().await;
        }
    })
    .await;
    assert!(
        wait_result.is_ok(),
        "timed out waiting for fake transport event"
    );
}
