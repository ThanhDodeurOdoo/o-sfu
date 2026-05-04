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
        ConnectionId, DownloadStates, StreamType, UserId, UserInfo, UserPermissions,
        VideoLayoutIntent,
        media_transport::{
            ActiveSpeakerSource, MediaTransport, NegotiationPort, TransportMediaId,
            test_support::{FakeMediaTransport, FakeMediaTransportEvent},
        },
        source_model::{
            UserStreamId,
            test_support::{source_publish_intent_for_stream_type, stream_id_for_stream_type},
        },
    },
};

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

pub(super) async fn stage_negotiated_publish(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    media_transport: &MediaTransport,
) -> bool {
    room.stage_negotiated_publish(
        user_id,
        connection_id,
        &source_publish_intent_for_stream_type(stream_type),
        media_transport,
    )
    .await
    .is_ok_and(PublishStageOutcome::staged)
}

pub(super) async fn rollback_staged_publish(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    media_transport: &MediaTransport,
) -> bool {
    matches!(
        room.rollback_staged_publish(
            user_id,
            connection_id,
            &stream_id_for_stream_type(stream_type),
            media_transport,
        )
        .await,
        RollbackStagedPublishOutcome::RolledBack { .. }
    )
}

pub(super) async fn commit_staged_publishes(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    media_transport: &MediaTransport,
) {
    let session_key = room.transport_user_key(user_id, connection_id);
    let applied_answer = media_transport
        .apply_session_answer(&session_key, "")
        .await
        .unwrap_or_default();
    room.commit_staged_publishes(
        user_id,
        connection_id,
        &applied_answer,
        media_transport,
        media_transport,
    )
    .await;
}

pub(super) async fn rollback_staged_publishes_for_connection(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    media_transport: &MediaTransport,
) {
    room.rollback_staged_publishes_for_connection(user_id, connection_id, media_transport)
        .await;
}

pub(super) async fn staged_publish_count(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
) -> usize {
    room.staged_publish_count_for_connection(user_id, connection_id)
        .await
}

pub(super) async fn staged_publish_transport_media_id(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: StreamType,
) -> Option<TransportMediaId> {
    room.staged_publish_transport_media_id(user_id, connection_id, stream_type)
        .await
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
                StreamType::Camera,
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
