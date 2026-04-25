pub(super) use std::{sync::Arc, time::Duration};

pub(super) use o_sfu_protocol::shared::{
    DownloadStates, StreamType, UserId, UserInfo, UserPermissions, VideoLayoutIntent,
};
pub(super) use o_sfu_router::{
    ConsumerCapability, MediaCapabilities, MediaKind, MediaKind as RouterMediaKind, MediaStream,
    RouterId, SessionPermissions as RouterSessionPermissions, StreamType as RouterStreamType,
};
pub(super) use tokio::{sync::mpsc, task::yield_now, time::timeout};

pub(super) use super::super::{
    JoinUserRequest, RoomAdmissionPolicy, RoomConfig, RoomEventMessage, RoomEventRequest,
    RoomJoinError, RoomManager, RoomManagerJoinError, UserCloseReason, UserOutbound,
    topology::RoomTopology,
};
pub(super) use crate::runtime::{
    ConnectionId,
    transport_adapter::{
        ActiveSpeakerSource, NegotiationPort, RuntimeTransportAdapter, TransportMediaId,
        test_support::{FakeWebRtcAdapter, FakeWebRtcEvent},
    },
};
use crate::runtime::{
    room::user_negotiation::{UserNegotiationUpdate, UserTransportReady},
    test_rtp_samples::{
        sample_audio_rtp_parameters, sample_client_rtp_capabilities,
        sample_client_rtp_capabilities_without_video_rtx, sample_simulcast_video_rtp_parameters,
        sample_video_rtp_parameters,
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

pub(super) fn fake_adapter() -> (RuntimeTransportAdapter, Arc<FakeWebRtcAdapter>) {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    (
        RuntimeTransportAdapter::from_fake_adapter(Arc::clone(&adapter)),
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
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    apply_transport_ready(
        room,
        user_id,
        connection_id,
        UserTransportReady::Publish,
        transport_adapter,
    )
    .await
}

pub(super) async fn apply_consume_transport_ready(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    apply_transport_ready(
        room,
        user_id,
        connection_id,
        UserTransportReady::Consume,
        transport_adapter,
    )
    .await
}

async fn apply_transport_ready(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    readiness: UserTransportReady,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    let update = {
        let mut state = room.state.write().await;
        state.set_transport_ready_for_test(user_id, connection_id, readiness)
    };
    apply_negotiation_update(room, user_id, connection_id, update, transport_adapter).await
}

pub(super) async fn apply_client_rtp_capabilities(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    capabilities: MediaCapabilities,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    let update = {
        let mut state = room.state.write().await;
        state.set_client_rtp_capabilities_for_test(user_id, connection_id, &capabilities)
    };
    apply_negotiation_update(room, user_id, connection_id, update, transport_adapter).await
}

async fn apply_negotiation_update(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    update: UserNegotiationUpdate,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    if !update.session_present {
        return false;
    }
    if update.became_consumer_ready {
        return room
            .bootstrap_missing_consumers_for_connection(user_id, connection_id, transport_adapter)
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
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    room.apply_session_refreshed(
        user_id,
        user_connection_id(room, user_id).await,
        transport_adapter,
    )
    .await
}

pub(super) async fn stage_negotiated_publish(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    room.stage_negotiated_publish(user_id, connection_id, stream_type, transport_adapter)
        .await
}

pub(super) async fn rollback_staged_publish(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    room.rollback_staged_publish(user_id, connection_id, stream_type, transport_adapter)
        .await
}

pub(super) async fn commit_staged_publishes(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    transport_adapter: &RuntimeTransportAdapter,
) {
    let session_key = room.transport_user_key(user_id, connection_id);
    let applied_answer = transport_adapter
        .apply_session_answer(&session_key, "")
        .await
        .unwrap_or_default();
    room.commit_staged_publishes(
        user_id,
        connection_id,
        &applied_answer,
        transport_adapter,
        transport_adapter,
    )
    .await;
}

pub(super) async fn rollback_staged_publishes_for_connection(
    room: &super::super::Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    transport_adapter: &RuntimeTransportAdapter,
) {
    room.rollback_staged_publishes_for_connection(user_id, connection_id, transport_adapter)
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
    adapter: RuntimeTransportAdapter,
    fake: Option<Arc<FakeWebRtcAdapter>>,
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
        (RuntimeTransportAdapter::fake_for_testing(), None)
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
    RuntimeTransportAdapter,
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
    RuntimeTransportAdapter,
    Arc<FakeWebRtcAdapter>,
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
    RuntimeTransportAdapter,
    Arc<FakeWebRtcAdapter>,
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
    adapter: &FakeWebRtcAdapter,
    predicate: impl Fn(&FakeWebRtcEvent) -> bool,
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
