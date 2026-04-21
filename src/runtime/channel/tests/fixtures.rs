pub(super) use std::{sync::Arc, time::Duration};

pub(super) use o_sfu_router::{
    ConsumerCapability, MediaCapabilities, MediaKind, MediaKind as RouterMediaKind, MediaStream,
    RouterId, SessionPermissions as RouterSessionPermissions, StreamType as RouterStreamType,
};
pub(super) use tokio::sync::mpsc;
pub(super) use tokio::{task::yield_now, time::timeout};

pub(super) use super::super::{
    ChannelAdmissionPolicy, ChannelConfig, ChannelEventMessage, ChannelEventRequest,
    ChannelJoinError, ChannelManager, ChannelManagerJoinError, JoinSessionRequest,
    SessionCloseReason, SessionOutbound, topology::ChannelTopology,
};
pub(super) use crate::runtime::ConnectionId;
use crate::runtime::channel::session_negotiation::{
    SessionNegotiationUpdate, SessionTransportReady,
};
use crate::runtime::test_rtp_samples::{
    sample_audio_rtp_parameters, sample_client_rtp_capabilities,
    sample_client_rtp_capabilities_without_video_rtx, sample_simulcast_video_rtp_parameters,
    sample_video_rtp_parameters,
};
pub(super) use crate::runtime::transport_adapter::test_support::{
    FakeWebRtcAdapter, FakeWebRtcEvent,
};
pub(super) use crate::runtime::transport_adapter::{ActiveSpeakerSource, RuntimeTransportAdapter};
pub(super) use o_sfu_protocol::shared::{
    DownloadStates, SessionId, SessionInfo, SessionPermissions, StreamType,
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
    mpsc::UnboundedSender<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
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

pub(super) async fn session_connection_id(
    channel: &super::super::Channel,
    session_id: &SessionId,
) -> ConnectionId {
    channel
        .test_api()
        .inspect()
        .session_connection_id(session_id)
        .await
        .expect("test fixture requires a live session connection")
}

pub(super) async fn set_publish_transport_ready(
    channel: &super::super::Channel,
    session_id: &SessionId,
) -> SessionNegotiationUpdate {
    set_transport_ready(channel, session_id, SessionTransportReady::Publish).await
}

pub(super) async fn set_consume_transport_ready(
    channel: &super::super::Channel,
    session_id: &SessionId,
) -> SessionNegotiationUpdate {
    set_transport_ready(channel, session_id, SessionTransportReady::Consume).await
}

async fn set_transport_ready(
    channel: &super::super::Channel,
    session_id: &SessionId,
    readiness: SessionTransportReady,
) -> SessionNegotiationUpdate {
    let connection_id = session_connection_id(channel, session_id).await;
    let mut state = channel.state.write().await;
    state.set_transport_ready_for_test(session_id, connection_id, readiness)
}

pub(super) async fn set_client_rtp_capabilities(
    channel: &super::super::Channel,
    session_id: &SessionId,
    capabilities: MediaCapabilities,
) -> SessionNegotiationUpdate {
    let connection_id = session_connection_id(channel, session_id).await;
    let mut state = channel.state.write().await;
    state.set_client_rtp_capabilities_for_test(session_id, connection_id, &capabilities)
}

pub(super) async fn apply_publish_transport_ready(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    apply_transport_ready(
        channel,
        session_id,
        connection_id,
        SessionTransportReady::Publish,
        transport_adapter,
    )
    .await
}

pub(super) async fn apply_consume_transport_ready(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    apply_transport_ready(
        channel,
        session_id,
        connection_id,
        SessionTransportReady::Consume,
        transport_adapter,
    )
    .await
}

async fn apply_transport_ready(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    readiness: SessionTransportReady,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    let update = {
        let mut state = channel.state.write().await;
        state.set_transport_ready_for_test(session_id, connection_id, readiness)
    };
    apply_negotiation_update(
        channel,
        session_id,
        connection_id,
        update,
        transport_adapter,
    )
    .await
}

pub(super) async fn apply_client_rtp_capabilities(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    capabilities: MediaCapabilities,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    let update = {
        let mut state = channel.state.write().await;
        state.set_client_rtp_capabilities_for_test(session_id, connection_id, &capabilities)
    };
    apply_negotiation_update(
        channel,
        session_id,
        connection_id,
        update,
        transport_adapter,
    )
    .await
}

async fn apply_negotiation_update(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    update: SessionNegotiationUpdate,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    if !update.session_present {
        return false;
    }
    if update.became_consumer_ready {
        return channel
            .bootstrap_missing_consumers_for_connection(
                session_id,
                connection_id,
                transport_adapter,
            )
            .await;
    }
    true
}

pub(super) async fn make_session_ready(channel: &super::super::Channel, session_id: &SessionId) {
    let _ = set_publish_transport_ready(channel, session_id).await;
    let _ = set_consume_transport_ready(channel, session_id).await;
    let _ = set_client_rtp_capabilities(channel, session_id, test_client_rtp_capabilities()).await;
}

pub(super) async fn refresh_session_consumers(
    channel: &super::super::Channel,
    session_id: &SessionId,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    channel
        .apply_session_refreshed(
            session_id,
            session_connection_id(channel, session_id).await,
            transport_adapter,
        )
        .await
}

pub(super) async fn stage_negotiated_publish(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    channel
        .stage_negotiated_publish(session_id, connection_id, stream_type, transport_adapter)
        .await
}

pub(super) async fn rollback_staged_publish(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    transport_adapter: &RuntimeTransportAdapter,
) -> bool {
    channel
        .rollback_staged_publish(session_id, connection_id, stream_type, transport_adapter)
        .await
}

pub(super) async fn commit_staged_publishes(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    transport_adapter: &RuntimeTransportAdapter,
) {
    channel
        .commit_staged_publishes(
            session_id,
            connection_id,
            transport_adapter,
            transport_adapter,
        )
        .await;
}

pub(super) async fn staged_publish_count(
    channel: &super::super::Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
) -> usize {
    channel
        .staged_publish_count_for_connection(session_id, connection_id)
        .await
}

#[derive(Clone, Copy)]
struct ReadySessionScenarioOptions {
    include_fake_adapter: bool,
    publish_camera_before_subscriber_ready: bool,
}

struct ReadySessionScenario {
    channel: Arc<super::super::Channel>,
    adapter: RuntimeTransportAdapter,
    fake: Option<Arc<FakeWebRtcAdapter>>,
    first_rx: mpsc::UnboundedReceiver<SessionOutbound>,
    second_rx: mpsc::UnboundedReceiver<SessionOutbound>,
}

impl ReadySessionScenarioOptions {
    const fn two_ready_sessions() -> Self {
        Self {
            include_fake_adapter: false,
            publish_camera_before_subscriber_ready: false,
        }
    }

    const fn two_ready_sessions_with_fake() -> Self {
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

async fn setup_ready_session_scenario(
    options: ReadySessionScenarioOptions,
) -> ReadySessionScenario {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (first_tx, first_rx) = test_sender();
    let (second_tx, second_rx) = test_sender();
    channel
        .test_api()
        .lifecycle()
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            first_tx,
        )
        .await
        .unwrap();
    channel
        .test_api()
        .lifecycle()
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
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

    make_session_ready(&channel, &SessionId::Integer(1)).await;

    if options.publish_camera_before_subscriber_ready {
        channel
            .test_api()
            .media()
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await;
    }

    if !options.publish_camera_before_subscriber_ready {
        make_session_ready(&channel, &SessionId::Integer(2)).await;
    }

    ReadySessionScenario {
        channel,
        adapter,
        fake,
        first_rx,
        second_rx,
    }
}

/// Set up a channel with two joined sessions that both have upload and download
/// transports connected plus client RTP capabilities, ready for publish/consume tests.
pub(super) async fn setup_two_ready_sessions() -> (
    Arc<super::super::Channel>,
    RuntimeTransportAdapter,
    mpsc::UnboundedReceiver<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    let scenario =
        setup_ready_session_scenario(ReadySessionScenarioOptions::two_ready_sessions()).await;
    (
        scenario.channel,
        scenario.adapter,
        scenario.first_rx,
        scenario.second_rx,
    )
}

pub(super) async fn setup_two_ready_sessions_with_fake() -> (
    Arc<super::super::Channel>,
    RuntimeTransportAdapter,
    Arc<FakeWebRtcAdapter>,
    mpsc::UnboundedReceiver<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    let scenario =
        setup_ready_session_scenario(ReadySessionScenarioOptions::two_ready_sessions_with_fake())
            .await;
    (
        scenario.channel,
        scenario.adapter,
        scenario.fake.unwrap(),
        scenario.first_rx,
        scenario.second_rx,
    )
}

pub(super) async fn setup_late_join_bootstrap_scenario() -> (
    Arc<super::super::Channel>,
    RuntimeTransportAdapter,
    Arc<FakeWebRtcAdapter>,
    mpsc::UnboundedReceiver<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    let scenario =
        setup_ready_session_scenario(ReadySessionScenarioOptions::late_join_bootstrap()).await;
    (
        scenario.channel,
        scenario.adapter,
        scenario.fake.unwrap(),
        scenario.first_rx,
        scenario.second_rx,
    )
}

pub(super) fn drain_outbound(
    rx: &mut mpsc::UnboundedReceiver<SessionOutbound>,
) -> Vec<SessionOutbound> {
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
