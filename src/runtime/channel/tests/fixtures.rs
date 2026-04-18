pub(super) use std::{sync::Arc, time::Duration};

pub(super) use o_sfu_router::{
    ConsumerCapability, MediaKind, MediaKind as RouterMediaKind, RouterId, RtpCapabilities,
    RtpParameters, SessionPermissions as RouterSessionPermissions, StreamType as RouterStreamType,
};
pub(super) use tokio::sync::mpsc;
pub(super) use tokio::{task::yield_now, time::timeout};

pub(super) use super::super::{
    ChannelAdmissionPolicy, ChannelConfig, ChannelEventMessage, ChannelEventRequest,
    ChannelJoinError, ChannelManager, ChannelManagerJoinError, JoinSessionRequest,
    SessionCloseReason, SessionOutbound, topology::ChannelTopology,
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
pub(super) fn test_client_rtp_capabilities() -> RtpCapabilities {
    sample_client_rtp_capabilities()
}

pub(super) fn test_audio_rtp_parameters() -> RtpParameters {
    sample_audio_rtp_parameters(11_111)
}

pub(super) fn test_client_rtp_capabilities_without_video_rtx() -> RtpCapabilities {
    sample_client_rtp_capabilities_without_video_rtx()
}

pub(super) fn test_video_rtp_parameters() -> RtpParameters {
    sample_video_rtp_parameters(None, 22_222)
}

pub(super) fn test_simulcast_video_rtp_parameters() -> RtpParameters {
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
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            first_tx,
        )
        .await
        .unwrap();
    channel
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

    channel
        .set_publish_transport_ready(&SessionId::Integer(1))
        .await;
    channel
        .set_consume_transport_ready(&SessionId::Integer(1))
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(1), test_client_rtp_capabilities())
        .await;

    if options.publish_camera_before_subscriber_ready {
        channel
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
        channel
            .set_publish_transport_ready(&SessionId::Integer(2))
            .await;
        channel
            .set_consume_transport_ready(&SessionId::Integer(2))
            .await;
        channel
            .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
            .await;
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
