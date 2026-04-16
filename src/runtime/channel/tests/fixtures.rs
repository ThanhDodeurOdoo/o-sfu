pub(super) use std::{sync::Arc, time::Duration};

pub(super) use o_sfu_router::{
    ConsumerCapability, MediaKind, MediaKind as RouterMediaKind, RouterId, RtpCapabilities,
    RtpParameters, SessionPermissions as RouterSessionPermissions, StreamType as RouterStreamType,
};
pub(super) use tokio::sync::mpsc;
pub(super) use tokio::{task::yield_now, time::timeout};

pub(super) use super::super::{
    ChannelAdmissionPolicy, ChannelConfig, ChannelEventMessage, ChannelEventRequest,
    ChannelJoinError, ChannelManager, ChannelManagerJoinError, JoinSessionRequest, SessionOutbound,
    topology::ChannelTopology,
};
use crate::runtime::test_rtp_samples::{
    sample_audio_rtp_parameters, sample_client_rtp_capabilities,
    sample_client_rtp_capabilities_without_video_rtx, sample_simulcast_video_rtp_parameters,
    sample_video_rtp_parameters,
};
pub(super) use crate::runtime::transport_adapter::{
    ActiveSpeakerSource, FakeWebRtcAdapter, FakeWebRtcEvent, RuntimeTransportAdapter,
};
pub(super) use crate::signaling::{
    protocol::WebSocketCloseCode,
    shared::{DownloadStates, SessionId, SessionInfo, SessionPermissions, StreamType},
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

pub(super) fn stub_adapter() -> (RuntimeTransportAdapter, Arc<FakeWebRtcAdapter>) {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    (
        RuntimeTransportAdapter::from_fake_adapter(Arc::clone(&adapter)),
        adapter,
    )
}

/// Set up a channel with two joined sessions that both have upload and download
/// transports connected plus client RTP capabilities, ready for publish/consume tests.
pub(super) async fn setup_two_ready_sessions() -> (
    Arc<super::super::Channel>,
    RuntimeTransportAdapter,
    mpsc::UnboundedReceiver<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, rx1) = test_sender();
    let (tx2, rx2) = test_sender();
    channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
        )
        .await
        .unwrap();
    channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
        )
        .await
        .unwrap();
    let (adapter, _stub) = stub_adapter();
    for session_id in &[SessionId::Integer(1), SessionId::Integer(2)] {
        channel.set_publish_transport_ready(session_id).await;
        channel.set_consume_transport_ready(session_id).await;
        channel
            .set_client_rtp_capabilities(session_id, test_client_rtp_capabilities())
            .await;
    }
    (channel, adapter, rx1, rx2)
}

pub(super) async fn setup_two_ready_sessions_with_stub() -> (
    Arc<super::super::Channel>,
    RuntimeTransportAdapter,
    Arc<FakeWebRtcAdapter>,
    mpsc::UnboundedReceiver<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, rx1) = test_sender();
    let (tx2, rx2) = test_sender();
    channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
        )
        .await
        .unwrap();
    channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
        )
        .await
        .unwrap();
    let (adapter, stub) = stub_adapter();
    for session_id in &[SessionId::Integer(1), SessionId::Integer(2)] {
        channel.set_publish_transport_ready(session_id).await;
        channel.set_consume_transport_ready(session_id).await;
        channel
            .set_client_rtp_capabilities(session_id, test_client_rtp_capabilities())
            .await;
    }
    (channel, adapter, stub, rx1, rx2)
}

pub(super) async fn setup_late_join_bootstrap_scenario() -> (
    Arc<super::super::Channel>,
    RuntimeTransportAdapter,
    Arc<FakeWebRtcAdapter>,
    mpsc::UnboundedReceiver<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (publisher_tx, publisher_rx) = test_sender();
    let (subscriber_tx, subscriber_rx) = test_sender();
    channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            publisher_tx,
        )
        .await
        .unwrap();
    channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            subscriber_tx,
        )
        .await
        .unwrap();

    let (transport_adapter, stub) = stub_adapter();
    channel
        .set_publish_transport_ready(&SessionId::Integer(1))
        .await;
    channel
        .set_consume_transport_ready(&SessionId::Integer(1))
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(1), test_client_rtp_capabilities())
        .await;
    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &transport_adapter,
        )
        .await;

    (
        channel,
        transport_adapter,
        stub,
        publisher_rx,
        subscriber_rx,
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

pub(super) async fn wait_for_stub_event(
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
