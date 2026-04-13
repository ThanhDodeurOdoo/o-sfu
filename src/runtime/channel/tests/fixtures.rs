pub(super) use std::{sync::Arc, time::Duration};

pub(super) use o_sfu_router::{
    ConsumerCapability, MediaKind as RouterMediaKind, RouterId,
    SessionPermissions as RouterSessionPermissions, StreamType as RouterStreamType,
};
pub(super) use tokio::sync::mpsc;
pub(super) use tokio::{task::yield_now, time::timeout};

pub(super) use super::super::{
    ChannelAdmissionPolicy, ChannelConfig, ChannelEventMessage, ChannelEventRequest,
    ChannelJoinError, ChannelManager, ChannelManagerJoinError, JoinSessionRequest, SessionOutbound,
    topology::ChannelTopology,
};
pub(super) use crate::runtime::stub_bus::{StubWebRtcAdapter, StubWebRtcEvent};
pub(super) use crate::runtime::transport_adapter::{
    RuntimeTransportAdapter, TransportConnectDirection,
};
pub(super) use crate::signaling::{
    protocol::WebSocketCloseCode,
    shared::{DownloadStates, SessionId, SessionInfo, SessionPermissions, StreamType},
    webrtc::{MediaKind, RtpCapabilities, RtpParameters},
};
pub(super) use serde_json::json;

/// Realistic client RTP capabilities (default codecs)
pub(super) fn test_client_rtp_capabilities() -> RtpCapabilities {
    RtpCapabilities(json!({
        "codecs": [
            {
                "mimeType": "audio/opus",
                "kind": "audio",
                "preferredPayloadType": 111,
                "clockRate": 48000,
                "channels": 2,
                "parameters": { "useinbandfec": "1" },
                "rtcpFeedback": [{ "type": "transport-cc" }]
            },
            {
                "mimeType": "video/VP8",
                "kind": "video",
                "preferredPayloadType": 96,
                "clockRate": 90000,
                "parameters": {},
                "rtcpFeedback": [
                    { "type": "nack" },
                    { "type": "nack", "parameter": "pli" },
                    { "type": "ccm", "parameter": "fir" },
                    { "type": "goog-remb" },
                    { "type": "transport-cc" }
                ]
            },
            {
                "mimeType": "video/rtx",
                "kind": "video",
                "preferredPayloadType": 97,
                "clockRate": 90000,
                "parameters": { "apt": "96" },
                "rtcpFeedback": []
            }
        ],
        "headerExtensions": [
            {
                "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
                "preferredId": 1,
                "preferredEncrypt": false,
                "kind": "audio",
                "direction": "sendrecv"
            },
            {
                "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
                "preferredId": 4,
                "preferredEncrypt": false,
                "kind": "audio",
                "direction": "sendrecv"
            },
            {
                "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
                "preferredId": 5,
                "preferredEncrypt": false,
                "kind": "audio",
                "direction": "sendrecv"
            },
            {
                "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
                "preferredId": 10,
                "preferredEncrypt": false,
                "kind": "audio",
                "direction": "sendrecv"
            }
        ]
    }))
}

pub(super) fn test_audio_rtp_parameters() -> RtpParameters {
    RtpParameters(json!({
        "codecs": [{
            "mimeType": "audio/opus",
            "payloadType": 111,
            "clockRate": 48000,
            "channels": 2,
            "parameters": { "useinbandfec": "1" },
            "rtcpFeedback": [{ "type": "transport-cc" }]
        }],
        "headerExtensions": [
            { "uri": "urn:ietf:params:rtp-hdrext:sdes:mid", "id": 1, "encrypt": false },
            { "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level", "id": 10, "encrypt": false }
        ],
        "encodings": [{ "ssrc": 11111 }]
    }))
}

pub(super) fn test_client_rtp_capabilities_without_video_rtx() -> RtpCapabilities {
    RtpCapabilities(json!({
        "codecs": [
            {
                "mimeType": "audio/opus",
                "kind": "audio",
                "preferredPayloadType": 111,
                "clockRate": 48000,
                "channels": 2,
                "parameters": { "useinbandfec": "1" },
                "rtcpFeedback": [{ "type": "transport-cc" }]
            },
            {
                "mimeType": "video/VP8",
                "kind": "video",
                "preferredPayloadType": 96,
                "clockRate": 90000,
                "parameters": {},
                "rtcpFeedback": [
                    { "type": "nack" },
                    { "type": "nack", "parameter": "pli" },
                    { "type": "ccm", "parameter": "fir" },
                    { "type": "goog-remb" }
                ]
            }
        ],
        "headerExtensions": [
            {
                "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
                "preferredId": 1,
                "preferredEncrypt": false,
                "kind": "audio",
                "direction": "sendrecv"
            }
        ]
    }))
}

pub(super) fn test_video_rtp_parameters() -> RtpParameters {
    RtpParameters(json!({
        "codecs": [
            {
                "mimeType": "video/VP8",
                "payloadType": 96,
                "clockRate": 90000,
                "parameters": {},
                "rtcpFeedback": [
                    { "type": "nack" },
                    { "type": "nack", "parameter": "pli" },
                    { "type": "ccm", "parameter": "fir" },
                    { "type": "goog-remb" },
                    { "type": "transport-cc" }
                ]
            },
            {
                "mimeType": "video/rtx",
                "payloadType": 97,
                "clockRate": 90000,
                "parameters": { "apt": "96" },
                "rtcpFeedback": []
            }
        ],
        "headerExtensions": [
            { "uri": "urn:ietf:params:rtp-hdrext:sdes:mid", "id": 1, "encrypt": false },
            { "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time", "id": 4, "encrypt": false },
            { "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01", "id": 5, "encrypt": false }
        ],
        "encodings": [{ "ssrc": 22222 }]
    }))
}

pub(super) fn test_sender() -> (
    mpsc::UnboundedSender<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    mpsc::unbounded_channel()
}

pub(super) fn stub_adapter() -> (RuntimeTransportAdapter, Arc<StubWebRtcAdapter>) {
    let adapter = Arc::new(StubWebRtcAdapter::default());
    (
        RuntimeTransportAdapter::from_stub_adapter(Arc::clone(&adapter)),
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
        channel
            .set_transport_connected(session_id, TransportConnectDirection::Upload)
            .await;
        channel
            .set_transport_connected(session_id, TransportConnectDirection::Download)
            .await;
        channel
            .set_client_rtp_capabilities(session_id, test_client_rtp_capabilities())
            .await;
    }
    (channel, adapter, rx1, rx2)
}

pub(super) async fn setup_two_ready_sessions_with_stub() -> (
    Arc<super::super::Channel>,
    RuntimeTransportAdapter,
    Arc<StubWebRtcAdapter>,
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
        channel
            .set_transport_connected(session_id, TransportConnectDirection::Upload)
            .await;
        channel
            .set_transport_connected(session_id, TransportConnectDirection::Download)
            .await;
        channel
            .set_client_rtp_capabilities(session_id, test_client_rtp_capabilities())
            .await;
    }
    (channel, adapter, stub, rx1, rx2)
}

pub(super) async fn setup_late_join_bootstrap_scenario() -> (
    Arc<super::super::Channel>,
    RuntimeTransportAdapter,
    Arc<StubWebRtcAdapter>,
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
        .set_transport_connected(&SessionId::Integer(1), TransportConnectDirection::Upload)
        .await;
    channel
        .set_transport_connected(&SessionId::Integer(1), TransportConnectDirection::Download)
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
    adapter: &StubWebRtcAdapter,
    predicate: impl Fn(&StubWebRtcEvent) -> bool,
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
        "timed out waiting for stub transport event"
    );
}
