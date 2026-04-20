use super::fixtures::*;
use crate::runtime::test_rtp_samples::sample_video_rtp_parameters as router_sample_video_rtp_parameters;
use o_sfu_protocol::signaling::{ServerMessage, ServerRequest, TrackBinding};
use o_sfu_router::MediaKind;

#[tokio::test]
async fn protocol_session_serializes_topology_renegotiations() {
    let Some((server, channel, mut publisher_socket, mut subscriber_socket)) =
        setup_negotiated_protocol_pair().await
    else {
        return;
    };

    assert!(
        publish_until_ready(&channel, &server, StreamType::Camera, "cam-queue",)
            .await
            .is_some(),
        "publisher should be ready"
    );
    assert!(
        assert_track_snapshot(
            &mut subscriber_socket,
            vec![track_binding("cam-queue", StreamType::Camera)],
        )
        .await
        .is_some()
    );

    let Some((first_renegotiation_id, first_renegotiation_request)) =
        expect_renegotiation_request(&mut subscriber_socket).await
    else {
        return;
    };

    assert!(
        publish_until_ready(&channel, &server, StreamType::Screen, "screen-queue",)
            .await
            .is_some(),
        "second publish should succeed"
    );
    assert!(
        assert_track_snapshot(
            &mut subscriber_socket,
            vec![
                track_binding("cam-queue", StreamType::Camera),
                track_binding("screen-queue", StreamType::Screen),
            ],
        )
        .await
        .is_some()
    );

    assert!(
        respond_to_protocol_negotiation_request(
            &mut subscriber_socket,
            first_renegotiation_id,
            first_renegotiation_request,
            "v=0\r\ns=subscriber-renegotiate-1\r\n",
        )
        .await
        .is_some()
    );

    let Some((_second_request_id, second_request)) =
        expect_renegotiation_request(&mut subscriber_socket).await
    else {
        return;
    };
    assert!(matches!(second_request, ServerRequest::Renegotiate(_)));
    let _ = publisher_socket.close(None).await;
}

async fn setup_negotiated_protocol_pair()
-> Option<(TestServer, Arc<Channel>, TestWebSocket, TestWebSocket)> {
    let server = spawn_protocol_test_server(1_000, 100).await?;
    let channel = create_channel(
        &server,
        "issuer-protocol-negotiation",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let publisher_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(81))?;
    let subscriber_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(82))?;
    let mut publisher_socket = authenticate_with_jwt(&server, &publisher_token).await?;
    let mut subscriber_socket = authenticate_with_jwt(&server, &subscriber_token).await?;

    read_welcome(&mut publisher_socket).await?;
    read_welcome(&mut subscriber_socket).await?;
    answer_initial_offer(&mut publisher_socket, "publisher-answer").await?;
    answer_initial_offer(&mut subscriber_socket, "subscriber-answer").await?;

    Some((server, channel, publisher_socket, subscriber_socket))
}

async fn answer_initial_offer(websocket: &mut TestWebSocket, answer_name: &str) -> Option<()> {
    let offer_batch = read_protocol_server_batch(websocket).await?;
    let (request_id, request) = first_protocol_server_request(&offer_batch)?;
    assert!(matches!(request, ServerRequest::Offer(_)));
    respond_to_protocol_negotiation_request(
        websocket,
        request_id,
        request,
        &format!("v=0\r\ns={answer_name}\r\n"),
    )
    .await
}

async fn publish_until_ready(
    channel: &Arc<Channel>,
    server: &TestServer,
    stream_type: StreamType,
    mid: &str,
) -> Option<String> {
    timeout(Duration::from_secs(1), async {
        loop {
            if let Some(producer_id) = channel
                .test_api()
                .publish_track(
                    &SessionId::Integer(81),
                    stream_type,
                    MediaKind::Video,
                    sample_video_rtp_parameters(mid),
                    &server.state.transport_adapter,
                )
                .await
            {
                return Some(producer_id);
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
    .flatten()
}

async fn assert_track_snapshot(
    websocket: &mut TestWebSocket,
    bindings: Vec<TrackBinding>,
) -> Option<()> {
    let batch = read_protocol_server_batch(websocket).await?;
    let messages = protocol_server_messages(&batch)?;
    assert_eq!(messages, vec![ServerMessage::Tracks(bindings)]);
    Some(())
}

async fn expect_renegotiation_request(
    websocket: &mut TestWebSocket,
) -> Option<(RequestId, ServerRequest)> {
    let batch = read_protocol_server_batch(websocket).await?;
    let (request_id, request) = first_protocol_server_request(&batch)?;
    assert!(matches!(request, ServerRequest::Renegotiate(_)));
    Some((request_id, request))
}

fn track_binding(mid: &str, stream_type: StreamType) -> TrackBinding {
    TrackBinding {
        mid: mid.to_owned(),
        session_id: SessionId::Integer(81),
        stream_type,
        active: true,
    }
}

fn sample_video_rtp_parameters(mid: &str) -> o_sfu_router::RtpParameters {
    router_sample_video_rtp_parameters(Some(mid), 22_222)
}
