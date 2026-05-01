use o_sfu_protocol::signaling::{ServerMessage, ServerRequest, TrackBinding};
use o_sfu_router::{MediaKind, test_sample::sample_simulcast_video_rtp_parameters};

use super::fixtures::*;

#[tokio::test]
async fn protocol_user_serializes_topology_renegotiations() {
    let Some((server, room, mut publisher_socket, mut subscriber_socket)) =
        setup_negotiated_protocol_pair().await
    else {
        return;
    };

    assert!(
        publish_until_ready(&room, &server, StreamType::Camera, "cam-queue",)
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
        publish_until_ready(&room, &server, StreamType::Screen, "screen-queue",)
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
-> Option<(TestServer, Arc<Room>, TestWebSocket, TestWebSocket)> {
    let server = spawn_protocol_test_server(1_000, 100).await?;
    let room = create_room(
        &server,
        "issuer-protocol-negotiation",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let publisher_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(81))?;
    let subscriber_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(82))?;
    let mut publisher_socket = authenticate_with_jwt(&server, &publisher_token).await?;
    let mut subscriber_socket = authenticate_with_jwt(&server, &subscriber_token).await?;

    read_welcome(&mut publisher_socket).await?;
    read_welcome(&mut subscriber_socket).await?;
    answer_initial_offer(&mut publisher_socket, "publisher-answer").await?;
    answer_initial_offer(&mut subscriber_socket, "subscriber-answer").await?;

    Some((server, room, publisher_socket, subscriber_socket))
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
    room: &Arc<Room>,
    server: &TestServer,
    stream_type: StreamType,
    mid: &str,
) -> Option<String> {
    timeout(Duration::from_secs(1), async {
        loop {
            if let Some(producer_id) = room
                .test_api()
                .media()
                .publish_track(
                    &UserId::Integer(81),
                    stream_type,
                    MediaKind::Video,
                    sample_video_rtp_parameters(mid),
                    &server.media_transport,
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
    let mut messages = protocol_server_messages(&batch)?;
    let Some(ServerMessage::Tracks(actual_bindings)) = messages.pop() else {
        panic!("expected track snapshot");
    };
    assert!(messages.is_empty());
    assert_eq!(actual_bindings.len(), bindings.len());
    for (actual, expected) in actual_bindings.iter().zip(bindings.iter()) {
        assert_eq!(actual.mid, expected.mid);
        assert_eq!(actual.user_id, expected.user_id);
        assert_eq!(actual.stream_type, expected.stream_type);
        assert_eq!(actual.active, expected.active);
        let Some(source) = actual.source.as_ref() else {
            panic!("track snapshot should carry source descriptors");
        };
        assert_eq!(source.user_id, expected.user_id);
        assert_eq!(source.stream_type, expected.stream_type);
        assert_eq!(source.active, expected.active);
        assert_eq!(source.mid.as_deref(), Some(expected.mid.as_str()));
        assert_eq!(
            source
                .encodings
                .iter()
                .filter_map(|encoding| encoding.rid.as_deref())
                .collect::<Vec<_>>(),
            vec!["lo", "hi"],
        );
    }
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
        user_id: UserId::Integer(81),
        stream_type,
        active: true,
        source: None,
    }
}

fn sample_video_rtp_parameters(mid: &str) -> o_sfu_router::MediaStream {
    sample_simulcast_video_rtp_parameters(Some(mid))
}
