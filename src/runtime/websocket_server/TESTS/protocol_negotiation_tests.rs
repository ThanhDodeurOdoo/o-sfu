use o_sfu_protocol::wire::{ServerMessage, ServerRequest, SourceDescriptor, TrackBinding};
use o_sfu_router::{
    MediaKind, rtp::MediaStream, test_support::rtp_samples::sample_simulcast_video_rtp_parameters,
};

use super::fixtures::*;

#[tokio::test]
async fn protocol_user_does_not_overlap_topology_renegotiations() {
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
    let first_snapshot_request = assert_track_snapshot(
        &mut subscriber_socket,
        vec![track_binding("cam-queue", StreamType::Camera)],
    )
    .await;
    assert!(first_snapshot_request.is_some());
    let (first_renegotiation_id, first_renegotiation_request) =
        if let Some(request) = first_snapshot_request.flatten() {
            request
        } else {
            let Some(request) = expect_renegotiation_request(&mut subscriber_socket).await else {
                panic!("subscriber should receive the first renegotiation request");
            };
            request
        };

    assert!(
        publish_until_ready(&room, &server, StreamType::Screen, "screen-queue",)
            .await
            .is_some(),
        "second publish should succeed"
    );
    assert_no_protocol_request_before_idle(&mut subscriber_socket).await;

    assert!(
        respond_to_protocol_negotiation_request_with_test_rtc(
            &mut subscriber_socket,
            first_renegotiation_id,
            first_renegotiation_request,
            "v=0\r\ns=subscriber-renegotiate-1\r\n",
        )
        .await
        .is_some()
    );

    let _ = publisher_socket.close(None).await;
}

async fn assert_no_protocol_request_before_idle(websocket: &mut TestWebSocket) {
    while let Ok(Some(payload)) =
        timeout(Duration::from_millis(50), read_text_message(websocket)).await
    {
        let Ok(batch) = serde_json::from_str::<EnvelopeBatch>(&payload) else {
            panic!("server text frame should be a protocol envelope batch, payload: {payload}");
        };
        assert!(
            first_protocol_server_request(&batch).is_none(),
            "second publish should not send another subscriber offer while the first answer is pending"
        );
    }
}

async fn setup_negotiated_protocol_pair()
-> Option<(TestServer, Arc<Room>, TestWebSocket, TestWebSocket)> {
    let server = TestServerBuilder::new().spawn().await?;
    let room = create_room(
        &server,
        "issuer-protocol-negotiation",
        CreateRoomQuery::default(),
    )
    .await;
    let publisher_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(81))?;
    let subscriber_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(82))?;
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
    respond_to_protocol_negotiation_request_with_test_rtc(
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
) -> Option<()> {
    timeout(Duration::from_secs(1), async {
        loop {
            if room
                .test_api()
                .media()
                .publish_intent(
                    &UserId::Integer(81),
                    &source_publish_intent_for_stream_type(stream_type),
                    MediaKind::Video,
                    sample_video_rtp_parameters(mid),
                    &server.media_transport,
                )
                .await
                .is_some()
            {
                return Some(());
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
) -> Option<Option<(RequestId, ServerRequest)>> {
    let batch = read_protocol_server_batch(websocket).await?;
    let request = first_protocol_server_request(&batch);
    let (actual_bindings, actual_sources) = media_snapshots_in_batch(batch)?;
    assert_eq!(actual_bindings.len(), bindings.len());
    assert_eq!(actual_sources.len(), bindings.len());
    for expected in &bindings {
        let Some(actual) = actual_bindings.iter().find(|binding| {
            binding.user_id == expected.user_id && binding.stream_type == expected.stream_type
        }) else {
            panic!("expected track snapshot binding {}", expected.mid);
        };
        assert!(!actual.mid.is_empty());
        assert_eq!(actual.active, expected.active);
        let Some(source) = actual_sources.iter().find(|source| {
            source.user_id == expected.user_id
                && source.stream_type == expected.stream_type
                && source.mid.as_deref() == Some(expected.mid.as_str())
        }) else {
            panic!("expected source snapshot descriptor {}", expected.mid);
        };
        assert_eq!(source.active, expected.active);
        assert_eq!(
            source
                .encodings
                .iter()
                .filter_map(|encoding| encoding.rid.as_deref())
                .collect::<Vec<_>>(),
            vec!["lo", "hi"],
        );
    }
    Some(request)
}

async fn expect_renegotiation_request(
    websocket: &mut TestWebSocket,
) -> Option<(RequestId, ServerRequest)> {
    let batch = read_protocol_server_batch(websocket).await?;
    let (request_id, request) = first_protocol_server_request(&batch)?;
    assert!(matches!(request, ServerRequest::Renegotiate(_)));
    Some((request_id, request))
}

fn media_snapshots_in_batch(
    batch: EnvelopeBatch,
) -> Option<(Vec<TrackBinding>, Vec<SourceDescriptor>)> {
    let mut tracks = None;
    let mut sources = None;
    for envelope in batch {
        match ServerEnvelope::decode(envelope).ok()? {
            ServerEnvelope::Message(ServerMessage::Tracks(track_bindings)) => {
                assert!(tracks.replace(track_bindings).is_none());
            }
            ServerEnvelope::Message(ServerMessage::Sources(snapshot)) => {
                assert!(sources.replace(snapshot).is_none());
            }
            ServerEnvelope::Message(_)
            | ServerEnvelope::Request { .. }
            | ServerEnvelope::Response { .. } => {}
        }
    }
    let Some(tracks) = tracks else {
        panic!("expected track snapshot");
    };
    let Some(sources) = sources else {
        panic!("expected source snapshot");
    };
    Some((tracks, sources))
}

fn track_binding(mid: &str, stream_type: StreamType) -> TrackBinding {
    TrackBinding {
        mid: mid.to_owned(),
        user_id: UserId::Integer(81),
        stream_type,
        active: true,
    }
}

fn sample_video_rtp_parameters(mid: &str) -> MediaStream {
    sample_simulcast_video_rtp_parameters(Some(mid))
}
