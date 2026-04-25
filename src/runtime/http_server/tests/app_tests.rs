use o_sfu_protocol::shared::StreamType;
use o_sfu_router::{MediaKind, MediaStream};

use super::fixtures::*;
use crate::runtime::{
    room::Room,
    test_rtp_samples::{sample_client_rtp_capabilities, sample_video_rtp_parameters},
};

fn test_video_rtp_parameters(ssrc: u64) -> MediaStream {
    sample_video_rtp_parameters(None, u32::try_from(ssrc).unwrap_or(u32::MAX))
}

async fn publish_video_stream(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    ssrc: u64,
    transport_adapter: &RuntimeTransportAdapter,
) {
    assert!(
        room.apply_session_negotiated(
            user_id,
            connection_id,
            sample_client_rtp_capabilities(),
            transport_adapter,
        )
        .await
    );
    assert!(
        room.test_api()
            .media()
            .publish_track(
                user_id,
                stream_type,
                MediaKind::Video,
                test_video_rtp_parameters(ssrc),
                transport_adapter,
            )
            .await
            .is_some()
    );
}

#[tokio::test]
async fn noop_returns_ok_response() {
    let request = build_request(Request::get(NOOP_PATH), Body::empty());
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "noop request should succeed: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Option<NoopResponse> = parse_json(response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_eq!(payload.result, "ok");
}

#[tokio::test]
async fn stats_returns_live_room_data() {
    let state = test_state();
    let query = CreateRoomQuery::default();
    let room = state
        .room_manager
        .serve_room(
            "issuer-a",
            None,
            &RoomConfig {
                web_rtc_enabled: query.web_rtc_enabled(),
                recording_address: query.recording_address.clone(),
            },
            Some("203.0.113.10"),
        )
        .await;
    let (alice_tx, _alice_rx) = mpsc::unbounded_channel();
    let (bob_tx, _bob_rx) = mpsc::unbounded_channel();
    let alice_join = room
        .test_api()
        .lifecycle()
        .join_user(
            UserId::Integer(1),
            None,
            UserPermissions::default(),
            alice_tx,
        )
        .await;
    let bob_join = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), bob_tx)
        .await;
    assert!(alice_join.is_ok());
    assert!(bob_join.is_ok());
    let Some(alice_connection_id) = alice_join.ok() else {
        return;
    };
    let Some(bob_connection_id) = bob_join.ok() else {
        return;
    };
    publish_video_stream(
        &room,
        &UserId::Integer(1),
        alice_connection_id,
        StreamType::Camera,
        22_222,
        &state.transport_adapter,
    )
    .await;
    publish_video_stream(
        &room,
        &UserId::Integer(2),
        bob_connection_id,
        StreamType::Screen,
        33_333,
        &state.transport_adapter,
    )
    .await;

    let request = build_request(Request::get(STATS_PATH), Body::empty());
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(state).oneshot(request).await;
    assert!(
        response.is_ok(),
        "stats request should succeed: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Option<StatsResponse> = parse_json(response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_eq!(payload.len(), 1);
    let first = payload.first();
    assert!(first.is_some());
    let Some(first) = first else {
        return;
    };
    assert_eq!(first.uuid, room.uuid());
    assert_eq!(first.remote_address, "203.0.113.10");
    assert_eq!(first.users_stats.count, 2);
    assert_eq!(first.users_stats.camera_count, 1);
    assert_eq!(first.users_stats.screen_count, 1);
    assert_eq!(first.users_stats.incoming_bit_rate.total, 0);
    assert_eq!(first.users_stats.incoming_bit_rate.audio, 0);
    assert_eq!(first.users_stats.incoming_bit_rate.camera, 0);
    assert_eq!(first.users_stats.incoming_bit_rate.screen, 0);
    assert!(first.web_rtc_enabled);
    assert!(first.create_date.contains('T'));
    assert!(first.create_date.ends_with('Z'));
}
