use super::fixtures::*;
use o_sfu_router::{MediaKind, RtpParameters};

use crate::runtime::channel::Channel;
use crate::runtime::test_rtp_samples::sample_video_rtp_parameters;
use o_sfu_protocol::shared::StreamType;

fn test_video_rtp_parameters(ssrc: u64) -> RtpParameters {
    sample_video_rtp_parameters(None, u32::try_from(ssrc).unwrap_or(u32::MAX))
}

async fn publish_video_stream(
    channel: &Channel,
    session_id: &SessionId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    ssrc: u64,
    transport_adapter: &RuntimeTransportAdapter,
) {
    channel
        .test_api()
        .negotiation()
        .apply_publish_transport_ready(session_id, connection_id, transport_adapter)
        .await;
    assert!(
        channel
            .test_api()
            .media()
            .publish_track(
                session_id,
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
async fn stats_returns_live_channel_data() {
    let state = test_state();
    let query = CreateChannelQuery::default();
    let channel = state
        .channels
        .create_or_get(
            "issuer-a",
            None,
            &ChannelConfig {
                web_rtc_enabled: query.web_rtc_enabled(),
                recording_address: query.recording_address.clone(),
            },
            Some("203.0.113.10"),
        )
        .await;
    let (alice_tx, _alice_rx) = mpsc::unbounded_channel();
    let (bob_tx, _bob_rx) = mpsc::unbounded_channel();
    let alice_join = channel
        .test_api()
        .lifecycle()
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            alice_tx,
        )
        .await;
    let bob_join = channel
        .test_api()
        .lifecycle()
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            bob_tx,
        )
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
        &channel,
        &SessionId::Integer(1),
        alice_connection_id,
        StreamType::Camera,
        22_222,
        &state.transport_adapter,
    )
    .await;
    publish_video_stream(
        &channel,
        &SessionId::Integer(2),
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
    assert_eq!(first.uuid, channel.uuid());
    assert_eq!(first.remote_address, "203.0.113.10");
    assert_eq!(first.sessions_stats.count, 2);
    assert_eq!(first.sessions_stats.camera_count, 1);
    assert_eq!(first.sessions_stats.screen_count, 1);
    assert_eq!(first.sessions_stats.incoming_bit_rate.total, 0);
    assert_eq!(first.sessions_stats.incoming_bit_rate.audio, 0);
    assert_eq!(first.sessions_stats.incoming_bit_rate.camera, 0);
    assert_eq!(first.sessions_stats.incoming_bit_rate.screen, 0);
    assert!(first.web_rtc_enabled);
    assert!(first.create_date.contains('T'));
    assert!(first.create_date.ends_with('Z'));
}
