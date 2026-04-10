use super::fixtures::*;

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
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            alice_tx,
            10,
        )
        .await;
    let bob_join = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            bob_tx,
            10,
        )
        .await;
    assert!(alice_join.is_ok());
    assert!(bob_join.is_ok());
    channel
        .update_session_info(
            &SessionId::Integer(1),
            SessionInfo {
                is_camera_on: Some(true),
                ..SessionInfo::default()
            },
            false,
        )
        .await;
    channel
        .update_session_info(
            &SessionId::Integer(2),
            SessionInfo {
                is_screen_sharing_on: Some(true),
                ..SessionInfo::default()
            },
            false,
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
