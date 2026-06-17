#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

use std::{collections::BTreeMap, future::Future, pin::Pin};

use o_sfu::{
    config::Config,
    http::{DISCONNECT_PATH, STATS_PATH, StatsResponse},
};
use o_sfu_protocol::wire::{
    ClientBroadcastPayload, ClientMessage, DownloadStates, ServerMessage, ServerRequest,
    StreamIntentPayload, StreamType, SubscribePayload, UserId, UserInfo,
};
use o_sfu_tests::support::{
    TEST_ROOM_KEY, TestResult, TestServer, create_room, disconnect_sessions_via_http, metrics_text,
    protocol_harness::{ProtocolWebSocketClient, connect_protocol_pair, read_until_server_message},
    require_some, signed_connect_claims, spawn_test_server, test_config,
};
use reqwest::StatusCode;
use tokio::time::{Duration, timeout};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const SLOW_CONSUMER_BATCH_LEN: usize = 64;
const SLOW_CONSUMER_PAYLOAD_BYTES: usize = 1_024;

async fn default_server() -> TestResult<TestServer> {
    spawn_test_server(test_config(1_000, 10)).await
}

async fn server_with_room(issuer: &str) -> TestResult<(TestServer, String)> {
    server_with_configured_room(test_config(1_000, 10), issuer).await
}

async fn server_with_configured_room(
    config: Config,
    issuer: &str,
) -> TestResult<(TestServer, String)> {
    let server = spawn_test_server(config).await?;
    let room = room(&server, issuer).await?;
    Ok((server, room))
}

async fn room(server: &TestServer, issuer: &str) -> TestResult<String> {
    require_some(
        create_room(server, issuer, TEST_ROOM_KEY).await,
        "room should be created",
    )
}

fn token(room: &str, user_id: UserId) -> TestResult<String> {
    require_some(
        signed_connect_claims(TEST_ROOM_KEY, room, user_id),
        "connect token should sign",
    )
}

async fn client_in_room(
    server: &TestServer,
    room: &str,
    user_id: UserId,
) -> TestResult<ProtocolWebSocketClient> {
    let token = token(room, user_id)?;
    require_some(
        ProtocolWebSocketClient::authenticate_with_room(server, &token, room).await,
        "websocket client should authenticate",
    )
}

type ProtocolPairFuture<'a> = Pin<
    Box<dyn Future<Output = TestResult<(ProtocolWebSocketClient, ProtocolWebSocketClient)>> + 'a>,
>;

fn protocol_pair<'a>(
    server: &'a TestServer,
    room: &'a str,
    first_user_id: UserId,
    second_user_id: UserId,
) -> ProtocolPairFuture<'a> {
    Box::pin(async move {
        let first_token = token(room, first_user_id)?;
        let second_token = token(room, second_user_id.clone())?;
        require_some(
            Box::pin(connect_protocol_pair(
                server,
                &first_token,
                &second_token,
                second_user_id,
            ))
            .await,
            "protocol pair should connect",
        )
    })
}

async fn initial_offer(client: &mut ProtocolWebSocketClient) -> TestResult<ServerRequest> {
    let welcome = require_some(client.read_welcome().await, "welcome should be sent")?;
    assert!(welcome.features.rtc);
    let (_request_id, request) = require_some(
        client.read_server_request().await,
        "initial offer should be sent",
    )?;
    Ok(request)
}

#[tokio::test]
async fn websocket_welcome_and_initial_offer_work_from_integration_test() -> TestResult {
    let (server, room) = server_with_room("issuer-a").await?;
    let mut client = client_in_room(&server, &room, UserId::Integer(7)).await?;
    let request = initial_offer(&mut client).await?;
    assert!(matches!(request, ServerRequest::Offer(_)));
    Ok(())
}

#[tokio::test]
async fn websocket_welcome_and_initial_offer_expose_real_rtc_transport_details() -> TestResult {
    let config = test_config(1_000, 10);
    let rtc_port_range = config.transport.rtc_port_range;
    let (server, room) = server_with_configured_room(config, "issuer-a").await?;
    let mut client = client_in_room(&server, &room, UserId::Integer(701)).await?;
    let request = initial_offer(&mut client).await?;
    let ServerRequest::Offer(payload) = request else {
        panic!("expected offer request");
    };
    assert!(payload.sdp.contains("a=ice-lite"));
    assert!(payload.sdp.contains("a=fingerprint:sha-256"));
    assert!(payload.sdp.contains("a=candidate:"));
    assert!(payload.sdp.contains("127.0.0.1"));
    let candidate_line = require_some(
        payload
            .sdp
            .lines()
            .find(|line| line.starts_with("a=candidate:")),
        "expected SDP candidate line",
    )?;
    assert!(candidate_line.contains(" 127.0.0.1 "));
    let port = require_some(
        candidate_line
            .split_whitespace()
            .nth(5)
            .and_then(|value| value.parse::<u16>().ok()),
        "candidate should expose an RTC port",
    )?;
    assert!(rtc_port_range.ports().any(|candidate| candidate == port));
    Ok(())
}

#[tokio::test]
async fn websocket_offer_advertises_configured_public_ip_in_rtc_mode() -> TestResult {
    let mut config = test_config(1_000, 10);
    config.transport.public_ip = "203.0.113.44".parse().unwrap_or(config.transport.public_ip);
    let (server, room) = server_with_configured_room(config, "issuer-public-ip").await?;
    let mut client = client_in_room(&server, &room, UserId::Integer(702)).await?;
    let request = initial_offer(&mut client).await?;
    let ServerRequest::Offer(payload) = request else {
        panic!("expected offer request");
    };
    assert!(payload.sdp.contains("a=ice-lite"));
    assert!(payload.sdp.contains("203.0.113.44"));
    if let Some(candidate_line) = payload
        .sdp
        .lines()
        .find(|line| line.starts_with("a=candidate:"))
    {
        assert!(candidate_line.contains(" 203.0.113.44 "));
    } else {
        panic!("expected SDP candidate line");
    }
    Ok(())
}

#[tokio::test]
async fn websocket_timeout_is_reported_from_integration_test() -> TestResult {
    let server = spawn_test_server(test_config(25, 10)).await?;

    let client = ProtocolWebSocketClient::connect(&server).await;
    let mut client = require_some(client, "websocket client should connect")?;

    assert_eq!(
        client.read_close_code().await,
        Some(CloseCode::Library(4107))
    );
    Ok(())
}

#[tokio::test]
async fn invalid_jwt_is_rejected_from_integration_test() -> TestResult {
    let server = default_server().await?;

    let client = ProtocolWebSocketClient::authenticate_with_jwt(&server, "not-a-jwt").await;
    let mut client = require_some(client, "websocket client should connect")?;

    assert_eq!(
        client.read_close_code().await,
        Some(CloseCode::Library(4106))
    );
    Ok(())
}

#[tokio::test]
async fn room_creation_is_idempotent_by_issuer_from_integration_test() -> TestResult {
    let server = default_server().await?;

    let first = room(&server, "issuer-a").await?;
    let second = room(&server, "issuer-a").await?;
    let third = room(&server, "issuer-b").await?;

    assert_eq!(first, second);
    assert_ne!(first, third);
    Ok(())
}

#[tokio::test]
async fn oversized_disconnect_body_is_rejected_before_handler_metrics_from_integration_test()
-> TestResult {
    let server = default_server().await?;

    let response = reqwest::Client::new()
        .post(format!("{}{DISCONNECT_PATH}", server.http_base_url()))
        .body("x".repeat((16 * 1024) + 1))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let metrics = metrics_text(&server).await;
    let metrics = require_some(metrics, "metrics text should be exposed")?;
    assert!(metrics.contains("osfu_http_disconnect_requests_total 0"));
    assert!(metrics.contains("osfu_http_disconnect_responses_total{status=\"success\"} 0"));
    assert!(metrics.contains("osfu_http_disconnect_responses_total{status=\"bad_request\"} 0"));
    assert!(
        metrics.contains("osfu_http_disconnect_responses_total{status=\"unprocessable_entity\"} 0")
    );
    Ok(())
}

#[tokio::test]
async fn broadcast_reaches_other_user_from_integration_test() -> TestResult {
    let (server, room) = server_with_room("issuer-a").await?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(1), UserId::Integer(2)).await?;

    require_some(
        alice
            .send_broadcast(serde_json::json!({
                "type": StreamType::Audio,
                "text": "hello"
            }))
            .await,
        "broadcast should send",
    )?;

    let message = bob
        .read_server_message_with_timeout(Duration::from_secs(1))
        .await;
    let message = require_some(message, "broadcast should reach peer")?;
    if let ServerMessage::Broadcast(payload) = message {
        assert_eq!(payload.sender_id, UserId::Integer(1));
        assert_eq!(
            payload.message,
            serde_json::json!({
                "type": "audio",
                "text": "hello"
            })
        );
    } else {
        panic!("expected broadcast update");
    }
    Ok(())
}

#[tokio::test]
async fn websocket_slow_consumer_overflow_closes_only_slow_socket_from_integration_test()
-> TestResult {
    let (server, room) = server_with_configured_room(
        slow_consumer_overflow_config(),
        "issuer-slow-consumer-overflow",
    )
    .await?;
    let (mut slow, mut driver) =
        protocol_pair(&server, &room, UserId::Integer(31), UserId::Integer(32)).await?;
    let witness_token = token(&room, UserId::Integer(33))?;

    require_some(
        driver.send_messages(slow_consumer_broadcast_batch()).await,
        "slow-consumer batch should send",
    )?;

    let slow_close = timeout(Duration::from_secs(5), slow.read_close_code())
        .await
        .ok()
        .flatten();
    assert_eq!(slow_close, Some(CloseCode::Library(4108)));

    let departed = read_until_server_message(&mut driver, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(31))
    })
    .await;
    require_some(departed, "driver should observe slow peer departure")?;

    let witness =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &witness_token).await;
    let (witness, _welcome) = require_some(witness, "witness should reconnect")?;
    let rejoined = read_until_server_message(&mut driver, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == UserId::Integer(33))
    })
    .await;
    require_some(rejoined, "driver should observe witness join")?;
    require_some(witness.close().await, "witness should close")?;

    let metrics = metrics_text(&server).await;
    let metrics = require_some(metrics, "metrics text should be exposed")?;
    assert!(metric_value(&metrics, "osfu_ws_outbound_queue_overflows_total").unwrap_or(0) > 0);
    assert_eq!(
        metric_value(
            &metrics,
            "osfu_ws_user_loop_exits_total{reason=\"outbound_queue_overflow\"}"
        ),
        Some(1)
    );
    Ok(())
}

#[tokio::test]
async fn user_info_change_reaches_other_user_from_integration_test() -> TestResult {
    let (server, room) = server_with_room("issuer-a").await?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(1), UserId::Integer(2)).await?;

    require_some(
        alice
            .send_info(UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            })
            .await,
        "info update should send",
    )?;

    let message = read_until_server_message(&mut bob, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerInfo(payload) if payload.user_id == UserId::Integer(1))
    })
    .await;
    let message = require_some(message, "peer should receive info update")?;
    if let ServerMessage::PeerInfo(payload) = message {
        assert_eq!(payload.info.is_talking, Some(true));
    } else {
        panic!("expected user info update");
    }
    Ok(())
}

#[tokio::test]
async fn stats_reports_live_user_aggregates_from_integration_test() -> TestResult {
    let (server, room) = server_with_room("issuer-a").await?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(1), UserId::Integer(2)).await?;

    require_some(
        bob.send_info(UserInfo {
            is_talking: Some(true),
            ..UserInfo::default()
        })
        .await,
        "info update should send",
    )?;

    let peer_info = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerInfo(payload) if payload.user_id == UserId::Integer(2))
    })
    .await;
    require_some(peer_info, "peer info should reach alice")?;

    let response = reqwest::get(format!("{}{STATS_PATH}", server.http_base_url())).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let stats = response.json::<StatsResponse>().await?;
    assert_eq!(stats.len(), 1);
    let first = require_some(stats.first(), "stats should contain the room")?;
    assert_eq!(first.uuid, room);
    assert_eq!(first.users_stats.count, 2);
    assert_eq!(first.users_stats.camera_count, 0);
    assert_eq!(first.users_stats.screen_count, 0);
    assert_eq!(first.users_stats.incoming_bit_rate.total, 0);
    assert!(first.web_rtc_enabled);
    assert_eq!(first.remote_address, "127.0.0.1");
    Ok(())
}

#[tokio::test]
async fn room_full_and_last_disconnect_cleanup_are_observable_from_integration_test() -> TestResult
{
    let (server, first_room) =
        server_with_configured_room(test_config(1_000, 1), "issuer-a").await?;
    let first_token = token(&first_room, UserId::Integer(1))?;
    let second_token = token(&first_room, UserId::Integer(2))?;

    let (first_client, _welcome) = require_some(
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &first_token).await,
        "first client should negotiate",
    )?;

    let second_client =
        ProtocolWebSocketClient::authenticate_with_jwt(&server, &second_token).await;
    let mut second_client = require_some(second_client, "second client should connect")?;
    assert_eq!(
        second_client.read_close_code().await,
        Some(CloseCode::Library(4109)),
    );

    require_some(first_client.close().await, "first client should close")?;
    assert!(server.wait_for_room_absence(&first_room).await);

    let second_room = room(&server, "issuer-a").await?;
    assert_ne!(first_room, second_room);

    let third_token = token(&second_room, UserId::Integer(3))?;
    let third_client =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &third_token).await;
    require_some(
        third_client,
        "third client should negotiate in recreated room",
    )?;
    Ok(())
}

#[tokio::test]
async fn disconnect_api_kicks_target_and_notifies_remaining_from_integration_test() -> TestResult {
    let (server, room) = server_with_room("issuer-a").await?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(1), UserId::Integer(2)).await?;

    let status = disconnect_sessions_via_http(
        &server,
        BTreeMap::from([(room.clone(), vec![UserId::Integer(2)])]),
    )
    .await;
    assert_eq!(status, Some(StatusCode::OK));

    assert_eq!(bob.read_close_code().await, Some(CloseCode::Library(4108)));

    let message = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(2))
    })
    .await;
    let message = require_some(message, "alice should receive peer departure")?;
    if let ServerMessage::PeerLeft(payload) = message {
        assert_eq!(payload.user_id, UserId::Integer(2));
    } else {
        panic!("expected user departure notification");
    }
    require_some(alice.close().await, "alice should close")?;
    assert!(server.wait_for_room_absence(&room).await);
    Ok(())
}

#[tokio::test]
async fn replaced_socket_cannot_broadcast_or_change_info_from_integration_test() -> TestResult {
    let (server, room) = server_with_room("issuer-replacement-guard").await?;
    let bob_token = token(&room, UserId::Integer(2))?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(1), UserId::Integer(2)).await?;
    let (replacement, _welcome) = require_some(
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &bob_token).await,
        "replacement should negotiate",
    )?;

    let departed = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(2))
    })
    .await;
    require_some(departed, "alice should observe replaced peer departure")?;
    let rejoined = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == UserId::Integer(2))
    })
    .await;
    require_some(rejoined, "alice should observe replacement join")?;

    let _ = bob
        .send_broadcast(serde_json::json!({ "text": "stale" }))
        .await;
    assert_eq!(
        alice
            .read_server_message_with_timeout(Duration::from_millis(150))
            .await,
        None,
        "stale replacement socket must not broadcast into the room"
    );

    let _ = bob
        .send_info(UserInfo {
            is_talking: Some(true),
            ..UserInfo::default()
        })
        .await;
    assert_eq!(
        alice
            .read_server_message_with_timeout(Duration::from_millis(150))
            .await,
        None,
        "stale replacement socket must not overwrite presence"
    );
    assert_eq!(bob.read_close_code().await, Some(CloseCode::Library(4108)));
    require_some(replacement.close().await, "replacement should close")?;
    require_some(alice.close().await, "alice should close")?;
    assert!(server.wait_for_room_absence(&room).await);
    Ok(())
}

#[tokio::test]
async fn replaced_socket_recording_request_is_kicked_from_integration_test() -> TestResult {
    let (server, room) = server_with_room("issuer-replacement-recording-guard").await?;
    let bob_token = token(&room, UserId::Integer(2))?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(1), UserId::Integer(2)).await?;
    let (replacement, _welcome) = require_some(
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &bob_token).await,
        "replacement should negotiate",
    )?;

    assert_peer_left(&mut alice, UserId::Integer(2)).await?;
    assert_peer_joined(&mut alice, UserId::Integer(2)).await?;

    let _ = bob.send_start_recording("stale-recording-start").await;
    assert_eq!(bob.read_close_code().await, Some(CloseCode::Library(4108)));
    require_some(replacement.close().await, "replacement should close")?;
    require_some(alice.close().await, "alice should close")?;
    assert!(server.wait_for_room_absence(&room).await);
    Ok(())
}

#[tokio::test]
async fn numeric_string_user_ids_share_one_runtime_identity() -> TestResult {
    let (server, room) = server_with_room("issuer-runtime-user-id-normalization").await?;
    let string_token = token(&room, UserId::String("42".to_owned()))?;
    let (mut observer, mut numeric_user) =
        protocol_pair(&server, &room, UserId::Integer(7), UserId::Integer(42)).await?;

    let (mut replacement, _welcome) = require_some(
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &string_token).await,
        "string-id replacement should negotiate",
    )?;

    assert_peer_left(&mut observer, UserId::Integer(42)).await?;
    assert_peer_joined(&mut observer, UserId::Integer(42)).await?;
    assert_eq!(
        numeric_user.read_close_code().await,
        Some(CloseCode::Library(4108))
    );

    require_some(
        observer
            .send_message(ClientMessage::Subscribe(SubscribePayload {
                user_id: UserId::String("42".to_owned()),
                states: DownloadStates {
                    audio: Some(true),
                    ..DownloadStates::default()
                },
            }))
            .await,
        "observer should subscribe using string user id",
    )?;

    assert_diagnostics_user(&server, &room, 42).await?;

    let status = disconnect_sessions_via_http(
        &server,
        BTreeMap::from([(room.clone(), vec![UserId::String("42".to_owned())])]),
    )
    .await;
    assert_eq!(status, Some(StatusCode::OK));
    assert_eq!(
        replacement.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_peer_left(&mut observer, UserId::Integer(42)).await?;
    require_some(observer.close().await, "observer should close")?;
    assert!(server.wait_for_room_absence(&room).await);
    Ok(())
}

async fn assert_peer_left(client: &mut ProtocolWebSocketClient, user_id: UserId) -> TestResult {
    let message = read_until_server_message(
        client,
        Duration::from_secs(1),
        |message| matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == user_id),
    )
    .await;
    require_some(message, "peer departure should be delivered")?;
    Ok(())
}

async fn assert_peer_joined(client: &mut ProtocolWebSocketClient, user_id: UserId) -> TestResult {
    let message = read_until_server_message(client, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == user_id)
    })
    .await;
    require_some(message, "peer join should be delivered")?;
    Ok(())
}

async fn assert_diagnostics_user(server: &TestServer, room_id: &str, user_id: i64) -> TestResult {
    let response = reqwest::Client::new()
        .get(format!(
            "{}/internal/diagnostics/users/{user_id}",
            server.http_base_url()
        ))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload = response.json::<serde_json::Value>().await?;
    assert_eq!(
        payload.get("roomId").and_then(serde_json::Value::as_str),
        Some(room_id)
    );
    assert_eq!(
        payload
            .get("user")
            .and_then(|user| user.get("userId"))
            .and_then(serde_json::Value::as_i64),
        Some(user_id)
    );
    Ok(())
}

#[tokio::test]
async fn replaced_socket_cannot_finish_a_queued_publish_negotiation_from_integration_test()
-> TestResult {
    let (server, room) = server_with_room("issuer-replacement-queued-publish").await?;
    let bob_token = token(&room, UserId::Integer(12))?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(11), UserId::Integer(12)).await?;

    require_some(
        bob.send_message(ClientMessage::Publish(StreamIntentPayload {
            stream_type: StreamType::Audio,
        }))
        .await,
        "bob should send queued publish intent",
    )?;
    let request = bob.read_server_request().await;
    let (request_id, request) = require_some(request, "bob should receive queued renegotiation")?;
    assert!(
        matches!(request, ServerRequest::Renegotiate(_)),
        "publish should queue a renegotiation request before the replacement arrives"
    );

    let replacement =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &bob_token).await;
    let (replacement, _welcome) = require_some(replacement, "replacement should negotiate")?;

    let departed = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(12))
    })
    .await;
    require_some(departed, "alice should observe replaced peer departure")?;
    let rejoined = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == UserId::Integer(12))
    })
    .await;
    require_some(rejoined, "alice should observe replacement join")?;

    require_some(
        bob.respond_to_negotiation_request(request_id, request)
            .await,
        "stale socket should send queued negotiation response",
    )?;
    assert_eq!(
        alice
            .read_server_message_with_timeout(Duration::from_millis(150))
            .await,
        None,
        "stale queued publish answers must not create observable room state"
    );
    assert_eq!(bob.read_close_code().await, Some(CloseCode::Library(4108)));
    require_some(replacement.close().await, "replacement should close")?;
    Ok(())
}

#[tokio::test]
async fn bulk_disconnected_socket_cannot_broadcast_after_logical_removal() -> TestResult {
    let (server, room) = server_with_room("issuer-disconnect-guard").await?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(21), UserId::Integer(22)).await?;

    let status = disconnect_sessions_via_http(
        &server,
        BTreeMap::from([(room.clone(), vec![UserId::Integer(22)])]),
    )
    .await;
    assert_eq!(status, Some(StatusCode::OK));

    let _ = bob
        .send_broadcast(serde_json::json!({ "text": "post-kick" }))
        .await;
    let message = alice
        .read_server_message_with_timeout(Duration::from_secs(1))
        .await;
    let message = require_some(message, "alice should receive peer departure")?;
    if let ServerMessage::PeerLeft(payload) = message {
        assert_eq!(payload.user_id, UserId::Integer(22));
    } else {
        panic!("expected bulk disconnect to surface peerleft before any stale broadcast");
    }
    assert_eq!(
        alice
            .read_server_message_with_timeout(Duration::from_millis(150))
            .await,
        None,
        "bulk-disconnected sockets must not squeeze extra broadcast traffic through after removal"
    );
    assert_eq!(bob.read_close_code().await, Some(CloseCode::Library(4108)));
    require_some(alice.close().await, "alice should close")?;
    assert!(server.wait_for_room_absence(&room).await);
    Ok(())
}

#[tokio::test]
async fn bulk_disconnect_scopes_each_room_independently_from_integration_test() -> TestResult {
    let server = default_server().await?;
    let room_a = room(&server, "issuer-a").await?;
    let room_b = room(&server, "issuer-b").await?;

    let (mut a_keep, mut a_drop) =
        protocol_pair(&server, &room_a, UserId::Integer(1), UserId::Integer(2)).await?;
    let b_drop_token = token(&room_b, UserId::Integer(1))?;
    let b_keep_token = token(&room_b, UserId::Integer(2))?;
    let (mut b_drop, mut b_keep) = require_some(
        Box::pin(connect_protocol_pair(
            &server,
            &b_drop_token,
            &b_keep_token,
            UserId::Integer(2),
        ))
        .await,
        "room B protocol pair should connect",
    )?;

    let status = disconnect_sessions_via_http(
        &server,
        BTreeMap::from([
            (room_a.clone(), vec![UserId::Integer(2)]),
            (room_b.clone(), vec![UserId::Integer(1)]),
        ]),
    )
    .await;
    assert_eq!(status, Some(StatusCode::OK));

    assert_eq!(
        a_drop.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_eq!(
        b_drop.read_close_code().await,
        Some(CloseCode::Library(4108))
    );

    let a_departure = read_until_server_message(&mut a_keep, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(2))
    })
    .await;
    let a_departure = require_some(a_departure, "room A survivor should receive departure")?;
    if let ServerMessage::PeerLeft(payload) = a_departure {
        assert_eq!(payload.user_id, UserId::Integer(2));
    } else {
        panic!("expected room A to receive the disconnected peerleft notification");
    }

    let b_departure = read_until_server_message(&mut b_keep, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(1))
    })
    .await;
    let b_departure = require_some(b_departure, "room B survivor should receive departure")?;
    if let ServerMessage::PeerLeft(payload) = b_departure {
        assert_eq!(payload.user_id, UserId::Integer(1));
    } else {
        panic!("expected room B to receive the disconnected peerleft notification");
    }

    assert_eq!(
        a_keep
            .read_server_message_with_timeout(Duration::from_millis(150))
            .await,
        None,
        "room A survivor must not receive cross-room traffic after the bulk disconnect"
    );
    assert_eq!(
        b_keep
            .read_server_message_with_timeout(Duration::from_millis(150))
            .await,
        None,
        "room B survivor must not receive cross-room traffic after the bulk disconnect"
    );
    Ok(())
}

#[tokio::test]
async fn mismatched_explicit_room_id_is_rejected_from_integration_test() -> TestResult {
    let server = default_server().await?;
    let first_room = room(&server, "issuer-a").await?;
    let second_room = room(&server, "issuer-b").await?;
    let token = token(&first_room, UserId::Integer(3))?;

    let client =
        ProtocolWebSocketClient::authenticate_with_room(&server, &token, &second_room).await;
    let mut client = require_some(client, "client should connect before auth rejection")?;

    assert_eq!(
        client.read_close_code().await,
        Some(CloseCode::Library(4106))
    );
    Ok(())
}

fn slow_consumer_overflow_config() -> Config {
    let mut config = test_config(1_000, 10);
    config.user.outbound_queue_capacity = 1;
    config.user.outbound_queue_byte_capacity = 64 * 1024;
    config
}

fn slow_consumer_broadcast_batch() -> Vec<ClientMessage> {
    let payload = "x".repeat(SLOW_CONSUMER_PAYLOAD_BYTES);
    (0..SLOW_CONSUMER_BATCH_LEN)
        .map(|index| {
            ClientMessage::Broadcast(ClientBroadcastPayload {
                message: serde_json::json!({
                    "index": index,
                    "payload": payload.clone(),
                }),
            })
        })
        .collect()
}

fn metric_value(metrics_text: &str, sample_name: &str) -> Option<u64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(sample_name)?.trim().parse().ok())
}
