#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

use std::collections::BTreeMap;

use o_sfu::{
    config::{Config, RtcPortRange},
    http::{DISCONNECT_PATH, STATS_PATH, StatsResponse},
};
use o_sfu_protocol::{
    shared::{DownloadStates, StreamType, UserId, UserInfo},
    signaling::{
        ClientBroadcastPayload, ClientMessage, ServerMessage, ServerRequest, StreamIntentPayload,
        SubscribePayload,
    },
};
use o_sfu_tests::support::{
    TEST_ROOM_KEY, TestServer, create_room, disconnect_sessions_via_http, metrics_text,
    protocol_harness::{
        ProtocolWebSocketClient, connect_protocol_pair, protocol_test_config,
        read_until_server_message,
    },
    signed_connect_claims, spawn_test_server,
};
use reqwest::StatusCode;
use tokio::time::{Duration, timeout};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const SLOW_CONSUMER_BATCH_LEN: usize = 64;
const SLOW_CONSUMER_PAYLOAD_BYTES: usize = 1_024;

#[tokio::test]
async fn websocket_welcome_and_initial_offer_work_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(7));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let client = ProtocolWebSocketClient::authenticate_with_room(&server, &token, &room).await;
    assert!(client.is_some());
    let Some(mut client) = client else {
        return;
    };

    let welcome = client.read_welcome().await;
    assert!(welcome.is_some());
    let Some(welcome) = welcome else {
        return;
    };
    assert!(welcome.features.rtc);

    let request = client.read_server_request().await;
    assert!(request.is_some());
    let Some((_request_id, request)) = request else {
        return;
    };
    assert!(matches!(request, ServerRequest::Offer(_)));
}

#[tokio::test]
async fn websocket_welcome_and_initial_offer_expose_real_rtc_transport_details() {
    let config = protocol_test_config(1_000, 10);
    let server = spawn_test_server(config).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(701));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let client = ProtocolWebSocketClient::authenticate_with_room(&server, &token, &room).await;
    assert!(client.is_some());
    let Some(mut client) = client else {
        return;
    };

    let welcome = client.read_welcome().await;
    assert!(welcome.is_some());
    let Some(welcome) = welcome else {
        return;
    };
    assert!(welcome.features.rtc);

    let request = client.read_server_request().await;
    assert!(request.is_some(), "initial offer should be sent");
    let Some((_request_id, request)) = request else {
        return;
    };
    let ServerRequest::Offer(payload) = request else {
        panic!("expected offer request");
    };
    assert!(payload.sdp.contains("a=ice-lite"));
    assert!(payload.sdp.contains("a=fingerprint:sha-256"));
    assert!(payload.sdp.contains("a=candidate:"));
    assert!(payload.sdp.contains("127.0.0.1"));
    if let Some(candidate_line) = payload
        .sdp
        .lines()
        .find(|line| line.starts_with("a=candidate:"))
    {
        assert!(candidate_line.contains(" 127.0.0.1 "));
    } else {
        panic!("expected SDP candidate line");
    }
    let Some(port) = payload
        .sdp
        .lines()
        .find(|line| line.starts_with("a=candidate:"))
        .and_then(|line| line.split_whitespace().nth(5))
        .and_then(|value| value.parse::<u16>().ok())
    else {
        return;
    };
    assert!((40_000..=49_999).contains(&port));
}

#[tokio::test]
async fn websocket_offer_advertises_configured_public_ip_in_rtc_mode() {
    let mut config = protocol_test_config(1_000, 10);
    config.transport.public_ip = "203.0.113.44".parse().unwrap_or(config.transport.public_ip);
    config.transport.rtc_port_range = RtcPortRange::new(45_000, 45_099);
    let server = spawn_test_server(config).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-public-ip", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(702));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let client = ProtocolWebSocketClient::authenticate_with_room(&server, &token, &room).await;
    assert!(client.is_some());
    let Some(mut client) = client else {
        return;
    };

    assert!(client.read_welcome().await.is_some());
    let request = client.read_server_request().await;
    assert!(request.is_some(), "initial offer should be sent");
    let Some((_request_id, request)) = request else {
        return;
    };
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
}

#[tokio::test]
async fn websocket_timeout_is_reported_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(25, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };

    let client = ProtocolWebSocketClient::connect(&server).await;
    assert!(client.is_some());
    let Some(mut client) = client else {
        return;
    };

    assert_eq!(
        client.read_close_code().await,
        Some(CloseCode::Library(4107))
    );
}

#[tokio::test]
async fn invalid_jwt_is_rejected_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };

    let client = ProtocolWebSocketClient::authenticate_with_jwt(&server, "not-a-jwt").await;
    assert!(client.is_some());
    let Some(mut client) = client else {
        return;
    };

    assert_eq!(
        client.read_close_code().await,
        Some(CloseCode::Library(4106))
    );
}

#[tokio::test]
async fn room_creation_is_idempotent_by_issuer_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };

    let first = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    let second = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    let third = create_room(&server, "issuer-b", TEST_ROOM_KEY).await;
    assert!(first.is_some());
    assert!(second.is_some());
    assert!(third.is_some());
    let (Some(first), Some(second), Some(third)) = (first, second, third) else {
        return;
    };

    assert_eq!(first, second);
    assert_ne!(first, third);
}

#[tokio::test]
async fn oversized_disconnect_body_is_rejected_before_handler_metrics_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };

    let response = reqwest::Client::new()
        .post(format!("{}{DISCONNECT_PATH}", server.http_base_url()))
        .body("x".repeat((16 * 1024) + 1))
        .send()
        .await;
    assert!(
        response.is_ok(),
        "oversized disconnect request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let metrics = metrics_text(&server).await;
    assert!(metrics.is_some());
    let Some(metrics) = metrics else {
        return;
    };
    assert!(metrics.contains("osfu_http_disconnect_requests_total 0"));
    assert!(metrics.contains("osfu_http_disconnect_responses_total{status=\"success\"} 0"));
    assert!(metrics.contains("osfu_http_disconnect_responses_total{status=\"bad_request\"} 0"));
    assert!(
        metrics.contains("osfu_http_disconnect_responses_total{status=\"unprocessable_entity\"} 0")
    );
}

#[tokio::test]
async fn broadcast_reaches_other_user_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(1));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(2));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let peers = connect_protocol_pair(&server, &alice_token, &bob_token, UserId::Integer(2)).await;
    assert!(peers.is_some());
    let Some((mut alice, mut bob)) = peers else {
        return;
    };

    let sent = alice
        .send_broadcast(serde_json::json!({
            "type": StreamType::Audio,
            "text": "hello"
        }))
        .await;
    assert!(sent.is_some());

    let message = bob
        .read_server_message_with_timeout(Duration::from_secs(1))
        .await;
    assert!(message.is_some());
    if let Some(ServerMessage::Broadcast(payload)) = message {
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
}

#[tokio::test]
async fn websocket_slow_consumer_overflow_closes_only_slow_socket_from_integration_test() {
    let server = spawn_test_server(slow_consumer_overflow_config()).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-slow-consumer-overflow", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let slow_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(31));
    let driver_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(32));
    let witness_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(33));
    assert!(slow_token.is_some());
    assert!(driver_token.is_some());
    assert!(witness_token.is_some());
    let (Some(slow_token), Some(driver_token), Some(witness_token)) =
        (slow_token, driver_token, witness_token)
    else {
        return;
    };

    let peers =
        connect_protocol_pair(&server, &slow_token, &driver_token, UserId::Integer(32)).await;
    assert!(peers.is_some());
    let Some((mut slow, mut driver)) = peers else {
        return;
    };

    let sent = driver.send_messages(slow_consumer_broadcast_batch()).await;
    assert!(sent.is_some());

    let slow_close = timeout(Duration::from_secs(5), slow.read_close_code())
        .await
        .ok()
        .flatten();
    assert_eq!(slow_close, Some(CloseCode::Library(4108)));

    let departed = read_until_server_message(&mut driver, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(31))
    })
    .await;
    assert!(departed.is_some());

    let witness =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &witness_token).await;
    assert!(witness.is_some());
    let Some((witness, _welcome)) = witness else {
        return;
    };
    let rejoined = read_until_server_message(&mut driver, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == UserId::Integer(33))
    })
    .await;
    assert!(rejoined.is_some());
    assert!(witness.close().await.is_some());

    let metrics = metrics_text(&server).await;
    assert!(metrics.is_some());
    let Some(metrics) = metrics else {
        return;
    };
    assert!(metric_value(&metrics, "osfu_ws_outbound_queue_overflows_total").unwrap_or(0) > 0);
    assert_eq!(
        metric_value(
            &metrics,
            "osfu_ws_user_loop_exits_total{reason=\"outbound_queue_overflow\"}"
        ),
        Some(1)
    );
}

#[tokio::test]
async fn user_info_change_reaches_other_user_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(1));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(2));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let peers = connect_protocol_pair(&server, &alice_token, &bob_token, UserId::Integer(2)).await;
    assert!(peers.is_some());
    let Some((mut alice, mut bob)) = peers else {
        return;
    };

    let sent = alice
        .send_info(UserInfo {
            is_talking: Some(true),
            ..UserInfo::default()
        })
        .await;
    assert!(sent.is_some());

    let message = read_until_server_message(&mut bob, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerInfo(payload) if payload.user_id == UserId::Integer(1))
    })
    .await;
    assert!(message.is_some());
    if let Some(ServerMessage::PeerInfo(payload)) = message {
        assert_eq!(payload.info.is_talking, Some(true));
    } else {
        panic!("expected user info update");
    }
}

#[tokio::test]
async fn stats_reports_live_user_aggregates_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(1));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(2));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let peers = connect_protocol_pair(&server, &alice_token, &bob_token, UserId::Integer(2)).await;
    assert!(peers.is_some());
    let Some((mut alice, mut bob)) = peers else {
        return;
    };

    let bob_sent = bob
        .send_info(UserInfo {
            is_talking: Some(true),
            ..UserInfo::default()
        })
        .await;
    assert!(bob_sent.is_some());

    let peer_info = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerInfo(payload) if payload.user_id == UserId::Integer(2))
    })
    .await;
    assert!(peer_info.is_some());

    let response = reqwest::get(format!("{}{STATS_PATH}", server.http_base_url()))
        .await
        .ok();
    assert!(response.is_some());
    let Some(response) = response else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let stats = response.json::<StatsResponse>().await.ok();
    assert!(stats.is_some());
    let Some(stats) = stats else {
        return;
    };
    assert_eq!(stats.len(), 1);
    let first = stats.first();
    assert!(first.is_some());
    let Some(first) = first else {
        return;
    };
    assert_eq!(first.uuid, room);
    assert_eq!(first.users_stats.count, 2);
    assert_eq!(first.users_stats.camera_count, 0);
    assert_eq!(first.users_stats.screen_count, 0);
    assert_eq!(first.users_stats.incoming_bit_rate.total, 0);
    assert!(first.web_rtc_enabled);
    assert_eq!(first.remote_address, "127.0.0.1");
}

#[tokio::test]
async fn room_full_and_last_disconnect_cleanup_are_observable_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 1)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let first_room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    assert!(first_room.is_some());
    let Some(first_room) = first_room else {
        return;
    };
    let first_token = signed_connect_claims(TEST_ROOM_KEY, &first_room, UserId::Integer(1));
    let second_token = signed_connect_claims(TEST_ROOM_KEY, &first_room, UserId::Integer(2));
    assert!(first_token.is_some());
    assert!(second_token.is_some());
    let (Some(first_token), Some(second_token)) = (first_token, second_token) else {
        return;
    };

    let first_client =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &first_token).await;
    assert!(first_client.is_some());
    let Some((first_client, _welcome)) = first_client else {
        return;
    };

    let second_client =
        ProtocolWebSocketClient::authenticate_with_jwt(&server, &second_token).await;
    assert!(second_client.is_some());
    let Some(mut second_client) = second_client else {
        return;
    };
    assert_eq!(
        second_client.read_close_code().await,
        Some(CloseCode::Library(4109)),
    );

    assert!(first_client.close().await.is_some());
    assert!(server.wait_for_room_absence(&first_room).await);

    let second_room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    assert!(second_room.is_some());
    let Some(second_room) = second_room else {
        return;
    };
    assert_ne!(first_room, second_room);

    let third_token = signed_connect_claims(TEST_ROOM_KEY, &second_room, UserId::Integer(3));
    assert!(third_token.is_some());
    let Some(third_token) = third_token else {
        return;
    };
    let third_client =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &third_token).await;
    assert!(third_client.is_some());
}

#[tokio::test]
async fn disconnect_api_kicks_target_and_notifies_remaining_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(1));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(2));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let peers = connect_protocol_pair(&server, &alice_token, &bob_token, UserId::Integer(2)).await;
    assert!(peers.is_some());
    let Some((mut alice, mut bob)) = peers else {
        return;
    };

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
    assert!(message.is_some());
    if let Some(ServerMessage::PeerLeft(payload)) = message {
        assert_eq!(payload.user_id, UserId::Integer(2));
    } else {
        panic!("expected user departure notification");
    }
}

#[tokio::test]
async fn replaced_socket_cannot_broadcast_or_change_info_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-replacement-guard", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(1));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(2));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let peers = connect_protocol_pair(&server, &alice_token, &bob_token, UserId::Integer(2)).await;
    assert!(peers.is_some());
    let Some((mut alice, mut bob)) = peers else {
        return;
    };
    let replacement =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &bob_token).await;
    assert!(replacement.is_some());
    let Some((replacement, _welcome)) = replacement else {
        return;
    };

    let departed = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(2))
    })
    .await;
    assert!(departed.is_some());
    let rejoined = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == UserId::Integer(2))
    })
    .await;
    assert!(rejoined.is_some());

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
    assert!(replacement.close().await.is_some());
}

#[tokio::test]
async fn numeric_string_user_ids_share_one_runtime_identity() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-runtime-user-id-normalization",
        TEST_ROOM_KEY,
    )
    .await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let observer_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(7));
    let numeric_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(42));
    let string_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::String("42".to_owned()));
    assert!(observer_token.is_some());
    assert!(numeric_token.is_some());
    assert!(string_token.is_some());
    let (Some(observer_token), Some(numeric_token), Some(string_token)) =
        (observer_token, numeric_token, string_token)
    else {
        return;
    };

    let peers = connect_protocol_pair(
        &server,
        &observer_token,
        &numeric_token,
        UserId::Integer(42),
    )
    .await;
    assert!(peers.is_some());
    let Some((mut observer, mut numeric_user)) = peers else {
        return;
    };

    let replacement =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &string_token).await;
    assert!(replacement.is_some());
    let Some((mut replacement, _welcome)) = replacement else {
        return;
    };

    assert_peer_left(&mut observer, UserId::Integer(42)).await;
    assert_peer_joined(&mut observer, UserId::Integer(42)).await;
    assert_eq!(
        numeric_user.read_close_code().await,
        Some(CloseCode::Library(4108))
    );

    assert!(
        observer
            .send_message(ClientMessage::Subscribe(SubscribePayload {
                user_id: UserId::String("42".to_owned()),
                states: DownloadStates {
                    audio: Some(true),
                    ..DownloadStates::default()
                },
            }))
            .await
            .is_some()
    );

    assert_diagnostics_user(&server, &room, 42).await;

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
    assert_peer_left(&mut observer, UserId::Integer(42)).await;
}

async fn assert_peer_left(client: &mut ProtocolWebSocketClient, user_id: UserId) {
    let message = read_until_server_message(
        client,
        Duration::from_secs(1),
        |message| matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == user_id),
    )
    .await;
    assert!(message.is_some());
}

async fn assert_peer_joined(client: &mut ProtocolWebSocketClient, user_id: UserId) {
    let message = read_until_server_message(client, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == user_id)
    })
    .await;
    assert!(message.is_some());
}

async fn assert_diagnostics_user(server: &TestServer, room_id: &str, user_id: i64) {
    let response = reqwest::Client::new()
        .get(format!(
            "{}/internal/diagnostics/users/{user_id}",
            server.http_base_url()
        ))
        .send()
        .await;
    assert!(response.is_ok());
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let payload = response.json::<serde_json::Value>().await;
    assert!(payload.is_ok());
    let Some(payload) = payload.ok() else {
        return;
    };
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
}

#[tokio::test]
async fn replaced_socket_cannot_finish_a_queued_publish_negotiation_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-replacement-queued-publish", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(11));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(12));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let peers = connect_protocol_pair(&server, &alice_token, &bob_token, UserId::Integer(12)).await;
    assert!(peers.is_some());
    let Some((mut alice, mut bob)) = peers else {
        return;
    };

    assert!(
        bob.send_message(ClientMessage::Publish(StreamIntentPayload {
            stream_type: StreamType::Audio,
        }))
        .await
        .is_some()
    );
    let request = bob.read_server_request().await;
    assert!(request.is_some());
    let Some((request_id, request)) = request else {
        return;
    };
    assert!(
        matches!(request, ServerRequest::Renegotiate(_)),
        "publish should queue a renegotiation request before the replacement arrives"
    );

    let replacement =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &bob_token).await;
    assert!(replacement.is_some());
    let Some((replacement, _welcome)) = replacement else {
        return;
    };

    let departed = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(12))
    })
    .await;
    assert!(departed.is_some());
    let rejoined = read_until_server_message(&mut alice, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == UserId::Integer(12))
    })
    .await;
    assert!(rejoined.is_some());

    assert!(
        bob.respond_to_negotiation_request(request_id, request)
            .await
            .is_some()
    );
    assert_eq!(
        alice
            .read_server_message_with_timeout(Duration::from_millis(150))
            .await,
        None,
        "stale queued publish answers must not create observable room state"
    );
    assert_eq!(bob.read_close_code().await, Some(CloseCode::Library(4108)));
    assert!(replacement.close().await.is_some());
}

#[tokio::test]
async fn bulk_disconnected_socket_cannot_broadcast_after_logical_removal() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room = create_room(&server, "issuer-disconnect-guard", TEST_ROOM_KEY).await;
    assert!(room.is_some());
    let Some(room) = room else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(21));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, &room, UserId::Integer(22));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let peers = connect_protocol_pair(&server, &alice_token, &bob_token, UserId::Integer(22)).await;
    assert!(peers.is_some());
    let Some((mut alice, mut bob)) = peers else {
        return;
    };

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
    assert!(message.is_some());
    if let Some(ServerMessage::PeerLeft(payload)) = message {
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
}

#[tokio::test]
async fn bulk_disconnect_scopes_each_room_independently_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let room_a = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    let room_b = create_room(&server, "issuer-b", TEST_ROOM_KEY).await;
    assert!(room_a.is_some());
    assert!(room_b.is_some());
    let (Some(room_a), Some(room_b)) = (room_a, room_b) else {
        return;
    };

    let a_keep_token = signed_connect_claims(TEST_ROOM_KEY, &room_a, UserId::Integer(1));
    let a_drop_token = signed_connect_claims(TEST_ROOM_KEY, &room_a, UserId::Integer(2));
    let b_drop_token = signed_connect_claims(TEST_ROOM_KEY, &room_b, UserId::Integer(1));
    let b_keep_token = signed_connect_claims(TEST_ROOM_KEY, &room_b, UserId::Integer(2));
    assert!(a_keep_token.is_some());
    assert!(a_drop_token.is_some());
    assert!(b_drop_token.is_some());
    assert!(b_keep_token.is_some());
    let (Some(a_keep_token), Some(a_drop_token), Some(b_drop_token), Some(b_keep_token)) =
        (a_keep_token, a_drop_token, b_drop_token, b_keep_token)
    else {
        return;
    };

    let peers_in_room_a =
        connect_protocol_pair(&server, &a_keep_token, &a_drop_token, UserId::Integer(2)).await;
    assert!(peers_in_room_a.is_some());
    let Some((mut a_keep, mut a_drop)) = peers_in_room_a else {
        return;
    };

    let peers_in_room_b =
        connect_protocol_pair(&server, &b_drop_token, &b_keep_token, UserId::Integer(2)).await;
    assert!(peers_in_room_b.is_some());
    let Some((mut b_drop, mut b_keep)) = peers_in_room_b else {
        return;
    };

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
    assert!(a_departure.is_some());
    if let Some(ServerMessage::PeerLeft(payload)) = a_departure {
        assert_eq!(payload.user_id, UserId::Integer(2));
    } else {
        panic!("expected room A to receive the disconnected peerleft notification");
    }

    let b_departure = read_until_server_message(&mut b_keep, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(1))
    })
    .await;
    assert!(b_departure.is_some());
    if let Some(ServerMessage::PeerLeft(payload)) = b_departure {
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
}

#[tokio::test]
async fn mismatched_explicit_room_id_is_rejected_from_integration_test() {
    let server = spawn_test_server(protocol_test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let first_room = create_room(&server, "issuer-a", TEST_ROOM_KEY).await;
    let second_room = create_room(&server, "issuer-b", TEST_ROOM_KEY).await;
    assert!(first_room.is_some());
    assert!(second_room.is_some());
    let (Some(first_room), Some(second_room)) = (first_room, second_room) else {
        return;
    };
    let token = signed_connect_claims(TEST_ROOM_KEY, &first_room, UserId::Integer(3));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let client =
        ProtocolWebSocketClient::authenticate_with_room(&server, &token, &second_room).await;
    assert!(client.is_some());
    let Some(mut client) = client else {
        return;
    };

    assert_eq!(
        client.read_close_code().await,
        Some(CloseCode::Library(4106))
    );
}

fn slow_consumer_overflow_config() -> Config {
    let mut config = protocol_test_config(1_000, 10);
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
