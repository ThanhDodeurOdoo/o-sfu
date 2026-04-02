#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

mod support;

use std::collections::BTreeMap;

use reqwest::StatusCode;
use tokio::time::{Duration, sleep};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use o_sfu::{
    runtime::testing::spawn_test_server,
    signaling::{
        current_protocol::{
            CurrentClientMessage, CurrentServerMessage, CurrentSessionInfoUpdatePayload,
            CurrentWebSocketCredentials,
        },
        shared::{SessionId, SessionInfo, StreamType},
    },
};

use crate::support::{
    FakeWebSocketClient, TEST_AUTH_KEY, TEST_CHANNEL_KEY, create_channel,
    disconnect_sessions_via_http, signed_connect_claims, test_config,
};

#[tokio::test]
async fn websocket_startup_and_transport_bootstrap_work_from_integration_test() {
    let server = spawn_test_server(test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", Some(TEST_CHANNEL_KEY)).await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };
    let token = signed_connect_claims(TEST_CHANNEL_KEY, &channel, SessionId::Integer(7));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let client = FakeWebSocketClient::authenticate_with_credentials(
        &server,
        &CurrentWebSocketCredentials {
            channel_uuid: Some(channel),
            jwt: token,
        },
    )
    .await;
    assert!(client.is_some());
    let Some(mut client) = client else {
        return;
    };

    let startup = client.read_startup().await;
    assert!(startup.is_some());
    let Some(startup) = startup else {
        return;
    };
    assert!(startup.available_features.rtc);

    let batch = client.read_bus_batch().await;
    assert!(batch.is_some());
    let Some(batch) = batch else {
        return;
    };
    assert_eq!(batch.len(), 1);
}

#[tokio::test]
async fn websocket_timeout_is_reported_from_integration_test() {
    let server = spawn_test_server(test_config(25, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };

    let client = FakeWebSocketClient::connect(&server).await;
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
    let server = spawn_test_server(test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };

    let client = FakeWebSocketClient::authenticate_with_jwt(&server, "not-a-jwt").await;
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
async fn channel_creation_is_idempotent_by_issuer_from_integration_test() {
    let server = spawn_test_server(test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };

    let first = create_channel(&server, "issuer-a", None).await;
    let second = create_channel(&server, "issuer-a", Some(TEST_CHANNEL_KEY)).await;
    let third = create_channel(&server, "issuer-b", None).await;
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
async fn broadcast_reaches_other_session_from_integration_test() {
    let server = spawn_test_server(test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None).await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, &channel, SessionId::Integer(1));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, &channel, SessionId::Integer(2));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let alice = FakeWebSocketClient::authenticate_and_bootstrap(&server, &alice_token).await;
    let bob = FakeWebSocketClient::authenticate_and_bootstrap(&server, &bob_token).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some((mut alice, _startup)) = alice else {
        return;
    };
    let Some((mut bob, _startup)) = bob else {
        return;
    };

    let sent = alice
        .send_bus_message(CurrentClientMessage::Broadcast(serde_json::json!({
            "type": StreamType::Audio,
            "text": "hello"
        })))
        .await;
    assert!(sent.is_some());

    let message = bob.read_server_message().await;
    assert!(message.is_some());
    if let Some(CurrentServerMessage::Broadcast(payload)) = message {
        assert_eq!(payload.sender_id, SessionId::Integer(1));
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
async fn session_info_change_reaches_other_session_from_integration_test() {
    let server = spawn_test_server(test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None).await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, &channel, SessionId::Integer(1));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, &channel, SessionId::Integer(2));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let alice = FakeWebSocketClient::authenticate_and_bootstrap(&server, &alice_token).await;
    let bob = FakeWebSocketClient::authenticate_and_bootstrap(&server, &bob_token).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some((mut alice, _startup)) = alice else {
        return;
    };
    let Some((mut bob, _startup)) = bob else {
        return;
    };

    let sent = alice
        .send_bus_message(CurrentClientMessage::UpdateSessionInfo(
            CurrentSessionInfoUpdatePayload {
                info: SessionInfo {
                    is_talking: Some(true),
                    ..SessionInfo::default()
                },
                need_refresh: None,
            },
        ))
        .await;
    assert!(sent.is_some());

    let message = bob.read_server_message().await;
    assert!(message.is_some());
    if let Some(CurrentServerMessage::SessionInfoChanged(snapshot)) = message {
        assert!(snapshot.contains_key("1"));
        assert_eq!(
            snapshot.get("1").and_then(|info| info.is_talking),
            Some(true)
        );
    } else {
        panic!("expected session info update");
    }
}

#[tokio::test]
async fn channel_full_and_last_disconnect_cleanup_are_observable_from_integration_test() {
    let server = spawn_test_server(test_config(1_000, 1)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let first_channel = create_channel(&server, "issuer-a", None).await;
    assert!(first_channel.is_some());
    let Some(first_channel) = first_channel else {
        return;
    };
    let first_token = signed_connect_claims(TEST_AUTH_KEY, &first_channel, SessionId::Integer(1));
    let second_token = signed_connect_claims(TEST_AUTH_KEY, &first_channel, SessionId::Integer(2));
    assert!(first_token.is_some());
    assert!(second_token.is_some());
    let (Some(first_token), Some(second_token)) = (first_token, second_token) else {
        return;
    };

    let first_client = FakeWebSocketClient::authenticate_and_bootstrap(&server, &first_token).await;
    assert!(first_client.is_some());
    let Some((first_client, _startup)) = first_client else {
        return;
    };

    let second_client = FakeWebSocketClient::authenticate_with_jwt(&server, &second_token).await;
    assert!(second_client.is_some());
    let Some(mut second_client) = second_client else {
        return;
    };
    assert_eq!(
        second_client.read_close_code().await,
        Some(CloseCode::Library(4109)),
    );

    assert!(first_client.close().await.is_some());
    sleep(Duration::from_millis(20)).await;

    let second_channel = create_channel(&server, "issuer-a", None).await;
    assert!(second_channel.is_some());
    let Some(second_channel) = second_channel else {
        return;
    };
    assert_ne!(first_channel, second_channel);

    let third_token = signed_connect_claims(TEST_AUTH_KEY, &second_channel, SessionId::Integer(3));
    assert!(third_token.is_some());
    let Some(third_token) = third_token else {
        return;
    };
    let third_client = FakeWebSocketClient::authenticate_and_bootstrap(&server, &third_token).await;
    assert!(third_client.is_some());
}

#[tokio::test]
async fn disconnect_api_kicks_target_and_notifies_remaining_from_integration_test() {
    let server = spawn_test_server(test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None).await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, &channel, SessionId::Integer(1));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, &channel, SessionId::Integer(2));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let alice = FakeWebSocketClient::authenticate_and_bootstrap(&server, &alice_token).await;
    let bob = FakeWebSocketClient::authenticate_and_bootstrap(&server, &bob_token).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some((mut alice, _startup)) = alice else {
        return;
    };
    let Some((mut bob, _startup)) = bob else {
        return;
    };

    let status = disconnect_sessions_via_http(
        &server,
        BTreeMap::from([(channel.clone(), vec![SessionId::Integer(2)])]),
    )
    .await;
    assert_eq!(status, Some(StatusCode::OK));

    assert_eq!(bob.read_close_code().await, Some(CloseCode::Library(4108)));

    let message = alice.read_server_message().await;
    assert!(message.is_some());
    if let Some(CurrentServerMessage::SessionDeparted(payload)) = message {
        assert_eq!(payload.session_id, SessionId::Integer(2));
    } else {
        panic!("expected session departure notification");
    }
}

#[tokio::test]
async fn mismatched_explicit_channel_uuid_is_rejected_from_integration_test() {
    let server = spawn_test_server(test_config(1_000, 10)).await;
    assert!(server.is_ok());
    let Some(server) = server.ok() else {
        return;
    };
    let first_channel = create_channel(&server, "issuer-a", None).await;
    let second_channel = create_channel(&server, "issuer-b", None).await;
    assert!(first_channel.is_some());
    assert!(second_channel.is_some());
    let (Some(first_channel), Some(second_channel)) = (first_channel, second_channel) else {
        return;
    };
    let token = signed_connect_claims(TEST_AUTH_KEY, &first_channel, SessionId::Integer(3));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let client = FakeWebSocketClient::authenticate_with_credentials(
        &server,
        &CurrentWebSocketCredentials {
            channel_uuid: Some(second_channel),
            jwt: token,
        },
    )
    .await;
    assert!(client.is_some());
    let Some(mut client) = client else {
        return;
    };

    assert_eq!(
        client.read_close_code().await,
        Some(CloseCode::Library(4106))
    );
}
