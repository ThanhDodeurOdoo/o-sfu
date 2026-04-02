#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

mod support;

use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use o_sfu::{
    runtime::testing::spawn_test_server,
    signaling::{
        current_protocol::{
            CurrentClientMessage, CurrentServerMessage, CurrentStartupPayload,
            CurrentWebSocketCredentials,
        },
        shared::{SessionId, StreamType},
    },
};

use crate::support::{
    TEST_AUTH_KEY, TEST_CHANNEL_KEY, acknowledge_transport_bootstrap,
    authenticate_and_read_startup, authenticate_with_credentials, create_channel, read_bus_batch,
    read_close_code, read_server_message, read_text_message, send_bus_message,
    signed_connect_claims, test_config,
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

    let websocket = authenticate_with_credentials(
        &server,
        &CurrentWebSocketCredentials {
            channel_uuid: Some(channel),
            jwt: token,
        },
    )
    .await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };

    let startup = read_text_message(&mut websocket).await;
    assert!(startup.is_some());
    let Some(startup) = startup else {
        return;
    };
    let startup = serde_json::from_str::<CurrentStartupPayload>(&startup);
    assert!(startup.is_ok());
    let Some(startup) = startup.ok() else {
        return;
    };
    assert!(startup.available_features.rtc);

    let batch = read_bus_batch(&mut websocket).await;
    assert!(batch.is_some());
    let Some(batch) = batch else {
        return;
    };
    assert_eq!(batch.len(), 1);
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

    let alice = authenticate_and_read_startup(&server, &alice_token).await;
    let bob = authenticate_and_read_startup(&server, &bob_token).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some((mut alice, _startup)) = alice else {
        return;
    };
    let Some((mut bob, _startup)) = bob else {
        return;
    };
    assert!(acknowledge_transport_bootstrap(&mut alice).await.is_some());
    assert!(acknowledge_transport_bootstrap(&mut bob).await.is_some());

    let sent = send_bus_message(
        &mut alice,
        CurrentClientMessage::Broadcast(serde_json::json!({
            "type": StreamType::Audio,
            "text": "hello"
        })),
    )
    .await;
    assert!(sent.is_some());

    let message = read_server_message(&mut bob).await;
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

    let websocket = authenticate_with_credentials(
        &server,
        &CurrentWebSocketCredentials {
            channel_uuid: Some(second_channel),
            jwt: token,
        },
    )
    .await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Library(4106)),
    );
}
