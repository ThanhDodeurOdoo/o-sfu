use std::time::Duration;

use futures_util::SinkExt;
use o_sfu_protocol::wire::{
    AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest,
    EnvelopeBatch, RecordingOptions, RequestId, ServerEnvelope, ServerMessage, ServerRequest,
    UserId, UserInfo, WelcomePayload,
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{self, protocol::frame::coding::CloseCode};

use super::{
    fake_rtc_peer::FakeRtcPeer,
    harness::{
        TestServer, TestWebSocket, connect_websocket, decode_protocol_welcome_batch,
        read_close_code, read_text_message,
    },
    protocol_wire::{encode_client_batch, read_protocol_batch, send_server_request_response},
};

pub struct ProtocolWebSocketClient {
    websocket: TestWebSocket,
    rtc_peer: FakeRtcPeer,
}

impl ProtocolWebSocketClient {
    pub async fn connect(server: &TestServer) -> Option<Self> {
        Some(Self {
            websocket: connect_websocket(server).await?,
            rtc_peer: FakeRtcPeer::bind(0).await?,
        })
    }

    pub async fn authenticate_with_jwt(server: &TestServer, token: &str) -> Option<Self> {
        Self::authenticate(
            server,
            AuthPayload {
                jwt: token.to_owned(),
                channel: None,
            },
        )
        .await
    }

    pub async fn authenticate_with_room(
        server: &TestServer,
        token: &str,
        room_id: &str,
    ) -> Option<Self> {
        Self::authenticate(
            server,
            AuthPayload {
                jwt: token.to_owned(),
                channel: Some(room_id.to_owned()),
            },
        )
        .await
    }

    pub async fn authenticate_and_negotiate(
        server: &TestServer,
        token: &str,
    ) -> Option<(Self, WelcomePayload)> {
        let mut client = Self::authenticate_with_jwt(server, token).await?;
        let welcome = client.read_welcome().await?;
        client.finish_initial_negotiation().await?;
        Some((client, welcome))
    }

    async fn authenticate(server: &TestServer, auth_payload: AuthPayload) -> Option<Self> {
        let mut websocket = connect_websocket(server).await?;
        websocket
            .send(tungstenite::Message::Text(
                encode_auth(auth_payload)?.into(),
            ))
            .await
            .ok()?;
        Some(Self {
            websocket,
            rtc_peer: FakeRtcPeer::bind(0).await?,
        })
    }

    pub async fn read_welcome(&mut self) -> Option<WelcomePayload> {
        let payload = read_text_message(&mut self.websocket).await?;
        decode_protocol_welcome_batch(&payload)
    }

    pub async fn finish_initial_negotiation(&mut self) -> Option<()> {
        let (request_id, request) = self.read_server_request().await?;
        let ServerRequest::Offer(_) = request else {
            return None;
        };
        self.respond_to_negotiation_request(request_id, request)
            .await
    }

    pub async fn send_message(&mut self, message: ClientMessage) -> Option<()> {
        self.send_messages(vec![message]).await
    }

    pub async fn send_messages(&mut self, messages: Vec<ClientMessage>) -> Option<()> {
        self.websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(messages.into_iter().map(ClientEnvelope::Message).collect())?
                    .into(),
            ))
            .await
            .ok()?;
        Some(())
    }

    pub async fn send_broadcast(&mut self, message: serde_json::Value) -> Option<()> {
        self.send_message(ClientMessage::Broadcast(ClientBroadcastPayload { message }))
            .await
    }

    pub async fn send_info(&mut self, info: UserInfo) -> Option<()> {
        self.send_message(ClientMessage::Info(info)).await
    }

    pub async fn send_start_recording(&mut self, request_id: &str) -> Option<()> {
        self.websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(vec![ClientEnvelope::Request {
                    request_id: RequestId::new(request_id),
                    request: ClientRequest::StartRecording(RecordingOptions {
                        audio: Some(true),
                        video: None,
                        transcription: None,
                    }),
                }])?
                .into(),
            ))
            .await
            .ok()?;
        Some(())
    }

    pub async fn read_server_message(&mut self) -> Option<ServerMessage> {
        let batch = self.read_server_batch().await?;
        let envelope = batch.first()?.clone();
        match ServerEnvelope::decode(envelope).ok()? {
            ServerEnvelope::Message(message) => Some(message),
            ServerEnvelope::Request { .. } | ServerEnvelope::Response { .. } => None,
        }
    }

    pub async fn read_server_request(&mut self) -> Option<(RequestId, ServerRequest)> {
        let batch = self.read_server_batch().await?;
        let envelope = batch.first()?.clone();
        match ServerEnvelope::decode(envelope).ok()? {
            ServerEnvelope::Request {
                request_id,
                request,
            } => Some((request_id, request)),
            ServerEnvelope::Message(_) | ServerEnvelope::Response { .. } => None,
        }
    }

    pub async fn read_server_message_with_timeout(
        &mut self,
        duration: Duration,
    ) -> Option<ServerMessage> {
        timeout(duration, self.read_server_message()).await.ok()?
    }

    pub async fn respond_to_negotiation_request(
        &mut self,
        response_to: RequestId,
        request: ServerRequest,
    ) -> Option<()> {
        send_server_request_response(
            &mut self.websocket,
            &mut self.rtc_peer,
            response_to,
            request,
        )
        .await
    }

    pub async fn read_close_code(&mut self) -> Option<CloseCode> {
        read_close_code(&mut self.websocket).await
    }

    pub async fn close(mut self) -> Option<()> {
        self.websocket.close(None).await.ok()?;
        Some(())
    }

    async fn read_server_batch(&mut self) -> Option<EnvelopeBatch> {
        read_protocol_batch(&mut self.websocket).await
    }
}

fn encode_auth(auth_payload: AuthPayload) -> Option<String> {
    encode_client_batch(vec![ClientEnvelope::Message(ClientMessage::Auth(
        auth_payload,
    ))])
}

pub async fn read_until_server_message(
    client: &mut ProtocolWebSocketClient,
    timeout_duration: Duration,
    predicate: impl Fn(&ServerMessage) -> bool,
) -> Option<ServerMessage> {
    loop {
        let message = client
            .read_server_message_with_timeout(timeout_duration)
            .await?;
        if predicate(&message) {
            return Some(message);
        }
    }
}

pub async fn connect_protocol_pair(
    server: &TestServer,
    first_token: &str,
    second_token: &str,
    second_user_id: UserId,
) -> Option<(ProtocolWebSocketClient, ProtocolWebSocketClient)> {
    let (mut first, _welcome) = Box::pin(ProtocolWebSocketClient::authenticate_and_negotiate(
        server,
        first_token,
    ))
    .await?;
    let (second, _welcome) = Box::pin(ProtocolWebSocketClient::authenticate_and_negotiate(
        server,
        second_token,
    ))
    .await?;
    Box::pin(read_until_server_message(
        &mut first,
        Duration::from_secs(1),
        |message| {
            matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == second_user_id)
        },
    ))
    .await?;
    Some((first, second))
}
