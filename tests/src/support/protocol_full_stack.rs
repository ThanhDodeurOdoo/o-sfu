#![allow(
    dead_code,
    reason = "the protocol full-stack harness is shared by multiple RTC integration scenarios"
)]

use std::{future::Future, pin::Pin, time::Duration};

use futures_util::SinkExt;
use o_sfu_protocol::wire::{
    AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, DownloadStates, EnvelopeBatch,
    RequestId, ServerEnvelope, ServerMessage, ServerRequest, StreamIntentPayload, StreamType,
    SubscribePayload, UserId, UserInfo, WelcomePayload,
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{self, protocol::frame::coding::CloseCode};

use super::{
    TEST_ROOM_KEY, TestServer, TestWebSocket, connect_websocket, decode_protocol_welcome_batch,
    fake_media::{FakeClock, FakeMediaSource},
    fake_rtc_peer::{FakeRtcPeer, ReceivedRtpPacket},
    read_close_code, read_text_message, signed_connect_claims,
};

pub type ProtocolFakePeerPair = (ProtocolFakePeer, ProtocolFakePeer);
pub type ProtocolFakePeerPairFuture<'a> =
    Pin<Box<dyn Future<Output = Option<ProtocolFakePeerPair>> + 'a>>;

pub async fn connect_two_fake_peers(
    server: &TestServer,
    room_id: &str,
    first_user_id: UserId,
    second_user_id: UserId,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer)> {
    let first = connect_fake_peer(server, room_id, first_user_id, TEST_ROOM_KEY).await?;
    let second = connect_fake_peer(server, room_id, second_user_id, TEST_ROOM_KEY).await?;
    Some((first, second))
}

#[must_use]
pub fn connect_two_rtc_ready_fake_peers<'a>(
    server: &'a TestServer,
    room_id: &'a str,
    first_user_id: UserId,
    second_user_id: UserId,
    timeout_window: Duration,
) -> ProtocolFakePeerPairFuture<'a> {
    Box::pin(async move {
        let (mut first, mut second) =
            connect_two_fake_peers(server, room_id, first_user_id, second_user_id).await?;
        first.wait_until_connected(timeout_window).await?;
        second.wait_until_connected(timeout_window).await?;
        Some((first, second))
    })
}

pub async fn connect_fake_peer(
    server: &TestServer,
    room_id: &str,
    user_id: UserId,
    key: &str,
) -> Option<ProtocolFakePeer> {
    let token = signed_connect_claims(key, room_id, user_id.clone())?;
    let mut websocket = connect_websocket(server).await?;
    websocket
        .send(tungstenite::Message::Text(
            encode_client_batch(vec![ClientEnvelope::Message(ClientMessage::Auth(
                AuthPayload {
                    jwt: token,
                    channel: Some(room_id.to_owned()),
                },
            ))])?
            .into(),
        ))
        .await
        .ok()?;

    let welcome = decode_protocol_welcome_batch(&read_text_message(&mut websocket).await?)?;
    let mut rtc_peer = FakeRtcPeer::bind(0).await?;
    answer_next_server_request(&mut websocket, &mut rtc_peer).await?;

    Some(ProtocolFakePeer {
        user_id,
        websocket,
        welcome,
        rtc_peer,
    })
}

pub struct ProtocolFakePeer {
    user_id: UserId,
    websocket: TestWebSocket,
    welcome: WelcomePayload,
    rtc_peer: FakeRtcPeer,
}

impl ProtocolFakePeer {
    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    #[must_use]
    pub fn welcome(&self) -> &WelcomePayload {
        &self.welcome
    }

    pub async fn publish_track(&mut self, source: &FakeMediaSource) -> Option<()> {
        self.send_message(ClientMessage::Publish(StreamIntentPayload {
            stream_type: source.stream_type(),
        }))
        .await
    }

    pub async fn set_publication_active(
        &mut self,
        stream_type: StreamType,
        active: bool,
    ) -> Option<()> {
        let message = if active {
            ClientMessage::Publish(StreamIntentPayload { stream_type })
        } else {
            ClientMessage::Unpublish(StreamIntentPayload { stream_type })
        };
        self.send_message(message).await
    }

    pub async fn update_subscription(
        &mut self,
        target_user_id: UserId,
        states: DownloadStates,
    ) -> Option<()> {
        self.send_message(ClientMessage::Subscribe(SubscribePayload {
            user_id: target_user_id,
            states,
        }))
        .await
    }

    pub async fn send_info(&mut self, info: UserInfo) -> Option<()> {
        self.send_message(ClientMessage::Info(info)).await
    }

    pub async fn read_next_server_message(&mut self) -> Option<ServerMessage> {
        loop {
            let batch = read_protocol_batch(&mut self.websocket).await?;
            for envelope in batch {
                match ServerEnvelope::decode(envelope).ok()? {
                    ServerEnvelope::Message(message) => return Some(message),
                    ServerEnvelope::Request {
                        request_id,
                        request,
                    } => {
                        self.respond_to_server_request(request_id, request).await?;
                    }
                    ServerEnvelope::Response { .. } => {}
                }
            }
        }
    }

    pub async fn read_next_server_request(&mut self) -> Option<(RequestId, ServerRequest)> {
        loop {
            let batch = read_protocol_batch(&mut self.websocket).await?;
            for envelope in batch {
                match ServerEnvelope::decode(envelope).ok()? {
                    ServerEnvelope::Request {
                        request_id,
                        request,
                    } => return Some((request_id, request)),
                    ServerEnvelope::Message(_) | ServerEnvelope::Response { .. } => {}
                }
            }
        }
    }

    pub async fn read_server_message_with_timeout(
        &mut self,
        duration: Duration,
    ) -> Option<ServerMessage> {
        timeout(duration, self.read_next_server_message())
            .await
            .ok()?
    }

    pub async fn complete_next_negotiation(&mut self) -> Option<()> {
        let (request_id, request) = self.read_next_server_request().await?;
        self.respond_to_server_request(request_id, request).await
    }

    pub async fn close(mut self) -> Option<()> {
        self.websocket.close(None).await.ok()?;
        Some(())
    }

    pub async fn wait_until_connected(&mut self, timeout_window: Duration) -> Option<()> {
        self.rtc_peer.wait_until_connected(timeout_window).await
    }

    pub async fn send_rtp_packets(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
        frame_count: usize,
    ) -> Option<()> {
        self.rtc_peer
            .send_rtp_packets(source, clock, frame_count)
            .await
    }

    pub async fn send_rtp_packet(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
    ) -> Option<Vec<u8>> {
        self.rtc_peer.send_rtp_packet(source, clock).await
    }

    pub async fn read_rtp_packet(&mut self, timeout_window: Duration) -> Option<ReceivedRtpPacket> {
        self.rtc_peer.read_rtp_packet(timeout_window).await
    }

    pub async fn read_close_code(&mut self) -> Option<CloseCode> {
        read_close_code(&mut self.websocket).await
    }

    async fn send_message(&mut self, message: ClientMessage) -> Option<()> {
        self.websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(vec![ClientEnvelope::Message(message)])?.into(),
            ))
            .await
            .ok()?;
        Some(())
    }

    pub async fn respond_to_server_request(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
    ) -> Option<()> {
        let response = match request {
            ServerRequest::Offer(payload) => {
                ClientResponse::Offer(self.rtc_peer.answer_offer(&payload.sdp)?)
            }
            ServerRequest::Renegotiate(payload) => {
                ClientResponse::Renegotiate(self.rtc_peer.answer_offer(&payload.sdp)?)
            }
        };
        self.websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(vec![ClientEnvelope::Response {
                    response_to: request_id,
                    response,
                }])?
                .into(),
            ))
            .await
            .ok()?;
        Some(())
    }
}

async fn answer_next_server_request(
    websocket: &mut TestWebSocket,
    rtc_peer: &mut FakeRtcPeer,
) -> Option<()> {
    let batch = read_protocol_batch(websocket).await?;
    let envelope = batch.into_iter().next()?;
    let ServerEnvelope::Request {
        request_id,
        request,
    } = ServerEnvelope::decode(envelope).ok()?
    else {
        return None;
    };
    let response = match request {
        ServerRequest::Offer(payload) => {
            ClientResponse::Offer(rtc_peer.answer_offer(&payload.sdp)?)
        }
        ServerRequest::Renegotiate(payload) => {
            ClientResponse::Renegotiate(rtc_peer.answer_offer(&payload.sdp)?)
        }
    };
    websocket
        .send(tungstenite::Message::Text(
            encode_client_batch(vec![ClientEnvelope::Response {
                response_to: request_id,
                response,
            }])?
            .into(),
        ))
        .await
        .ok()?;
    Some(())
}

fn encode_client_batch(batch: Vec<ClientEnvelope>) -> Option<String> {
    let envelopes = batch
        .into_iter()
        .map(ClientEnvelope::into_envelope)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    serde_json::to_string(&envelopes).ok()
}

async fn read_protocol_batch(websocket: &mut TestWebSocket) -> Option<EnvelopeBatch> {
    serde_json::from_str(&read_text_message(websocket).await?).ok()
}
