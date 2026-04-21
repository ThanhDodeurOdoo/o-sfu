#![allow(
    dead_code,
    reason = "the protocol full-stack harness is shared by multiple RTC integration scenarios"
)]

use std::time::Duration;

use futures_util::SinkExt;
use o_sfu_protocol::{
    shared::{DownloadStates, SessionId, SessionInfo, StreamType},
    signaling::{
        AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, EnvelopeBatch, RequestId,
        ServerEnvelope, ServerMessage, ServerRequest, StreamIntentPayload, SubscribePayload,
        WelcomePayload,
    },
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{self, protocol::frame::coding::CloseCode};

use o_sfu::{
    config::Config,
    testing::{
        http::{METRICS_PATH, STATS_PATH, StatsResponse},
        server::{TestServer, decode_protocol_welcome_batch, spawn_test_server},
    },
};

use super::{
    TestWebSocket, connect_websocket, create_channel,
    fake_media::{FakeClock, FakeMediaSource},
    fake_rtc_peer::{FakeRtcPeer, ReceivedRtpPacket},
    read_close_code, read_text_message, signed_connect_claims,
};

pub struct ProtocolLocalNetwork {
    server: TestServer,
}

impl ProtocolLocalNetwork {
    pub async fn start(config: Config) -> Option<Self> {
        Some(Self {
            server: spawn_test_server(config).await.ok()?,
        })
    }

    pub async fn create_channel(&self, issuer: &str, key: Option<&str>) -> Option<String> {
        create_channel(&self.server, issuer, key).await
    }

    pub async fn stats(&self) -> Option<StatsResponse> {
        reqwest::Client::new()
            .get(format!("{}{STATS_PATH}", self.server.http_base_url()))
            .send()
            .await
            .ok()?
            .json::<StatsResponse>()
            .await
            .ok()
    }

    pub async fn metrics_text(&self) -> Option<String> {
        reqwest::Client::new()
            .get(format!("{}{METRICS_PATH}", self.server.http_base_url()))
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()
    }

    pub async fn connect_fake_peer(
        &self,
        channel_uuid: &str,
        session_id: SessionId,
        key: &str,
    ) -> Option<ProtocolFakePeer> {
        let token = signed_connect_claims(key, channel_uuid, session_id.clone())?;
        let mut websocket = connect_websocket(&self.server).await?;
        websocket
            .send(tungstenite::Message::Text(
                encode_client_batch(vec![ClientEnvelope::Message(ClientMessage::Auth(
                    AuthPayload {
                        jwt: token,
                        channel: Some(channel_uuid.to_owned()),
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
            session_id,
            websocket,
            welcome,
            rtc_peer,
        })
    }

    #[must_use]
    pub fn server(&self) -> &TestServer {
        &self.server
    }

    pub async fn wait_for_consumer_route_active(
        &self,
        channel_uuid: &str,
        consumer_session_id: &SessionId,
        producer_session_id: &SessionId,
        stream_type: StreamType,
    ) -> bool {
        self.server
            .wait_for_consumer_route_active(
                channel_uuid,
                consumer_session_id,
                producer_session_id,
                stream_type,
            )
            .await
    }

    pub async fn wait_for_consumer_route_inactive(
        &self,
        channel_uuid: &str,
        consumer_session_id: &SessionId,
        producer_session_id: &SessionId,
        stream_type: StreamType,
    ) -> bool {
        self.server
            .wait_for_consumer_route_inactive(
                channel_uuid,
                consumer_session_id,
                producer_session_id,
                stream_type,
            )
            .await
    }

    pub async fn wait_for_consumer_route_absence(
        &self,
        channel_uuid: &str,
        consumer_session_id: &SessionId,
        producer_session_id: &SessionId,
        stream_type: StreamType,
    ) -> bool {
        self.server
            .wait_for_consumer_route_absence(
                channel_uuid,
                consumer_session_id,
                producer_session_id,
                stream_type,
            )
            .await
    }

    pub async fn wait_for_channel_absence(&self, channel_uuid: &str) -> bool {
        self.server.wait_for_channel_absence(channel_uuid).await
    }
}

pub struct ProtocolFakePeer {
    session_id: SessionId,
    websocket: TestWebSocket,
    welcome: WelcomePayload,
    rtc_peer: FakeRtcPeer,
}

impl ProtocolFakePeer {
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
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
        target_session_id: SessionId,
        states: DownloadStates,
    ) -> Option<()> {
        self.send_message(ClientMessage::Subscribe(SubscribePayload {
            session_id: target_session_id,
            states,
        }))
        .await
    }

    pub async fn send_info(&mut self, info: SessionInfo) -> Option<()> {
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
