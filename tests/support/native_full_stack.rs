#![allow(
    dead_code,
    reason = "the native full-stack harness is shared by a subset of integration scenarios while the remaining legacy-wire media flows are migrated incrementally"
)]

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU16, Ordering},
    time::{Duration, Instant},
};

use futures_util::SinkExt;
use str0m::{
    Candidate, Rtc,
    change::SdpOffer,
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{self, protocol::frame::coding::CloseCode};

use o_sfu::{
    config::Config,
    runtime::testing::{TestServer, decode_native_welcome_batch, spawn_test_server},
    signaling::{
        http::{STATS_PATH, StatsResponse},
        protocol::{
            AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, EnvelopeBatch, RequestId,
            ServerEnvelope, ServerMessage, ServerRequest, SessionDescriptionPayload,
            StreamIntentPayload, SubscribePayload, WelcomePayload,
        },
        shared::{DownloadStates, SessionId, SessionInfo, StreamType},
    },
};

use super::{
    TestWebSocket, connect_websocket, create_channel, fake_media::FakeMediaSource,
    read_close_code, read_text_message, signed_connect_claims,
};

const RTC_NEGOTIATION_PORT_BASE: u16 = 57_000;
static NEXT_RTC_NEGOTIATION_PORT: AtomicU16 = AtomicU16::new(RTC_NEGOTIATION_PORT_BASE);

pub struct NativeLocalNetwork {
    server: TestServer,
}

impl NativeLocalNetwork {
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

    pub async fn connect_fake_peer(
        &self,
        channel_uuid: &str,
        session_id: SessionId,
        key: &str,
    ) -> Option<NativeFakePeer> {
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

        let welcome = decode_native_welcome_batch(&read_text_message(&mut websocket).await?)?;
        let mut rtc_peer = NativeNegotiationRtcPeer::new(next_negotiation_port())?;
        answer_next_server_request(&mut websocket, &mut rtc_peer).await?;

        Some(NativeFakePeer {
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
}

pub struct NativeFakePeer {
    session_id: SessionId,
    websocket: TestWebSocket,
    welcome: WelcomePayload,
    rtc_peer: NativeNegotiationRtcPeer,
}

impl NativeFakePeer {
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
            let batch = read_native_batch(&mut self.websocket).await?;
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

    pub async fn read_server_message_with_timeout(
        &mut self,
        duration: Duration,
    ) -> Option<ServerMessage> {
        timeout(duration, self.read_next_server_message()).await.ok()?
    }

    pub async fn complete_next_negotiation(&mut self) -> Option<()> {
        loop {
            let batch = read_native_batch(&mut self.websocket).await?;
            for envelope in batch {
                match ServerEnvelope::decode(envelope).ok()? {
                    ServerEnvelope::Request {
                        request_id,
                        request,
                    } => return self.respond_to_server_request(request_id, request).await,
                    ServerEnvelope::Message(_) | ServerEnvelope::Response { .. } => {}
                }
            }
        }
    }

    pub async fn close(mut self) -> Option<()> {
        self.websocket.close(None).await.ok()?;
        Some(())
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

    async fn respond_to_server_request(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
    ) -> Option<()> {
        let response = match request {
            ServerRequest::Offer(payload) => ClientResponse::Offer(
                self.rtc_peer.answer_offer(&payload.sdp)?,
            ),
            ServerRequest::Renegotiate(payload) => ClientResponse::Renegotiate(
                self.rtc_peer.answer_offer(&payload.sdp)?,
            ),
            ServerRequest::Ping => ClientResponse::Ping,
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

struct NativeNegotiationRtcPeer {
    rtc: Rtc,
}

impl NativeNegotiationRtcPeer {
    fn new(port: u16) -> Option<Self> {
        let mut rtc = Rtc::new(Instant::now());
        rtc.add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp").ok()?,
        )?;
        Some(Self { rtc })
    }

    fn answer_offer(
        &mut self,
        offer_sdp: &str,
    ) -> Option<SessionDescriptionPayload> {
        let answer = self
            .rtc
            .sdp_api()
            .accept_offer(SdpOffer::from_sdp_string(offer_sdp).ok()?)
            .ok()?;
        Some(SessionDescriptionPayload {
            sdp: answer.to_sdp_string(),
        })
    }
}

async fn answer_next_server_request(
    websocket: &mut TestWebSocket,
    rtc_peer: &mut NativeNegotiationRtcPeer,
) -> Option<()> {
    let batch = read_native_batch(websocket).await?;
    let envelope = batch.into_iter().next()?;
    let ServerEnvelope::Request {
        request_id,
        request,
    } = ServerEnvelope::decode(envelope).ok()?
    else {
        return None;
    };
    let response = match request {
        ServerRequest::Offer(payload) => ClientResponse::Offer(rtc_peer.answer_offer(&payload.sdp)?),
        ServerRequest::Renegotiate(payload) => {
            ClientResponse::Renegotiate(rtc_peer.answer_offer(&payload.sdp)?)
        }
        ServerRequest::Ping => ClientResponse::Ping,
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

fn next_negotiation_port() -> u16 {
    NEXT_RTC_NEGOTIATION_PORT.fetch_add(1, Ordering::Relaxed)
}

fn encode_client_batch(batch: Vec<ClientEnvelope>) -> Option<String> {
    let envelopes = batch
        .into_iter()
        .map(ClientEnvelope::into_envelope)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    serde_json::to_string(&envelopes).ok()
}

async fn read_native_batch(websocket: &mut TestWebSocket) -> Option<EnvelopeBatch> {
    serde_json::from_str(&read_text_message(websocket).await?).ok()
}
