#![allow(
    dead_code,
    reason = "the full-stack harness surface is shared by multiple integration scenarios and lands incrementally before every scenario is added"
)]

use futures_util::SinkExt;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use o_sfu::{
    config::Config,
    runtime::testing::{TestServer, spawn_test_server},
    signaling::{
        current_bus::{CurrentBusBatch, CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
        current_protocol::{
            CurrentClientMessage, CurrentClientRequest, CurrentDownloadStateChangePayload,
            CurrentPublishTrackResponse, CurrentServerMessage, CurrentServerRequest,
            CurrentTransportBootstrapPayload, CurrentTransportConnectPayload,
            CurrentUploadStateChangePayload, CurrentWebSocketCredentials,
        },
        http::{STATS_PATH, StatsResponse},
        protocol::WelcomePayload,
        shared::{DownloadStates, SessionId, StreamType},
        webrtc::{DtlsFingerprint, DtlsParameters, IceParameters},
    },
};
use tokio_tungstenite::tungstenite::{self, protocol::frame::coding::CloseCode};

use super::{
    FakeWebSocketClient, create_channel, fake_media::FakeMediaSource,
    supported_client_rtp_capabilities,
};

fn client_dtls_parameters() -> DtlsParameters {
    DtlsParameters {
        role: String::from("client"),
        fingerprints: vec![DtlsFingerprint {
            algorithm: String::from("sha-256"),
            value: String::from(
                "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
            ),
        }],
    }
}

pub struct LocalNetwork {
    server: TestServer,
}

impl LocalNetwork {
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
    ) -> Option<FakePeer> {
        let token = super::signed_connect_claims(key, channel_uuid, session_id.clone())?;
        let mut client = FakeWebSocketClient::authenticate_with_credentials(
            &self.server,
            &CurrentWebSocketCredentials {
                channel_uuid: Some(channel_uuid.to_owned()),
                jwt: token,
            },
        )
        .await?;
        let welcome = client.read_welcome().await?;
        let (request_id, request) = client.read_server_request().await?;
        let CurrentServerRequest::BootstrapTransports(transport_bootstrap) = request else {
            return None;
        };
        let request_id = request_id?;
        client
            .respond_to_server_request(&request_id, supported_client_rtp_capabilities())
            .await?;

        Some(FakePeer {
            session_id,
            client,
            welcome,
            transport_bootstrap,
            next_request_counter: 1,
        })
    }
}

pub struct FakePeer {
    session_id: SessionId,
    client: FakeWebSocketClient,
    welcome: WelcomePayload,
    transport_bootstrap: CurrentTransportBootstrapPayload,
    next_request_counter: u64,
}

impl FakePeer {
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    #[must_use]
    pub fn welcome(&self) -> &WelcomePayload {
        &self.welcome
    }

    #[must_use]
    pub fn transport_bootstrap(&self) -> &CurrentTransportBootstrapPayload {
        &self.transport_bootstrap
    }

    pub async fn connect_transports(&mut self) -> Option<()> {
        self.connect_transports_with_dtls(&client_dtls_parameters())
            .await
    }

    pub async fn connect_transports_with_dtls(
        &mut self,
        dtls_parameters: &DtlsParameters,
    ) -> Option<()> {
        self.connect_transports_with_ice(dtls_parameters, None)
            .await
    }

    pub async fn connect_transports_with_ice(
        &mut self,
        dtls_parameters: &DtlsParameters,
        ice_parameters: Option<IceParameters>,
    ) -> Option<()> {
        let payload = CurrentTransportConnectPayload {
            dtls_parameters: dtls_parameters.clone(),
            ice_parameters,
            sdp_offer: None,
        };
        let upload_response: Value = self
            .send_request(CurrentClientRequest::ConnectUploadTransport(
                payload.clone(),
            ))
            .await?;
        if !matches!(&upload_response, Value::Object(object) if object.is_empty()) {
            return None;
        }
        let download_response: Value = self
            .send_request(CurrentClientRequest::ConnectDownloadTransport(payload))
            .await?;
        if !matches!(&download_response, Value::Object(object) if object.is_empty()) {
            return None;
        }
        Some(())
    }

    pub async fn publish_track(&mut self, source: &FakeMediaSource) -> Option<String> {
        let response: CurrentPublishTrackResponse = self
            .send_request(CurrentClientRequest::PublishTrack(source.publish_payload()))
            .await?;
        Some(response.id)
    }

    pub async fn set_upload_active(&mut self, stream_type: StreamType, active: bool) -> Option<()> {
        self.send_message(CurrentClientMessage::UpdateUploadState(
            CurrentUploadStateChangePayload {
                stream_type,
                active,
            },
        ))
        .await
    }

    pub async fn unpublish_upload(&mut self, stream_type: StreamType) -> Option<()> {
        self.set_upload_active(stream_type, false).await
    }

    pub async fn set_download_state(
        &mut self,
        target_session_id: SessionId,
        states: DownloadStates,
    ) -> Option<()> {
        self.send_message(CurrentClientMessage::UpdateDownloadState(
            CurrentDownloadStateChangePayload {
                session_id: target_session_id,
                states,
            },
        ))
        .await
    }

    pub async fn read_next_server_message(&mut self) -> Option<CurrentServerMessage> {
        self.client.read_server_message().await
    }

    pub async fn read_next_bus_batch(&mut self) -> Option<CurrentBusBatch> {
        self.client.read_bus_batch().await
    }

    pub async fn respond_to_server_request(
        &mut self,
        request_id: &CurrentBusRequestId,
        response: Value,
    ) -> Option<()> {
        self.client
            .respond_to_server_request(request_id, response)
            .await
    }

    pub async fn close(self) -> Option<()> {
        self.client.close().await
    }

    pub async fn read_close_code(&mut self) -> Option<CloseCode> {
        self.client.read_close_code().await
    }

    pub async fn read_next_server_request(&mut self) -> Option<CurrentServerRequest> {
        loop {
            let (request_id, request) = self.client.read_server_request().await?;
            match request {
                CurrentServerRequest::Ping => {
                    let request_id = request_id?;
                    self.client
                        .respond_to_server_request(&request_id, json!({}))
                        .await?;
                }
                request => return Some(request),
            }
        }
    }

    async fn send_request<T>(&mut self, request: CurrentClientRequest) -> Option<T>
    where
        T: DeserializeOwned,
    {
        let request_id =
            CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, self.next_request_counter);
        self.next_request_counter = self.next_request_counter.saturating_add(1);

        let payload = serde_json::to_string(&vec![CurrentBusEnvelope {
            message: serde_json::to_value(request).ok()?,
            need_response: Some(request_id.clone()),
            response_to: None,
        }])
        .ok()?;
        self.client
            .websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await
            .ok()?;

        loop {
            let batch = self.client.read_bus_batch().await?;
            if let Some(value) = extract_matching_response(&batch, &request_id) {
                return serde_json::from_value(value).ok();
            }
            self.handle_unsolicited_requests(batch).await?;
        }
    }

    async fn send_message(&mut self, message: CurrentClientMessage) -> Option<()> {
        self.client.send_bus_message(message).await
    }

    async fn handle_unsolicited_requests(&mut self, batch: CurrentBusBatch) -> Option<()> {
        for envelope in batch {
            let Some(server_request_id) = envelope.need_response else {
                continue;
            };
            let request: CurrentServerRequest = serde_json::from_value(envelope.message).ok()?;
            if request != CurrentServerRequest::Ping {
                return None;
            }
            self.client
                .respond_to_server_request(&server_request_id, json!({}))
                .await?;
        }
        Some(())
    }
}

fn extract_matching_response(
    batch: &CurrentBusBatch,
    request_id: &CurrentBusRequestId,
) -> Option<Value> {
    batch
        .iter()
        .find(|envelope| envelope.response_to.as_ref() == Some(request_id))
        .map(|envelope| envelope.message.clone())
}
