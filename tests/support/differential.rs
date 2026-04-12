use std::{
    collections::{BTreeMap, VecDeque},
    env,
    future::Future,
    net::TcpListener,
    path::{Path, PathBuf},
    pin::Pin,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use o_sfu::signaling::{
    current_bus::{CurrentBusBatch, CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    current_protocol::{
        CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackResponse,
        CurrentRemoteTrackBootstrapPayload, CurrentServerMessage, CurrentServerRequest,
        CurrentSessionInfoSnapshotById, CurrentTransportConnectPayload,
        CurrentUploadStateChangePayload, CurrentWebSocketCredentials,
    },
    http::{CHANNEL_PATH, ChannelResponse, CreateChannelQuery, NOOP_PATH},
    shared::{SessionId, StreamType},
    webrtc::{DtlsFingerprint, DtlsParameters, MediaKind},
};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite};

use super::{
    TEST_AUTH_KEY, TEST_CHANNEL_KEY, TestWebSocket,
    fake_media::FakeMediaSource,
    full_stack::{FakePeer, LocalNetwork},
    read_bus_batch, read_close_code, respond_to_server_request, signed_channel_claims,
    signed_connect_claims, supported_client_rtp_capabilities,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
const LEGACY_RTC_MIN_PORT: u16 = 52_000;
const LEGACY_RTC_MAX_PORT: u16 = 52_099;
const LEGACY_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const LEGACY_STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const SCENARIO_EVENT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityTranscript {
    pub backend_name: &'static str,
    pub scenario_name: &'static str,
    pub events: Vec<CompatibilityEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityEvent {
    SessionClosed {
        session_id: SessionId,
        close_code: u16,
    },
    RemoteTrackBootstrap {
        observer_session_id: SessionId,
        owner_session_id: SessionId,
        source_token: String,
        stream_type: StreamType,
        media_kind: MediaKind,
        active: bool,
    },
    SessionCameraState {
        observer_session_id: SessionId,
        owner_session_id: SessionId,
        active: bool,
    },
    SessionDeparted {
        observer_session_id: SessionId,
        departed_session_id: SessionId,
    },
}

pub trait ScenarioBackend {
    type Peer: ScenarioPeer;

    fn backend_name(&self) -> &'static str;

    fn create_channel<'a>(
        &'a self,
        issuer: &'a str,
        key: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, String>>;

    fn connect_peer<'a>(
        &'a self,
        channel_uuid: &'a str,
        session_id: SessionId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Self::Peer, String>>;
}

pub trait ScenarioPeer: Sized {
    fn rtc_feature_enabled(&self) -> bool;

    fn connect_transports(&mut self) -> BoxFuture<'_, Option<()>>;

    fn publish_track<'a>(
        &'a mut self,
        source: &'a FakeMediaSource,
    ) -> BoxFuture<'a, Option<String>>;

    fn set_upload_active(
        &mut self,
        stream_type: StreamType,
        active: bool,
    ) -> BoxFuture<'_, Option<()>>;

    fn read_next_bus_batch(&mut self) -> BoxFuture<'_, Option<CurrentBusBatch>>;

    fn respond_to_server_request(
        &mut self,
        request_id: CurrentBusRequestId,
        response: Value,
    ) -> BoxFuture<'_, Option<()>>;

    fn read_close_code(&mut self) -> BoxFuture<'_, Option<u16>>;

    fn close(self) -> Pin<Box<dyn Future<Output = Option<()>> + Send>>;
}

impl ScenarioBackend for LocalNetwork {
    type Peer = FakePeer;

    fn backend_name(&self) -> &'static str {
        "o-sfu"
    }

    fn create_channel<'a>(
        &'a self,
        issuer: &'a str,
        key: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            self.create_channel(issuer, key)
                .await
                .ok_or_else(|| String::from("local create_channel helper returned None"))
        })
    }

    fn connect_peer<'a>(
        &'a self,
        channel_uuid: &'a str,
        session_id: SessionId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Self::Peer, String>> {
        Box::pin(async move {
            let session_label = session_id_value(&session_id);
            self.connect_fake_peer(channel_uuid, session_id, key)
                .await
                .ok_or_else(|| {
                    format!("local fake peer connection failed for session {session_label}")
                })
        })
    }
}

impl ScenarioPeer for FakePeer {
    fn rtc_feature_enabled(&self) -> bool {
        self.welcome().features.rtc
    }

    fn connect_transports(&mut self) -> BoxFuture<'_, Option<()>> {
        Box::pin(async move { Self::connect_transports(self).await })
    }

    fn publish_track<'a>(
        &'a mut self,
        source: &'a FakeMediaSource,
    ) -> BoxFuture<'a, Option<String>> {
        Box::pin(async move { Self::publish_track(self, source).await })
    }

    fn set_upload_active(
        &mut self,
        stream_type: StreamType,
        active: bool,
    ) -> BoxFuture<'_, Option<()>> {
        Box::pin(async move { Self::set_upload_active(self, stream_type, active).await })
    }

    fn read_next_bus_batch(&mut self) -> BoxFuture<'_, Option<CurrentBusBatch>> {
        Box::pin(async move { Self::read_next_bus_batch(self).await })
    }

    fn respond_to_server_request(
        &mut self,
        request_id: CurrentBusRequestId,
        response: Value,
    ) -> BoxFuture<'_, Option<()>> {
        Box::pin(async move { Self::respond_to_server_request(self, &request_id, response).await })
    }

    fn read_close_code(&mut self) -> BoxFuture<'_, Option<u16>> {
        Box::pin(async move { Self::read_close_code(self).await.map(u16::from) })
    }

    fn close(self) -> Pin<Box<dyn Future<Output = Option<()>> + Send>> {
        Box::pin(async move { Self::close(self).await })
    }
}

#[derive(Debug)]
pub struct LegacySfuBackend {
    server: LegacySfuServer,
}

#[derive(Debug)]
struct LegacySfuServer {
    http_base_url: String,
    ws_url: String,
    child: Child,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyStartupPayload {
    #[serde(rename = "availableFeatures")]
    available_features: LegacyAvailableFeatures,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyAvailableFeatures {
    rtc: bool,
}

#[derive(Debug)]
pub struct LegacyFakePeer {
    websocket: TestWebSocket,
    startup: LegacyStartupPayload,
    next_request_counter: u64,
    pending_batches: VecDeque<CurrentBusBatch>,
}

impl LegacySfuBackend {
    pub async fn start() -> Result<Self, String> {
        Ok(Self {
            server: LegacySfuServer::start().await?,
        })
    }

    fn http_base_url(&self) -> &str {
        &self.server.http_base_url
    }

    fn ws_url(&self) -> &str {
        &self.server.ws_url
    }
}

impl Drop for LegacySfuServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl LegacySfuServer {
    async fn start() -> Result<Self, String> {
        let http_port = reserve_unused_port()
            .ok_or_else(|| String::from("failed to reserve legacy SFU port"))?;
        let workdir = legacy_sfu_workdir()?;
        let child = Command::new("node")
            .args(["--experimental-transform-types", "./src/server.ts"])
            .current_dir(&workdir)
            .env("AUTH_KEY", TEST_AUTH_KEY)
            .env("PUBLIC_IP", "127.0.0.1")
            .env("HTTP_INTERFACE", "127.0.0.1")
            .env("PORT", http_port.to_string())
            .env("NUM_WORKERS", "1")
            .env("RTC_MIN_PORT", LEGACY_RTC_MIN_PORT.to_string())
            .env("RTC_MAX_PORT", LEGACY_RTC_MAX_PORT.to_string())
            .env("LOG_LEVEL", "none")
            .env("WORKER_LOG_LEVEL", "none")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| {
                format!(
                    "failed to start legacy SFU from {}: {error}",
                    workdir.display()
                )
            })?;
        let mut server = Self {
            http_base_url: format!("http://127.0.0.1:{http_port}"),
            ws_url: format!("ws://127.0.0.1:{http_port}"),
            child,
        };
        server.wait_until_ready().await?;
        Ok(server)
    }

    async fn wait_until_ready(&mut self) -> Result<(), String> {
        let client = reqwest::Client::new();
        let started_at = Instant::now();
        loop {
            if started_at.elapsed() > LEGACY_STARTUP_TIMEOUT {
                return Err(format!(
                    "legacy SFU did not become ready within {LEGACY_STARTUP_TIMEOUT:?}"
                ));
            }
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|error| format!("failed to query legacy SFU process status: {error}"))?
            {
                return Err(format!(
                    "legacy SFU exited before readiness check with status {status}"
                ));
            }
            match client
                .get(format!("{}{NOOP_PATH}", self.http_base_url))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(_) | Err(_) => {}
            }
            sleep(LEGACY_STARTUP_POLL_INTERVAL).await;
        }
    }
}

impl ScenarioBackend for LegacySfuBackend {
    type Peer = LegacyFakePeer;

    fn backend_name(&self) -> &'static str {
        "sfu"
    }

    fn create_channel<'a>(
        &'a self,
        issuer: &'a str,
        key: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, String>> {
        Box::pin(async move { legacy_create_channel(self.http_base_url(), issuer, key).await })
    }

    fn connect_peer<'a>(
        &'a self,
        channel_uuid: &'a str,
        session_id: SessionId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Self::Peer, String>> {
        Box::pin(async move {
            LegacyFakePeer::connect(self.ws_url(), channel_uuid, session_id, key).await
        })
    }
}

impl LegacyFakePeer {
    async fn connect(
        ws_url: &str,
        channel_uuid: &str,
        session_id: SessionId,
        key: &str,
    ) -> Result<Self, String> {
        let session_label = session_id_value(&session_id);
        let token = signed_connect_claims(key, channel_uuid, session_id).ok_or_else(|| {
            format!("failed to sign legacy connect claims for session {session_label}")
        })?;
        let (mut websocket, _response) = connect_async(ws_url).await.map_err(|error| {
            format!("failed to open legacy websocket for session {session_label}: {error}")
        })?;
        let credentials = serde_json::to_string(&CurrentWebSocketCredentials {
            channel_uuid: Some(channel_uuid.to_owned()),
            jwt: token,
        })
        .map_err(|error| {
            format!(
                "failed to serialize legacy websocket credentials for session {session_label}: {error}"
            )
        })?;
        websocket
            .send(tungstenite::Message::Text(credentials.into()))
            .await
            .map_err(|error| {
                format!(
                    "failed to send legacy websocket credentials for session {session_label}: {error}"
                )
            })?;
        let startup = serde_json::from_str::<LegacyStartupPayload>(
            &read_text_message_with_context(
                &mut websocket,
                &format!("legacy startup payload for session {session_label}"),
            )
            .await?,
        )
        .map_err(|error| {
            format!("failed to decode legacy startup payload for session {session_label}: {error}")
        })?;
        let batch = read_bus_batch_with_context(
            &mut websocket,
            &format!("legacy transport bootstrap for session {session_label}"),
        )
        .await?;
        let envelope = batch.first().ok_or_else(|| {
            format!("empty legacy transport bootstrap batch for session {session_label}")
        })?;
        let request_name = envelope
            .message
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!(
                    "legacy transport bootstrap for session {session_label} is missing a request name"
                )
            })?;
        if request_name != "INIT_TRANSPORTS" {
            return Err(format!(
                "expected legacy transport bootstrap for session {session_label}, received {request_name}"
            ));
        }
        let request_id = envelope.need_response.clone().ok_or_else(|| {
            format!("legacy transport bootstrap missing request id for session {session_label}")
        })?;
        respond_to_server_request(
            &mut websocket,
            &request_id,
            supported_client_rtp_capabilities(),
        )
        .await
        .ok_or_else(|| {
            format!("failed to acknowledge legacy transport bootstrap for session {session_label}")
        })?;
        Ok(Self {
            websocket,
            startup,
            next_request_counter: 1,
            pending_batches: VecDeque::new(),
        })
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
        self.websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await
            .ok()?;
        loop {
            let batch = read_bus_batch(&mut self.websocket).await?;
            if let Some(value) = extract_matching_response(&batch, &request_id) {
                self.queue_unsolicited_envelopes(batch, Some(&request_id))
                    .await?;
                return serde_json::from_value(value).ok();
            }
            self.queue_unsolicited_envelopes(batch, Some(&request_id))
                .await?;
        }
    }

    async fn send_message(&mut self, message: CurrentClientMessage) -> Option<()> {
        let payload = serde_json::to_string(&vec![CurrentBusEnvelope {
            message: serde_json::to_value(message).ok()?,
            need_response: None,
            response_to: None,
        }])
        .ok()?;
        self.websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await
            .ok()?;
        Some(())
    }

    async fn queue_unsolicited_envelopes(
        &mut self,
        batch: CurrentBusBatch,
        ignored_response_to: Option<&CurrentBusRequestId>,
    ) -> Option<()> {
        let mut queued_batch = CurrentBusBatch::new();
        for envelope in batch {
            if ignored_response_to
                .is_some_and(|request_id| envelope.response_to.as_ref() == Some(request_id))
            {
                continue;
            }
            let Some(server_request_id) = envelope.need_response.clone() else {
                queued_batch.push(envelope);
                continue;
            };
            let request: CurrentServerRequest =
                serde_json::from_value(envelope.message.clone()).ok()?;
            if request != CurrentServerRequest::Ping {
                queued_batch.push(envelope);
                continue;
            }
            respond_to_server_request(&mut self.websocket, &server_request_id, json!({})).await?;
        }
        if !queued_batch.is_empty() {
            self.pending_batches.push_back(queued_batch);
        }
        Some(())
    }

    async fn read_next_batch(&mut self) -> Option<CurrentBusBatch> {
        if let Some(batch) = self.pending_batches.pop_front() {
            return Some(batch);
        }
        read_bus_batch(&mut self.websocket).await
    }

    async fn read_next_non_ping_batch(&mut self) -> Option<CurrentBusBatch> {
        loop {
            let batch = self.read_next_batch().await?;
            self.queue_unsolicited_envelopes(batch.clone(), None)
                .await?;
            if let Some(non_ping_batch) = self.pending_batches.pop_back() {
                return Some(non_ping_batch);
            }
        }
    }
}

impl ScenarioPeer for LegacyFakePeer {
    fn rtc_feature_enabled(&self) -> bool {
        self.startup.available_features.rtc
    }

    fn connect_transports(&mut self) -> BoxFuture<'_, Option<()>> {
        Box::pin(async move {
            let payload = CurrentTransportConnectPayload {
                dtls_parameters: client_dtls_parameters(),
                ice_parameters: None,
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
            matches!(&download_response, Value::Object(object) if object.is_empty()).then_some(())
        })
    }

    fn publish_track<'a>(
        &'a mut self,
        source: &'a FakeMediaSource,
    ) -> BoxFuture<'a, Option<String>> {
        Box::pin(async move {
            let response: CurrentPublishTrackResponse = self
                .send_request(CurrentClientRequest::PublishTrack(source.publish_payload()))
                .await?;
            Some(response.id)
        })
    }

    fn set_upload_active(
        &mut self,
        stream_type: StreamType,
        active: bool,
    ) -> BoxFuture<'_, Option<()>> {
        Box::pin(async move {
            self.send_message(CurrentClientMessage::UpdateUploadState(
                CurrentUploadStateChangePayload {
                    stream_type,
                    active,
                },
            ))
            .await
        })
    }

    fn read_next_bus_batch(&mut self) -> BoxFuture<'_, Option<CurrentBusBatch>> {
        Box::pin(async move { self.read_next_non_ping_batch().await })
    }

    fn respond_to_server_request(
        &mut self,
        request_id: CurrentBusRequestId,
        response: Value,
    ) -> BoxFuture<'_, Option<()>> {
        Box::pin(async move {
            respond_to_server_request(&mut self.websocket, &request_id, response).await
        })
    }

    fn read_close_code(&mut self) -> BoxFuture<'_, Option<u16>> {
        Box::pin(async move { read_close_code(&mut self.websocket).await.map(u16::from) })
    }

    fn close(mut self) -> Pin<Box<dyn Future<Output = Option<()>> + Send>> {
        Box::pin(async move {
            self.websocket.close(None).await.ok()?;
            Some(())
        })
    }
}

pub async fn run_camera_publish_oracle_scenario<B>(backend: &B) -> Option<CompatibilityTranscript>
where
    B: ScenarioBackend,
{
    run_camera_publish_oracle_scenario_result(backend)
        .await
        .ok()
}

pub async fn run_camera_publish_oracle_scenario_result<B>(
    backend: &B,
) -> Result<CompatibilityTranscript, String>
where
    B: ScenarioBackend,
{
    let channel_uuid = backend
        .create_channel("issuer-differential-camera", Some(TEST_CHANNEL_KEY))
        .await
        .map_err(|error| format!("channel creation failed: {error}"))?;
    let (mut publisher, mut subscriber) =
        connect_initial_camera_flow_peers(backend, &channel_uuid).await?;

    let mut track_tokens = BTreeMap::<String, String>::new();
    let mut next_track_index = 0_u64;
    let mut transcript = CompatibilityTranscript {
        backend_name: backend.backend_name(),
        scenario_name: "camera_publish_toggle_late_join_departure",
        events: Vec::new(),
    };

    let published_track_token = publish_camera_and_record_initial_events(
        &mut publisher,
        &mut subscriber,
        &mut transcript,
        &mut track_tokens,
        &mut next_track_index,
    )
    .await
    .map_err(|error| format!("initial camera publish flow failed: {error}"))?;
    let mut late_subscriber = connect_late_camera_subscriber(backend, &channel_uuid).await?;
    record_late_join_track_event(
        &mut late_subscriber,
        &mut transcript,
        &mut track_tokens,
        &mut next_track_index,
        &published_track_token,
    )
    .await
    .map_err(|error| format!("late join track bootstrap failed: {error}"))?;

    publisher
        .close()
        .await
        .ok_or_else(|| String::from("publisher close failed"))?;
    record_departures(&mut subscriber, &mut late_subscriber, &mut transcript)
        .await
        .map_err(|error| format!("departure capture failed: {error}"))?;

    Ok(transcript)
}

pub async fn run_session_replacement_oracle_scenario_result<B>(
    backend: &B,
) -> Result<CompatibilityTranscript, String>
where
    B: ScenarioBackend,
{
    let channel_uuid = backend
        .create_channel("issuer-differential-replacement", Some(TEST_CHANNEL_KEY))
        .await
        .map_err(|error| format!("channel creation failed: {error}"))?;
    let (mut initial_publisher, mut subscriber) =
        connect_initial_replacement_flow_peers(backend, &channel_uuid).await?;

    let mut transcript = CompatibilityTranscript {
        backend_name: backend.backend_name(),
        scenario_name: "session_replacement_republish",
        events: Vec::new(),
    };

    let mut replacement = backend
        .connect_peer(&channel_uuid, SessionId::Integer(40), TEST_CHANNEL_KEY)
        .await
        .map_err(|error| format!("replacement peer connect failed: {error}"))?;
    ensure_rtc_startup(&replacement)
        .ok_or_else(|| String::from("replacement startup missing rtc feature"))?;

    let close_code = timeout(SCENARIO_EVENT_TIMEOUT, initial_publisher.read_close_code())
        .await
        .map_err(|_elapsed| String::from("timed out waiting for replaced publisher close"))?
        .ok_or_else(|| String::from("replaced publisher did not receive a close code"))?;
    record_close_event(&mut transcript, SessionId::Integer(40), close_code);

    record_departure_event(
        &mut transcript,
        SessionId::Integer(50),
        timeout(SCENARIO_EVENT_TIMEOUT, expect_departure(&mut subscriber))
            .await
            .map_err(|_elapsed| {
                String::from("timed out waiting for subscriber departure after replacement")
            })?
            .ok_or_else(|| {
                String::from("subscriber did not observe departure after replacement")
            })?,
    );

    timeout(SCENARIO_EVENT_TIMEOUT, replacement.connect_transports())
        .await
        .map_err(|_elapsed| String::from("timed out waiting for replacement transport connect"))?
        .ok_or_else(|| String::from("replacement transport connect failed"))?;
    let producer_id = replacement
        .publish_track(&FakeMediaSource::audio())
        .await
        .ok_or_else(|| String::from("replacement publish_track returned None"))?;
    let track = timeout(SCENARIO_EVENT_TIMEOUT, expect_remote_track(&mut subscriber))
        .await
        .map_err(|_elapsed| {
            String::from("timed out waiting for subscriber track bootstrap after replacement")
        })?
        .ok_or_else(|| {
            String::from("subscriber did not receive track bootstrap after replacement")
        })?;
    if track.source_id != producer_id {
        return Err(format!(
            "subscriber observed producer {} instead of replacement producer {}",
            track.source_id, producer_id
        ));
    }
    record_track_event(&mut transcript, SessionId::Integer(50), "track-0", &track);

    Ok(transcript)
}

async fn connect_initial_camera_flow_peers<B>(
    backend: &B,
    channel_uuid: &str,
) -> Result<(B::Peer, B::Peer), String>
where
    B: ScenarioBackend,
{
    let mut publisher = backend
        .connect_peer(channel_uuid, SessionId::Integer(10), TEST_CHANNEL_KEY)
        .await
        .map_err(|error| format!("publisher peer connect failed: {error}"))?;
    let mut subscriber = backend
        .connect_peer(channel_uuid, SessionId::Integer(20), TEST_CHANNEL_KEY)
        .await
        .map_err(|error| format!("subscriber peer connect failed: {error}"))?;
    ensure_rtc_startup(&publisher)
        .ok_or_else(|| String::from("publisher startup missing rtc feature"))?;
    ensure_rtc_startup(&subscriber)
        .ok_or_else(|| String::from("subscriber startup missing rtc feature"))?;
    timeout(SCENARIO_EVENT_TIMEOUT, publisher.connect_transports())
        .await
        .map_err(|_elapsed| String::from("timed out waiting for publisher transport connect"))?
        .ok_or_else(|| String::from("publisher transport connect failed"))?;
    timeout(SCENARIO_EVENT_TIMEOUT, subscriber.connect_transports())
        .await
        .map_err(|_elapsed| String::from("timed out waiting for subscriber transport connect"))?
        .ok_or_else(|| String::from("subscriber transport connect failed"))?;
    Ok((publisher, subscriber))
}

async fn connect_initial_replacement_flow_peers<B>(
    backend: &B,
    channel_uuid: &str,
) -> Result<(B::Peer, B::Peer), String>
where
    B: ScenarioBackend,
{
    let mut initial_publisher = backend
        .connect_peer(channel_uuid, SessionId::Integer(40), TEST_CHANNEL_KEY)
        .await
        .map_err(|error| format!("initial publisher peer connect failed: {error}"))?;
    let mut subscriber = backend
        .connect_peer(channel_uuid, SessionId::Integer(50), TEST_CHANNEL_KEY)
        .await
        .map_err(|error| format!("subscriber peer connect failed: {error}"))?;
    ensure_rtc_startup(&initial_publisher)
        .ok_or_else(|| String::from("initial publisher startup missing rtc feature"))?;
    ensure_rtc_startup(&subscriber)
        .ok_or_else(|| String::from("subscriber startup missing rtc feature"))?;
    timeout(
        SCENARIO_EVENT_TIMEOUT,
        initial_publisher.connect_transports(),
    )
    .await
    .map_err(|_elapsed| String::from("timed out waiting for initial publisher transport connect"))?
    .ok_or_else(|| String::from("initial publisher transport connect failed"))?;
    timeout(SCENARIO_EVENT_TIMEOUT, subscriber.connect_transports())
        .await
        .map_err(|_elapsed| String::from("timed out waiting for subscriber transport connect"))?
        .ok_or_else(|| String::from("subscriber transport connect failed"))?;
    Ok((initial_publisher, subscriber))
}

async fn publish_camera_and_record_initial_events<P>(
    publisher: &mut P,
    subscriber: &mut P,
    transcript: &mut CompatibilityTranscript,
    track_tokens: &mut BTreeMap<String, String>,
    next_track_index: &mut u64,
) -> Result<String, String>
where
    P: ScenarioPeer,
{
    let producer_id = publisher
        .publish_track(&FakeMediaSource::camera())
        .await
        .ok_or_else(|| String::from("publisher publish_track returned None"))?;
    let subscriber_track = timeout(SCENARIO_EVENT_TIMEOUT, expect_remote_track(subscriber))
        .await
        .map_err(|_elapsed| {
            String::from("timed out waiting for subscriber camera track bootstrap")
        })?
        .ok_or_else(|| String::from("subscriber did not receive camera track bootstrap"))?;
    let published_track_token = normalize_track_token(track_tokens, next_track_index, &producer_id);
    record_track_event(
        transcript,
        SessionId::Integer(20),
        &published_track_token,
        &subscriber_track,
    );
    record_camera_toggle_event(publisher, subscriber, transcript, true).await?;
    record_camera_toggle_event(publisher, subscriber, transcript, false).await?;
    Ok(published_track_token)
}

async fn record_camera_toggle_event<P>(
    publisher: &mut P,
    subscriber: &mut P,
    transcript: &mut CompatibilityTranscript,
    active: bool,
) -> Result<(), String>
where
    P: ScenarioPeer,
{
    publisher
        .set_upload_active(StreamType::Camera, active)
        .await
        .ok_or_else(|| format!("publisher failed to set camera active={active}"))?;
    let snapshot = timeout(SCENARIO_EVENT_TIMEOUT, expect_session_info(subscriber))
        .await
        .map_err(|_elapsed| {
            format!("timed out waiting for subscriber session info after camera active={active}")
        })?
        .ok_or_else(|| {
            format!("subscriber did not receive session info after camera active={active}")
        })?;
    record_camera_state_event(
        transcript,
        SessionId::Integer(20),
        SessionId::Integer(10),
        camera_state_for_session(&snapshot, &SessionId::Integer(10)).ok_or_else(|| {
            format!("session info snapshot missing camera state after camera active={active}")
        })?,
    );
    Ok(())
}

async fn connect_late_camera_subscriber<B>(
    backend: &B,
    channel_uuid: &str,
) -> Result<B::Peer, String>
where
    B: ScenarioBackend,
{
    let mut late_subscriber = backend
        .connect_peer(channel_uuid, SessionId::Integer(30), TEST_CHANNEL_KEY)
        .await
        .map_err(|error| format!("late subscriber peer connect failed: {error}"))?;
    ensure_rtc_startup(&late_subscriber)
        .ok_or_else(|| String::from("late subscriber startup missing rtc feature"))?;
    timeout(SCENARIO_EVENT_TIMEOUT, late_subscriber.connect_transports())
        .await
        .map_err(|_elapsed| {
            String::from("timed out waiting for late subscriber transport connect")
        })?
        .ok_or_else(|| String::from("late subscriber transport connect failed"))?;
    Ok(late_subscriber)
}

async fn record_late_join_track_event<P>(
    late_subscriber: &mut P,
    transcript: &mut CompatibilityTranscript,
    track_tokens: &mut BTreeMap<String, String>,
    next_track_index: &mut u64,
    expected_track_token: &str,
) -> Result<(), String>
where
    P: ScenarioPeer,
{
    let late_track = timeout(SCENARIO_EVENT_TIMEOUT, expect_remote_track(late_subscriber))
        .await
        .map_err(|_elapsed| {
            String::from("timed out waiting for late subscriber camera track bootstrap")
        })?
        .ok_or_else(|| String::from("late subscriber did not receive camera track bootstrap"))?;
    let late_track_token =
        normalize_track_token(track_tokens, next_track_index, &late_track.source_id);
    if late_track_token != expected_track_token {
        return Err(format!(
            "late subscriber observed track token {late_track_token} instead of {expected_track_token}"
        ));
    }
    record_track_event(
        transcript,
        SessionId::Integer(30),
        &late_track_token,
        &late_track,
    );
    Ok(())
}

async fn record_departures<P>(
    subscriber: &mut P,
    late_subscriber: &mut P,
    transcript: &mut CompatibilityTranscript,
) -> Result<(), String>
where
    P: ScenarioPeer,
{
    record_departure_event(
        transcript,
        SessionId::Integer(20),
        timeout(SCENARIO_EVENT_TIMEOUT, expect_departure(subscriber))
            .await
            .map_err(|_elapsed| String::from("timed out waiting for subscriber departure event"))?
            .ok_or_else(|| String::from("subscriber did not observe publisher departure"))?,
    );
    record_departure_event(
        transcript,
        SessionId::Integer(30),
        timeout(SCENARIO_EVENT_TIMEOUT, expect_departure(late_subscriber))
            .await
            .map_err(|_elapsed| {
                String::from("timed out waiting for late subscriber departure event")
            })?
            .ok_or_else(|| String::from("late subscriber did not observe publisher departure"))?,
    );
    Ok(())
}

fn ensure_rtc_startup<P>(peer: &P) -> Option<()>
where
    P: ScenarioPeer,
{
    peer.rtc_feature_enabled().then_some(())
}

fn normalize_track_token(
    tokens_by_source_id: &mut BTreeMap<String, String>,
    next_track_index: &mut u64,
    source_id: &str,
) -> String {
    if let Some(token) = tokens_by_source_id.get(source_id) {
        return token.clone();
    }
    let token = format!("track-{next_track_index}");
    *next_track_index = next_track_index.saturating_add(1);
    tokens_by_source_id.insert(source_id.to_owned(), token.clone());
    token
}

async fn expect_remote_track<P>(peer: &mut P) -> Option<CurrentRemoteTrackBootstrapPayload>
where
    P: ScenarioPeer,
{
    loop {
        let batch = peer.read_next_bus_batch().await?;
        if let Some(track) = extract_remote_track_from_batch(peer, &batch).await? {
            return Some(track);
        }
    }
}

async fn expect_session_info<P>(peer: &mut P) -> Option<CurrentSessionInfoSnapshotById>
where
    P: ScenarioPeer,
{
    loop {
        let batch = peer.read_next_bus_batch().await?;
        if let Some(snapshot) = extract_session_info_from_batch(peer, &batch).await? {
            return Some(snapshot);
        }
    }
}

async fn expect_departure<P>(peer: &mut P) -> Option<SessionId>
where
    P: ScenarioPeer,
{
    loop {
        let batch = peer.read_next_bus_batch().await?;
        if let Some(session_id) = extract_departure_from_batch(peer, &batch).await? {
            return Some(session_id);
        }
    }
}

fn record_track_event(
    transcript: &mut CompatibilityTranscript,
    observer_session_id: SessionId,
    source_token: &str,
    track: &CurrentRemoteTrackBootstrapPayload,
) {
    transcript
        .events
        .push(CompatibilityEvent::RemoteTrackBootstrap {
            observer_session_id,
            owner_session_id: track.session_id.clone(),
            source_token: source_token.to_owned(),
            stream_type: track.stream_type,
            media_kind: track.media_kind,
            active: track.active,
        });
}

fn record_close_event(
    transcript: &mut CompatibilityTranscript,
    session_id: SessionId,
    close_code: u16,
) {
    transcript.events.push(CompatibilityEvent::SessionClosed {
        session_id,
        close_code: normalize_close_code(close_code),
    });
}

fn normalize_close_code(close_code: u16) -> u16 {
    match close_code {
        4106 | 4001 => 4001,
        4107 | 4002 => 4002,
        4108 | 4003 => 4003,
        4109 | 4004 => 4004,
        _ => close_code,
    }
}

fn record_camera_state_event(
    transcript: &mut CompatibilityTranscript,
    observer_session_id: SessionId,
    owner_session_id: SessionId,
    active: bool,
) {
    transcript
        .events
        .push(CompatibilityEvent::SessionCameraState {
            observer_session_id,
            owner_session_id,
            active,
        });
}

fn record_departure_event(
    transcript: &mut CompatibilityTranscript,
    observer_session_id: SessionId,
    departed_session_id: SessionId,
) {
    transcript.events.push(CompatibilityEvent::SessionDeparted {
        observer_session_id,
        departed_session_id,
    });
}

fn camera_state_for_session(
    snapshot: &CurrentSessionInfoSnapshotById,
    session_id: &SessionId,
) -> Option<bool> {
    snapshot
        .get(&session_info_key(session_id))
        .and_then(|info| info.is_camera_on)
}

fn session_info_key(session_id: &SessionId) -> String {
    match session_id {
        SessionId::Integer(value) => value.to_string(),
        SessionId::String(value) => value.clone(),
    }
}

async fn extract_remote_track_from_batch<P>(
    peer: &mut P,
    batch: &CurrentBusBatch,
) -> Option<Option<CurrentRemoteTrackBootstrapPayload>>
where
    P: ScenarioPeer,
{
    for envelope in batch {
        if handle_ping_envelope(peer, envelope).await? {
            continue;
        }
        let Ok(request) = serde_json::from_value::<CurrentServerRequest>(envelope.message.clone())
        else {
            continue;
        };
        if let CurrentServerRequest::BootstrapRemoteTrack(track) = request {
            return Some(Some(track));
        }
    }
    Some(None)
}

async fn extract_session_info_from_batch<P>(
    peer: &mut P,
    batch: &CurrentBusBatch,
) -> Option<Option<CurrentSessionInfoSnapshotById>>
where
    P: ScenarioPeer,
{
    for envelope in batch {
        if handle_ping_envelope(peer, envelope).await? {
            continue;
        }
        let Ok(message) = serde_json::from_value::<CurrentServerMessage>(envelope.message.clone())
        else {
            continue;
        };
        if let CurrentServerMessage::SessionInfoChanged(snapshot) = message {
            return Some(Some(snapshot));
        }
    }
    Some(None)
}

async fn extract_departure_from_batch<P>(
    peer: &mut P,
    batch: &CurrentBusBatch,
) -> Option<Option<SessionId>>
where
    P: ScenarioPeer,
{
    for envelope in batch {
        if handle_ping_envelope(peer, envelope).await? {
            continue;
        }
        let Ok(message) = serde_json::from_value::<CurrentServerMessage>(envelope.message.clone())
        else {
            continue;
        };
        if let CurrentServerMessage::SessionDeparted(payload) = message {
            return Some(Some(payload.session_id));
        }
    }
    Some(None)
}

async fn handle_ping_envelope<P>(peer: &mut P, envelope: &CurrentBusEnvelope) -> Option<bool>
where
    P: ScenarioPeer,
{
    let Some(request_id) = envelope.need_response.clone() else {
        return Some(false);
    };
    let Ok(request) = serde_json::from_value::<CurrentServerRequest>(envelope.message.clone())
    else {
        return Some(false);
    };
    if request != CurrentServerRequest::Ping {
        return Some(false);
    }
    send_pong_response(peer, request_id).await?;
    Some(true)
}

async fn send_pong_response<P>(peer: &mut P, request_id: CurrentBusRequestId) -> Option<()>
where
    P: ScenarioPeer,
{
    peer.respond_to_server_request(request_id, json!({})).await
}

fn reserve_unused_port() -> Option<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).ok()?;
    let port = listener.local_addr().ok()?.port();
    drop(listener);
    Some(port)
}

async fn legacy_create_channel(
    http_base_url: &str,
    issuer: &str,
    key: Option<&str>,
) -> Result<String, String> {
    let token = signed_channel_claims(issuer, key)
        .ok_or_else(|| String::from("failed to sign legacy channel claims"))?;
    let response = reqwest::Client::new()
        .get(format!("{http_base_url}{CHANNEL_PATH}"))
        .bearer_auth(token)
        .query(&CreateChannelQuery::default())
        .send()
        .await
        .map_err(|error| format!("legacy channel creation request failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "legacy channel creation returned HTTP {}",
            response.status()
        ));
    }
    let payload = response
        .json::<ChannelResponse>()
        .await
        .map_err(|error| format!("failed to decode legacy channel response: {error}"))?;
    Ok(payload.uuid)
}

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

fn extract_matching_response(
    batch: &CurrentBusBatch,
    request_id: &CurrentBusRequestId,
) -> Option<Value> {
    batch
        .iter()
        .find(|envelope| envelope.response_to.as_ref() == Some(request_id))
        .map(|envelope| envelope.message.clone())
}

fn legacy_sfu_workdir() -> Result<PathBuf, String> {
    if let Some(workdir) = env::var_os("O_SFU_LEGACY_SFU_DIR") {
        let workdir = PathBuf::from(workdir);
        if workdir.is_dir() {
            return Ok(workdir);
        }
        return Err(format!(
            "O_SFU_LEGACY_SFU_DIR does not point to a directory: {}",
            workdir.display()
        ));
    }
    // TODO: not that clean atm but i just assume the other sfu is in the same parent repository
    // that's just how its setup on my computer and for now it's good enough, will make it more generic later
    // or even just remove these noisy tests once I no longer need to test against the old sfu
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| String::from("failed to resolve repository root from CARGO_MANIFEST_DIR"))?;
    let workdir = repo_root.join("sfu");
    if workdir.is_dir() {
        return Ok(workdir);
    }
    Err(format!(
        "legacy SFU repository not found at {}. Set O_SFU_LEGACY_SFU_DIR to override",
        workdir.display()
    ))
}

async fn read_text_message_with_context(
    websocket: &mut TestWebSocket,
    context: &str,
) -> Result<String, String> {
    let message = websocket
        .next()
        .await
        .ok_or_else(|| format!("missing websocket message while reading {context}"))?
        .map_err(|error| format!("websocket error while reading {context}: {error}"))?;
    match message {
        tungstenite::Message::Text(payload) => Ok(payload.to_string()),
        tungstenite::Message::Close(frame) => Err(format!(
            "websocket closed while reading {context}: {:?}",
            frame.map(|close_frame| close_frame.code)
        )),
        other => Err(format!(
            "unexpected websocket message while reading {context}: {other:?}"
        )),
    }
}

async fn read_bus_batch_with_context(
    websocket: &mut TestWebSocket,
    context: &str,
) -> Result<CurrentBusBatch, String> {
    let payload = read_text_message_with_context(websocket, context).await?;
    serde_json::from_str(&payload)
        .map_err(|error| format!("failed to decode bus batch for {context}: {error}"))
}

fn session_id_value(session_id: &SessionId) -> String {
    match session_id {
        SessionId::Integer(value) => value.to_string(),
        SessionId::String(value) => value.clone(),
    }
}
