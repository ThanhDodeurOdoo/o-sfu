use std::{collections::BTreeMap, future::Future, pin::Pin};

use o_sfu::signaling::{
    current_protocol::{
        CurrentRemoteTrackBootstrapPayload, CurrentServerMessage, CurrentServerRequest,
        CurrentSessionInfoSnapshotById,
    },
    shared::{SessionId, StreamType},
    webrtc::MediaKind,
};

use super::{
    TEST_CHANNEL_KEY,
    fake_media::FakeMediaSource,
    full_stack::{FakePeer, LocalNetwork},
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityTranscript {
    pub backend_name: &'static str,
    pub scenario_name: &'static str,
    pub events: Vec<CompatibilityEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityEvent {
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
    ) -> BoxFuture<'a, Option<String>>;

    fn connect_peer<'a>(
        &'a self,
        channel_uuid: &'a str,
        session_id: SessionId,
        key: &'a str,
    ) -> BoxFuture<'a, Option<Self::Peer>>;
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

    fn read_next_server_request(&mut self) -> BoxFuture<'_, Option<CurrentServerRequest>>;

    fn read_next_server_message(&mut self) -> BoxFuture<'_, Option<CurrentServerMessage>>;

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
    ) -> BoxFuture<'a, Option<String>> {
        Box::pin(async move { self.create_channel(issuer, key).await })
    }

    fn connect_peer<'a>(
        &'a self,
        channel_uuid: &'a str,
        session_id: SessionId,
        key: &'a str,
    ) -> BoxFuture<'a, Option<Self::Peer>> {
        Box::pin(async move { self.connect_fake_peer(channel_uuid, session_id, key).await })
    }
}

impl ScenarioPeer for FakePeer {
    fn rtc_feature_enabled(&self) -> bool {
        self.startup().available_features.rtc
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

    fn read_next_server_request(&mut self) -> BoxFuture<'_, Option<CurrentServerRequest>> {
        Box::pin(async move { Self::read_next_server_request(self).await })
    }

    fn read_next_server_message(&mut self) -> BoxFuture<'_, Option<CurrentServerMessage>> {
        Box::pin(async move { Self::read_next_server_message(self).await })
    }

    fn close(self) -> Pin<Box<dyn Future<Output = Option<()>> + Send>> {
        Box::pin(async move { Self::close(self).await })
    }
}

pub async fn run_camera_publish_oracle_scenario<B>(backend: &B) -> Option<CompatibilityTranscript>
where
    B: ScenarioBackend,
{
    let channel_uuid = backend
        .create_channel("issuer-differential-camera", Some(TEST_CHANNEL_KEY))
        .await?;
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
    .await?;
    let mut late_subscriber = connect_late_camera_subscriber(backend, &channel_uuid).await?;
    record_late_join_track_event(
        &mut late_subscriber,
        &mut transcript,
        &mut track_tokens,
        &mut next_track_index,
        &published_track_token,
    )
    .await?;

    publisher.close().await?;
    record_departures(&mut subscriber, &mut late_subscriber, &mut transcript).await?;

    Some(transcript)
}

async fn connect_initial_camera_flow_peers<B>(
    backend: &B,
    channel_uuid: &str,
) -> Option<(B::Peer, B::Peer)>
where
    B: ScenarioBackend,
{
    let mut publisher = backend
        .connect_peer(channel_uuid, SessionId::Integer(10), TEST_CHANNEL_KEY)
        .await?;
    let mut subscriber = backend
        .connect_peer(channel_uuid, SessionId::Integer(20), TEST_CHANNEL_KEY)
        .await?;
    ensure_rtc_startup(&publisher)?;
    ensure_rtc_startup(&subscriber)?;
    publisher.connect_transports().await?;
    subscriber.connect_transports().await?;
    Some((publisher, subscriber))
}

async fn publish_camera_and_record_initial_events<P>(
    publisher: &mut P,
    subscriber: &mut P,
    transcript: &mut CompatibilityTranscript,
    track_tokens: &mut BTreeMap<String, String>,
    next_track_index: &mut u64,
) -> Option<String>
where
    P: ScenarioPeer,
{
    let producer_id = publisher.publish_track(&FakeMediaSource::camera()).await?;
    let subscriber_track = expect_remote_track(subscriber).await?;
    let published_track_token = normalize_track_token(track_tokens, next_track_index, &producer_id);
    record_track_event(
        transcript,
        SessionId::Integer(20),
        &published_track_token,
        &subscriber_track,
    );
    record_camera_toggle_event(publisher, subscriber, transcript, true).await?;
    record_camera_toggle_event(publisher, subscriber, transcript, false).await?;
    Some(published_track_token)
}

async fn record_camera_toggle_event<P>(
    publisher: &mut P,
    subscriber: &mut P,
    transcript: &mut CompatibilityTranscript,
    active: bool,
) -> Option<()>
where
    P: ScenarioPeer,
{
    publisher
        .set_upload_active(StreamType::Camera, active)
        .await?;
    let snapshot = expect_session_info(subscriber).await?;
    record_camera_state_event(
        transcript,
        SessionId::Integer(20),
        SessionId::Integer(10),
        camera_state_for_session(&snapshot, &SessionId::Integer(10))?,
    );
    Some(())
}

async fn connect_late_camera_subscriber<B>(backend: &B, channel_uuid: &str) -> Option<B::Peer>
where
    B: ScenarioBackend,
{
    let mut late_subscriber = backend
        .connect_peer(channel_uuid, SessionId::Integer(30), TEST_CHANNEL_KEY)
        .await?;
    ensure_rtc_startup(&late_subscriber)?;
    late_subscriber.connect_transports().await?;
    Some(late_subscriber)
}

async fn record_late_join_track_event<P>(
    late_subscriber: &mut P,
    transcript: &mut CompatibilityTranscript,
    track_tokens: &mut BTreeMap<String, String>,
    next_track_index: &mut u64,
    expected_track_token: &str,
) -> Option<()>
where
    P: ScenarioPeer,
{
    let late_track = expect_remote_track(late_subscriber).await?;
    let late_track_token =
        normalize_track_token(track_tokens, next_track_index, &late_track.source_id);
    if late_track_token != expected_track_token {
        return None;
    }
    record_track_event(
        transcript,
        SessionId::Integer(30),
        &late_track_token,
        &late_track,
    );
    Some(())
}

async fn record_departures<P>(
    subscriber: &mut P,
    late_subscriber: &mut P,
    transcript: &mut CompatibilityTranscript,
) -> Option<()>
where
    P: ScenarioPeer,
{
    record_departure_event(
        transcript,
        SessionId::Integer(20),
        expect_departure(subscriber).await?,
    );
    record_departure_event(
        transcript,
        SessionId::Integer(30),
        expect_departure(late_subscriber).await?,
    );
    Some(())
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
    let request = peer.read_next_server_request().await?;
    let CurrentServerRequest::BootstrapRemoteTrack(track) = request else {
        return None;
    };
    Some(track)
}

async fn expect_session_info<P>(peer: &mut P) -> Option<CurrentSessionInfoSnapshotById>
where
    P: ScenarioPeer,
{
    let message = peer.read_next_server_message().await?;
    let CurrentServerMessage::SessionInfoChanged(snapshot) = message else {
        return None;
    };
    Some(snapshot)
}

async fn expect_departure<P>(peer: &mut P) -> Option<SessionId>
where
    P: ScenarioPeer,
{
    let message = peer.read_next_server_message().await?;
    let CurrentServerMessage::SessionDeparted(payload) = message else {
        return None;
    };
    Some(payload.session_id)
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
