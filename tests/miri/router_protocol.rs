use o_sfu_protocol::{
    core::{Command, ProtocolCore},
    shared::{AvailableFeatures, RecordingState, StreamType},
    signaling::{
        ClientEnvelope, ClientMessage, EnvelopeBatch, StreamIntentPayload, WelcomePayload,
    },
};
use o_sfu_router::{
    Consumer, ConsumerCapability, ConsumerId, MediaKind, Producer, ProducerId, Router, RouterId,
    Session, SessionId as RouterSessionId, Transport, TransportDirection, TransportId,
};

fn user(id: RouterSessionId) -> Session {
    Session::new(id)
}

fn sent_frames(commands: &[Command]) -> Vec<&str> {
    commands
        .iter()
        .filter_map(|command| match command {
            Command::SendWebSocket(frame) => Some(frame.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn router_session_teardown_keeps_remaining_routing_consistent() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(user(RouterSessionId(10))), Ok(()));
    assert_eq!(router.join_session(user(RouterSessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            RouterSessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            RouterSessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.remove_session(RouterSessionId(10)), Ok(()));
    assert_eq!(router.session_count(), 1);
    assert_eq!(router.sessions().count(), 1);
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            RouterSessionId(20),
            TransportDirection::Send,
        )),
        Err(o_sfu_router::RouterError::DuplicateTransport(TransportId(
            200
        )))
    );
    assert_eq!(
        router.remove_producer(ProducerId(300)),
        Err(o_sfu_router::RouterError::MissingProducer(ProducerId(300)))
    );
    assert_eq!(
        router.remove_consumer(ConsumerId(400)),
        Err(o_sfu_router::RouterError::MissingConsumer(ConsumerId(400)))
    );
}

#[test]
fn signaling_codec_round_trip_preserves_subscribe_payload() {
    let encoded = ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
        stream_type: StreamType::Screen,
    }))
    .into_envelope();
    assert!(encoded.is_ok());

    let Ok(encoded) = encoded else {
        return;
    };
    let decoded = ClientEnvelope::decode(encoded);
    assert_eq!(
        decoded,
        Ok(ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Screen,
            },
        )))
    );
}

#[test]
fn protocol_core_replays_sticky_publish_after_welcome() {
    let mut core = ProtocolCore::new();

    let connect_commands = core.connect("wss://example.invalid/ws", "jwt-token", None);
    assert!(connect_commands.iter().any(
        |command| matches!(command, Command::Connect { url } if url == "wss://example.invalid/ws")
    ));

    assert!(core.publish(StreamType::Camera, true).is_empty());

    let auth_commands = core.on_ws_open();
    assert_eq!(sent_frames(&auth_commands).len(), 1);

    let welcome_commands = core.on_welcome(WelcomePayload {
        features: AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: false,
            video_recording: false,
        },
        recording: RecordingState::default(),
        peers: Vec::new(),
    });
    let frames = sent_frames(&welcome_commands);
    assert_eq!(frames.len(), 1);

    let first_frame = frames.first();
    assert!(first_frame.is_some());
    let Some(first_frame) = first_frame else {
        return;
    };
    let batch = serde_json::from_str::<EnvelopeBatch>(first_frame);
    assert!(batch.is_ok());
    let Ok(batch) = batch else {
        return;
    };
    assert_eq!(batch.len(), 1);

    let mut batch_iter = batch.into_iter();
    let first_envelope = batch_iter.next();
    assert!(first_envelope.is_some());
    let Some(first_envelope) = first_envelope else {
        return;
    };
    let decoded = ClientEnvelope::decode(first_envelope);
    assert_eq!(
        decoded,
        Ok(ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Camera,
            },
        )))
    );
}
