use super::*;

#[test]
fn protocol_core_tracks_server_mid_bindings_and_clears_stale_snapshot_entries() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let peer_1_audio = TrackBinding {
        mid: String::from("0"),
        user_id: String::from("peer-1").into(),
        stream_type: StreamType::Audio,
        active: true,
    };
    let peer_2_camera = TrackBinding {
        mid: String::from("1"),
        user_id: String::from("peer-2").into(),
        stream_type: StreamType::Camera,
        active: true,
    };
    let peer_2_camera_inactive = TrackBinding {
        mid: String::from("2"),
        user_id: String::from("peer-2").into(),
        stream_type: StreamType::Camera,
        active: false,
    };
    let first_tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        peer_1_audio.clone(),
        peer_2_camera.clone(),
    ])));
    let second_tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        peer_2_camera_inactive.clone(),
    ])));

    assert_eq!(
        core.on_ws_message(&first_tracks),
        vec![Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![peer_1_audio.clone(), peer_2_camera.clone()],
            },
        }]
    );
    assert_eq!(core.track_binding("0"), Some(&peer_1_audio));
    assert_eq!(core.track_binding("1"), Some(&peer_2_camera));

    assert_eq!(
        core.on_ws_message(&second_tracks),
        vec![Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![peer_2_camera_inactive.clone()],
            },
        }]
    );
    assert_eq!(core.track_binding("0"), None);
    assert_eq!(core.track_binding("1"), None);
    assert_eq!(core.track_binding("2"), Some(&peer_2_camera_inactive));
}

#[test]
fn protocol_core_peer_left_clears_track_bindings_for_that_session() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        TrackBinding {
            mid: String::from("0"),
            user_id: String::from("peer-1").into(),
            stream_type: StreamType::Audio,
            active: true,
        },
        TrackBinding {
            mid: String::from("1"),
            user_id: String::from("peer-2").into(),
            stream_type: StreamType::Camera,
            active: true,
        },
    ])));
    let _ = core.on_ws_message(&tracks);

    let peer_left = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
        PeerLeftPayload {
            user_id: String::from("peer-1").into(),
        },
    )));

    assert_eq!(
        core.on_ws_message(&peer_left),
        vec![Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                user_id: String::from("peer-1").into(),
            },
        }]
    );
    assert_eq!(core.track_binding("0"), None);
    assert!(core.track_binding("1").is_some());
}

#[test]
fn protocol_core_peer_left_does_not_rewrite_source_snapshots() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let source = SourceDescriptor {
        source_id: String::from("source-7"),
        user_id: String::from("peer-1").into(),
        stream_type: StreamType::Camera,
        active: true,
        mid: Some(String::from("cam-0")),
        encodings: Vec::new(),
    };
    let sources = encode_server_batch(ServerEnvelope::Message(ServerMessage::Sources(vec![
        source.clone(),
    ])));

    assert_eq!(
        core.on_ws_message(&sources),
        vec![Command::EmitEvent {
            event: ProtocolEvent::SourceSnapshot {
                sources: vec![source],
            },
        }]
    );

    let peer_left = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
        PeerLeftPayload {
            user_id: String::from("peer-1").into(),
        },
    )));

    assert_eq!(
        core.on_ws_message(&peer_left),
        vec![Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                user_id: String::from("peer-1").into(),
            },
        }]
    );
}

#[test]
fn protocol_core_emits_peer_and_recording_updates_from_server_messages() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let peer_info_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerInfo(
        PeerInfoPayload {
            user_id: String::from("peer-1").into(),
            info: UserInfo {
                is_camera_on: Some(true),
                ..UserInfo::default()
            },
        },
    )));
    let peer_left_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
        PeerLeftPayload {
            user_id: String::from("peer-1").into(),
        },
    )));
    let recording_frame = encode_server_batch(ServerEnvelope::Message(
        ServerMessage::RecordingChange(RecordingStateUpdate {
            state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            stop_code: Some(StopCode::UserRequest),
        }),
    ));
    let broadcast_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::Broadcast(
        ServerBroadcastPayload {
            sender_id: String::from("peer-2").into(),
            message: serde_json::json!({ "body": "hello" }),
        },
    )));

    assert_eq!(
        core.on_ws_message(&peer_info_frame),
        vec![Command::EmitEvent {
            event: ProtocolEvent::PeerInfo {
                user_id: String::from("peer-1").into(),
                info: UserInfo {
                    is_camera_on: Some(true),
                    ..UserInfo::default()
                },
            },
        }]
    );
    assert_eq!(
        core.on_ws_message(&peer_left_frame),
        vec![Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                user_id: String::from("peer-1").into(),
            },
        }]
    );
    assert_eq!(
        core.on_ws_message(&broadcast_frame),
        vec![Command::EmitEvent {
            event: ProtocolEvent::Broadcast {
                sender_id: String::from("peer-2").into(),
                message: serde_json::json!({ "body": "hello" }),
            },
        }]
    );
    assert_eq!(
        core.on_ws_message(&recording_frame),
        vec![Command::EmitEvent {
            event: ProtocolEvent::RecordingStateChanged {
                state: RecordingStateUpdate {
                    state: RecordingState {
                        recording: Some(false),
                        audio: Some(false),
                        transcription: Some(false),
                        video: Some(false),
                    },
                    stop_code: Some(StopCode::UserRequest),
                },
            },
        }]
    );
}
