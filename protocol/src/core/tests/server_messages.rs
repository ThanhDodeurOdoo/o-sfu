use super::*;

#[test]
fn protocol_core_tracks_server_mid_bindings_and_clears_stale_snapshot_entries() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let first_tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        TrackBinding {
            mid: String::from("0"),
            session_id: String::from("peer-1").into(),
            stream_type: StreamType::Audio,
            active: true,
            source: None,
        },
        TrackBinding {
            mid: String::from("1"),
            session_id: String::from("peer-2").into(),
            stream_type: StreamType::Camera,
            active: true,
            source: None,
        },
    ])));
    let second_tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        TrackBinding {
            mid: String::from("2"),
            session_id: String::from("peer-2").into(),
            stream_type: StreamType::Camera,
            active: false,
            source: None,
        },
    ])));

    assert_eq!(
        core.on_ws_message(&first_tracks),
        vec![Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![
                    TrackBinding {
                        mid: String::from("0"),
                        session_id: String::from("peer-1").into(),
                        stream_type: StreamType::Audio,
                        active: true,
                        source: None,
                    },
                    TrackBinding {
                        mid: String::from("1"),
                        session_id: String::from("peer-2").into(),
                        stream_type: StreamType::Camera,
                        active: true,
                        source: None,
                    },
                ],
            },
        }]
    );
    assert_eq!(
        core.track_binding("0"),
        Some(&TrackBinding {
            mid: String::from("0"),
            session_id: String::from("peer-1").into(),
            stream_type: StreamType::Audio,
            active: true,
            source: None,
        })
    );
    assert_eq!(
        core.track_binding("1"),
        Some(&TrackBinding {
            mid: String::from("1"),
            session_id: String::from("peer-2").into(),
            stream_type: StreamType::Camera,
            active: true,
            source: None,
        })
    );

    assert_eq!(
        core.on_ws_message(&second_tracks),
        vec![Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![TrackBinding {
                    mid: String::from("2"),
                    session_id: String::from("peer-2").into(),
                    stream_type: StreamType::Camera,
                    active: false,
                    source: None,
                }],
            },
        }]
    );
    assert_eq!(core.track_binding("0"), None);
    assert_eq!(core.track_binding("1"), None);
    assert_eq!(
        core.track_binding("2"),
        Some(&TrackBinding {
            mid: String::from("2"),
            session_id: String::from("peer-2").into(),
            stream_type: StreamType::Camera,
            active: false,
            source: None,
        })
    );
}

#[test]
fn protocol_core_peer_left_clears_track_bindings_for_that_session() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        TrackBinding {
            mid: String::from("0"),
            session_id: String::from("peer-1").into(),
            stream_type: StreamType::Audio,
            active: true,
            source: None,
        },
        TrackBinding {
            mid: String::from("1"),
            session_id: String::from("peer-2").into(),
            stream_type: StreamType::Camera,
            active: true,
            source: None,
        },
    ])));
    let _ = core.on_ws_message(&tracks);

    let peer_left = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
        PeerLeftPayload {
            session_id: String::from("peer-1").into(),
        },
    )));

    assert_eq!(
        core.on_ws_message(&peer_left),
        vec![Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                session_id: String::from("peer-1").into(),
            },
        }]
    );
    assert_eq!(core.track_binding("0"), None);
    assert!(core.track_binding("1").is_some());
}

#[test]
fn protocol_core_tracks_source_descriptors_from_track_snapshot() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let source = SourceDescriptor {
        source_id: String::from("source-7"),
        session_id: String::from("peer-1").into(),
        stream_type: StreamType::Camera,
        active: true,
        mid: Some(String::from("cam-0")),
        encodings: vec![
            SourceEncodingDescriptor {
                encoding_id: String::from("encoding-1"),
                rid: Some(String::from("lo")),
                max_bitrate: Some(150_000),
            },
            SourceEncodingDescriptor {
                encoding_id: String::from("encoding-2"),
                rid: Some(String::from("hi")),
                max_bitrate: Some(900_000),
            },
        ],
    };
    let tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        TrackBinding {
            mid: String::from("cam-0"),
            session_id: String::from("peer-1").into(),
            stream_type: StreamType::Camera,
            active: true,
            source: Some(source.clone()),
        },
    ])));

    assert_eq!(
        core.on_ws_message(&tracks),
        vec![
            Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: vec![TrackBinding {
                        mid: String::from("cam-0"),
                        session_id: String::from("peer-1").into(),
                        stream_type: StreamType::Camera,
                        active: true,
                        source: Some(source.clone()),
                    }],
                },
            },
            Command::EmitEvent {
                event: ProtocolEvent::SourceSnapshot {
                    sources: vec![source],
                },
            },
        ]
    );
}

#[test]
fn protocol_core_emits_peer_and_recording_updates_from_server_messages() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let peer_info_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerInfo(
        PeerInfoPayload {
            session_id: String::from("peer-1").into(),
            info: SessionInfo {
                is_camera_on: Some(true),
                ..SessionInfo::default()
            },
        },
    )));
    let peer_left_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
        PeerLeftPayload {
            session_id: String::from("peer-1").into(),
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
                session_id: String::from("peer-1").into(),
                info: SessionInfo {
                    is_camera_on: Some(true),
                    ..SessionInfo::default()
                },
            },
        }]
    );
    assert_eq!(
        core.on_ws_message(&peer_left_frame),
        vec![Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                session_id: String::from("peer-1").into(),
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
