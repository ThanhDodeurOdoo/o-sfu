use o_sfu_protocol::{
    core::{
        Command, ConnectionState, NegotiationKind, PendingRequestKind, ProtocolCore, ProtocolEvent,
    },
    shared::{DownloadStates, SessionId, SessionInfo, StreamType},
    signaling::{
        AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, RecordingOptions, RequestId,
        ServerEnvelope, ServerMessage, ServerRequest, SessionDescriptionPayload,
        StreamIntentPayload, SubscribePayload, TrackBinding, WebSocketCloseCode,
    },
};
use o_sfu_tests::miri_support::{
    decode_sent_client_envelopes, empty_welcome_payload, encode_server_batch,
};

fn extract_registered_request(commands: &[Command]) -> Option<(RequestId, u32)> {
    let request_id = commands.iter().find_map(|command| match command {
        Command::RegisterPendingRequest { request_id, .. } => Some(request_id.clone()),
        _ => None,
    })?;
    let timeout_timer_id = commands.iter().find_map(|command| match command {
        Command::ScheduleTimer { id, .. } => Some(*id),
        _ => None,
    })?;
    Some((request_id, timeout_timer_id))
}

#[test]
fn recovery_replay_flushes_sticky_state_once_after_welcome() {
    let mut core = ProtocolCore::new();

    assert_eq!(
        core.connect("wss://sfu.example.com/socket", "signed-token", None),
        vec![
            Command::EmitStateChange {
                state: ConnectionState::Connecting,
                cause: None,
            },
            Command::Connect {
                url: "wss://sfu.example.com/socket".to_owned(),
            },
        ]
    );
    assert_eq!(
        decode_sent_client_envelopes(&core.on_ws_open()),
        vec![ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
            jwt: "signed-token".to_owned(),
            channel: None,
        }))]
    );

    assert!(core.publish(StreamType::Camera, true).is_empty());
    assert!(
        core.subscribe(
            SessionId::String("peer-7".to_owned()),
            DownloadStates {
                audio: Some(true),
                camera: Some(false),
                screen: None,
                ..DownloadStates::default()
            },
        )
        .is_empty()
    );
    assert!(
        core.update_info(SessionInfo {
            is_camera_on: Some(true),
            is_raising_hand: Some(true),
            ..SessionInfo::default()
        })
        .is_empty()
    );

    let welcome_commands = core.on_welcome(empty_welcome_payload());
    assert_eq!(
        welcome_commands.first(),
        Some(&Command::EmitStateChange {
            state: ConnectionState::Authenticated,
            cause: None,
        })
    );
    assert_eq!(
        decode_sent_client_envelopes(&welcome_commands),
        vec![
            ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
                stream_type: StreamType::Camera,
            })),
            ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                session_id: SessionId::String("peer-7".to_owned()),
                states: DownloadStates {
                    audio: Some(true),
                    camera: Some(false),
                    screen: None,
                    ..DownloadStates::default()
                },
            })),
            ClientEnvelope::Message(ClientMessage::Info(SessionInfo {
                is_camera_on: Some(true),
                is_raising_hand: Some(true),
                ..SessionInfo::default()
            })),
        ]
    );

    assert_eq!(
        core.on_transport_ready(),
        vec![Command::EmitStateChange {
            state: ConnectionState::Connected,
            cause: None,
        }]
    );
}

#[test]
fn request_timeouts_ignore_unrelated_timer_ids_and_resolve_only_matching_request() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(empty_welcome_payload());

    let start_commands = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: Some(true),
        transcription: None,
    });
    let Some((start_request_id, start_timeout_timer_id)) =
        extract_registered_request(&start_commands)
    else {
        return;
    };
    assert!(matches!(
        start_commands.first(),
        Some(Command::RegisterPendingRequest {
            kind: PendingRequestKind::StartRecording,
            ..
        })
    ));

    let stop_commands = core.stop_recording();
    let Some((stop_request_id, stop_timeout_timer_id)) = extract_registered_request(&stop_commands)
    else {
        return;
    };
    assert!(matches!(
        stop_commands.first(),
        Some(Command::RegisterPendingRequest {
            kind: PendingRequestKind::StopRecording,
            ..
        })
    ));

    assert!(core.on_timer(99_999).is_empty());
    assert_eq!(
        core.on_timer(stop_timeout_timer_id),
        vec![Command::ResolvePendingRequest {
            request_id: stop_request_id,
            ok: false,
        }]
    );
    assert!(core.on_timer(stop_timeout_timer_id).is_empty());
    assert_eq!(
        core.on_timer(start_timeout_timer_id),
        vec![Command::ResolvePendingRequest {
            request_id: start_request_id,
            ok: false,
        }]
    );
}

#[test]
fn negotiation_answer_mismatches_do_not_resolve_pending_request() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(empty_welcome_payload());

    let offer_frame = encode_server_batch(ServerEnvelope::Request {
        request_id: RequestId::new("offer-1"),
        request: ServerRequest::Offer(SessionDescriptionPayload {
            sdp: "v=0\r\ns=offer\r\n".to_owned(),
            upload_slots: Vec::new(),
        }),
    });
    assert_eq!(
        core.on_ws_message(&offer_frame),
        vec![
            Command::CreatePeerConnection,
            Command::ApplyNegotiation {
                request_id: RequestId::new("offer-1"),
                kind: NegotiationKind::Offer,
                sdp: "v=0\r\ns=offer\r\n".to_owned(),
                upload_slots: Vec::new(),
            },
        ]
    );

    assert!(
        core.submit_negotiation_answer(
            &RequestId::new("offer-2"),
            NegotiationKind::Offer,
            "v=0\r\ns=wrong-id\r\n",
        )
        .is_empty()
    );
    assert!(
        core.submit_negotiation_answer(
            &RequestId::new("offer-1"),
            NegotiationKind::Renegotiate,
            "v=0\r\ns=wrong-kind\r\n",
        )
        .is_empty()
    );

    let answer_commands = core.submit_negotiation_answer(
        &RequestId::new("offer-1"),
        NegotiationKind::Offer,
        "v=0\r\ns=answer\r\n",
    );
    assert_eq!(
        decode_sent_client_envelopes(&answer_commands),
        vec![ClientEnvelope::Response {
            response_to: RequestId::new("offer-1"),
            response: ClientResponse::Offer(SessionDescriptionPayload {
                sdp: "v=0\r\ns=answer\r\n".to_owned(),
                upload_slots: Vec::new(),
            }),
        }]
    );
    assert!(
        core.submit_negotiation_answer(
            &RequestId::new("offer-1"),
            NegotiationKind::Offer,
            "v=0\r\ns=stale\r\n",
        )
        .is_empty()
    );
}

#[test]
fn malformed_server_batches_close_the_socket_with_protocol_error() {
    let mut core = ProtocolCore::new();

    assert_eq!(
        core.on_ws_message("{not json"),
        vec![Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );

    let invalid_envelope = r#"[{"t":"offer","p":{"sdp":"v=0\r\n"},"q":"1","r":"2"}]"#;
    assert_eq!(
        core.on_ws_message(invalid_envelope),
        vec![Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
}

#[test]
fn disconnect_clears_pending_requests_track_snapshots_and_runtime_obligations() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(empty_welcome_payload());
    let _ = core.on_transport_ready();

    let track_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        TrackBinding {
            mid: "0".to_owned(),
            session_id: SessionId::String("peer-1".to_owned()),
            stream_type: StreamType::Audio,
            active: true,
            source: None,
        },
    ])));
    assert_eq!(
        core.on_ws_message(&track_frame),
        vec![Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![TrackBinding {
                    mid: "0".to_owned(),
                    session_id: SessionId::String("peer-1".to_owned()),
                    stream_type: StreamType::Audio,
                    active: true,
                    source: None,
                }],
            },
        }]
    );

    let request_commands = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: None,
        transcription: None,
    });
    let Some((request_id, request_timeout_timer_id)) =
        extract_registered_request(&request_commands)
    else {
        return;
    };

    assert_eq!(
        core.disconnect(),
        vec![
            Command::CancelTimer { id: 1 },
            Command::CancelTimer { id: 2 },
            Command::CancelTimer {
                id: request_timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id,
                ok: false,
            },
            Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: Vec::new(),
                },
            },
            Command::CloseWebSocket { code: 1000 },
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: ConnectionState::Disconnected,
                cause: None,
            },
        ]
    );
    assert_eq!(core.state(), ConnectionState::Disconnected);
}
