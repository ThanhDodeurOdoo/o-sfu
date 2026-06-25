use o_sfu_protocol::{
    host::{
        Command, ConnectionState, NegotiationKind, PendingRequest, PendingRequestKind,
        ProtocolCore, ProtocolEvent, ProtocolRequestResult,
    },
    wire::{
        AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, DownloadStates,
        RecordingOptions, RequestId, ServerEnvelope, ServerMessage, ServerRequest,
        SessionDescriptionPayload, SourceDescriptor, StreamIntentPayload, StreamType,
        SubscribePayload, TrackBinding, UserId, UserInfo, WebSocketCloseCode,
    },
};
use o_sfu_tests::miri_support::{
    decode_sent_client_envelopes, empty_welcome_payload, encode_server_batch,
};

fn extract_pending_request(
    result: &ProtocolRequestResult,
    kind: PendingRequestKind,
) -> Option<&PendingRequest> {
    assert!(
        result.pending_request.is_some(),
        "recording request result should carry a pending request"
    );
    let request = result.pending_request.as_ref()?;
    assert_eq!(request.kind, kind);
    Some(request)
}

#[test]
fn recovery_replay_splits_session_and_publication_phases() {
    let mut core = ProtocolCore::new();

    assert_eq!(
        core.connect("wss://sfu.example.com/socket", "signed-token", None)
            .as_slice(),
        &[
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
            UserId::String("peer-7".to_owned()),
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
        core.update_info(UserInfo {
            is_camera_on: Some(true),
            is_raising_hand: Some(true),
            ..UserInfo::default()
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
            ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                user_id: UserId::String("peer-7".to_owned()),
                states: DownloadStates {
                    audio: Some(true),
                    camera: Some(false),
                    screen: None,
                    ..DownloadStates::default()
                },
            })),
            ClientEnvelope::Message(ClientMessage::Info(UserInfo {
                is_camera_on: Some(true),
                is_raising_hand: Some(true),
                ..UserInfo::default()
            })),
        ]
    );

    let transport_ready_commands = core.on_transport_ready();
    assert_eq!(
        transport_ready_commands.first(),
        Some(&Command::EmitStateChange {
            state: ConnectionState::Connected,
            cause: None,
        })
    );
    assert_eq!(
        decode_sent_client_envelopes(&transport_ready_commands),
        vec![ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Camera,
            },
        ))]
    );
}

#[test]
fn request_timeouts_ignore_unrelated_timer_ids_and_resolve_only_matching_request() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(empty_welcome_payload());

    let start_result = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: Some(true),
        transcription: None,
    });
    let Some(start_request) =
        extract_pending_request(&start_result, PendingRequestKind::StartRecording)
    else {
        return;
    };

    let stop_result = core.stop_recording();
    let Some(stop_request) =
        extract_pending_request(&stop_result, PendingRequestKind::StopRecording)
    else {
        return;
    };

    assert!(core.on_timer(99_999).is_empty());
    assert_eq!(
        core.on_timer(stop_request.timeout_timer_id).as_slice(),
        &[
            Command::CancelTimer {
                id: stop_request.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id: stop_request.request_id.clone(),
                ok: false,
            },
        ]
    );
    assert!(core.on_timer(stop_request.timeout_timer_id).is_empty());
    assert_eq!(
        core.on_timer(start_request.timeout_timer_id).as_slice(),
        &[
            Command::CancelTimer {
                id: start_request.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id: start_request.request_id.clone(),
                ok: false,
            },
        ]
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
        core.on_ws_message(&offer_frame).as_slice(),
        &[
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
        core.on_ws_message("{not json").as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );

    let invalid_envelope = r#"[{"t":"offer","p":{"sdp":"v=0\r\n"},"q":"1","r":"2"}]"#;
    assert_eq!(
        core.on_ws_message(invalid_envelope).as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
}

#[test]
fn disconnect_clears_pending_requests_snapshots_and_runtime_obligations() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(empty_welcome_payload());
    let _ = core.on_transport_ready();

    let binding = TrackBinding {
        mid: "0".to_owned(),
        user_id: UserId::String("peer-1".to_owned()),
        stream_type: StreamType::Audio,
        active: true,
    };
    let track_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        binding.clone(),
    ])));
    assert_eq!(
        core.on_ws_message(&track_frame).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![binding],
            },
        }]
    );

    let source = SourceDescriptor {
        source_id: "source-1".to_owned(),
        user_id: UserId::String("peer-1".to_owned()),
        stream_type: StreamType::Audio,
        active: true,
        mid: Some("0".to_owned()),
        encodings: Vec::new(),
    };
    let source_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::Sources(vec![
        source.clone(),
    ])));
    assert_eq!(
        core.on_ws_message(&source_frame).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::SourceSnapshot {
                sources: vec![source],
            },
        }]
    );

    let request_result = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: None,
        transcription: None,
    });
    let Some(request) =
        extract_pending_request(&request_result, PendingRequestKind::StartRecording)
    else {
        return;
    };

    assert_eq!(
        core.disconnect().as_slice(),
        &[
            Command::CancelTimer { id: 1 },
            Command::CancelTimer { id: 2 },
            Command::CancelTimer {
                id: request.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id: request.request_id.clone(),
                ok: false,
            },
            Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: Vec::new(),
                },
            },
            Command::EmitEvent {
                event: ProtocolEvent::SourceSnapshot {
                    sources: Vec::new(),
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
