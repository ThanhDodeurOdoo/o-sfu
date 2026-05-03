use super::*;

#[test]
fn protocol_core_disconnect_cleans_up_live_session() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());
    let _ = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: None,
        transcription: None,
    });
    let _ = core.on_timer(BATCH_FLUSH_TIMER_ID);
    let _ = core.on_transport_ready();

    let commands = core.disconnect();

    assert_eq!(core.state(), ConnectionState::Disconnected);
    assert!(commands.contains(&Command::CancelTimer {
        id: RECOVERY_TIMER_ID,
    }));
    assert!(commands.contains(&Command::CloseWebSocket { code: 1000 }));
    assert!(commands.contains(&Command::ClosePeerConnection));
    assert!(commands.contains(&Command::EmitStateChange {
        state: ConnectionState::Disconnected,
        cause: None,
    }));
    assert_eq!(
        core.features(),
        &AvailableFeatures {
            rtc: false,
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    );
    let recording_state = serde_json::to_value(core.recording_state());
    assert_eq!(recording_state.unwrap_or_default(), empty_recording_json());
}

#[test]
fn protocol_core_non_terminal_close_enters_recovering() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();

    let commands = core.on_ws_close(1011);

    assert_eq!(core.state(), ConnectionState::Recovering);
    assert_eq!(
        commands,
        vec![
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: ConnectionState::Recovering,
                cause: None,
            },
            Command::ScheduleTimer {
                id: RECOVERY_TIMER_ID,
                ms: 1_000,
            },
        ]
    );
}

#[test]
fn protocol_core_replays_sticky_intents_after_recovery_authentication() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.publish(StreamType::Camera, true);
    let _ = core.subscribe(
        String::from("peer-7").into(),
        DownloadStates {
            audio: Some(true),
            camera: Some(false),
            screen: None,
            ..DownloadStates::default()
        },
    );
    let _ = core.update_info(UserInfo {
        is_camera_on: Some(true),
        is_raising_hand: Some(true),
        ..UserInfo::default()
    });
    let _ = core.on_ws_close(1011);
    let _ = core.on_timer(RECOVERY_TIMER_ID);

    let commands = core.on_welcome(sample_welcome_payload());
    let envelopes = decode_sent_client_envelopes(&commands);

    assert_eq!(core.state(), ConnectionState::Authenticated);
    assert_eq!(
        commands.first(),
        Some(&Command::EmitStateChange {
            state: ConnectionState::Authenticated,
            cause: None,
        })
    );
    assert_eq!(
        envelopes,
        vec![
            ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
                stream_type: StreamType::Camera,
            })),
            ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                user_id: String::from("peer-7").into(),
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
}

#[test]
fn protocol_core_updates_sticky_intents_while_recovering_before_replay() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.publish(StreamType::Camera, true);
    let _ = core.subscribe(
        String::from("peer-7").into(),
        DownloadStates {
            audio: Some(true),
            camera: None,
            screen: None,
            ..DownloadStates::default()
        },
    );
    let _ = core.on_ws_close(1011);
    let _ = core.publish(StreamType::Camera, false);
    let _ = core.subscribe(
        String::from("peer-7").into(),
        DownloadStates {
            audio: Some(false),
            camera: Some(true),
            screen: None,
            ..DownloadStates::default()
        },
    );
    let _ = core.update_info(UserInfo {
        is_self_muted: Some(true),
        ..UserInfo::default()
    });
    let _ = core.on_timer(RECOVERY_TIMER_ID);

    let commands = core.on_welcome(sample_welcome_payload());
    let envelopes = decode_sent_client_envelopes(&commands);

    assert_eq!(
        envelopes,
        vec![
            ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                user_id: String::from("peer-7").into(),
                states: DownloadStates {
                    audio: Some(false),
                    camera: Some(true),
                    screen: None,
                    ..DownloadStates::default()
                },
            })),
            ClientEnvelope::Message(ClientMessage::Info(UserInfo {
                is_self_muted: Some(true),
                ..UserInfo::default()
            })),
        ]
    );
}

#[test]
fn protocol_core_recovery_timer_retries_the_saved_url() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.on_ws_close(1011);

    let commands = core.on_timer(RECOVERY_TIMER_ID);

    assert_eq!(core.state(), ConnectionState::Connecting);
    assert_eq!(
        commands,
        vec![
            Command::EmitStateChange {
                state: ConnectionState::Connecting,
                cause: None,
            },
            Command::Connect {
                url: String::from("wss://sfu.example.com/socket"),
            },
        ]
    );
}

#[test]
fn protocol_core_successful_recovery_resets_backoff_delay() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.on_ws_close(1011);
    let _ = core.on_timer(RECOVERY_TIMER_ID);
    let _ = core.on_welcome(sample_welcome_payload());

    let commands = core.on_ws_close(1011);

    assert_eq!(
        commands,
        vec![
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: ConnectionState::Recovering,
                cause: None,
            },
            Command::ScheduleTimer {
                id: RECOVERY_TIMER_ID,
                ms: 1_000,
            },
        ]
    );
}

#[test]
fn protocol_core_terminal_close_enters_closed_with_cause() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let commands = core.on_ws_close(4109);

    assert_eq!(core.state(), ConnectionState::Closed);
    assert_eq!(
        commands,
        vec![
            Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            },
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: ConnectionState::Closed,
                cause: Some(String::from("full")),
            },
        ]
    );
}
