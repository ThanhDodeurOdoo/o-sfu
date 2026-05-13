use super::*;

#[test]
fn protocol_core_tracks_recording_request_until_matching_response() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let commands = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: Some(true),
        transcription: None,
    });

    assert!(matches!(
        commands.as_slice(),
        [
            Command::RegisterPendingRequest {
                request_id: _,
                kind: PendingRequestKind::StartRecording,
            },
            Command::ScheduleTimer {
                id: _,
                ms: REQUEST_TIMEOUT_MS,
            },
            Command::ScheduleTimer {
                id: BATCH_FLUSH_TIMER_ID,
                ms: 100,
            },
        ]
    ));

    let Some(Command::RegisterPendingRequest { request_id, .. }) = commands.first() else {
        return;
    };
    let request_id = request_id.clone();
    let Some(Command::ScheduleTimer {
        id: timeout_timer_id,
        ..
    }) = commands.get(1)
    else {
        return;
    };

    let flush_commands = core.on_timer(BATCH_FLUSH_TIMER_ID);
    let mut batch = decode_sent_batch(&flush_commands).into_iter();
    let Some(envelope) = batch.next() else {
        return;
    };
    assert_eq!(
        ClientEnvelope::decode(envelope),
        Ok(ClientEnvelope::Request {
            request_id: request_id.clone(),
            request: ClientRequest::StartRecording(RecordingOptions {
                audio: Some(true),
                video: Some(true),
                transcription: None,
            }),
        })
    );

    let response_frame = encode_server_batch(ServerEnvelope::Response {
        response_to: request_id.clone(),
        response: ServerResponse::StartRecording(RecordingActionResult { ok: true }),
    });
    let response_commands = core.on_ws_message(&response_frame);

    assert_eq!(
        response_commands,
        vec![
            Command::CancelTimer {
                id: *timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id,
                ok: true,
            },
        ]
    );
}

#[test]
fn protocol_core_request_timeout_resolves_pending_request_as_failed() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let commands = core.stop_recording();
    let Some(Command::RegisterPendingRequest { request_id, .. }) = commands.first() else {
        return;
    };
    let request_id = request_id.clone();
    let Some(Command::ScheduleTimer {
        id: timeout_timer_id,
        ..
    }) = commands.get(1)
    else {
        return;
    };

    let timeout_commands = core.on_timer(*timeout_timer_id);

    assert_eq!(
        timeout_commands,
        vec![
            Command::CancelTimer {
                id: *timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id,
                ok: false,
            },
        ]
    );
}
