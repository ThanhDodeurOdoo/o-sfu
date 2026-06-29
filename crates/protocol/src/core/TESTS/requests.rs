use super::*;

#[test]
fn protocol_core_tracks_recording_request_until_matching_response() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let commands = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: Some(true),
        transcription: None,
    });

    let [
        Command::BeginPendingRequest {
            request: pending_request,
        },
        Command::ScheduleTimer {
            id: flush_timer_id,
            ms: 100,
        },
    ] = commands.as_slice()
    else {
        return Err(format!("expected recording request, got {commands:?}"));
    };
    assert_eq!(pending_request.kind, PendingRequestKind::StartRecording);
    assert_eq!(pending_request.timeout_ms, REQUEST_TIMEOUT_MS);
    let request_id = pending_request.request_id.clone();

    let flush_commands = core.on_timer(*flush_timer_id);
    let mut batch = decode_sent_batch(&flush_commands).into_iter();
    let Some(envelope) = batch.next() else {
        return Err(format!(
            "expected flushed request envelope, got {flush_commands:?}"
        ));
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
        response_commands.as_slice(),
        &[
            Command::CancelTimer {
                id: pending_request.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id,
                ok: true,
            },
        ]
    );
    Ok(())
}

#[test]
fn protocol_core_request_timeout_resolves_pending_request_as_failed() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let commands = core.stop_recording();
    let [
        Command::BeginPendingRequest {
            request: pending_request,
        },
        ..,
    ] = commands.as_slice()
    else {
        return Err(format!("expected pending request, got {commands:?}"));
    };
    let request_id = pending_request.request_id.clone();

    let timeout_commands = core.on_timer(pending_request.timeout_timer_id);

    assert_eq!(
        timeout_commands.as_slice(),
        &[
            Command::CancelTimer {
                id: pending_request.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id,
                ok: false,
            },
        ]
    );
    Ok(())
}
