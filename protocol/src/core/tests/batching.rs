use super::*;

#[test]
fn protocol_core_batches_outbound_control_plane_messages_until_flush_timer() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let first_commands = core.update_info(SessionInfo {
        is_talking: Some(true),
        ..SessionInfo::default()
    });
    let second_commands = core.broadcast(serde_json::json!({ "kind": "notice" }));

    assert_eq!(
        first_commands,
        vec![Command::ScheduleTimer {
            id: BATCH_FLUSH_TIMER_ID,
            ms: 100,
        }]
    );
    assert!(second_commands.is_empty());

    let flush_commands = core.on_timer(BATCH_FLUSH_TIMER_ID);
    let mut batch = decode_sent_batch(&flush_commands).into_iter();
    let Some(first_envelope) = batch.next() else {
        return;
    };
    let Some(second_envelope) = batch.next() else {
        return;
    };

    assert_eq!(
        ClientEnvelope::decode(first_envelope),
        Ok(ClientEnvelope::Message(ClientMessage::Info(SessionInfo {
            is_talking: Some(true),
            ..SessionInfo::default()
        })))
    );
    assert_eq!(
        ClientEnvelope::decode(second_envelope),
        Ok(ClientEnvelope::Message(ClientMessage::Broadcast(
            ClientBroadcastPayload {
                message: serde_json::json!({ "kind": "notice" }),
            }
        )))
    );
}

#[test]
fn protocol_core_responds_to_server_ping_immediately() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let frame = encode_server_batch(ServerEnvelope::Request {
        request_id: RequestId::new("ping-1"),
        request: ServerRequest::Ping,
    });
    let commands = core.on_ws_message(&frame);
    let mut batch = decode_sent_batch(&commands).into_iter();
    let Some(envelope) = batch.next() else {
        return;
    };

    assert_eq!(
        ClientEnvelope::decode(envelope),
        Ok(ClientEnvelope::Response {
            response_to: RequestId::new("ping-1"),
            response: ClientResponse::Ping,
        })
    );
}
