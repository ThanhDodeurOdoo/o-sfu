use super::*;

#[test]
fn protocol_core_batches_outbound_control_plane_messages_until_flush_timer() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let first_commands = core.update_info(UserInfo {
        is_talking: Some(true),
        ..UserInfo::default()
    });
    let second_commands = core.broadcast(serde_json::json!({ "kind": "notice" }));

    let [
        Command::ScheduleTimer {
            id: flush_timer_id,
            ms: 100,
        },
    ] = first_commands.as_slice()
    else {
        return Err(format!("expected one flush timer, got {first_commands:?}"));
    };
    assert!(second_commands.is_empty());

    let flush_commands = core.on_timer(*flush_timer_id);
    let mut batch = decode_sent_batch(&flush_commands).into_iter();
    let Some(first_envelope) = batch.next() else {
        return Err(format!(
            "expected first flushed envelope, got {flush_commands:?}"
        ));
    };
    let Some(second_envelope) = batch.next() else {
        return Err(format!(
            "expected second flushed envelope, got {flush_commands:?}"
        ));
    };

    assert_eq!(
        ClientEnvelope::decode(first_envelope),
        Ok(ClientEnvelope::Message(ClientMessage::Info(UserInfo {
            is_talking: Some(true),
            ..UserInfo::default()
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
    Ok(())
}
