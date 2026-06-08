use super::*;

async fn read_next_server_payload(websocket: &mut TestWebSocket) -> Option<String> {
    timeout(Duration::from_secs(1), read_text_message(websocket))
        .await
        .ok()
        .flatten()
}

pub(crate) async fn no_server_frame(peer: &mut ProtocolHarnessPeer, wait: Duration) -> bool {
    let Some(websocket) = peer.websocket.as_mut() else {
        return false;
    };
    timeout(wait, read_text_message(websocket)).await.is_err()
}

pub(crate) async fn read_track_snapshot(
    peer: &mut ProtocolHarnessPeer,
) -> Option<Vec<TrackBinding>> {
    let websocket = peer.websocket.as_mut()?;
    let payload = read_next_server_payload(websocket).await?;
    let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
    let messages = protocol_server_messages(&batch)?;
    let ServerMessage::Tracks(track_bindings) = messages.into_iter().next()? else {
        return None;
    };
    let commands = peer.core.on_ws_message(&payload);
    peer.run_commands(commands).await?;
    Some(track_bindings)
}

pub(crate) async fn read_track_snapshot_with_pending_negotiation(
    peer: &mut ProtocolHarnessPeer,
    pending_negotiations: usize,
) -> Option<Vec<TrackBinding>> {
    let mut track_snapshot = None;
    for _ in 0..4 {
        if track_snapshot.is_some() && peer.pending_negotiations.len() >= pending_negotiations {
            return track_snapshot;
        }
        let websocket = peer.websocket.as_mut()?;
        let payload = read_next_server_payload(websocket).await?;
        let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
        if track_snapshot.is_none()
            && let Some(messages) = protocol_server_messages(&batch)
        {
            track_snapshot = messages.into_iter().find_map(|message| match message {
                ServerMessage::Tracks(track_bindings) => Some(track_bindings),
                _ => None,
            });
        }
        let commands = peer.core.on_ws_message(&payload);
        peer.run_commands(commands).await?;
    }
    if peer.pending_negotiations.len() >= pending_negotiations {
        return track_snapshot;
    }
    None
}

pub(crate) async fn read_until_server_request(peer: &mut ProtocolHarnessPeer) -> Option<()> {
    for _ in 0..4 {
        let websocket = peer.websocket.as_mut()?;
        let payload = read_next_server_payload(websocket).await?;
        let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
        let saw_request = first_protocol_server_request(&batch).is_some();
        let commands = peer.core.on_ws_message(&payload);
        peer.run_commands(commands).await?;
        if saw_request {
            return Some(());
        }
    }
    None
}

pub(crate) async fn read_single_protocol_server_message(
    peer: &mut ProtocolHarnessPeer,
) -> Option<ServerMessage> {
    let payload = read_next_server_payload(peer.websocket.as_mut()?).await?;
    let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
    let mut messages = protocol_server_messages(&batch)?;
    if messages.len() != 1 {
        return None;
    }
    let message = messages.pop()?;
    let commands = peer.core.on_ws_message(&payload);
    peer.run_commands(commands).await?;
    Some(message)
}
