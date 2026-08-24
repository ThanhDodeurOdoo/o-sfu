use super::*;

fn track_snapshot_in_batch(batch: &EnvelopeBatch) -> Option<Vec<TrackBinding>> {
    protocol_server_messages(batch)?
        .into_iter()
        .find_map(|message| match message {
            ServerMessage::Tracks(bindings) => Some(bindings),
            _ => None,
        })
}

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
    for _ in 0..4 {
        let websocket = peer.websocket.as_mut()?;
        let payload = read_next_server_payload(websocket).await?;
        let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
        let tracks = track_snapshot_in_batch(&batch);
        let commands = peer.core.on_ws_message(&payload);
        peer.run_commands(commands).await?;
        if let Some(tracks) = tracks {
            return Some(tracks);
        }
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
