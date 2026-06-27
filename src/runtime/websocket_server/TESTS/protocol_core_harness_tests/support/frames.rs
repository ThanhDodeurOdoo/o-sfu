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
    read_track_snapshot_until_pending_negotiations(peer, 0).await
}

pub(crate) async fn read_track_snapshot_until_pending_negotiations(
    peer: &mut ProtocolHarnessPeer,
    pending_negotiations: usize,
) -> Option<Vec<TrackBinding>> {
    read_media_snapshot_until_pending_negotiations(peer, pending_negotiations)
        .await
        .map(|(tracks, _sources)| tracks)
}

pub(crate) async fn read_media_snapshot(
    peer: &mut ProtocolHarnessPeer,
) -> Option<(Vec<TrackBinding>, Vec<SourceDescriptor>)> {
    read_media_snapshot_until_pending_negotiations(peer, 0).await
}

pub(crate) async fn read_media_snapshot_until_pending_negotiations(
    peer: &mut ProtocolHarnessPeer,
    pending_negotiations: usize,
) -> Option<(Vec<TrackBinding>, Vec<SourceDescriptor>)> {
    let mut track_snapshot = None;
    let mut source_snapshot = None;
    for _ in 0..4 {
        if track_snapshot.is_some()
            && source_snapshot.is_some()
            && peer.pending_negotiations.len() >= pending_negotiations
        {
            return track_snapshot.zip(source_snapshot);
        }
        let websocket = peer.websocket.as_mut()?;
        let payload = read_next_server_payload(websocket).await?;
        let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
        if let Some(messages) = protocol_server_messages(&batch) {
            for message in messages {
                match message {
                    ServerMessage::Tracks(track_bindings) if track_snapshot.is_none() => {
                        track_snapshot = Some(track_bindings);
                    }
                    ServerMessage::Sources(sources) if source_snapshot.is_none() => {
                        source_snapshot = Some(sources);
                    }
                    _ => {}
                }
            }
        }
        let commands = peer.core.on_ws_message(&payload);
        peer.run_commands(commands).await?;
    }
    if peer.pending_negotiations.len() >= pending_negotiations {
        return track_snapshot.zip(source_snapshot);
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
