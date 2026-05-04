use o_sfu_protocol::{
    host_bridge::HostCommand,
    shared::{RecordingStateUpdate, UserPermissions},
};

use super::*;

fn recording_permissions() -> UserPermissions {
    UserPermissions {
        transcription: Some(true),
        audio_recording: Some(true),
        video_recording: Some(true),
    }
}

fn has_resolved_pending_request(
    commands: &[HostCommand],
    request_id: &RequestId,
    ok: bool,
) -> bool {
    commands.contains(&HostCommand::ResolvePendingRequest {
        request_id: request_id.clone(),
        ok,
    })
}

fn has_recording_update(
    updates: &[BundleUpdate],
    state: &RecordingState,
    stop_code: Option<ProtocolStopCode>,
) -> bool {
    updates.iter().any(|update| {
        matches!(
            update,
            BundleUpdate::ChannelInfoChange(RecordingStateUpdate {
                state: update_state,
                stop_code: update_stop_code,
            }) if *update_state == *state && *update_stop_code == stop_code
        )
    })
}

async fn drain_peer_until_recording_update(
    peer: &mut ProtocolHarnessPeer,
    state: &RecordingState,
    stop_code: Option<ProtocolStopCode>,
) -> bool {
    matches!(
        timeout(Duration::from_secs(1), async {
            loop {
                if peer
                    .pending_request_commands
                    .iter()
                    .any(|command| matches!(command, HostCommand::ResolvePendingRequest { .. }))
                    && has_recording_update(&peer.updates, state, stop_code)
                {
                    return Some(());
                }
                peer.read_server_frame().await?;
            }
        })
        .await,
        Ok(Some(()))
    )
}

pub(crate) async fn connect_protocol_recording_peer(
    server: &TestServer,
    room: &Room,
) -> Option<ProtocolHarnessPeer> {
    let token = signed_connect_claims_with_permissions(
        TEST_AUTH_KEY,
        room.uuid(),
        UserId::Integer(63),
        Some(recording_permissions()),
    )?;
    let mut peer = ProtocolHarnessPeer::default();
    peer.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &token, None)
        .await?;
    Some(peer)
}

fn pending_request_id(
    commands: &[HostCommand],
    request_kind: HostPendingRequestKind,
) -> Option<RequestId> {
    commands.iter().find_map(|command| match command {
        HostCommand::RegisterPendingRequest {
            request_id,
            request_kind: pending_kind,
        } if *pending_kind == request_kind => Some(request_id.clone()),
        _ => None,
    })
}

pub(crate) async fn assert_recording_request_roundtrip(
    peer: &mut ProtocolHarnessPeer,
    request_kind: HostPendingRequestKind,
    stop_code: Option<ProtocolStopCode>,
    expected_state: RecordingState,
) -> Option<RequestId> {
    if !drain_peer_until_recording_update(peer, &expected_state, stop_code).await {
        return None;
    }
    let request_id = pending_request_id(&peer.pending_request_commands, request_kind)?;
    has_resolved_pending_request(&peer.pending_request_commands, &request_id, true)
        .then_some(request_id)
}
