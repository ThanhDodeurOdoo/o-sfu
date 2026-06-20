use o_sfu_protocol::{host::HostCommand, wire::UserPermissions};

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

fn has_channel_info_update(updates: &[BundleUpdate]) -> bool {
    updates
        .iter()
        .any(|update| matches!(update, BundleUpdate::ChannelInfoChange(_)))
}

async fn drain_peer_until_pending_request_resolution(
    peer: &mut ProtocolHarnessPeer,
    request_kind: HostPendingRequestKind,
    ok: bool,
) -> Option<RequestId> {
    timeout(Duration::from_secs(1), async {
        loop {
            let Some((request_id, ..)) =
                pending_request_start(&peer.pending_request_commands, request_kind)
            else {
                peer.read_server_frame().await?;
                continue;
            };
            if has_resolved_pending_request(&peer.pending_request_commands, &request_id, ok) {
                return Some(request_id);
            }
            peer.read_server_frame().await?;
        }
    })
    .await
    .unwrap_or_default()
}

pub(crate) async fn connect_protocol_recording_peer(
    server: &TestServer,
    room: &Room,
) -> Option<ProtocolHarnessPeer> {
    let token = signed_connect_claims_with_permissions(
        TEST_ROOM_KEY,
        room.uuid(),
        UserId::Integer(63),
        Some(recording_permissions()),
    )?;
    let mut peer = ProtocolHarnessPeer::default();
    peer.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &token, None)
        .await?;
    Some(peer)
}

fn pending_request_start(
    commands: &[HostCommand],
    request_kind: HostPendingRequestKind,
) -> Option<(RequestId, u32, u32)> {
    commands.iter().find_map(|command| match command {
        HostCommand::BeginPendingRequest {
            request_id,
            request_kind: pending_kind,
            timeout_timer_id,
            timeout_ms,
            ..
        } if *pending_kind == request_kind => {
            Some((request_id.clone(), *timeout_timer_id, *timeout_ms))
        }
        _ => None,
    })
}

pub(crate) async fn assert_recording_request_rejected(
    peer: &mut ProtocolHarnessPeer,
    request_kind: HostPendingRequestKind,
) -> Option<RequestId> {
    let (_, timer_id, timeout_ms) =
        pending_request_start(&peer.pending_request_commands, request_kind)?;
    if peer.timers.get(&timer_id) != Some(&timeout_ms) {
        return None;
    }
    let request_id = drain_peer_until_pending_request_resolution(peer, request_kind, false).await?;
    (!has_channel_info_update(&peer.updates)).then_some(request_id)
}
