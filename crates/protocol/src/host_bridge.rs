use serde::Serialize;

use crate::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate,
        BundleRemoteMediaUpdate, BundleSessionInfoSnapshotById, BundleSourceUpdate, BundleUpdate,
        bundle_session_info_key,
    },
    core::{
        Command, CommandBatch, ConnectionState, NegotiationKind, PendingRequest, ProtocolEvent,
        ProtocolRequestResult,
    },
    shared::StreamType,
    signaling::{NegotiationUploadSlot, RequestId},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum HostCommand {
    SendWebSocket {
        frame: String,
    },
    SetLocalUploadIntent {
        #[serde(rename = "streamType")]
        stream_type: StreamType,
        active: bool,
    },
    ApplyNegotiation {
        #[serde(rename = "requestId")]
        request_id: RequestId,
        #[serde(rename = "negotiationKind")]
        negotiation_kind: NegotiationKind,
        sdp: String,
        #[serde(rename = "uploadSlots")]
        upload_slots: Vec<NegotiationUploadSlot>,
    },
    CreatePeerConnection,
    ClosePeerConnection,
    CloseWebSocket {
        code: u16,
    },
    EmitStateChange {
        state: String,
        cause: Option<String>,
    },
    EmitUpdate {
        update: BundleUpdate,
    },
    ResolvePendingRequest {
        #[serde(rename = "requestId")]
        request_id: RequestId,
        ok: bool,
    },
    ScheduleTimer {
        id: u32,
        ms: u32,
    },
    CancelTimer {
        id: u32,
    },
    Connect {
        url: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostRequestResult {
    pub commands: Vec<HostCommand>,
    pub pending_request: Option<PendingRequest>,
}

fn push_host_commands_for_event(host_commands: &mut Vec<HostCommand>, event: ProtocolEvent) {
    let update = match event {
        ProtocolEvent::TrackSnapshot { bindings } => {
            BundleUpdate::RemoteMedia(BundleRemoteMediaUpdate { bindings })
        }
        ProtocolEvent::SourceSnapshot { sources } => {
            BundleUpdate::Source(BundleSourceUpdate { sources })
        }
        ProtocolEvent::PeerSnapshot { peers } => BundleUpdate::SessionInfoChange(
            peers
                .into_iter()
                .map(|peer| (bundle_session_info_key(&peer.user_id), peer.info))
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::PeerInfo { user_id, info } => BundleUpdate::SessionInfoChange(
            BundleSessionInfoSnapshotById::from([(bundle_session_info_key(&user_id), info)]),
        ),
        ProtocolEvent::PeerLeft { user_id } => {
            BundleUpdate::Disconnect(BundleDisconnectUpdate { user_id })
        }
        ProtocolEvent::Broadcast { sender_id, message } => {
            BundleUpdate::Broadcast(BundleBroadcastUpdate { sender_id, message })
        }
        ProtocolEvent::RecordingStateChanged { state } => BundleUpdate::ChannelInfoChange(state),
    };
    host_commands.push(HostCommand::EmitUpdate { update });
}

#[must_use]
pub fn project_commands(core_commands: CommandBatch) -> Vec<HostCommand> {
    let mut host_commands = Vec::with_capacity(core_commands.len());
    for command in core_commands {
        match command {
            Command::SendWebSocket(frame) => {
                host_commands.push(HostCommand::SendWebSocket { frame });
            }
            Command::SetLocalUploadIntent {
                stream_type,
                active,
            } => {
                host_commands.push(HostCommand::SetLocalUploadIntent {
                    stream_type,
                    active,
                });
            }
            Command::ApplyNegotiation {
                request_id,
                kind,
                sdp,
                upload_slots,
            } => host_commands.push(HostCommand::ApplyNegotiation {
                request_id,
                negotiation_kind: kind,
                sdp,
                upload_slots,
            }),
            Command::CreatePeerConnection => {
                host_commands.push(HostCommand::CreatePeerConnection);
            }
            Command::ClosePeerConnection => host_commands.push(HostCommand::ClosePeerConnection),
            Command::CloseWebSocket { code } => {
                host_commands.push(HostCommand::CloseWebSocket { code });
            }
            Command::EmitStateChange { state, cause } => {
                host_commands.push(HostCommand::EmitStateChange {
                    state: connection_state_tag(state).to_owned(),
                    cause,
                });
            }
            Command::EmitEvent { event } => {
                push_host_commands_for_event(&mut host_commands, event);
            }
            Command::ResolvePendingRequest { request_id, ok } => {
                host_commands.push(HostCommand::ResolvePendingRequest { request_id, ok });
            }
            Command::ScheduleTimer { id, ms } => {
                host_commands.push(HostCommand::ScheduleTimer { id, ms });
            }
            Command::CancelTimer { id } => host_commands.push(HostCommand::CancelTimer { id }),
            Command::Connect { url } => host_commands.push(HostCommand::Connect { url }),
        }
    }
    host_commands
}

#[must_use]
pub fn project_request_result(result: ProtocolRequestResult) -> HostRequestResult {
    HostRequestResult {
        commands: project_commands(result.commands),
        pending_request: result.pending_request,
    }
}

#[must_use]
pub fn connection_state_tag(state: ConnectionState) -> &'static str {
    match state {
        BundleConnectionState::Disconnected => "disconnected",
        BundleConnectionState::Connecting => "connecting",
        BundleConnectionState::Authenticated => "authenticated",
        BundleConnectionState::Connected => "connected",
        BundleConnectionState::Recovering => "recovering",
        BundleConnectionState::Closed => "closed",
    }
}

#[cfg(test)]
#[path = "host_bridge/TESTS/mod.rs"]
mod tests;
