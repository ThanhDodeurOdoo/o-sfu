use serde::Serialize;

use crate::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate,
        BundleSessionInfoSnapshotById, BundleSourceUpdate, BundleUpdate, bundle_session_info_key,
    },
    core::{
        Command, CommandBatch, ConnectionState, NegotiationKind, PendingRequestKind, ProtocolCore,
        ProtocolEvent,
    },
    shared::{AvailableFeatures, RecordingState, StreamType, UserId},
    signaling::{NegotiationUploadSlot, RequestId, TrackBinding},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreSnapshot {
    pub state: String,
    pub features: AvailableFeatures,
    pub recording_state: RecordingState,
}

impl From<&ProtocolCore> for CoreSnapshot {
    fn from(core: &ProtocolCore) -> Self {
        Self {
            state: connection_state_tag(core.state()).to_owned(),
            features: core.features().clone(),
            recording_state: core.recording_state().clone(),
        }
    }
}

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
    AttachTrack {
        mid: String,
        #[serde(rename = "streamType")]
        stream_type: StreamType,
    },
    DetachTrack {
        #[serde(rename = "streamType")]
        stream_type: StreamType,
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
    ReplaceTrackBindings {
        bindings: Vec<TrackBinding>,
    },
    RemoveSessionTracks {
        #[serde(rename = "sessionId")]
        user_id: UserId,
    },
    EmitUpdate {
        update: BundleUpdate,
    },
    BeginPendingRequest {
        #[serde(rename = "requestId")]
        request_id: RequestId,
        #[serde(rename = "requestKind")]
        request_kind: PendingRequestKind,
        #[serde(rename = "timeoutTimerId")]
        timeout_timer_id: u32,
        #[serde(rename = "timeoutMs")]
        timeout_ms: u32,
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

fn push_commands_for_event(commands: &mut Vec<HostCommand>, event: ProtocolEvent) {
    let update = match event {
        ProtocolEvent::TrackSnapshot { bindings } => {
            commands.push(HostCommand::ReplaceTrackBindings { bindings });
            return;
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
            commands.push(HostCommand::RemoveSessionTracks {
                user_id: user_id.clone(),
            });
            BundleUpdate::Disconnect(BundleDisconnectUpdate { user_id })
        }
        ProtocolEvent::Broadcast { sender_id, message } => {
            BundleUpdate::Broadcast(BundleBroadcastUpdate { sender_id, message })
        }
        ProtocolEvent::RecordingStateChanged { state } => BundleUpdate::ChannelInfoChange(state),
    };
    commands.push(HostCommand::EmitUpdate { update });
}

#[must_use]
pub fn project_commands(commands: CommandBatch) -> Vec<HostCommand> {
    let mut project_commands = Vec::with_capacity(commands.len());
    for command in commands {
        match command {
            Command::SendWebSocket(frame) => {
                project_commands.push(HostCommand::SendWebSocket { frame });
            }
            Command::SetLocalUploadIntent {
                stream_type,
                active,
            } => {
                project_commands.push(HostCommand::SetLocalUploadIntent {
                    stream_type,
                    active,
                });
            }
            Command::ApplyNegotiation {
                request_id,
                kind,
                sdp,
                upload_slots,
            } => project_commands.push(HostCommand::ApplyNegotiation {
                request_id,
                negotiation_kind: kind,
                sdp,
                upload_slots,
            }),
            Command::AttachTrack { mid, stream_type } => {
                project_commands.push(HostCommand::AttachTrack { mid, stream_type });
            }
            Command::DetachTrack { stream_type } => {
                project_commands.push(HostCommand::DetachTrack { stream_type });
            }
            Command::CreatePeerConnection => {
                project_commands.push(HostCommand::CreatePeerConnection);
            }
            Command::ClosePeerConnection => project_commands.push(HostCommand::ClosePeerConnection),
            Command::CloseWebSocket { code } => {
                project_commands.push(HostCommand::CloseWebSocket { code });
            }
            Command::EmitStateChange { state, cause } => {
                project_commands.push(HostCommand::EmitStateChange {
                    state: connection_state_tag(state).to_owned(),
                    cause,
                });
            }
            Command::EmitEvent { event } => {
                push_commands_for_event(&mut project_commands, event);
            }
            Command::BeginPendingRequest {
                request_id,
                kind,
                timeout_timer_id,
                timeout_ms,
            } => {
                project_commands.push(HostCommand::BeginPendingRequest {
                    request_id,
                    request_kind: kind,
                    timeout_timer_id,
                    timeout_ms,
                });
            }
            Command::ResolvePendingRequest { request_id, ok } => {
                project_commands.push(HostCommand::ResolvePendingRequest { request_id, ok });
            }
            Command::ScheduleTimer { id, ms } => {
                project_commands.push(HostCommand::ScheduleTimer { id, ms });
            }
            Command::CancelTimer { id } => project_commands.push(HostCommand::CancelTimer { id }),
            Command::Connect { url } => project_commands.push(HostCommand::Connect { url }),
        }
    }
    project_commands
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
