use serde::Serialize;

use crate::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate,
        BundleSessionInfoSnapshotById, BundleUpdate, bundle_session_info_key,
    },
    core::{
        Command, CommandBatch, ConnectionState, NegotiationKind, PendingRequestKind, ProtocolCore,
        ProtocolEvent,
    },
    shared::{AvailableFeatures, RecordingState, StreamType, UserId},
    signaling::{NegotiationUploadSlot, RequestId, SourceDescriptor, TrackBinding},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum HostNegotiationKind {
    Offer,
    Renegotiate,
}

impl From<NegotiationKind> for HostNegotiationKind {
    fn from(value: NegotiationKind) -> Self {
        match value {
            NegotiationKind::Offer => Self::Offer,
            NegotiationKind::Renegotiate => Self::Renegotiate,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum HostPendingRequestKind {
    StartRecording,
    StopRecording,
}

impl From<PendingRequestKind> for HostPendingRequestKind {
    fn from(value: PendingRequestKind) -> Self {
        match value {
            PendingRequestKind::StartRecording => Self::StartRecording,
            PendingRequestKind::StopRecording => Self::StopRecording,
        }
    }
}

#[cfg(feature = "ts-bindings")]
pub(crate) const HOST_COMMAND_KINDS: &[(&str, &str)] = &[
    ("CONNECT", "connect"),
    ("SEND_WEB_SOCKET", "sendWebSocket"),
    ("SET_LOCAL_UPLOAD_INTENT", "setLocalUploadIntent"),
    ("CLOSE_WEB_SOCKET", "closeWebSocket"),
    ("APPLY_NEGOTIATION", "applyNegotiation"),
    ("CREATE_PEER_CONNECTION", "createPeerConnection"),
    ("CLOSE_PEER_CONNECTION", "closePeerConnection"),
    ("ATTACH_TRACK", "attachTrack"),
    ("DETACH_TRACK", "detachTrack"),
    ("REPLACE_TRACK_BINDINGS", "replaceTrackBindings"),
    ("REPLACE_SOURCE_DESCRIPTORS", "replaceSourceDescriptors"),
    ("REMOVE_SESSION_TRACKS", "removeSessionTracks"),
    ("EMIT_STATE_CHANGE", "emitStateChange"),
    ("EMIT_UPDATE", "emitUpdate"),
    ("REGISTER_PENDING_REQUEST", "registerPendingRequest"),
    ("RESOLVE_PENDING_REQUEST", "resolvePendingRequest"),
    ("SCHEDULE_TIMER", "scheduleTimer"),
    ("CANCEL_TIMER", "cancelTimer"),
];

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
        negotiation_kind: HostNegotiationKind,
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
    ReplaceSourceDescriptors {
        sources: Vec<SourceDescriptor>,
    },
    RemoveSessionTracks {
        #[serde(rename = "sessionId")]
        user_id: UserId,
    },
    EmitUpdate {
        update: BundleUpdate,
    },
    RegisterPendingRequest {
        #[serde(rename = "requestId")]
        request_id: RequestId,
        #[serde(rename = "requestKind")]
        request_kind: HostPendingRequestKind,
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

fn host_commands_for_event(event: ProtocolEvent) -> Vec<HostCommand> {
    match event {
        ProtocolEvent::TrackSnapshot { bindings } => {
            vec![HostCommand::ReplaceTrackBindings { bindings }]
        }
        ProtocolEvent::SourceSnapshot { sources } => {
            vec![HostCommand::ReplaceSourceDescriptors { sources }]
        }
        ProtocolEvent::PeerLeft { user_id } => vec![
            HostCommand::RemoveSessionTracks {
                user_id: user_id.clone(),
            },
            HostCommand::EmitUpdate {
                update: BundleUpdate::Disconnect(BundleDisconnectUpdate { user_id }),
            },
        ],
        other_event => project_bundle_update(other_event)
            .into_iter()
            .map(|update| HostCommand::EmitUpdate { update })
            .collect(),
    }
}

#[must_use]
pub fn host_commands(commands: CommandBatch) -> Vec<HostCommand> {
    let mut host_commands = Vec::new();
    for command in commands {
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
                negotiation_kind: kind.into(),
                sdp,
                upload_slots,
            }),
            Command::AttachTrack { mid, stream_type } => {
                host_commands.push(HostCommand::AttachTrack { mid, stream_type });
            }
            Command::DetachTrack { stream_type } => {
                host_commands.push(HostCommand::DetachTrack { stream_type });
            }
            Command::CreatePeerConnection => host_commands.push(HostCommand::CreatePeerConnection),
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
            Command::EmitEvent { event } => host_commands.extend(host_commands_for_event(event)),
            Command::RegisterPendingRequest { request_id, kind } => {
                host_commands.push(HostCommand::RegisterPendingRequest {
                    request_id,
                    request_kind: kind.into(),
                });
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

#[must_use]
pub fn cloned_track_binding(core: &ProtocolCore, mid: &str) -> Option<TrackBinding> {
    core.track_binding(mid).cloned()
}

fn project_bundle_update(event: ProtocolEvent) -> Option<BundleUpdate> {
    Some(match event {
        ProtocolEvent::PeerSnapshot { peers } => BundleUpdate::SessionInfoChange(
            peers
                .into_iter()
                .map(|peer| (bundle_session_info_key(&peer.user_id), peer.info))
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::TrackSnapshot { .. } | ProtocolEvent::SourceSnapshot { .. } => return None,
        ProtocolEvent::PeerInfo { user_id, info } => BundleUpdate::SessionInfoChange(
            [(bundle_session_info_key(&user_id), info)]
                .into_iter()
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::PeerLeft { user_id } => {
            BundleUpdate::Disconnect(BundleDisconnectUpdate { user_id })
        }
        ProtocolEvent::Broadcast { sender_id, message } => {
            BundleUpdate::Broadcast(BundleBroadcastUpdate { sender_id, message })
        }
        ProtocolEvent::RecordingStateChanged { state } => BundleUpdate::ChannelInfoChange(state),
    })
}

#[cfg(test)]
mod tests;
