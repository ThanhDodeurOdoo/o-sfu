use serde::Serialize;

use crate::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate,
        BundleSessionInfoSnapshotById, bundle_session_info_key,
    },
    core::{
        Command, CommandBatch, ConnectionState, NegotiationKind, PendingRequestKind, ProtocolCore,
        ProtocolEvent,
    },
    shared::{AvailableFeatures, RecordingState, RecordingStateUpdate, StreamType, UserId},
    signaling::{
        NegotiationUploadSlot, RequestId, SourceDescriptor, TrackBinding, WebSocketCloseCode,
    },
};

const BROWSER_RECOVERABLE_CLOSE_CODE: u16 = 4000;
const BROWSER_CUSTOM_CLOSE_CODE_START: u16 = 3000;
const BROWSER_CUSTOM_CLOSE_CODE_END: u16 = 4999;

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
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
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
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum HostConnectionState {
    Disconnected,
    Connecting,
    Authenticated,
    Connected,
    Recovering,
    Closed,
}

impl From<ConnectionState> for HostConnectionState {
    fn from(value: ConnectionState) -> Self {
        match value {
            BundleConnectionState::Disconnected => Self::Disconnected,
            BundleConnectionState::Connecting => Self::Connecting,
            BundleConnectionState::Authenticated => Self::Authenticated,
            BundleConnectionState::Connected => Self::Connected,
            BundleConnectionState::Recovering => Self::Recovering,
            BundleConnectionState::Closed => Self::Closed,
        }
    }
}

#[cfg(feature = "ts-bindings")]
pub(crate) const HOST_COMMAND_KINDS: &[(&str, &str)] = &[
    ("CONNECT", "connect"),
    ("SEND_WEB_SOCKET", "sendWebSocket"),
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
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(tag = "name", content = "payload")]
pub enum HostUpdate {
    #[serde(rename = "broadcast")]
    Broadcast(BundleBroadcastUpdate),
    #[serde(rename = "disconnect")]
    Disconnect(BundleDisconnectUpdate),
    #[serde(rename = "info_change")]
    SessionInfoChange(BundleSessionInfoSnapshotById),
    #[serde(rename = "channel_info_change")]
    ChannelInfoChange(RecordingStateUpdate),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum HostCommand {
    SendWebSocket {
        frame: String,
    },
    ApplyNegotiation {
        #[serde(rename = "requestId")]
        #[cfg_attr(feature = "ts-bindings", ts(type = "RequestId"))]
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
        state: HostConnectionState,
        #[cfg_attr(feature = "ts-bindings", ts(optional))]
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
        update: HostUpdate,
    },
    RegisterPendingRequest {
        #[serde(rename = "requestId")]
        #[cfg_attr(feature = "ts-bindings", ts(type = "RequestId"))]
        request_id: RequestId,
        #[serde(rename = "requestKind")]
        request_kind: HostPendingRequestKind,
    },
    ResolvePendingRequest {
        #[serde(rename = "requestId")]
        #[cfg_attr(feature = "ts-bindings", ts(type = "RequestId"))]
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
                update: HostUpdate::Disconnect(BundleDisconnectUpdate { user_id }),
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
                host_commands.push(HostCommand::CloseWebSocket {
                    code: browser_close_code(code),
                });
            }
            Command::EmitStateChange { state, cause } => {
                host_commands.push(HostCommand::EmitStateChange {
                    state: state.into(),
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

fn browser_close_code(code: u16) -> u16 {
    if code == u16::from(WebSocketCloseCode::Clean)
        || (BROWSER_CUSTOM_CLOSE_CODE_START..=BROWSER_CUSTOM_CLOSE_CODE_END).contains(&code)
    {
        code
    } else {
        BROWSER_RECOVERABLE_CLOSE_CODE
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

#[must_use]
pub fn cloned_track_binding(core: &ProtocolCore, mid: &str) -> Option<TrackBinding> {
    core.track_binding(mid).cloned()
}

fn project_bundle_update(event: ProtocolEvent) -> Option<HostUpdate> {
    Some(match event {
        ProtocolEvent::PeerSnapshot { peers } => HostUpdate::SessionInfoChange(
            peers
                .into_iter()
                .map(|peer| (bundle_session_info_key(&peer.user_id), peer.info))
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::TrackSnapshot { .. } | ProtocolEvent::SourceSnapshot { .. } => return None,
        ProtocolEvent::PeerInfo { user_id, info } => HostUpdate::SessionInfoChange(
            [(bundle_session_info_key(&user_id), info)]
                .into_iter()
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::PeerLeft { user_id } => {
            HostUpdate::Disconnect(BundleDisconnectUpdate { user_id })
        }
        ProtocolEvent::Broadcast { sender_id, message } => {
            HostUpdate::Broadcast(BundleBroadcastUpdate { sender_id, message })
        }
        ProtocolEvent::RecordingStateChanged { state } => HostUpdate::ChannelInfoChange(state),
    })
}

#[cfg(test)]
mod tests;
