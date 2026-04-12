use serde::Serialize;

use crate::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate,
        BundleSessionInfoSnapshotById, BundleUpdate, bundle_session_info_key,
    },
    core::{
        Command, ConnectionState, NegotiationKind, PendingRequestKind, ProtocolCore, ProtocolEvent,
    },
    shared::{AvailableFeatures, RecordingState, StreamType},
    signaling::{RequestId, TrackBinding},
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum HostCommand {
    SendWebSocket {
        frame: String,
    },
    ApplyNegotiation {
        #[serde(rename = "requestId")]
        request_id: RequestId,
        #[serde(rename = "negotiationKind")]
        negotiation_kind: HostNegotiationKind,
        sdp: String,
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

impl From<Command> for HostCommand {
    fn from(command: Command) -> Self {
        match command {
            Command::SendWebSocket(frame) => Self::SendWebSocket { frame },
            Command::ApplyNegotiation {
                request_id,
                kind,
                sdp,
            } => Self::ApplyNegotiation {
                request_id,
                negotiation_kind: kind.into(),
                sdp,
            },
            Command::AttachTrack { mid, stream_type } => Self::AttachTrack { mid, stream_type },
            Command::DetachTrack { stream_type } => Self::DetachTrack { stream_type },
            Command::CreatePeerConnection => Self::CreatePeerConnection,
            Command::ClosePeerConnection => Self::ClosePeerConnection,
            Command::CloseWebSocket { code } => Self::CloseWebSocket { code },
            Command::EmitStateChange { state, cause } => Self::EmitStateChange {
                state: connection_state_tag(state).to_owned(),
                cause,
            },
            Command::EmitEvent { event } => Self::EmitUpdate {
                update: project_bundle_update(event),
            },
            Command::RegisterPendingRequest { request_id, kind } => Self::RegisterPendingRequest {
                request_id,
                request_kind: kind.into(),
            },
            Command::ResolvePendingRequest { request_id, ok } => {
                Self::ResolvePendingRequest { request_id, ok }
            }
            Command::ScheduleTimer { id, ms } => Self::ScheduleTimer { id, ms },
            Command::CancelTimer { id } => Self::CancelTimer { id },
            Command::Connect { url } => Self::Connect { url },
        }
    }
}

#[must_use]
pub fn host_commands(commands: Vec<Command>) -> Vec<HostCommand> {
    commands.into_iter().map(HostCommand::from).collect()
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

fn project_bundle_update(event: ProtocolEvent) -> BundleUpdate {
    match event {
        ProtocolEvent::PeerSnapshot { peers } => BundleUpdate::SessionInfoChange(
            peers
                .into_iter()
                .map(|peer| (bundle_session_info_key(&peer.session_id), peer.info))
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::PeerInfo { session_id, info } => BundleUpdate::SessionInfoChange(
            [(bundle_session_info_key(&session_id), info)]
                .into_iter()
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::PeerLeft { session_id } => {
            BundleUpdate::Disconnect(BundleDisconnectUpdate { session_id })
        }
        ProtocolEvent::Broadcast { sender_id, message } => {
            BundleUpdate::Broadcast(BundleBroadcastUpdate { sender_id, message })
        }
        ProtocolEvent::RecordingStateChanged { state } => BundleUpdate::ChannelInfoChange(state),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{CoreSnapshot, HostCommand, connection_state_tag, host_commands};
    use crate::{
        bundle_api::BundleConnectionState,
        core::{Command, NegotiationKind, PendingRequestKind, ProtocolCore},
        shared::StreamType,
        signaling::RequestId,
    };

    #[test]
    fn connection_state_tag_matches_public_client_surface() {
        assert_eq!(
            connection_state_tag(BundleConnectionState::Disconnected),
            "disconnected"
        );
        assert_eq!(
            connection_state_tag(BundleConnectionState::Closed),
            "closed"
        );
    }

    #[test]
    fn core_snapshot_uses_public_state_labels() {
        let core = ProtocolCore::new();

        let snapshot = CoreSnapshot::from(&core);

        assert_eq!(snapshot.state, "disconnected");
        assert!(!snapshot.features.rtc);
        assert_eq!(snapshot.recording_state.recording, None);
    }

    #[test]
    fn host_command_bridge_converts_commands_to_camel_case_payloads() {
        let commands = host_commands(vec![
            Command::ApplyNegotiation {
                request_id: RequestId::new("7"),
                kind: NegotiationKind::Renegotiate,
                sdp: String::from("v=0"),
            },
            Command::EmitStateChange {
                state: BundleConnectionState::Connected,
                cause: Some(String::from("recovered")),
            },
            Command::RegisterPendingRequest {
                request_id: RequestId::new("11"),
                kind: PendingRequestKind::StartRecording,
            },
            Command::DetachTrack {
                stream_type: StreamType::Screen,
            },
        ]);

        let encoded = serde_json::to_value(commands).unwrap_or_default();

        assert_eq!(
            encoded,
            json!([
                {
                    "kind": "applyNegotiation",
                    "requestId": "7",
                    "negotiationKind": "renegotiate",
                    "sdp": "v=0"
                },
                {
                    "kind": "emitStateChange",
                    "state": "connected",
                    "cause": "recovered"
                },
                {
                    "kind": "registerPendingRequest",
                    "requestId": "11",
                    "requestKind": "startRecording"
                },
                {
                    "kind": "detachTrack",
                    "streamType": "screen"
                }
            ])
        );
    }

    #[test]
    fn host_command_bridge_preserves_simple_commands() {
        let command = HostCommand::from(Command::CloseWebSocket { code: 4002 });

        let encoded = serde_json::to_value(command).unwrap_or_default();

        assert_eq!(
            encoded,
            json!({
                "kind": "closeWebSocket",
                "code": 4002
            })
        );
    }
}
