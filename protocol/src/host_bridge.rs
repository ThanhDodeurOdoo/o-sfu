use serde::Serialize;

use crate::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate,
        BundleSessionInfoSnapshotById, BundleUpdate, bundle_session_info_key,
    },
    core::{
        Command, ConnectionState, NegotiationKind, PendingRequestKind, ProtocolCore, ProtocolEvent,
    },
    shared::{AvailableFeatures, RecordingState, SessionId, StreamType},
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
        session_id: SessionId,
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
        ProtocolEvent::PeerLeft { session_id } => vec![
            HostCommand::RemoveSessionTracks {
                session_id: session_id.clone(),
            },
            HostCommand::EmitUpdate {
                update: BundleUpdate::Disconnect(BundleDisconnectUpdate { session_id }),
            },
        ],
        other_event => project_bundle_update(other_event)
            .into_iter()
            .map(|update| HostCommand::EmitUpdate { update })
            .collect(),
    }
}

#[must_use]
pub fn host_commands(commands: Vec<Command>) -> Vec<HostCommand> {
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
                .map(|peer| (bundle_session_info_key(&peer.session_id), peer.info))
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::TrackSnapshot { .. } | ProtocolEvent::SourceSnapshot { .. } => return None,
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
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{CoreSnapshot, HostCommand, connection_state_tag, host_commands};
    use crate::{
        bundle_api::BundleConnectionState,
        core::{Command, NegotiationKind, PendingRequestKind, ProtocolCore, ProtocolEvent},
        shared::{SessionId, StreamType},
        signaling::{RequestId, SourceDescriptor, SourceEncodingDescriptor, TrackBinding},
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
                upload_slots: Vec::new(),
            },
            Command::EmitStateChange {
                state: BundleConnectionState::Connected,
                cause: Some(String::from("recovered")),
            },
            Command::RegisterPendingRequest {
                request_id: RequestId::new("11"),
                kind: PendingRequestKind::StartRecording,
            },
            Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: vec![TrackBinding {
                        mid: String::from("0"),
                        session_id: SessionId::Integer(7),
                        stream_type: StreamType::Camera,
                        active: true,
                        source: None,
                    }],
                },
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
                    "sdp": "v=0",
                    "uploadSlots": []
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
                    "kind": "replaceTrackBindings",
                    "bindings": [
                        {
                            "mid": "0",
                            "sessionId": 7,
                            "type": "camera",
                            "active": true
                        }
                    ]
                },
                {
                    "kind": "detachTrack",
                    "streamType": "screen"
                }
            ])
        );
    }

    #[test]
    fn host_command_bridge_projects_source_snapshots() {
        let commands = host_commands(vec![Command::EmitEvent {
            event: ProtocolEvent::SourceSnapshot {
                sources: vec![SourceDescriptor {
                    source_id: String::from("source-7"),
                    session_id: SessionId::Integer(7),
                    stream_type: StreamType::Camera,
                    active: true,
                    mid: Some(String::from("0")),
                    encodings: vec![SourceEncodingDescriptor {
                        encoding_id: String::from("encoding-1"),
                        rid: Some(String::from("lo")),
                        max_bitrate: Some(150_000),
                        max_temporal_layer_id: Some(1),
                    }],
                }],
            },
        }]);

        assert_eq!(
            serde_json::to_value(commands).unwrap_or_default(),
            json!([{
                "kind": "replaceSourceDescriptors",
                "sources": [{
                    "sourceId": "source-7",
                    "sessionId": 7,
                    "type": "camera",
                    "active": true,
                    "mid": "0",
                    "encodings": [{
                        "encodingId": "encoding-1",
                        "rid": "lo",
                        "maxBitrate": 150_000,
                        "maxTemporalLayerId": 1
                    }]
                }]
            }])
        );
    }

    #[test]
    fn host_command_bridge_expands_peer_departure_into_track_cleanup_and_update() {
        let commands = host_commands(vec![Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                session_id: SessionId::Integer(9),
            },
        }]);

        let encoded = serde_json::to_value(commands).unwrap_or_default();

        assert_eq!(
            encoded,
            json!([
                {
                    "kind": "removeSessionTracks",
                    "sessionId": 9
                },
                {
                    "kind": "emitUpdate",
                    "update": {
                        "name": "disconnect",
                        "payload": {
                            "sessionId": 9
                        }
                    }
                }
            ])
        );
    }

    #[test]
    fn host_command_bridge_preserves_simple_commands() {
        let command = host_commands(vec![Command::CloseWebSocket { code: 4002 }])
            .into_iter()
            .next()
            .unwrap_or(HostCommand::ClosePeerConnection);

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
