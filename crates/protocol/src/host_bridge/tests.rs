#![allow(
    clippy::expect_used,
    reason = "host-bridge fixtures must fail loudly if the production CommandBatch validator rejects them"
)]

use serde_json::json;

use super::{CoreSnapshot, HostCommand, connection_state_tag, host_commands};
use crate::{
    bundle_api::BundleConnectionState,
    core::{
        Command, CommandBatch, NegotiationKind, PendingRequestKind, ProtocolCore, ProtocolEvent,
    },
    shared::{StreamType, UserId},
    signaling::{
        RequestId, SourceDescriptor, SourceEncodingDescriptor, TrackBinding, UploadLayerPolicyRole,
    },
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
    let commands = host_commands(
        CommandBatch::try_from_vec(vec![
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
                        user_id: UserId::Integer(7),
                        stream_type: StreamType::Camera,
                        active: true,
                        source: None,
                    }],
                },
            },
            Command::DetachTrack {
                stream_type: StreamType::Screen,
            },
        ])
        .expect("valid test command batch"),
    );

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
    let commands = host_commands(
        CommandBatch::try_from_vec(vec![Command::EmitEvent {
            event: ProtocolEvent::SourceSnapshot {
                sources: vec![SourceDescriptor {
                    source_id: String::from("source-7"),
                    user_id: UserId::Integer(7),
                    stream_type: StreamType::Camera,
                    active: true,
                    mid: Some(String::from("0")),
                    encodings: vec![SourceEncodingDescriptor {
                        encoding_id: String::from("encoding-1"),
                        rid: Some(String::from("lo")),
                        max_bitrate: Some(150_000),
                        resolution_scale: Some(4),
                        max_framerate: None,
                        policy_role: Some(UploadLayerPolicyRole::Thumbnail),
                        max_temporal_layer_id: Some(1),
                    }],
                }],
            },
        }])
        .expect("valid test command batch"),
    );

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
                    "resolutionScale": 4,
                    "policyRole": "thumbnail",
                    "maxTemporalLayerId": 1
                }]
            }]
        }])
    );
}

#[test]
fn host_command_bridge_expands_peer_departure_into_track_cleanup_and_update() {
    let commands = host_commands(
        CommandBatch::try_from_vec(vec![Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                user_id: UserId::Integer(9),
            },
        }])
        .expect("valid test command batch"),
    );

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
    let command = host_commands(
        CommandBatch::try_from_vec(vec![Command::CloseWebSocket { code: 4107 }])
            .expect("valid test command batch"),
    )
    .into_iter()
    .next()
    .unwrap_or(HostCommand::ClosePeerConnection);

    let encoded = serde_json::to_value(command).unwrap_or_default();

    assert_eq!(
        encoded,
        json!({
            "kind": "closeWebSocket",
            "code": 4107
        })
    );
}
