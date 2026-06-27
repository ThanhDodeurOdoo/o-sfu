#![allow(
    clippy::expect_used,
    reason = "host-bridge fixtures must fail loudly if the production CommandBatch validator rejects them"
)]

use serde_json::json;

use super::{HostCommand, connection_state_tag, project_commands, project_request_result};
use crate::{
    bundle_api::BundleConnectionState,
    core::{
        Command, CommandBatch, NegotiationKind, PendingRequest, PendingRequestKind, ProtocolEvent,
        ProtocolRequestResult, test_support::command_batch,
    },
    shared::{StreamType, UserId},
    signaling::{RequestId, SourceDescriptor, TrackBinding},
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
fn host_command_bridge_projects_commands_to_browser_payloads() {
    let host_commands = project_commands(
        command_batch(vec![
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
            Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: vec![TrackBinding {
                        mid: String::from("0"),
                        user_id: UserId::Integer(7),
                        stream_type: StreamType::Camera,
                        active: true,
                    }],
                },
            },
            Command::SetLocalUploadIntent {
                stream_type: StreamType::Camera,
                active: true,
            },
        ])
        .expect("valid test command batch"),
    );

    let encoded = serde_json::to_value(host_commands).unwrap_or_default();

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
                "kind": "emitUpdate",
                "update": {
                    "name": "remote_media",
                    "payload": {
                        "bindings": [{
                            "mid": "0",
                            "sessionId": 7,
                            "type": "camera",
                            "active": true
                        }]
                    },
                }
            },
            {
                "kind": "setLocalUploadIntent",
                "streamType": "camera",
                "active": true
            }
        ])
    );
}

#[test]
fn host_request_result_serializes_pending_request_fields() {
    let result = project_request_result(ProtocolRequestResult {
        commands: CommandBatch::default(),
        pending_request: Some(PendingRequest {
            request_id: RequestId::new("11"),
            kind: PendingRequestKind::StartRecording,
            timeout_timer_id: 10_000,
            timeout_ms: 5_000,
        }),
    });

    assert_eq!(
        serde_json::to_value(result).unwrap_or_default(),
        json!({
            "commands": [],
            "pendingRequest": {
                "requestId": "11",
                "kind": "startRecording",
                "timeoutTimerId": 10_000,
                "timeoutMs": 5_000
            }
        })
    );
}

#[test]
fn host_command_bridge_emits_source_update_for_source_snapshot() {
    let host_commands = project_commands(
        command_batch(vec![Command::EmitEvent {
            event: ProtocolEvent::SourceSnapshot {
                sources: vec![SourceDescriptor {
                    source_id: String::from("source-7"),
                    user_id: UserId::Integer(7),
                    stream_type: StreamType::Camera,
                    active: true,
                    mid: None,
                    encodings: Vec::new(),
                }],
            },
        }])
        .expect("valid test command batch"),
    );

    assert_eq!(
        serde_json::to_value(host_commands).unwrap_or_default(),
        json!([{
            "kind": "emitUpdate",
            "update": {
                "name": "source",
                "payload": {
                    "sources": [{
                        "sourceId": "source-7",
                        "sessionId": 7,
                        "type": "camera",
                        "active": true,
                        "encodings": []
                    }]
                }
            }
        }])
    );
}

#[test]
fn host_command_bridge_emits_disconnect_update_for_peer_departure() {
    let host_commands = project_commands(
        command_batch(vec![Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                user_id: UserId::Integer(9),
            },
        }])
        .expect("valid test command batch"),
    );

    let encoded = serde_json::to_value(host_commands).unwrap_or_default();

    assert_eq!(
        encoded,
        json!([{
            "kind": "emitUpdate",
            "update": {
                "name": "disconnect",
                "payload": {
                    "sessionId": 9
                }
            }
        }])
    );
}

#[test]
fn host_command_bridge_preserves_simple_commands() {
    let host_command = project_commands(
        command_batch(vec![Command::CloseWebSocket { code: 4107 }])
            .expect("valid test command batch"),
    )
    .into_iter()
    .next()
    .unwrap_or(HostCommand::ClosePeerConnection);

    let encoded = serde_json::to_value(host_command).unwrap_or_default();

    assert_eq!(
        encoded,
        json!({
            "kind": "closeWebSocket",
            "code": 4107
        })
    );
}
