use serde_json::json;

use super::project_protocol_event;
use crate::{
    core::{Command, ProtocolEvent},
    shared::{RecordingState, RecordingStateUpdate, StopCode, StreamType, UserId, UserInfo},
    signaling::{PeerSnapshot, TrackBinding},
};

#[test]
fn peer_departure_serializes_as_disconnect_update() {
    let command = Command::EmitEvent {
        event: ProtocolEvent::PeerLeft {
            user_id: UserId::Integer(9),
        },
    };

    assert_eq!(
        serde_json::to_value(command).unwrap_or_default(),
        json!({
            "kind": "emitUpdate",
            "update": {
                "name": "disconnect",
                "payload": {
                    "sessionId": 9
                }
            }
        })
    );
}

#[test]
fn borrowed_event_projection_matches_owned_bundle_shape() {
    let peer_snapshot = ProtocolEvent::PeerSnapshot {
        peers: vec![
            PeerSnapshot {
                user_id: UserId::Integer(5),
                info: UserInfo {
                    is_talking: Some(true),
                    ..UserInfo::default()
                },
            },
            PeerSnapshot {
                user_id: UserId::String("5".to_owned()),
                info: UserInfo {
                    is_camera_on: Some(true),
                    ..UserInfo::default()
                },
            },
        ],
    };
    let events = [
        ProtocolEvent::TrackSnapshot {
            bindings: vec![TrackBinding {
                mid: "video-1".to_owned(),
                user_id: UserId::Integer(7),
                stream_type: StreamType::Camera,
                active: true,
            }],
        },
        peer_snapshot.clone(),
        ProtocolEvent::PeerInfo {
            user_id: UserId::String("guest-8".to_owned()),
            info: UserInfo {
                is_raising_hand: Some(true),
                ..UserInfo::default()
            },
        },
        ProtocolEvent::PeerLeft {
            user_id: UserId::Integer(9),
        },
        ProtocolEvent::Broadcast {
            sender_id: UserId::Integer(10),
            message: json!({ "body": ["hello", 3] }),
        },
        ProtocolEvent::RecordingStateChanged {
            state: RecordingStateUpdate {
                state: RecordingState {
                    recording: Some(false),
                    audio: Some(true),
                    transcription: Some(false),
                    video: Some(true),
                },
                stop_code: Some(StopCode::UserRequest),
            },
        },
    ];

    for event in events {
        let command = Command::EmitEvent {
            event: event.clone(),
        };
        let actual = serde_json::to_value(command).unwrap_or_default();
        let expected = serde_json::to_value(project_protocol_event(event)).unwrap_or_default();
        assert_eq!(actual.get("update"), Some(&expected));
    }

    let command = Command::EmitEvent {
        event: peer_snapshot,
    };
    let actual = serde_json::to_value(command).unwrap_or_default();
    let collision_value = actual
        .get("update")
        .and_then(|update| update.get("payload"))
        .and_then(|payload| payload.get("5"));
    let expected = json!({ "isCameraOn": true });
    assert_eq!(collision_value, Some(&expected));
}
