use std::{collections::BTreeMap, fmt::Debug};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::{
    BundleBroadcastUpdate, BundleConnectionState, BundleRemoteMediaUpdate, BundleSourceUpdate,
    BundleStateChange, BundleTrackUpdate, BundleUpdate, bundle_session_info_key,
};
use crate::{
    shared::{RecordingState, RecordingStateUpdate, StopCode, StreamType, UserId, UserInfo},
    signaling::{SourceDescriptor, TrackBinding},
};

fn assert_round_trip<T>(value: &T, expected_json: Value) -> serde_json::Result<()>
where
    T: Debug + DeserializeOwned + PartialEq + Serialize,
{
    assert_eq!(serde_json::to_value(value)?, expected_json);
    assert_eq!(serde_json::from_value::<T>(expected_json)?, *value);
    Ok(())
}

#[test]
fn bundle_connection_states_round_trip() -> serde_json::Result<()> {
    for (state, expected_json) in [
        (BundleConnectionState::Disconnected, json!("disconnected")),
        (BundleConnectionState::Connecting, json!("connecting")),
        (BundleConnectionState::Authenticated, json!("authenticated")),
        (BundleConnectionState::Connected, json!("connected")),
        (BundleConnectionState::Recovering, json!("recovering")),
        (BundleConnectionState::Closed, json!("closed")),
    ] {
        assert_round_trip(&state, expected_json)?;
    }

    assert_round_trip(
        &BundleStateChange {
            state: BundleConnectionState::Closed,
            cause: Some(String::from("full")),
        },
        json!({
            "state": "closed",
            "cause": "full"
        }),
    )
}

#[test]
fn bundle_updates_round_trip() -> serde_json::Result<()> {
    let track_update = BundleUpdate::Track(BundleTrackUpdate {
        stream_type: StreamType::Camera,
        user_id: UserId::Integer(9),
        track: json!({
            "id": "camera-track",
            "kind": "video"
        }),
        active: true,
    });
    assert_round_trip(
        &track_update,
        json!({
            "name": "track",
            "payload": {
                "type": "camera",
                "sessionId": 9,
                "track": {
                    "id": "camera-track",
                    "kind": "video"
                },
                "active": true
            }
        }),
    )?;

    let broadcast = BundleUpdate::Broadcast(BundleBroadcastUpdate {
        sender_id: UserId::String("guest-7".to_owned()),
        message: json!("hello"),
    });
    assert_round_trip(
        &broadcast,
        json!({
            "name": "broadcast",
            "payload": {
                "senderId": "guest-7",
                "message": "hello"
            }
        }),
    )?;

    let session_info = BundleUpdate::SessionInfoChange(BTreeMap::from([(
        bundle_session_info_key(&UserId::Integer(5)),
        UserInfo {
            is_talking: Some(false),
            is_featured: None,
            is_camera_on: Some(true),
            is_screen_sharing_on: None,
            is_self_muted: None,
            is_deaf: None,
            is_raising_hand: None,
        },
    )]));
    assert_round_trip(
        &session_info,
        json!({
            "name": "info_change",
            "payload": {
                "5": {
                    "isTalking": false,
                    "isCameraOn": true
                }
            }
        }),
    )?;

    let channel_info = BundleUpdate::ChannelInfoChange(RecordingStateUpdate {
        state: RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        },
        stop_code: Some(StopCode::UserRequest),
    });
    assert_round_trip(
        &channel_info,
        json!({
            "name": "channel_info_change",
            "payload": {
                "state": {
                    "recording": false,
                    "audio": false,
                    "transcription": false,
                    "video": false
                },
                "stopCode": "user_request"
            }
        }),
    )
}

#[test]
fn bundle_source_update_round_trips() -> serde_json::Result<()> {
    let source_update = BundleUpdate::Source(BundleSourceUpdate {
        sources: vec![SourceDescriptor {
            source_id: String::from("source-7"),
            user_id: UserId::Integer(7),
            stream_type: StreamType::Camera,
            active: true,
            mid: None,
            encodings: Vec::new(),
        }],
    });

    assert_round_trip(
        &source_update,
        json!({
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
        }),
    )
}

#[test]
fn bundle_remote_media_update_round_trips() -> serde_json::Result<()> {
    let remote_media = BundleUpdate::RemoteMedia(BundleRemoteMediaUpdate {
        bindings: vec![TrackBinding {
            mid: String::from("0"),
            user_id: UserId::Integer(7),
            stream_type: StreamType::Camera,
            active: true,
        }],
    });
    assert_round_trip(
        &remote_media,
        json!({
            "name": "remote_media",
            "payload": {
                "bindings": [{
                    "mid": "0",
                    "sessionId": 7,
                    "type": "camera",
                    "active": true
                }]
            }
        }),
    )
}
