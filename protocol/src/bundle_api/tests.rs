use std::{collections::BTreeMap, fmt::Debug};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::{
    BundleBroadcastCall, BundleBroadcastUpdate, BundleConnectCall, BundleConnectOptions,
    BundleConnectionState, BundleMethodCall, BundleProtocolStrategy, BundlePublishCall,
    BundleRecordingOptions, BundleStartRecordingCall, BundleStateChange, BundleSubscribeCall,
    BundleTrackUpdate, BundleUpdate, BundleUpdateInfoCall, BundleUpdateKind,
    FIRST_BUNDLE_PROTOCOL_STRATEGY, FIRST_BUNDLE_PROTOCOL_VERSION, bundle_session_info_key,
};
use crate::shared::{
    DownloadStates, RecordingState, RecordingStateUpdate, StopCode, StreamType, UserId, UserInfo,
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
fn first_bundle_protocol_reuses_current_wire_v1() {
    assert_eq!(FIRST_BUNDLE_PROTOCOL_VERSION, 1);
    assert_eq!(
        FIRST_BUNDLE_PROTOCOL_STRATEGY,
        BundleProtocolStrategy::ReuseCurrentWireV1
    );
}

#[test]
fn bundle_connection_states_round_trip() -> serde_json::Result<()> {
    assert_round_trip(&BundleConnectionState::Disconnected, json!("disconnected"))?;
    assert_round_trip(
        &BundleConnectionState::Authenticated,
        json!("authenticated"),
    )?;
    assert_round_trip(&BundleConnectionState::Recovering, json!("recovering"))?;
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
fn bundle_update_kinds_round_trip() -> serde_json::Result<()> {
    assert_round_trip(&BundleUpdateKind::Track, json!("track"))?;
    assert_round_trip(&BundleUpdateKind::Broadcast, json!("broadcast"))?;
    assert_round_trip(&BundleUpdateKind::Disconnect, json!("disconnect"))?;
    assert_round_trip(&BundleUpdateKind::SessionInfoChange, json!("info_change"))?;
    assert_round_trip(
        &BundleUpdateKind::ChannelInfoChange,
        json!("channel_info_change"),
    )
}

#[test]
fn bundle_connect_and_broadcast_calls_round_trip() -> serde_json::Result<()> {
    let connect = BundleMethodCall::Connect(BundleConnectCall {
        url: "https://sfu.example.com".to_owned(),
        json_web_token: "signed-token".to_owned(),
        options: Some(BundleConnectOptions {
            room_id: Some("31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned()),
            ice_servers: Some(vec![json!({
                "urls": "stun:stun.example.com"
            })]),
        }),
    });
    assert_round_trip(
        &connect,
        json!({
            "method": "connect",
            "arguments": {
                "url": "https://sfu.example.com",
                "jsonWebToken": "signed-token",
                "options": {
                    "channelUUID": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
                    "iceServers": [{
                        "urls": "stun:stun.example.com"
                    }]
                }
            }
        }),
    )?;

    let broadcast = BundleMethodCall::Broadcast(BundleBroadcastCall {
        message: json!({ "kind": "wave" }),
    });
    assert_round_trip(
        &broadcast,
        json!({
            "method": "broadcast",
            "arguments": {
                "message": { "kind": "wave" }
            }
        }),
    )
}

#[test]
fn bundle_state_mutation_calls_round_trip() -> serde_json::Result<()> {
    let update_info = BundleMethodCall::UpdateInfo(BundleUpdateInfoCall {
        info: UserInfo {
            is_talking: Some(true),
            is_featured: None,
            is_camera_on: Some(false),
            is_screen_sharing_on: None,
            is_self_muted: None,
            is_deaf: None,
            is_raising_hand: Some(true),
        },
    });
    assert_round_trip(
        &update_info,
        json!({
            "method": "updateInfo",
            "arguments": {
                "info": {
                    "isTalking": true,
                    "isCameraOn": false,
                    "isRaisingHand": true
                }
            }
        }),
    )?;

    let legacy_update_info = serde_json::from_value::<BundleMethodCall>(json!({
        "method": "updateInfo",
        "arguments": {
            "info": {
                "isTalking": true,
                "isCameraOn": false,
                "isRaisingHand": true
            },
            "options": {
                "needRefresh": true
            }
        }
    }))?;
    assert_eq!(legacy_update_info, update_info);

    let subscribe_call = BundleMethodCall::Subscribe(BundleSubscribeCall {
        user_id: UserId::Integer(7),
        states: DownloadStates {
            audio: Some(false),
            camera: None,
            screen: Some(true),
            ..DownloadStates::default()
        },
    });
    assert_round_trip(
        &subscribe_call,
        json!({
            "method": "subscribe",
            "arguments": {
                "sessionId": 7,
                "states": {
                    "audio": false,
                    "screen": true
                }
            }
        }),
    )?;

    let publish_call = BundleMethodCall::Publish(BundlePublishCall {
        stream_type: StreamType::Audio,
        track: Some(json!({
            "id": "microphone-track",
            "kind": "audio"
        })),
    });
    assert_round_trip(
        &publish_call,
        json!({
            "method": "publish",
            "arguments": {
                "type": "audio",
                "track": {
                    "id": "microphone-track",
                    "kind": "audio"
                }
            }
        }),
    )
}

#[test]
fn bundle_state_mutation_calls_accept_legacy_method_names() -> serde_json::Result<()> {
    let subscribe = serde_json::from_value::<BundleMethodCall>(json!({
        "method": "updateDownload",
        "arguments": {
            "sessionId": 7,
            "states": {
                "audio": false
            }
        }
    }))?;
    assert_eq!(
        subscribe,
        BundleMethodCall::Subscribe(BundleSubscribeCall {
            user_id: UserId::Integer(7),
            states: DownloadStates {
                audio: Some(false),
                camera: None,
                screen: None,
                ..DownloadStates::default()
            },
        })
    );

    let publish = serde_json::from_value::<BundleMethodCall>(json!({
        "method": "updateUpload",
        "arguments": {
            "type": "audio"
        }
    }))?;
    assert_eq!(
        publish,
        BundleMethodCall::Publish(BundlePublishCall {
            stream_type: StreamType::Audio,
            track: None,
        })
    );

    Ok(())
}

#[test]
fn bundle_recording_and_control_calls_round_trip() -> serde_json::Result<()> {
    let start_recording = BundleMethodCall::StartRecording(BundleStartRecordingCall {
        options: BundleRecordingOptions {
            audio: Some(true),
            video: Some(false),
            transcription: Some(true),
        },
    });
    assert_round_trip(
        &start_recording,
        json!({
            "method": "startRecording",
            "arguments": {
                "options": {
                    "audio": true,
                    "video": false,
                    "transcription": true
                }
            }
        }),
    )?;

    assert_round_trip(&BundleMethodCall::GetStats, json!({ "method": "getStats" }))?;
    assert_round_trip(
        &BundleMethodCall::Disconnect,
        json!({ "method": "disconnect" }),
    )?;
    assert_round_trip(
        &BundleMethodCall::StopRecording,
        json!({ "method": "stopRecording" }),
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
    assert_eq!(track_update.kind(), BundleUpdateKind::Track);
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
    assert_eq!(broadcast.kind(), BundleUpdateKind::Broadcast);
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
    assert_eq!(session_info.kind(), BundleUpdateKind::SessionInfoChange);
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
    assert_eq!(channel_info.kind(), BundleUpdateKind::ChannelInfoChange);
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
