use o_sfu_rfc::webrtc::MediaKind;
use serde_json::json;

use super::*;

#[test]
fn protocol_welcome_message_round_trips_to_wire_envelope() -> serde_json::Result<()> {
    let welcome = ServerMessage::Welcome(WelcomePayload {
        features: AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: true,
            video_recording: false,
        },
        recording: RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        },
        peers: vec![PeerSnapshot {
            user_id: UserId::String(String::from("alice")),
            info: UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            },
        }],
    })
    .into_envelope()?;
    assert_eq!(
        serde_json::to_value(&welcome)?,
        json!({
            "t": "welcome",
            "p": {
                "features": {
                    "rtc": true,
                    "transcription": false,
                    "audioRecording": true,
                    "videoRecording": false,
                },
                "recording": {
                    "recording": false,
                    "audio": false,
                    "transcription": false,
                    "video": false,
                },
                "peers": [{
                    "sessionId": "alice",
                    "info": {
                        "isTalking": true,
                    },
                }],
            },
        })
    );
    Ok(())
}

#[test]
fn server_push_messages_round_trip() -> serde_json::Result<()> {
    let track_update = ServerMessage::Tracks(vec![TrackBinding {
        mid: String::from("0"),
        user_id: UserId::Integer(5),
        stream_type: StreamType::Camera,
        active: true,
        source: None,
    }])
    .into_envelope()?;
    assert_eq!(
        serde_json::to_value(&track_update)?,
        json!({
            "t": "tracks",
            "p": [{
                "mid": "0",
                "sessionId": 5,
                "type": "camera",
                "active": true,
            }],
        })
    );

    let peer_joined = ServerMessage::PeerJoined(PeerInfoPayload {
        user_id: UserId::Integer(9),
        info: UserInfo {
            is_camera_on: Some(true),
            ..UserInfo::default()
        },
    })
    .into_envelope()?;
    assert_eq!(
        serde_json::to_value(&peer_joined)?,
        json!({
            "t": "peerjoined",
            "p": {
                "sessionId": 9,
                "info": {
                    "isCameraOn": true,
                },
            },
        })
    );

    let peer_left = ServerMessage::PeerLeft(PeerLeftPayload {
        user_id: UserId::Integer(9),
    })
    .into_envelope()?;
    assert_eq!(
        serde_json::to_value(&peer_left)?,
        json!({
            "t": "peerleft",
            "p": {
                "sessionId": 9,
            },
        })
    );
    Ok(())
}

#[test]
fn protocol_server_welcome_message_decodes_without_routing_metadata() {
    let decoded = ServerEnvelope::decode(Envelope {
        tag: String::from("welcome"),
        payload: Some(json!({
            "features": {
                "rtc": true,
                "transcription": false,
                "audioRecording": true,
                "videoRecording": false,
            },
            "recording": {
                "recording": true,
            },
            "peers": [{
                "sessionId": 7,
                "info": {
                    "isTalking": false,
                },
            }],
        })),
        request_id: None,
        response_to: None,
    });

    assert_eq!(
        decoded,
        Ok(ServerEnvelope::Message(ServerMessage::Welcome(
            WelcomePayload {
                features: AvailableFeatures {
                    rtc: true,
                    transcription: false,
                    audio_recording: true,
                    video_recording: false,
                },
                recording: RecordingState {
                    recording: Some(true),
                    audio: None,
                    transcription: None,
                    video: None,
                },
                peers: vec![PeerSnapshot {
                    user_id: UserId::Integer(7),
                    info: UserInfo {
                        is_talking: Some(false),
                        ..UserInfo::default()
                    },
                }],
            }
        )))
    );
}

#[test]
fn protocol_server_offer_request_round_trips_through_server_envelope() -> serde_json::Result<()> {
    let envelope = ServerEnvelope::Request {
        request_id: RequestId::new("offer-1"),
        request: ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\n"),
            upload_slots: Vec::new(),
        }),
    }
    .into_envelope()?;

    assert_eq!(
        ServerEnvelope::decode(envelope),
        Ok(ServerEnvelope::Request {
            request_id: RequestId::new("offer-1"),
            request: ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0\r\n"),
                upload_slots: Vec::new(),
            }),
        })
    );
    Ok(())
}

#[test]
fn protocol_server_offer_serializes_upload_slot_metadata() -> serde_json::Result<()> {
    let envelope = ServerEnvelope::Request {
        request_id: RequestId::new("offer-1"),
        request: ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\n"),
            upload_slots: vec![NegotiationUploadSlot {
                mid: String::from("video-1"),
                kind: MediaKind::Video,
                codecs: vec![String::from("VP8")],
                simulcast_encodings: vec![
                    NegotiationUploadEncoding {
                        rid: String::from("lo"),
                        max_bitrate: Some(150_000),
                        resolution_scale: Some(4),
                        max_framerate: None,
                    },
                    NegotiationUploadEncoding {
                        rid: String::from("hi"),
                        max_bitrate: Some(900_000),
                        resolution_scale: Some(1),
                        max_framerate: None,
                    },
                ],
            }],
        }),
    }
    .into_envelope()?;

    assert_eq!(
        serde_json::to_value(&envelope)?,
        json!({
            "t": "offer",
            "q": "offer-1",
            "p": {
                "sdp": "v=0\r\n",
                "uploadSlots": [{
                    "mid": "video-1",
                    "kind": "video",
                    "codecs": ["VP8"],
                    "simulcastEncodings": [
                        {
                            "rid": "lo",
                            "maxBitrate": 150_000,
                            "resolutionScale": 4
                        },
                        {
                            "rid": "hi",
                            "maxBitrate": 900_000,
                            "resolutionScale": 1
                        }
                    ]
                }]
            }
        })
    );
    Ok(())
}

#[test]
fn protocol_server_broadcast_message_round_trips_to_wire_envelope() -> serde_json::Result<()> {
    let broadcast = ServerMessage::Broadcast(ServerBroadcastPayload {
        sender_id: UserId::String(String::from("bob")),
        message: json!({"text": "hello"}),
    })
    .into_envelope()?;

    assert_eq!(
        serde_json::to_value(&broadcast)?,
        json!({
            "t": "broadcast",
            "p": {
                "senderId": "bob",
                "message": {
                    "text": "hello",
                },
            },
        })
    );
    Ok(())
}

#[test]
fn protocol_server_offer_request_serializes_to_wire_envelope() -> serde_json::Result<()> {
    let offer = ServerRequest::Offer(SessionDescriptionPayload {
        sdp: String::from("v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n"),
        upload_slots: Vec::new(),
    })
    .into_envelope(RequestId::new("1"))?;

    assert_eq!(
        serde_json::to_value(&offer)?,
        json!({
            "t": "offer",
            "q": "1",
            "p": {
                "sdp": "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n",
            },
        })
    );
    Ok(())
}
