use serde_json::json;

use super::{
    AuthPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse, Envelope,
    EnvelopeDecodeError, PeerInfoPayload, PeerLeftPayload, PeerSnapshot, RecordingActionResult,
    RecordingOptions, RequestId, ServerBroadcastPayload, ServerEnvelope, ServerMessage,
    ServerRequest, ServerResponse, SessionDescriptionPayload, StreamIntentPayload,
    SubscribePayload, TrackBinding, WebSocketCloseCode, WelcomePayload,
};
use crate::shared::{
    AvailableFeatures, DownloadStates, RecordingState, RecordingStateUpdate, SessionId,
    SessionInfo, StopCode, StreamType,
};

#[test]
fn protocol_close_codes_follow_phase_nine_contract() {
    assert_eq!(u16::from(WebSocketCloseCode::AuthFailed), 4001);
    assert_eq!(u16::from(WebSocketCloseCode::AuthTimeout), 4002);
    assert_eq!(u16::from(WebSocketCloseCode::Kicked), 4003);
    assert_eq!(u16::from(WebSocketCloseCode::ChannelFull), 4004);
}

#[test]
fn protocol_client_auth_message_round_trips_to_wire_envelope() -> serde_json::Result<()> {
    let envelope = ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
        jwt: String::from("jwt-token"),
        channel: Some(String::from("channel-1")),
    }))
    .into_envelope()?;
    assert_eq!(
        serde_json::to_value(&envelope)?,
        json!({
            "t": "auth",
            "p": {
                "jwt": "jwt-token",
                "channel": "channel-1",
            },
        })
    );
    Ok(())
}

#[test]
fn protocol_start_recording_request_decodes_with_request_id() {
    let decoded = ClientEnvelope::decode(Envelope {
        tag: String::from("startrecording"),
        payload: Some(json!({
            "audio": true,
            "video": false,
        })),
        request_id: Some(RequestId::new("3")),
        response_to: None,
    });

    assert_eq!(
        decoded,
        Ok(ClientEnvelope::Request {
            request_id: RequestId::new("3"),
            request: ClientRequest::StartRecording(RecordingOptions {
                audio: Some(true),
                video: Some(false),
                transcription: None,
            }),
        })
    );
}

#[test]
fn protocol_offer_response_decodes_with_response_id() {
    let decoded = ClientEnvelope::decode(Envelope {
        tag: String::from("offer"),
        payload: Some(json!({
            "sdp": "v=0\r\n",
        })),
        request_id: None,
        response_to: Some(RequestId::new("1")),
    });

    assert_eq!(
        decoded,
        Ok(ClientEnvelope::Response {
            response_to: RequestId::new("1"),
            response: ClientResponse::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0\r\n"),
            }),
        })
    );
}

#[test]
fn protocol_subscribe_message_decodes_flat_download_state_shape() {
    let decoded = ClientEnvelope::decode(Envelope {
        tag: String::from("subscribe"),
        payload: Some(json!({
            "sessionId": 7,
            "audio": true,
            "camera": false,
        })),
        request_id: None,
        response_to: None,
    });

    assert_eq!(
        decoded,
        Ok(ClientEnvelope::Message(ClientMessage::Subscribe(
            SubscribePayload {
                session_id: SessionId::Integer(7),
                states: DownloadStates {
                    audio: Some(true),
                    camera: Some(false),
                    screen: None,
                },
            }
        )))
    );
}

#[test]
fn protocol_decode_rejects_envelopes_with_both_request_and_response_ids() {
    let decoded = ClientEnvelope::decode(Envelope {
        tag: String::from("ping"),
        payload: None,
        request_id: Some(RequestId::new("1")),
        response_to: Some(RequestId::new("2")),
    });

    assert_eq!(decoded, Err(EnvelopeDecodeError::InvalidRoutingMetadata));
}

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
            session_id: SessionId::String(String::from("alice")),
            info: SessionInfo {
                is_talking: Some(true),
                ..SessionInfo::default()
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
fn protocol_publish_message_uses_stream_type_field() -> serde_json::Result<()> {
    let envelope = ClientMessage::Publish(StreamIntentPayload {
        stream_type: StreamType::Screen,
    })
    .into_envelope()?;
    assert_eq!(
        serde_json::to_value(&envelope)?,
        json!({
            "t": "publish",
            "p": {
                "type": "screen",
            },
        })
    );
    Ok(())
}

#[test]
fn protocol_server_track_and_peer_messages_round_trip_to_wire_envelopes() -> serde_json::Result<()>
{
    let track_update = ServerMessage::Tracks(vec![TrackBinding {
        mid: String::from("0"),
        session_id: SessionId::Integer(5),
        stream_type: StreamType::Camera,
        active: true,
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
        session_id: SessionId::Integer(9),
        info: SessionInfo {
            is_camera_on: Some(true),
            ..SessionInfo::default()
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
        session_id: SessionId::Integer(9),
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
                    session_id: SessionId::Integer(7),
                    info: SessionInfo {
                        is_talking: Some(false),
                        ..SessionInfo::default()
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
        }),
    }
    .into_envelope()?;

    assert_eq!(
        ServerEnvelope::decode(envelope),
        Ok(ServerEnvelope::Request {
            request_id: RequestId::new("offer-1"),
            request: ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0\r\n"),
            }),
        })
    );
    Ok(())
}

#[test]
fn protocol_server_stop_recording_response_round_trips_through_server_envelope()
-> serde_json::Result<()> {
    let envelope = ServerEnvelope::Response {
        response_to: RequestId::new("recording-1"),
        response: ServerResponse::StopRecording(RecordingActionResult { ok: true }),
    }
    .into_envelope()?;

    assert_eq!(
        ServerEnvelope::decode(envelope),
        Ok(ServerEnvelope::Response {
            response_to: RequestId::new("recording-1"),
            response: ServerResponse::StopRecording(RecordingActionResult { ok: true }),
        })
    );
    Ok(())
}

#[test]
fn protocol_server_broadcast_and_recording_messages_round_trip_to_wire_envelopes()
-> serde_json::Result<()> {
    let broadcast = ServerMessage::Broadcast(ServerBroadcastPayload {
        sender_id: SessionId::String(String::from("bob")),
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

    let recording_change = ServerMessage::RecordingChange(RecordingStateUpdate {
        state: RecordingState {
            recording: Some(true),
            audio: Some(true),
            transcription: Some(false),
            video: Some(true),
        },
        stop_code: Some(StopCode::UserRequest),
    })
    .into_envelope()?;
    assert_eq!(
        serde_json::to_value(&recording_change)?,
        json!({
            "t": "recordingchange",
            "p": {
                "state": {
                    "recording": true,
                    "audio": true,
                    "transcription": false,
                    "video": true,
                },
                "stopCode": "user_request",
            },
        })
    );
    Ok(())
}

#[test]
fn protocol_server_requests_and_responses_round_trip_to_wire_envelopes() -> serde_json::Result<()> {
    let offer = ServerRequest::Offer(SessionDescriptionPayload {
        sdp: String::from("v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n"),
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

    let ping = ServerRequest::Ping.into_envelope(RequestId::new("4"))?;
    assert_eq!(
        serde_json::to_value(&ping)?,
        json!({
            "t": "ping",
            "q": "4",
        })
    );

    let start_recording = ServerResponse::StartRecording(RecordingActionResult { ok: true })
        .into_envelope(RequestId::new("3"))?;
    assert_eq!(
        serde_json::to_value(&start_recording)?,
        json!({
            "t": "startrecording",
            "r": "3",
            "p": {
                "ok": true,
            },
        })
    );
    Ok(())
}
