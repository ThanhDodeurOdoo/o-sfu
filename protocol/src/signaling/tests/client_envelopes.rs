use serde_json::json;

use super::*;

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
                upload_slots: Vec::new(),
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
            "cameraLayout": "pinned",
        })),
        request_id: None,
        response_to: None,
    });

    assert_eq!(
        decoded,
        Ok(ClientEnvelope::Message(ClientMessage::Subscribe(
            SubscribePayload {
                user_id: UserId::Integer(7),
                states: DownloadStates {
                    audio: Some(true),
                    camera: Some(false),
                    screen: None,
                    camera_layout: Some(VideoLayoutIntent::Pinned),
                    ..DownloadStates::default()
                },
            }
        )))
    );
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
