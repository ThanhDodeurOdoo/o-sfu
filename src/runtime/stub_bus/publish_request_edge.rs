use o_sfu_router::RtpParameters as RouterRtpParameters;
use serde::Deserialize;
use serde_json::Value;

use crate::signaling::{
    ortc_mapper,
    shared::StreamType,
    webrtc::{MediaKind as SignalingMediaKind, RtpParameters},
};

const LEGACY_PUBLISH_TRACK_REQUEST_NAME: &str = "INIT_PRODUCER";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LegacyPublishTrackRequest {
    stream_type: StreamType,
    media_kind: SignalingMediaKind,
    producer_rtp_parameters: RouterRtpParameters,
}

impl LegacyPublishTrackRequest {
    #[must_use]
    pub(super) fn decode_wire(message: &Value) -> Option<Result<Self, ()>> {
        let payload = matched_payload(message, LEGACY_PUBLISH_TRACK_REQUEST_NAME)?;
        Some(Self::decode_payload(payload))
    }

    fn decode_payload(payload: &Value) -> Result<Self, ()> {
        let payload = serde_json::from_value::<LegacyPublishTrackPayload>(payload.clone())
            .map_err(|_error| ())?;
        let producer_rtp_parameters =
            ortc_mapper::parse_rtp_parameters(&payload.rtp_parameters.0).ok_or(())?;
        Ok(Self {
            stream_type: payload.stream_type,
            media_kind: payload.media_kind,
            producer_rtp_parameters,
        })
    }

    #[must_use]
    pub(super) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    #[must_use]
    pub(super) const fn media_kind(&self) -> SignalingMediaKind {
        self.media_kind
    }

    #[must_use]
    pub(super) fn into_producer_rtp_parameters(self) -> RouterRtpParameters {
        self.producer_rtp_parameters
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyPublishTrackPayload {
    #[serde(rename = "type")]
    stream_type: StreamType,
    #[serde(rename = "kind")]
    media_kind: SignalingMediaKind,
    #[serde(rename = "rtpParameters")]
    rtp_parameters: RtpParameters,
}

fn matched_payload<'a>(message: &'a Value, request_name: &str) -> Option<&'a Value> {
    let object = message.as_object()?;
    let name = object.get("name")?.as_str()?;
    if name != request_name {
        return None;
    }
    object.get("payload")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::LegacyPublishTrackRequest;
    use crate::signaling::{shared::StreamType, webrtc::MediaKind as SignalingMediaKind};

    #[test]
    fn publish_track_request_translation_preserves_semantic_fields() {
        let translated = LegacyPublishTrackRequest::decode_wire(&json!({
            "name": "INIT_PRODUCER",
            "payload": {
                "type": "camera",
                "kind": "video",
                "rtpParameters": {
                    "codecs": [{
                        "mimeType": "video/VP8",
                        "payloadType": 96,
                        "clockRate": 90000,
                        "parameters": {},
                        "rtcpFeedback": [{ "type": "transport-cc" }]
                    }],
                    "headerExtensions": [],
                    "encodings": [{ "ssrc": 11111 }]
                }
            }
        }));

        assert!(matches!(translated, Some(Ok(_))));
        let Some(Ok(translated)) = translated else {
            return;
        };
        assert_eq!(translated.stream_type(), StreamType::Camera);
        assert_eq!(translated.media_kind(), SignalingMediaKind::Video);
    }

    #[test]
    fn malformed_publish_track_request_is_rejected() {
        let translated = LegacyPublishTrackRequest::decode_wire(&json!({
            "name": "INIT_PRODUCER",
            "payload": {
                "type": "camera",
                "kind": "video",
                "rtpParameters": false
            }
        }));

        assert!(matches!(translated, Some(Err(()))));
    }

    #[test]
    fn non_publish_request_is_ignored() {
        let translated = LegacyPublishTrackRequest::decode_wire(&json!({
            "name": "STOP_RECORDING"
        }));

        assert_eq!(translated, None);
    }
}
