use serde::Deserialize;
use serde_json::Value;

use crate::signaling::protocol::RecordingOptions;

const LEGACY_START_RECORDING_REQUEST_NAME: &str = "START_RECORDING";
const LEGACY_STOP_RECORDING_REQUEST_NAME: &str = "STOP_RECORDING";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LegacyRecordingControlRequest {
    Start(RecordingOptions),
    Stop,
}

impl LegacyRecordingControlRequest {
    #[must_use]
    pub(super) fn decode_wire(message: &Value) -> Option<Result<Self, ()>> {
        let request_name = request_name(message)?;
        match request_name {
            LEGACY_START_RECORDING_REQUEST_NAME => Some(Self::decode_start_request(message)),
            LEGACY_STOP_RECORDING_REQUEST_NAME => Some(Self::decode_stop_request(message)),
            _other => None,
        }
    }

    fn decode_start_request(message: &Value) -> Result<Self, ()> {
        let payload = message
            .as_object()
            .and_then(|object| object.get("payload"))
            .ok_or(())?;
        let payload = serde_json::from_value::<LegacyStartRecordingPayload>(payload.clone())
            .map_err(|_error| ())?;
        Ok(Self::Start(RecordingOptions {
            audio: payload.audio,
            video: payload.video,
            transcription: payload.transcription,
        }))
    }

    fn decode_stop_request(message: &Value) -> Result<Self, ()> {
        let Some(object) = message.as_object() else {
            return Err(());
        };
        match object.get("payload") {
            None | Some(Value::Null) => Ok(Self::Stop),
            Some(_payload) => Err(()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
struct LegacyStartRecordingPayload {
    #[serde(default)]
    audio: Option<bool>,
    #[serde(default)]
    video: Option<bool>,
    #[serde(default)]
    transcription: Option<bool>,
}

fn request_name(message: &Value) -> Option<&str> {
    message.as_object()?.get("name")?.as_str()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::LegacyRecordingControlRequest;
    use crate::signaling::protocol::RecordingOptions;

    #[test]
    fn start_recording_request_translation_preserves_options() {
        let translated = LegacyRecordingControlRequest::decode_wire(&json!({
            "name": "START_RECORDING",
            "payload": {
                "audio": true,
                "video": false,
                "transcription": true
            }
        }));

        assert_eq!(
            translated,
            Some(Ok(LegacyRecordingControlRequest::Start(RecordingOptions {
                audio: Some(true),
                video: Some(false),
                transcription: Some(true),
            })))
        );
    }

    #[test]
    fn stop_recording_request_translation_accepts_legacy_unit_shape() {
        let translated =
            LegacyRecordingControlRequest::decode_wire(&json!({ "name": "STOP_RECORDING" }));

        assert_eq!(translated, Some(Ok(LegacyRecordingControlRequest::Stop)));
    }

    #[test]
    fn stop_recording_request_rejects_unexpected_payload() {
        let translated = LegacyRecordingControlRequest::decode_wire(&json!({
            "name": "STOP_RECORDING",
            "payload": true
        }));

        assert!(matches!(translated, Some(Err(()))));
    }
}
