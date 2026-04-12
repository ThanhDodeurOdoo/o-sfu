//! Legacy ORTC compatibility conversions between JSON RTP payloads and router-native media models.
//!
//! This module exists only for the current compatibility websocket path.
//! The Phase 9 signaling protocol must not grow new dependencies on it.
//!
//! The signaling layer uses `RtpParameters(serde_json::Value)` and `RtpCapabilities(Value)`
//! as opaque wrappers over the mediasoup/ORTC wire format. The router crate uses typed
//! domain models (`o_sfu_router::MediaStream`, `o_sfu_router::MediaCapabilities`).
//!
//! - ORTC API dictionaries: <https://draft.ortc.org/>

use o_sfu_router::{
    HeaderExtension as RouterHeaderExtension, MediaCapabilities, MediaCodecCapability, MediaFormat,
    MediaKind as RouterMediaKind, MediaStream, RtcpFeedback, RtcpFeedbackKind, StreamBinding,
};
use serde_json::{Map, Value, json};

use crate::rfc::webrtc;
use crate::signaling::webrtc::{serialize_codec_settings, serialize_rtcp_feedback};

// ---------------------------------------------------------------------------
// Parse: ortc JSON -> router types
// ---------------------------------------------------------------------------

/// Expected shape:
/// ```json
/// {
///   "mid": "0",
///   "codecs": [{ "mimeType": "audio/opus", "payloadType": 111, ... }],
///   "headerExtensions": [{ "uri": "...", "id": 1, ... }],
///   "encodings": [{ "ssrc": 12345, ... }]
/// }
/// ```
pub(crate) fn parse_rtp_parameters(value: &Value) -> Option<MediaStream> {
    let obj = value.as_object()?;
    let codecs = parse_codec_parameters_array(obj.get("codecs")?)?;
    let header_extensions = parse_header_extension_array(obj.get("headerExtensions"));
    let encodings = parse_encoding_array(obj.get("encodings"));
    let mut params = MediaStream::new(codecs, header_extensions, encodings);
    if let Some(mid) = obj.get("mid").and_then(Value::as_str) {
        params = params.with_mid(mid.to_owned());
    }
    Some(params)
}

/// Expected shape:
/// ```json
/// {
///   "codecs": [{ "mimeType": "audio/opus", "kind": "audio", "preferredPayloadType": 111, ... }],
///   "headerExtensions": [{ "uri": "...", "preferredId": 1, "kind": "audio", ... }]
/// }
/// ```
pub(crate) fn parse_rtp_capabilities(value: &Value) -> Option<MediaCapabilities> {
    let obj = value.as_object()?;
    let codecs = parse_codec_capability_array(obj.get("codecs")?)?;
    let header_extensions = parse_header_extension_capability_array(obj.get("headerExtensions"));
    Some(MediaCapabilities::new(codecs, header_extensions))
}

// ---------------------------------------------------------------------------
// Serialize: router types -> ortc JSON
// ---------------------------------------------------------------------------

/// Serialize router-native media stream data to the mediasoup/ORTC wire JSON shape.
///
/// Used to build the `rtpParameters` field in `INIT_CONSUMER` payloads after negotiation.
pub(crate) fn serialize_rtp_parameters(params: &MediaStream) -> Value {
    let codecs: Vec<Value> = params.codecs().map(serialize_codec_parameters).collect();
    let header_extensions: Vec<Value> = params
        .header_extensions()
        .map(serialize_header_extension)
        .collect();
    let encodings: Vec<Value> = params.encodings().map(serialize_encoding).collect();
    let mut result = json!({
        "codecs": codecs,
        "headerExtensions": header_extensions,
        "encodings": encodings,
    });
    if let Some(mid) = params.mid()
        && let Some(obj) = result.as_object_mut()
    {
        obj.insert("mid".to_owned(), Value::String(mid.to_owned()));
    }
    result
}

// ---------------------------------------------------------------------------
// Codec parameter parsing (for MediaStream formats)
// ---------------------------------------------------------------------------

fn parse_codec_parameters_array(value: &Value) -> Option<Vec<MediaFormat>> {
    let arr = value.as_array()?;
    let mut codecs = Vec::with_capacity(arr.len());
    for entry in arr {
        codecs.push(parse_single_codec_parameters(entry)?);
    }
    Some(codecs)
}

fn parse_single_codec_parameters(value: &Value) -> Option<MediaFormat> {
    let obj = value.as_object()?;
    let (media_kind, codec_name) = parse_mime_type(obj)?;
    let payload_type = json_u8(obj, "payloadType")?;
    let clock_rate = json_u32(obj, "clockRate")?;
    let mut codec = MediaFormat::new(media_kind, codec_name, payload_type, clock_rate);
    if let Some(channels) = json_u16(obj, "channels") {
        codec = codec.with_channels(channels);
    }
    codec = apply_parameters(codec, obj);
    codec = apply_rtcp_feedback_to_parameters(codec, obj);
    Some(codec)
}

fn apply_parameters(mut codec: MediaFormat, obj: &Map<String, Value>) -> MediaFormat {
    if let Some(params) = obj.get("parameters").and_then(Value::as_object) {
        for (key, value) in params {
            let string_value = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => continue,
            };
            codec = codec.with_parameter(key.clone(), string_value);
        }
    }
    codec
}

fn apply_rtcp_feedback_to_parameters(
    mut codec: MediaFormat,
    obj: &Map<String, Value>,
) -> MediaFormat {
    if let Some(feedback_arr) = obj.get("rtcpFeedback").and_then(Value::as_array) {
        for entry in feedback_arr {
            if let Some(feedback) = parse_rtcp_feedback(entry) {
                codec = codec.with_rtcp_feedback(feedback);
            }
        }
    }
    codec
}

// ---------------------------------------------------------------------------
// Codec capability parsing (for MediaCapabilities codecs)
// ---------------------------------------------------------------------------

fn parse_codec_capability_array(value: &Value) -> Option<Vec<MediaCodecCapability>> {
    let arr = value.as_array()?;
    let mut codecs = Vec::with_capacity(arr.len());
    for entry in arr {
        codecs.push(parse_single_codec_capability(entry)?);
    }
    Some(codecs)
}

fn parse_single_codec_capability(value: &Value) -> Option<MediaCodecCapability> {
    let obj = value.as_object()?;
    let (media_kind, codec_name) = parse_mime_type(obj)?;
    let clock_rate = json_u32(obj, "clockRate")?;
    let mut codec = MediaCodecCapability::new(media_kind, codec_name, clock_rate);
    if let Some(pt) = json_u8(obj, "preferredPayloadType") {
        codec = codec.with_preferred_payload_type(pt);
    }
    if let Some(channels) = json_u16(obj, "channels") {
        codec = codec.with_channels(channels);
    }
    if let Some(params) = obj.get("parameters").and_then(Value::as_object) {
        for (key, val) in params {
            let string_value = match val {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => continue,
            };
            codec = codec.with_parameter(key.clone(), string_value);
        }
    }
    if let Some(feedback_arr) = obj.get("rtcpFeedback").and_then(Value::as_array) {
        for entry in feedback_arr {
            if let Some(feedback) = parse_rtcp_feedback(entry) {
                codec = codec.with_rtcp_feedback(feedback);
            }
        }
    }
    Some(codec)
}

// ---------------------------------------------------------------------------
// Header extension parsing and serialization
// ---------------------------------------------------------------------------

/// from `MediaStream` wire payloads (uses `id` field).
fn parse_header_extension_array(value: Option<&Value>) -> Vec<RouterHeaderExtension> {
    let Some(arr) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(parse_single_header_extension)
        .collect()
}

fn parse_single_header_extension(value: &Value) -> Option<RouterHeaderExtension> {
    let obj = value.as_object()?;
    let uri = obj.get("uri").and_then(Value::as_str)?;
    let id = json_u8(obj, "id")?;
    let mut ext = RouterHeaderExtension::new(uri.to_owned(), id);
    if obj.get("encrypt").and_then(Value::as_bool).unwrap_or(false) {
        ext = ext.with_encryption(true);
    }
    Some(ext)
}

/// Parse header extensions from `MediaCapabilities` payloads (uses `preferredId` field).
fn parse_header_extension_capability_array(value: Option<&Value>) -> Vec<RouterHeaderExtension> {
    let Some(arr) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(parse_single_header_extension_capability)
        .collect()
}

fn parse_single_header_extension_capability(value: &Value) -> Option<RouterHeaderExtension> {
    let obj = value.as_object()?;
    let uri = obj.get("uri").and_then(Value::as_str)?;
    let id = json_u8(obj, "preferredId")?;
    let mut ext = RouterHeaderExtension::new(uri.to_owned(), id);
    if obj
        .get("preferredEncrypt")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        ext = ext.with_encryption(true);
    }
    Some(ext)
}

fn serialize_header_extension(ext: &RouterHeaderExtension) -> Value {
    json!({
        "uri": ext.uri_kind().as_str(),
        "id": ext.id().value(),
        "encrypt": ext.encrypt(),
    })
}

// ---------------------------------------------------------------------------
// Encoding parsing and serialization
// ---------------------------------------------------------------------------

fn parse_encoding_array(value: Option<&Value>) -> Vec<StreamBinding> {
    let Some(arr) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter().filter_map(parse_single_encoding).collect()
}

fn parse_single_encoding(value: &Value) -> Option<StreamBinding> {
    let obj = value.as_object()?;
    let mut encoding = StreamBinding::new();
    if let Some(ssrc) = json_u32(obj, "ssrc") {
        encoding = encoding.with_ssrc(ssrc);
    }
    if let Some(rid) = obj.get("rid").and_then(Value::as_str) {
        encoding = encoding.with_rid(rid.to_owned());
    }
    if let Some(pt) = json_u8(obj, "codecPayloadType") {
        encoding = encoding.with_codec_payload_type(pt);
    }
    if let Some(max_bitrate) = json_u64(obj, "maxBitrate") {
        encoding = encoding.with_max_bitrate(max_bitrate);
    }
    Some(encoding)
}

fn serialize_encoding(encoding: &StreamBinding) -> Value {
    let mut obj = Map::new();
    if let Some(ssrc) = encoding.ssrc() {
        obj.insert("ssrc".to_owned(), json!(ssrc));
    }
    if let Some(rid) = encoding.rid() {
        obj.insert("rid".to_owned(), json!(rid));
    }
    if let Some(pt) = encoding.payload_type() {
        obj.insert("codecPayloadType".to_owned(), json!(pt));
    }
    if let Some(max_bitrate) = encoding.max_bitrate() {
        obj.insert("maxBitrate".to_owned(), json!(max_bitrate));
    }
    Value::Object(obj)
}

// ---------------------------------------------------------------------------
// RTCP feedback parsing
// ---------------------------------------------------------------------------

fn parse_rtcp_feedback(value: &Value) -> Option<RtcpFeedback> {
    let obj = value.as_object()?;
    let feedback_type = obj.get("type").and_then(Value::as_str)?;
    let parameter = obj.get("parameter").and_then(Value::as_str);
    let kind = match (feedback_type, parameter) {
        (webrtc::rtcp_feedback::kind::NACK, None) => RtcpFeedbackKind::Nack,
        (webrtc::rtcp_feedback::kind::NACK, Some(webrtc::rtcp_feedback::parameter::PLI)) => {
            RtcpFeedbackKind::NackPli
        }
        (webrtc::rtcp_feedback::kind::CCM, Some(webrtc::rtcp_feedback::parameter::FIR)) => {
            RtcpFeedbackKind::CcmFir
        }
        (webrtc::rtcp_feedback::kind::GOOG_REMB, _) => RtcpFeedbackKind::GoogRemb,
        (webrtc::rtcp_feedback::kind::TRANSPORT_CC, _) => RtcpFeedbackKind::TransportCc,
        _ => RtcpFeedbackKind::Other(feedback_type.to_owned()),
    };
    let rtcp_parameter = match &kind {
        RtcpFeedbackKind::Other(_) => parameter.map(str::to_owned),
        _ => None,
    };
    Some(RtcpFeedback::new(kind, rtcp_parameter))
}

// ---------------------------------------------------------------------------
// Codec parameter serialization
// ---------------------------------------------------------------------------

fn serialize_codec_parameters(codec: &MediaFormat) -> Value {
    let kind = media_kind_label(codec.media_kind());
    let mut obj = Map::new();
    obj.insert(
        "mimeType".to_owned(),
        json!(format!("{kind}/{}", codec.codec().as_str())),
    );
    obj.insert("payloadType".to_owned(), json!(codec.payload_type()));
    obj.insert("clockRate".to_owned(), json!(codec.clock_rate()));
    if let Some(channels) = codec.channels() {
        obj.insert("channels".to_owned(), json!(channels));
    }
    obj.insert(
        "parameters".to_owned(),
        serialize_codec_settings(codec.settings()),
    );
    obj.insert(
        "rtcpFeedback".to_owned(),
        Value::Array(codec.rtcp_feedback().map(serialize_rtcp_feedback).collect()),
    );
    Value::Object(obj)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Parse `mimeType` field ("audio/opus", "video/VP8") into router media kind and codec name.
fn parse_mime_type(obj: &Map<String, Value>) -> Option<(RouterMediaKind, String)> {
    let mime_type = obj.get("mimeType").and_then(Value::as_str)?;
    let (kind_str, codec_name) = mime_type.split_once('/')?;
    let media_kind = match kind_str {
        webrtc::media_kind::AUDIO => RouterMediaKind::Audio,
        webrtc::media_kind::VIDEO => RouterMediaKind::Video,
        _ => return None,
    };
    Some((media_kind, codec_name.to_owned()))
}

fn media_kind_label(media_kind: RouterMediaKind) -> &'static str {
    match media_kind {
        RouterMediaKind::Audio => webrtc::media_kind::AUDIO,
        RouterMediaKind::Video => webrtc::media_kind::VIDEO,
    }
}

fn json_u8(obj: &Map<String, Value>, key: &str) -> Option<u8> {
    obj.get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u8::try_from(v).ok())
}

fn json_u16(obj: &Map<String, Value>, key: &str) -> Option<u16> {
    obj.get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u16::try_from(v).ok())
}

fn json_u32(obj: &Map<String, Value>, key: &str) -> Option<u32> {
    obj.get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
}

fn json_u64(obj: &Map<String, Value>, key: &str) -> Option<u64> {
    obj.get(key).and_then(Value::as_u64)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test assertions use expect/unwrap for clear failure messages"
)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_and_serialize_rtp_parameters_round_trip() {
        let wire = json!({
            "mid": "0",
            "codecs": [{
                "mimeType": "audio/opus",
                "payloadType": 111,
                "clockRate": 48000,
                "channels": 2,
                "parameters": { "useinbandfec": "1" },
                "rtcpFeedback": [{ "type": "transport-cc" }]
            }],
            "headerExtensions": [{
                "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
                "id": 10,
                "encrypt": false
            }],
            "encodings": [{ "ssrc": 12345 }]
        });
        let parsed = parse_rtp_parameters(&wire).expect("parse should succeed");
        assert_eq!(parsed.mid(), Some("0"));
        assert_eq!(parsed.codecs().count(), 1);
        assert_eq!(parsed.header_extensions().count(), 1);
        assert_eq!(parsed.encodings().count(), 1);

        let serialized = serialize_rtp_parameters(&parsed);
        let reparsed = parse_rtp_parameters(&serialized).expect("reparse should succeed");
        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn parse_rtp_capabilities_with_codecs_and_extensions() {
        let wire = json!({
            "codecs": [
                {
                    "mimeType": "audio/opus",
                    "kind": "audio",
                    "preferredPayloadType": 111,
                    "clockRate": 48000,
                    "channels": 2,
                    "parameters": { "useinbandfec": "1" },
                    "rtcpFeedback": [{ "type": "transport-cc" }]
                },
                {
                    "mimeType": "video/VP8",
                    "kind": "video",
                    "preferredPayloadType": 96,
                    "clockRate": 90000,
                    "parameters": {},
                    "rtcpFeedback": [
                        { "type": "nack" },
                        { "type": "nack", "parameter": "pli" },
                        { "type": "ccm", "parameter": "fir" },
                        { "type": "goog-remb" },
                        { "type": "transport-cc" }
                    ]
                }
            ],
            "headerExtensions": [{
                "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
                "preferredId": 1,
                "preferredEncrypt": false,
                "kind": "audio",
                "direction": "sendrecv"
            }]
        });
        let parsed = parse_rtp_capabilities(&wire).expect("parse should succeed");
        assert_eq!(parsed.codecs().count(), 2);
        assert_eq!(parsed.header_extensions().count(), 1);
    }

    #[test]
    fn parse_minimal_rtp_parameters() {
        let wire = json!({
            "codecs": [],
            "headerExtensions": [],
            "encodings": []
        });
        let parsed = parse_rtp_parameters(&wire).expect("parse should succeed");
        assert_eq!(parsed.codecs().count(), 0);
        assert!(parsed.mid().is_none());
    }

    #[test]
    fn parse_rtp_parameters_returns_none_for_missing_codecs() {
        let wire = json!({ "headerExtensions": [] });
        assert!(parse_rtp_parameters(&wire).is_none());
    }

    #[test]
    fn parse_rtp_parameters_returns_none_for_non_object() {
        assert!(parse_rtp_parameters(&json!("not an object")).is_none());
    }

    #[test]
    fn codec_parameters_with_rtx_and_apt() {
        let wire = json!({
            "codecs": [
                {
                    "mimeType": "video/VP8",
                    "payloadType": 96,
                    "clockRate": 90000,
                    "parameters": {},
                    "rtcpFeedback": []
                },
                {
                    "mimeType": "video/rtx",
                    "payloadType": 97,
                    "clockRate": 90000,
                    "parameters": { "apt": "96" },
                    "rtcpFeedback": []
                }
            ]
        });
        let parsed = parse_rtp_parameters(&wire).expect("parse should succeed");
        let codecs: Vec<_> = parsed.codecs().collect();
        assert_eq!(codecs.len(), 2);
        assert_eq!(codecs.first().map(|c| c.codec_name()), Some("VP8"));
        assert_eq!(codecs.get(1).map(|c| c.codec_name()), Some("rtx"));
    }

    #[test]
    fn serialize_rtp_parameters_preserves_all_fields() {
        let params = MediaStream::new(
            vec![
                MediaFormat::new(RouterMediaKind::Audio, "opus", 111, 48000)
                    .with_channels(2)
                    .with_parameter("useinbandfec", "1")
                    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None)),
            ],
            vec![RouterHeaderExtension::new(
                "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
                10,
            )],
            vec![StreamBinding::new().with_ssrc(12345)],
        )
        .with_mid("0");

        let wire = serialize_rtp_parameters(&params);
        let obj = wire.as_object().expect("should be object");
        assert_eq!(obj.get("mid").and_then(Value::as_str), Some("0"));

        let codecs = obj.get("codecs").and_then(Value::as_array).expect("codecs");
        assert_eq!(codecs.len(), 1);
        let codec = codecs.first().and_then(Value::as_object).expect("codec");
        assert_eq!(
            codec.get("mimeType").and_then(Value::as_str),
            Some("audio/opus")
        );
        assert_eq!(codec.get("payloadType").and_then(Value::as_u64), Some(111));
    }
}
