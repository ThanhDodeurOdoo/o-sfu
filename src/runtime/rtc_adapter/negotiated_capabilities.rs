use serde_json::{Map, Value, json};
use str0m::{
    change::SdpAnswer,
    format::{Codec, PayloadParams},
};

use crate::{
    rfc::webrtc,
    signaling::webrtc::RtpCapabilities as SignalingRtpCapabilities,
};

pub(crate) fn client_rtp_capabilities_from_answer(
    answer_sdp: &str,
) -> Option<SignalingRtpCapabilities> {
    let answer = SdpAnswer::from_sdp_string(answer_sdp).ok()?;
    let mut codecs = Vec::new();
    let mut header_extensions = Vec::new();

    for media_line in &answer.media_lines {
        if media_line.disabled {
            continue;
        }
        let rtp_parameters = media_line.rtp_params();
        let Some(media_kind) = media_kind_label(&rtp_parameters) else {
            continue;
        };
        for payload in &rtp_parameters {
            let codec = serialize_codec_capability(media_kind, payload);
            if !codecs.contains(&codec) {
                codecs.push(codec);
            }
        }
        for (id, extension) in media_line.extmaps() {
            let header_extension = serialize_header_extension(media_kind, id, extension.as_uri());
            if !header_extensions.contains(&header_extension) {
                header_extensions.push(header_extension);
            }
        }
    }

    if codecs.is_empty() {
        return None;
    }

    Some(SignalingRtpCapabilities(json!({
        "codecs": codecs,
        "headerExtensions": header_extensions,
    })))
}

fn media_kind_label(payloads: &[PayloadParams]) -> Option<&'static str> {
    payloads
        .iter()
        .find(|payload| payload.spec().codec != Codec::Rtx)
        .or_else(|| payloads.first())
        .map(|payload| {
            if payload.spec().codec.is_audio() {
                webrtc::media_kind::AUDIO
            } else {
                webrtc::media_kind::VIDEO
            }
        })
}

fn serialize_codec_capability(media_kind: &str, payload: &PayloadParams) -> Value {
    let spec = payload.spec();
    let mut codec = Map::new();
    codec.insert("kind".to_owned(), json!(media_kind));
    codec.insert(
        "mimeType".to_owned(),
        json!(format!("{media_kind}/{}", payload.spec().codec)),
    );
    codec.insert("preferredPayloadType".to_owned(), json!(payload.pt()));
    codec.insert("clockRate".to_owned(), json!(spec.clock_rate.get()));
    if let Some(channels) = spec.channels {
        codec.insert("channels".to_owned(), json!(channels));
    }
    codec.insert(
        "parameters".to_owned(),
        Value::Object(serialize_codec_parameters(payload)),
    );
    codec.insert(
        "rtcpFeedback".to_owned(),
        Value::Array(serialize_rtcp_feedback(payload)),
    );
    Value::Object(codec)
}

fn serialize_codec_parameters(payload: &PayloadParams) -> Map<String, Value> {
    let spec = payload.spec();
    let mut parameters = Map::new();

    if let Some(resend_payload_type) = payload.resend() {
        parameters.insert("apt".to_owned(), json!(resend_payload_type.to_string()));
    }
    if let Some(min_p_time) = spec.format.min_p_time {
        parameters.insert("minptime".to_owned(), json!(min_p_time.to_string()));
    }
    if let Some(use_inband_fec) = spec.format.use_inband_fec {
        parameters.insert(
            "useinbandfec".to_owned(),
            json!(boolean_flag(use_inband_fec)),
        );
    }
    if let Some(use_dtx) = spec.format.use_dtx {
        parameters.insert("usedtx".to_owned(), json!(boolean_flag(use_dtx)));
    }
    if let Some(level_asymmetry_allowed) = spec.format.level_asymmetry_allowed {
        parameters.insert(
            "level-asymmetry-allowed".to_owned(),
            json!(boolean_flag(level_asymmetry_allowed)),
        );
    }
    if let Some(packetization_mode) = spec.format.packetization_mode {
        parameters.insert(
            "packetization-mode".to_owned(),
            json!(packetization_mode.to_string()),
        );
    }
    if let Some(profile_level_id) = spec.format.profile_level_id {
        parameters.insert(
            "profile-level-id".to_owned(),
            json!(format!("{profile_level_id:06x}")),
        );
    }
    if let Some(profile_id) = spec.format.profile_id {
        parameters.insert("profile-id".to_owned(), json!(profile_id.to_string()));
    }
    if let Some(profile) = spec.format.profile {
        parameters.insert("profile".to_owned(), json!(profile.to_string()));
    }
    if let Some(level_idx) = spec.format.level_idx {
        parameters.insert("level-idx".to_owned(), json!(level_idx.to_string()));
    }
    if let Some(tier) = spec.format.tier {
        parameters.insert("tier".to_owned(), json!(tier.to_string()));
    }
    if let Some(sprop_max_don_diff) = spec.format.sprop_max_don_diff {
        parameters.insert(
            "sprop-max-don-diff".to_owned(),
            json!(sprop_max_don_diff.to_string()),
        );
    }

    parameters
}

fn serialize_rtcp_feedback(payload: &PayloadParams) -> Vec<Value> {
    let mut feedback = Vec::new();
    if payload.fb_nack() {
        feedback.push(json!({ "type": webrtc::rtcp_feedback::kind::NACK }));
    }
    if payload.fb_pli() {
        feedback.push(json!({
            "type": webrtc::rtcp_feedback::kind::NACK,
            "parameter": webrtc::rtcp_feedback::parameter::PLI,
        }));
    }
    if payload.fb_fir() {
        feedback.push(json!({
            "type": webrtc::rtcp_feedback::kind::CCM,
            "parameter": webrtc::rtcp_feedback::parameter::FIR,
        }));
    }
    if payload.fb_remb() {
        feedback.push(json!({ "type": webrtc::rtcp_feedback::kind::GOOG_REMB }));
    }
    if payload.fb_transport_cc() {
        feedback.push(json!({ "type": webrtc::rtcp_feedback::kind::TRANSPORT_CC }));
    }
    feedback
}

fn serialize_header_extension(media_kind: &str, id: u8, uri: &str) -> Value {
    json!({
        "kind": media_kind,
        "uri": uri,
        "preferredId": id,
        "preferredEncrypt": false,
        "direction": webrtc::sdp::direction::SEND_RECV,
    })
}

fn boolean_flag(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}
