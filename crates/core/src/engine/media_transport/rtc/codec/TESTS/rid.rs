use std::fmt::Write as _;

use o_sfu_rfc::webrtc;
use o_sfu_router::rtp::{MediaStream, StreamBinding};
use str0m::media::{Mid, Rid};

use super::*;
use crate::Bitrate;

const HIGH_MAX_BITRATE: Bitrate = Bitrate::from_kbps(900);

fn send_rids(
    answer_sdp: &str,
    offered_encodings: &[SessionUploadEncoding],
) -> Result<Vec<NegotiatedRid>, SimulcastAnswerError> {
    negotiate_answer_rids(&parse_section_rids(answer_sdp)?, offered_encodings)
}

#[test]
fn answer_preserves_declared_bitrate() {
    let answer = answer(
        "video_0",
        &[("lo", Some("max-br=150000")), ("hi", Some("max-br=900000"))],
        Some("lo;hi"),
    );

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Ok(vec![
            NegotiatedRid {
                rid: Rid::from(DEFAULT_LOW_RID),
                max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE),
            },
            NegotiatedRid {
                rid: Rid::from(DEFAULT_HIGH_RID),
                max_bitrate: Some(HIGH_MAX_BITRATE),
            },
        ])
    );
}

#[test]
fn answer_keeps_only_accepted_simulcast_rids() {
    let answer = answer(
        "video_0",
        &[("lo", Some("max-br=150000")), ("hi", Some("max-br=900000"))],
        Some("lo"),
    );

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Ok(vec![NegotiatedRid {
            rid: Rid::from(DEFAULT_LOW_RID),
            max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE),
        }])
    );
}

#[test]
fn answer_preserves_simulcast_order() {
    let answer = answer(
        "video_0",
        &[("hi", Some("max-br=900000")), ("lo", Some("max-br=150000"))],
        Some("lo;hi"),
    );

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Ok(vec![
            NegotiatedRid {
                rid: Rid::from(DEFAULT_LOW_RID),
                max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE),
            },
            NegotiatedRid {
                rid: Rid::from(DEFAULT_HIGH_RID),
                max_bitrate: Some(HIGH_MAX_BITRATE),
            },
        ])
    );
}

#[test]
fn answer_uses_offered_bitrate_when_omitted() {
    let answer = answer("video_0", &[("lo", None), ("hi", None)], Some("lo;hi"));

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Ok(vec![
            NegotiatedRid {
                rid: Rid::from(DEFAULT_LOW_RID),
                max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE),
            },
            NegotiatedRid {
                rid: Rid::from(DEFAULT_HIGH_RID),
                max_bitrate: Some(HIGH_MAX_BITRATE),
            },
        ])
    );
}

#[test]
fn answer_rejects_bitrate_when_offer_has_none() {
    let answer = answer("video_0", &[("lo", Some("max-br=150000"))], Some("lo"));

    assert_eq!(
        send_rids(&answer, &custom_upload_encodings("lo", None)),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_accepts_lower_bitrate_than_offer() {
    let answer = answer("video_0", &[("lo", Some("max-br=149999"))], Some("lo"));

    assert_eq!(
        send_rids(&answer, &custom_upload_encodings("lo", Some(150_000))),
        Ok(vec![NegotiatedRid {
            rid: Rid::from("lo"),
            max_bitrate: Some(Bitrate::from_bps(149_999)),
        }])
    );
}

#[test]
fn answer_rejects_malformed_or_valueless_bitrate() {
    for restriction in ["max-br=bad", "max-br"] {
        let answer = answer("video_0", &[("lo", Some(restriction))], Some("lo"));

        assert_eq!(
            send_rids(&answer, &custom_upload_encodings("lo", Some(150_000))),
            Err(SimulcastAnswerError)
        );
    }
}

#[test]
fn answer_rejects_duplicate_rid_declaration() {
    let answer = answer(
        "video_0",
        &[("lo", Some("max-br=150000")), ("lo", Some("max-br=149999"))],
        Some("lo"),
    );

    assert_eq!(
        send_rids(&answer, &custom_upload_encodings("lo", Some(150_000))),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_rejects_unmodeled_restrictions() {
    for restriction in ["max-br=150000;max-width=640", "pt=96;max-br=150000"] {
        let answer = answer("video_0", &[("lo", Some(restriction))], Some("lo"));

        assert_eq!(
            send_rids(&answer, &custom_upload_encodings("lo", Some(150_000))),
            Err(SimulcastAnswerError)
        );
    }
}

#[test]
fn answer_rejects_rid_prefix_alias() {
    let answer = answer("video_0", &[("abcdefghX", None)], Some("abcdefghX"));

    assert_eq!(
        send_rids(&answer, &custom_upload_encodings("abcdefgh", None)),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_requires_accepted_simulcast_send_list() {
    let answer = answer(
        "video_0",
        &[("lo", Some("max-br=150000")), ("hi", Some("max-br=900000"))],
        None,
    );

    assert!(matches!(
        send_rids(&answer, &default_upload_encodings()),
        Ok(rids) if rids.is_empty()
    ));
}

#[test]
fn answer_rejects_simulcast_alternatives() {
    let send = format!(
        "{pause}lo{alternative}backup{separator}hi",
        pause = webrtc::sdp::simulcast::INITIAL_PAUSE_PREFIX,
        alternative = webrtc::sdp::simulcast::ALTERNATIVE_SEPARATOR,
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );
    let answer = answer(
        "video_0",
        &[
            ("lo", Some("max-br=150000")),
            ("backup", Some("max-br=450000")),
            ("hi", Some("max-br=900000")),
        ],
        Some(&send),
    );

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_matches_exact_mid_section() -> Result<(), SimulcastAnswerError> {
    let answer_sdp = format!(
        concat!(
            "v=0\r\n",
            "o=- 1 1 IN IP4 0.0.0.0\r\n",
            "s=-\r\n",
            "t=0 0\r\n",
            "a=group:BUNDLE camera cam\r\n",
            "a=mid:cam\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:camera\r\n",
            "a=sendonly\r\n",
            "a=rtpmap:96 VP8/90000\r\n",
            "a={rid}:wrong {send} {max_br}=111000\r\n",
            "a={simulcast}:{send} wrong\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:cam\r\n",
            "a=sendonly\r\n",
            "a=rtpmap:96 VP8/90000\r\n",
            "a={rid}:right {send} {max_br}=222000\r\n",
            "a={simulcast}:{send} right\r\n"
        ),
        rid = webrtc::sdp::attribute::RID,
        simulcast = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
    );
    let answer = SdpAnswer::from_sdp_string(&answer_sdp).map_err(|_error| SimulcastAnswerError)?;
    let rids = ParsedAnswerRids::parse(&answer_sdp, &answer);

    assert_eq!(
        rids.negotiate(
            Mid::from("cam"),
            &custom_upload_encodings("right", Some(222_000))
        ),
        Ok(vec![NegotiatedRid {
            rid: Rid::from("right"),
            max_bitrate: Some(Bitrate::from_bps(222_000)),
        }])
    );
    assert_eq!(
        rids.negotiate(
            Mid::from("camera"),
            &custom_upload_encodings("wrong", Some(111_000))
        ),
        Ok(vec![NegotiatedRid {
            rid: Rid::from("wrong"),
            max_bitrate: Some(Bitrate::from_bps(111_000)),
        }])
    );
    Ok(())
}

#[test]
fn answer_rejects_invalid_rfc8852_ids() {
    let send = format!(
        "low-1{separator}hi_2{separator}hi2",
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );
    let answer = answer(
        "video_0",
        &[
            ("low-1", Some("max-br=150000")),
            ("hi_2", Some("max-br=450000")),
            ("hi2", Some("max-br=900000")),
        ],
        Some(&send),
    );

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_rejects_extra_simulcast_streams() {
    let send = format!(
        "lo{separator}mid{separator}hi",
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );
    let answer = answer(
        "video_0",
        &[
            ("lo", Some("max-br=150000")),
            ("mid", Some("max-br=450000")),
            ("hi", Some("max-br=900000")),
        ],
        Some(&send),
    );

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn initial_gate_selects_lowest_bitrate_rid() {
    let parameters = MediaStream::new(
        Vec::new(),
        Vec::new(),
        vec![
            StreamBinding::new()
                .with_rid("hi")
                .with_max_bitrate(4_000_000),
            StreamBinding::new()
                .with_rid("lo")
                .with_max_bitrate(150_000),
        ],
    );

    assert_eq!(
        initial_packet_gate(&parameters),
        PacketLayerGate::Rid("lo".into())
    );
}

#[test]
fn initial_gate_uses_declared_order_without_bitrates() {
    let parameters = MediaStream::new(
        Vec::new(),
        Vec::new(),
        vec![
            StreamBinding::new().with_rid("lo"),
            StreamBinding::new().with_rid("hi"),
        ],
    );

    assert_eq!(
        initial_packet_gate(&parameters),
        PacketLayerGate::Rid("lo".into())
    );
}

#[test]
fn initial_gate_keeps_ridless_or_mixed_routes_open() {
    let ridless = MediaStream::new(Vec::new(), Vec::new(), vec![StreamBinding::new()]);
    let mixed = MediaStream::new(
        Vec::new(),
        Vec::new(),
        vec![
            StreamBinding::new().with_rid("lo"),
            StreamBinding::new().with_ssrc(72_002),
        ],
    );

    assert_eq!(initial_packet_gate(&ridless), PacketLayerGate::Open);
    assert_eq!(initial_packet_gate(&mixed), PacketLayerGate::Open);
}

fn answer(mid: &str, declarations: &[(&str, Option<&str>)], simulcast: Option<&str>) -> String {
    let mut answer = format!("v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:{mid}\r\n");
    for (rid, restriction) in declarations {
        let restriction = restriction.map_or(String::new(), |value| format!(" {value}"));
        let _ = write!(
            answer,
            "a={attribute}:{rid} {send}{restriction}\r\n",
            attribute = webrtc::sdp::attribute::RID,
            send = webrtc::sdp::rid::DIRECTION_SEND,
        );
    }
    if let Some(simulcast) = simulcast {
        let _ = write!(
            answer,
            "a={attribute}:{send} {simulcast}\r\n",
            attribute = webrtc::sdp::attribute::SIMULCAST,
            send = webrtc::sdp::rid::DIRECTION_SEND,
        );
    }
    answer
}

fn default_upload_encodings() -> Vec<SessionUploadEncoding> {
    vec![
        SessionUploadEncoding {
            rid: DEFAULT_LOW_RID.to_owned(),
            max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE),
            resolution_scale: Some(4),
            max_framerate: None,
        },
        SessionUploadEncoding {
            rid: DEFAULT_HIGH_RID.to_owned(),
            max_bitrate: Some(HIGH_MAX_BITRATE),
            resolution_scale: Some(1),
            max_framerate: None,
        },
    ]
}

fn custom_upload_encodings(rid: &str, max_bitrate: Option<u64>) -> Vec<SessionUploadEncoding> {
    vec![SessionUploadEncoding {
        rid: rid.to_owned(),
        max_bitrate: max_bitrate.map(Bitrate::from_bps),
        resolution_scale: None,
        max_framerate: None,
    }]
}
