#![allow(
    clippy::expect_used,
    reason = "profile invariants fail immediately when code-controlled projection is invalid"
)]

use o_sfu_rfc::webrtc::rtp_header_extension_uri;
use o_sfu_router::{MediaKind as RouterMediaKind, rtp::RtcpFeedbackKind};
use str0m::{format::Codec, media::MediaKind};

use super::*;

fn codec_flags(mask: u8) -> MediaCodecFlags {
    MediaCodecFlags::default()
        .with_opus(mask & 1 != 0)
        .with_pcmu(mask & 2 != 0)
        .with_pcma(mask & 4 != 0)
        .with_vp8(mask & 8 != 0)
        .with_h264(mask & 16 != 0)
        .with_h265(mask & 32 != 0)
        .with_vp9(mask & 64 != 0)
        .with_av1(mask & 128 != 0)
}

fn unique_names(config: &mut RtcConfig, audio: bool) -> Vec<String> {
    let mut names = config
        .codec_config()
        .params()
        .iter()
        .filter(|payload| payload.spec().codec.is_audio() == audio)
        .map(|payload| payload.spec().codec.to_string())
        .collect::<Vec<_>>();
    names.dedup();
    names
}

#[test]
fn all_codec_sets_keep_profile_views_aligned() {
    for mask in 0..=u8::MAX {
        let profile = RtpProfile::compile(codec_flags(mask), CodecPreferences::default())
            .expect("code-controlled RTP profile should project");
        let mut config = profile.session_config();
        let capabilities = profile.router_capabilities();
        let mut codecs = capabilities.codecs();
        for payload in config.codec_config().params() {
            let primary = codecs.next().expect("primary codec should project");
            assert_eq!(primary.codec_name(), payload.spec().codec.to_string());
            assert_eq!(primary.payload_type(), Some(*payload.pt()));
            if let Some(resend) = payload.resend() {
                let rtx = codecs.next().expect("RTX codec should project");
                assert_eq!(rtx.codec_name(), "rtx");
                assert_eq!(rtx.payload_type(), Some(*resend));
            }
        }
        assert!(codecs.next().is_none(), "codec mismatch for mask {mask}");
        for (kind, audio) in [(MediaKind::Audio, true), (MediaKind::Video, false)] {
            assert_eq!(profile.codec_names(kind), unique_names(&mut config, audio));
        }
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the complete wire contract is clearer as one exact profile regression"
)]
fn all_enabled_profile_keeps_the_browser_wire_contract() {
    let profile = RtpProfile::compile(codec_flags(u8::MAX), CodecPreferences::default())
        .expect("code-controlled RTP profile should project");
    let mut config = profile.session_config();
    let payloads = config.codec_config().params();
    assert_eq!(
        payloads
            .iter()
            .map(|payload| (
                payload.spec().codec,
                *payload.pt(),
                payload.resend().map(|pt| *pt)
            ))
            .collect::<Vec<_>>(),
        [
            (Codec::Opus, 111_u8, None),
            (Codec::PCMU, 0, None),
            (Codec::PCMA, 8, None),
            (Codec::Vp8, 96, Some(97)),
            (Codec::H264, 127, Some(121)),
            (Codec::H264, 125, Some(107)),
            (Codec::H264, 108, Some(109)),
            (Codec::H264, 124, Some(120)),
            (Codec::H264, 123, Some(119)),
            (Codec::H264, 35, Some(36)),
            (Codec::H264, 114, Some(115)),
            (Codec::H265, 102, Some(103)),
            (Codec::Vp9, 98, Some(99)),
            (Codec::Vp9, 100, Some(101)),
            (Codec::Av1, 45, Some(46)),
        ]
    );
    let fmtp = payloads
        .iter()
        .filter_map(|payload| {
            let fmtp = payload.spec().format.to_string();
            (!fmtp.is_empty()).then(|| format!("{} {fmtp}", *payload.pt()))
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        fmtp,
        concat!(
            "111 minptime=10;useinbandfec=1\n",
            "127 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f\n",
            "125 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f\n",
            "108 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f\n",
            "124 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f\n",
            "123 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=4d001f\n",
            "35 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=4d001f\n",
            "114 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=64001f\n",
            "102 profile-id=1;tier-flag=0;level-id=180\n",
            "98 profile-id=0\n",
            "100 profile-id=2"
        )
    );
    assert!(payloads.iter().all(|payload| {
        let video = payload.spec().codec.is_video();
        payload.fb_transport_cc()
            && payload.fb_nack() == video
            && payload.fb_pli() == video
            && payload.fb_fir() == video
            && payload.fb_remb() == video
    }));
    let capabilities = profile.router_capabilities();
    let mut codecs = capabilities.codecs();
    for payload in payloads {
        let codec = codecs.next().expect("primary router codec should project");
        let fmtp = payload.spec().format.to_string();
        let video = payload.spec().codec.is_video();
        let kind = if video {
            RouterMediaKind::Video
        } else {
            RouterMediaKind::Audio
        };
        assert_eq!(codec.media_kind(), kind);
        assert_eq!(codec.clock_rate(), payload.spec().clock_rate.get());
        assert_eq!(codec.channels(), payload.spec().channels.map(u16::from));
        let params = fmtp
            .split(';')
            .filter_map(|entry| entry.split_once('='))
            .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()));
        assert!(codec.parameters().eq(params));
        assert!(
            codec
                .rtcp_feedback()
                .map(|feedback| feedback.kind().clone())
                .eq([
                    Some(RtcpFeedbackKind::TransportCc),
                    video.then_some(RtcpFeedbackKind::Nack),
                    video.then_some(RtcpFeedbackKind::NackPli),
                    video.then_some(RtcpFeedbackKind::CcmFir),
                    video.then_some(RtcpFeedbackKind::GoogRemb),
                ]
                .into_iter()
                .flatten())
        );
        if let Some(resend) = payload.resend() {
            let rtx = codecs.next().expect("RTX router codec should project");
            assert_eq!(rtx.codec_name(), "rtx");
            assert_eq!(rtx.payload_type(), Some(*resend));
        }
    }
    assert!(codecs.next().is_none());
    assert_eq!(
        config
            .extension_map()
            .iter()
            .map(|(id, extension)| (id, extension.as_uri()))
            .collect::<Vec<_>>(),
        [
            (1_u8, rtp_header_extension_uri::SSRC_AUDIO_LEVEL),
            (2, rtp_header_extension_uri::ABS_SEND_TIME),
            (3, rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01),
            (4, rtp_header_extension_uri::MID),
            (10, rtp_header_extension_uri::RTP_STREAM_ID),
            (13, "urn:3gpp:video-orientation"),
        ]
    );
    let router_exts = capabilities
        .header_extensions()
        .map(|extension| (extension.id().value(), extension.uri()));
    let rtc_exts = config
        .extension_map()
        .iter()
        .map(|(id, extension)| (id, extension.as_uri()));
    assert!(router_exts.eq(rtc_exts));
}

#[test]
fn answer_validation_keeps_repair_only_for_producer_uploads() {
    for attribute in [
        "a=rtpmap:97 rtx/90000",
        "a=fmtp:97 apt=96",
        "a=rtcp-fb:96 nack",
        "a=extmap:11 urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
        "a=ssrc-group:FID 4000000001 4000000002",
    ] {
        let downstream =
            format!("m=video 9 UDP/TLS/RTP/SAVPF 96 97\r\na=recvonly\r\n{attribute}\r\n");
        let upload = format!("m=video 9 UDP/TLS/RTP/SAVPF 96 97\r\na=sendonly\r\n{attribute}\r\n");
        assert!(RtpProfile::validate_answer_sdp(&downstream).is_err());
        assert!(RtpProfile::validate_answer_sdp(&upload).is_ok());
    }
    for attribute in [
        "a=rtpmap:96 VP8/90000",
        "a=rtcp-fb:96 nack pli",
        "a=extmap:10 urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
    ] {
        assert!(RtpProfile::validate_answer_sdp(attribute).is_ok());
    }
}

#[test]
fn offer_projection_strips_repair_only_from_consumer_media() {
    let offer = concat!(
        "v=0\r\n",
        "m=video 9 UDP/TLS/RTP/SAVPF 96 97\r\n",
        "a=recvonly\r\n",
        "a=rtpmap:96 VP8/90000\r\n",
        "a=rtpmap:97 rtx/90000\r\n",
        "a=fmtp:97 apt=96\r\n",
        "a=rtcp-fb:96 nack\r\n",
        "m=video 9 UDP/TLS/RTP/SAVPF 96 97\r\n",
        "a=sendonly\r\n",
        "a=rtpmap:96 VP8/90000\r\n",
        "a=rtpmap:97 rtx/90000\r\n",
        "a=fmtp:97 apt=96\r\n",
        "a=rtcp-fb:96 nack\r\n",
    );

    let projected = RtpProfile::strip_downstream_repair(offer);
    let sections = projected.split("m=video").collect::<Vec<_>>();
    assert_eq!(sections.len(), 3);
    let upload = sections.get(1).copied().unwrap_or_default();
    let consumer = sections.get(2).copied().unwrap_or_default();
    assert!(upload.contains("SAVPF 96 97"));
    assert!(upload.contains("a=rtpmap:97 rtx/90000"));
    assert!(upload.contains("a=rtcp-fb:96 nack"));
    assert!(consumer.contains("SAVPF 96\r\n"));
    assert!(!consumer.contains("rtx/90000"));
    assert!(!consumer.contains(" apt="));
    assert!(!consumer.contains("a=rtcp-fb:96 nack\r\n"));
}
