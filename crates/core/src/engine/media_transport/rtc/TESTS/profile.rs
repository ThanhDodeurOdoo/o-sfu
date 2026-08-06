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
        assert!(
            capabilities
                .codecs()
                .filter(|codec| codec.codec_name() != "rtx")
                .map(|codec| (codec.codec_name().to_owned(), codec.payload_type()))
                .eq(config
                    .codec_config()
                    .params()
                    .iter()
                    .map(|payload| (payload.spec().codec.to_string(), Some(*payload.pt())))),
            "capability mismatch for codec mask {mask}"
        );
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
            (Codec::Vp8, 96, None),
            (Codec::H264, 127, None),
            (Codec::H264, 125, None),
            (Codec::H264, 108, None),
            (Codec::H264, 124, None),
            (Codec::H264, 123, None),
            (Codec::H264, 35, None),
            (Codec::H264, 114, None),
            (Codec::H265, 102, None),
            (Codec::Vp9, 98, None),
            (Codec::Vp9, 100, None),
            (Codec::Av1, 45, None),
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
            && !payload.fb_nack()
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
                    video.then_some(RtcpFeedbackKind::NackPli),
                    video.then_some(RtcpFeedbackKind::CcmFir),
                    video.then_some(RtcpFeedbackKind::GoogRemb),
                ]
                .into_iter()
                .flatten())
        );
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
fn answer_validation_rejects_retransmission_attributes() {
    for attribute in [
        "a=rtpmap:97 RTX/90000",
        "a=fmtp:97 apt=96",
        "a=extmap:5 urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
        "a=ssrc-group:FID 1234 5678",
    ] {
        assert!(RtpProfile::validate_answer_sdp(attribute).is_err());
    }
    assert!(RtpProfile::validate_answer_sdp("a=rtcp-fb:96 nack").is_ok());
}
