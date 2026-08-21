use std::{net::SocketAddr, time::Instant};

use str0m::{
    Candidate, Rtc,
    change::SdpOffer,
    format::{Codec, FormatParams},
    media::Frequency,
};

pub(crate) struct ProtocolHarnessRtcPeer {
    rtc: Rtc,
    declines_video_repair: bool,
}

impl ProtocolHarnessRtcPeer {
    fn new_with_rtc(port: u16, mut rtc: Rtc) -> Option<Self> {
        let mut video_payloads = rtc
            .codec_config()
            .params()
            .iter()
            .filter(|payload| payload.spec().codec.is_video())
            .peekable();
        let declines_video_repair = video_payloads.peek().is_some()
            && video_payloads.all(|payload| payload.resend().is_none() && !payload.fb_nack());
        rtc.add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp").ok()?,
        )?;
        Some(Self {
            rtc,
            declines_video_repair,
        })
    }

    pub(crate) fn answer_offer(&mut self, offer_sdp: &str) -> Option<String> {
        let reduced_offer = self
            .declines_video_repair
            .then(|| without_vp8_repair(offer_sdp));
        let offer_sdp = reduced_offer.as_deref().unwrap_or(offer_sdp);
        let answer = self
            .rtc
            .sdp_api()
            .accept_offer(SdpOffer::from_sdp_string(offer_sdp).ok()?)
            .ok()?;
        Some(answer.to_sdp_string())
    }
}

fn without_vp8_repair(sdp: &str) -> String {
    sdp.replace(" 96 97", " 96")
        .replace("a=rtcp-fb:96 nack\r\n", "")
        .replace("a=rtpmap:97 rtx/90000\r\n", "")
        .replace("a=fmtp:97 apt=96\r\n", "")
}

#[derive(Clone, Copy)]
pub(crate) struct ProtocolHarnessRtcPeerFactory {
    port: u16,
    build_rtc: fn() -> Rtc,
}

impl ProtocolHarnessRtcPeerFactory {
    pub(crate) fn new(port: u16, build_rtc: fn() -> Rtc) -> Self {
        Self { port, build_rtc }
    }

    pub(crate) fn build_peer(self) -> Option<ProtocolHarnessRtcPeer> {
        ProtocolHarnessRtcPeer::new_with_rtc(self.port, (self.build_rtc)())
    }
}

pub(crate) fn default_protocol_harness_rtc() -> Rtc {
    Rtc::new(Instant::now())
}

pub(crate) fn reduced_capability_rtc() -> Rtc {
    let mut config = Rtc::builder().clear_codecs();
    config.codec_config().add_config(
        111.into(),
        None,
        Codec::Opus,
        Frequency::FORTY_EIGHT_KHZ,
        Some(2),
        FormatParams {
            use_inband_fec: Some(true),
            ..Default::default()
        },
    );
    config.codec_config().add_config(
        96.into(),
        None,
        Codec::Vp8,
        Frequency::NINETY_KHZ,
        None,
        FormatParams::default(),
    );
    if let Some(video) = config.codec_config().last_mut() {
        video.set_fb_nack(false);
    }
    config.build(Instant::now())
}
