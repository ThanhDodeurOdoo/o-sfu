use std::net::SocketAddr;

use str0m::{
    Candidate, Rtc,
    change::SdpOffer,
    format::{Codec, FormatParams},
    media::Frequency,
};

use super::rtc_without_retransmission;

pub(crate) struct ProtocolHarnessRtcPeer {
    rtc: Rtc,
}

impl ProtocolHarnessRtcPeer {
    fn new_with_rtc(port: u16, mut rtc: Rtc) -> Option<Self> {
        rtc.add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp").ok()?,
        )?;
        Some(Self { rtc })
    }

    pub(crate) fn answer_offer(&mut self, offer_sdp: &str) -> Option<String> {
        let answer = self
            .rtc
            .sdp_api()
            .accept_offer(SdpOffer::from_sdp_string(offer_sdp).ok()?)
            .ok()?;
        Some(answer.to_sdp_string())
    }
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
    rtc_without_retransmission(Rtc::builder())
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
    rtc_without_retransmission(config)
}
