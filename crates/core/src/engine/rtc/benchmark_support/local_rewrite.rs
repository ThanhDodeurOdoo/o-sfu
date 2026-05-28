use str0m::rtp::Ssrc;

use super::super::local_send_rewrite::{
    ConsumerStreamStore, Vp8PayloadIdentity, next_projected_rtp_identity,
};

const RTP_REWRITE_PACKETS: usize = 4096;

#[derive(Clone, Copy)]
enum RewriteMode {
    Steady,
    Switching,
}

struct RewriteInput {
    source_ssrc: Ssrc,
    timestamp: u32,
    vp8_payload: Vp8PayloadIdentity,
}

/// fixed RTP identity rewrite fixture for local egress benchmarks
///
/// setup allocates one destination rewrite stream. the measured path projects
/// either a steady SSRC sequence or alternating simulcast SSRC packets with VP8
/// picture identifiers
pub struct LocalRewriteBenchFixture {
    streams: ConsumerStreamStore,
    inputs: Vec<RewriteInput>,
    stream_handle: super::super::slots::ConsumerStreamHandle,
}

impl LocalRewriteBenchFixture {
    #[must_use]
    pub fn steady_ssrc() -> Self {
        Self::new(RewriteMode::Steady)
    }

    #[must_use]
    pub fn switching_ssrc() -> Self {
        Self::new(RewriteMode::Switching)
    }

    fn new(mode: RewriteMode) -> Self {
        let mut streams = ConsumerStreamStore::default();
        let stream_handle = streams.allocate();
        Self {
            streams,
            inputs: rewrite_inputs(mode),
            stream_handle,
        }
    }

    #[must_use]
    pub fn project_packets(&mut self) -> u64 {
        let mut checksum = 0_u64;
        for input in &self.inputs {
            if let Some(identity) = next_projected_rtp_identity(
                &mut self.streams,
                self.stream_handle,
                input.source_ssrc,
                input.timestamp,
                input.vp8_payload,
            ) {
                let vp8_payload = identity.vp8_payload;
                checksum = checksum
                    .wrapping_add(u64::from(identity.rtp_timestamp))
                    .wrapping_add(u64::from(vp8_payload.picture_id.unwrap_or_default()))
                    .wrapping_add(u64::from(vp8_payload.tl0_pic_idx.unwrap_or_default()))
                    .wrapping_add(u64::from(u8::from(identity.source_switched)));
            }
        }
        checksum
    }
}

fn rewrite_inputs(mode: RewriteMode) -> Vec<RewriteInput> {
    let mut inputs = Vec::with_capacity(RTP_REWRITE_PACKETS);
    for packet_idx in 0..RTP_REWRITE_PACKETS {
        let packet_idx_u32 = u32::try_from(packet_idx).unwrap_or(0);
        let packet_idx_u16 = u16::try_from(packet_idx % 1024).unwrap_or(0);
        let packet_idx_u8 = u8::try_from(packet_idx % 64).unwrap_or(0);
        let source_ssrc = match mode {
            RewriteMode::Steady => Ssrc::from(11),
            RewriteMode::Switching if packet_idx % 2 == 0 => Ssrc::from(11),
            RewriteMode::Switching => Ssrc::from(12),
        };
        inputs.push(RewriteInput {
            source_ssrc,
            timestamp: 90_000_u32.wrapping_add(packet_idx_u32),
            vp8_payload: Vp8PayloadIdentity {
                picture_id: Some(packet_idx_u16),
                tl0_pic_idx: Some(packet_idx_u8),
            },
        });
    }
    inputs
}
