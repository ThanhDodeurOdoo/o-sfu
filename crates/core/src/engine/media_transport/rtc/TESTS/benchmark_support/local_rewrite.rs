use str0m::rtp::{SeqNo, Ssrc};

use super::super::{
    local_send_rewrite::{
        ConsumerStreamStore, DeliveryGenerations, SourceTransition, Vp8PayloadIdentity,
    },
    source_route::SourceFilterGeneration,
};

const RTP_REWRITE_PACKETS: usize = 4096;

#[derive(Clone, Copy)]
enum RewriteMode {
    Steady,
    Switching,
}

struct RewriteInput {
    source_ssrc: Ssrc,
    sequence_number: SeqNo,
    delivery_epoch: u64,
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
            if let Some(identity) = self.streams.project_identity(
                self.stream_handle,
                DeliveryGenerations::new(input.delivery_epoch, SourceFilterGeneration::default()),
                input.source_ssrc,
                input.sequence_number,
                input.timestamp,
                input.vp8_payload,
            ) {
                let vp8_payload = identity.vp8_payload;
                checksum = checksum
                    .wrapping_add(u64::from(identity.rtp_timestamp))
                    .wrapping_add(u64::from(vp8_payload.picture_id.unwrap_or_default()))
                    .wrapping_add(u64::from(vp8_payload.tl0_pic_idx.unwrap_or_default()))
                    .wrapping_add(u64::from(u8::from(matches!(
                        identity.transition,
                        SourceTransition::Switched { .. }
                    ))));
            }
        }
        checksum
    }
}

fn rewrite_inputs(mode: RewriteMode) -> Vec<RewriteInput> {
    let mut inputs = Vec::with_capacity(RTP_REWRITE_PACKETS);
    for pkt_idx in 0..RTP_REWRITE_PACKETS {
        let pkt_idx_u32 = u32::try_from(pkt_idx).unwrap_or(0);
        let pkt_idx_u16 = u16::try_from(pkt_idx % 1024).unwrap_or(0);
        let pkt_idx_u8 = u8::try_from(pkt_idx % 64).unwrap_or(0);
        let source_ssrc = match mode {
            RewriteMode::Steady => Ssrc::from(11),
            RewriteMode::Switching if pkt_idx % 2 == 0 => Ssrc::from(11),
            RewriteMode::Switching => Ssrc::from(12),
        };
        inputs.push(RewriteInput {
            source_ssrc,
            sequence_number: u64::from(pkt_idx_u16).into(),
            delivery_epoch: match mode {
                RewriteMode::Steady => 0,
                RewriteMode::Switching => pkt_idx_u32.into(),
            },
            timestamp: 90_000_u32.wrapping_add(pkt_idx_u32),
            vp8_payload: Vp8PayloadIdentity {
                picture_id: Some(pkt_idx_u16),
                tl0_pic_idx: Some(pkt_idx_u8),
            },
        });
    }
    inputs
}
