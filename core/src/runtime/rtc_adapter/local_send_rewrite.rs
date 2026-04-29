//! Receiver-local RTP identity rewriting for browser-bound media.
//!
//! # Boundary role
//!
//! Publisher RTP can arrive from several simulcast SSRCs while the browser
//! consumer receives one downstream stream. This module owns the receiver-local
//! identity for that stream: sequence number, RTP timestamp and the VP8 payload
//! descriptor fields browsers use to order frames across layer switches.
//!
//! It does not choose which RID should be forwarded. Route control has already
//! made that decision before packets reach the local send boundary.

use std::collections::HashMap;

use str0m::rtp::{SeqNo, Ssrc};

use crate::runtime::transport_adapter::TransportMediaId;

const VP8_LONG_PICTURE_ID_MODULUS: u16 = 1 << 15;

/// Key for one browser consumer's rewritten RTP stream.
///
/// The key is intentionally not scoped by publisher RID. Fallback and selected
/// simulcast layers must share one downstream identity because they are written
/// into the same rid-less browser `StreamTx`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct LocalSendRewriteKey {
    transport_media_id: TransportMediaId,
}

/// Mutable identity anchors for one downstream browser RTP stream.
///
/// The source SSRC can change when route control moves between simulcast
/// layers. The local anchors preserve a monotonic receiver view while keeping
/// source deltas within one SSRC, so jitter buffer and VP8 decoder state do not
/// see unrelated publisher identities as one discontinuous stream.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct LocalSendRewriteState {
    next_seq_no: SeqNo,
    source_ssrc: Option<Ssrc>,
    source_timestamp_anchor: u32,
    local_timestamp_anchor: u32,
    last_local_timestamp: Option<u32>,
    source_picture_id_anchor: Option<u16>,
    local_picture_id_anchor: Option<u16>,
    last_local_picture_id: Option<u16>,
    source_tl0_pic_idx_anchor: Option<u8>,
    local_tl0_pic_idx_anchor: Option<u8>,
    last_local_tl0_pic_idx: Option<u8>,
}

impl LocalSendRewriteState {
    fn take_seq_no(&mut self) -> SeqNo {
        self.next_seq_no.inc()
    }

    fn rewrite(
        &mut self,
        source_ssrc: Ssrc,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> LocalSendRewrite {
        let seq_no = self.take_seq_no();
        let previous_source_ssrc = self.source_ssrc;
        if previous_source_ssrc != Some(source_ssrc) {
            let rtp_timestamp = self
                .last_local_timestamp
                .map_or(source_timestamp, |timestamp| timestamp.wrapping_add(1));
            self.source_ssrc = Some(source_ssrc);
            self.source_timestamp_anchor = source_timestamp;
            self.local_timestamp_anchor = rtp_timestamp;
            self.last_local_timestamp = Some(rtp_timestamp);
            self.reset_vp8_source_anchors();
            let rewritten_vp8_payload = self.rewrite_vp8_payload(vp8_payload);
            return LocalSendRewrite {
                seq_no,
                rtp_timestamp,
                vp8_payload: rewritten_vp8_payload,
                previous_source_ssrc,
                source_switched: previous_source_ssrc.is_some(),
            };
        }
        let rtp_timestamp = self
            .local_timestamp_anchor
            .wrapping_add(source_timestamp.wrapping_sub(self.source_timestamp_anchor));
        self.last_local_timestamp = Some(rtp_timestamp);
        let rewritten_vp8_payload = self.rewrite_vp8_payload(vp8_payload);
        LocalSendRewrite {
            seq_no,
            rtp_timestamp,
            vp8_payload: rewritten_vp8_payload,
            previous_source_ssrc,
            source_switched: false,
        }
    }

    fn reset_vp8_source_anchors(&mut self) {
        self.source_picture_id_anchor = None;
        self.local_picture_id_anchor = None;
        self.source_tl0_pic_idx_anchor = None;
        self.local_tl0_pic_idx_anchor = None;
    }

    fn rewrite_vp8_payload(&mut self, vp8_payload: Vp8PayloadIdentity) -> Vp8PayloadIdentity {
        Vp8PayloadIdentity {
            picture_id: vp8_payload
                .picture_id
                .map(|picture_id| self.rewrite_picture_id(picture_id)),
            tl0_pic_idx: vp8_payload
                .tl0_pic_idx
                .map(|tl0_pic_idx| self.rewrite_tl0_pic_idx(tl0_pic_idx)),
        }
    }

    fn rewrite_picture_id(&mut self, source_picture_id: u16) -> u16 {
        let local_picture_id = match (self.source_picture_id_anchor, self.local_picture_id_anchor) {
            (Some(source_anchor), Some(local_anchor)) => {
                let source_delta =
                    source_picture_id.wrapping_sub(source_anchor) % VP8_LONG_PICTURE_ID_MODULUS;
                local_anchor.wrapping_add(source_delta) % VP8_LONG_PICTURE_ID_MODULUS
            }
            _ => self
                .last_local_picture_id
                .map_or(source_picture_id, |last| {
                    last.wrapping_add(1) % VP8_LONG_PICTURE_ID_MODULUS
                }),
        };
        self.source_picture_id_anchor = Some(source_picture_id);
        self.local_picture_id_anchor = Some(local_picture_id);
        self.last_local_picture_id = Some(local_picture_id);
        local_picture_id
    }

    fn rewrite_tl0_pic_idx(&mut self, source_tl0_pic_idx: u8) -> u8 {
        let local_tl0_pic_idx = match (
            self.source_tl0_pic_idx_anchor,
            self.local_tl0_pic_idx_anchor,
        ) {
            (Some(source_anchor), Some(local_anchor)) => {
                local_anchor.wrapping_add(source_tl0_pic_idx.wrapping_sub(source_anchor))
            }
            _ => self
                .last_local_tl0_pic_idx
                .map_or(source_tl0_pic_idx, |last| last.wrapping_add(1)),
        };
        self.source_tl0_pic_idx_anchor = Some(source_tl0_pic_idx);
        self.local_tl0_pic_idx_anchor = Some(local_tl0_pic_idx);
        self.last_local_tl0_pic_idx = Some(local_tl0_pic_idx);
        local_tl0_pic_idx
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LocalSendRewrite {
    seq_no: SeqNo,
    rtp_timestamp: u32,
    vp8_payload: Vp8PayloadIdentity,
    previous_source_ssrc: Option<Ssrc>,
    source_switched: bool,
}

/// VP8 payload descriptor fields that must stay continuous downstream.
///
/// RFC 7741 makes these fields optional, so packets without them can still be
/// forwarded. When they exist, rewriting them together with timestamps avoids a
/// browser decoder seeing one downstream SSRC with unrelated publisher picture
/// spaces after a simulcast layer switch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Vp8PayloadIdentity {
    pub(super) picture_id: Option<u16>,
    pub(super) tl0_pic_idx: Option<u8>,
}

impl LocalSendRewrite {
    pub(super) const fn seq_no(self) -> SeqNo {
        self.seq_no
    }

    pub(super) const fn rtp_timestamp(self) -> u32 {
        self.rtp_timestamp
    }

    pub(super) const fn vp8_payload(self) -> Vp8PayloadIdentity {
        self.vp8_payload
    }

    pub(super) const fn previous_source_ssrc(self) -> Option<Ssrc> {
        self.previous_source_ssrc
    }

    pub(super) const fn source_switched(self) -> bool {
        self.source_switched
    }
}

/// Return the next receiver-local RTP identity for a consumer stream.
///
/// Fresh subscribers must not inherit the publisher sequence space because the
/// browser does not have the corresponding SRTP rollover info. The key is
/// scoped by consumer transport media so fallback and selected simulcast layers
/// share one receiver-safe local sequence and timestamp space.
///
/// This is hot-path code. It mutates one small per-consumer state entry and
/// does not inspect room policy or signaling state.
pub(super) fn next_rewritten_rtp_identity(
    rewrites: &mut HashMap<LocalSendRewriteKey, LocalSendRewriteState>,
    transport_media_id: TransportMediaId,
    source_ssrc: Ssrc,
    source_timestamp: u32,
    vp8_payload: Vp8PayloadIdentity,
) -> LocalSendRewrite {
    rewrites
        .entry(LocalSendRewriteKey { transport_media_id })
        .or_default()
        .rewrite(source_ssrc, source_timestamp, vp8_payload)
}

/// Drop every local send rewrite entry owned by one consumer transport media.
///
/// consumer teardown and replacement must forget this state so any later
/// rebuilt consumer starts from a fresh local counter instead of continuing an
/// old stream identity.
pub(super) fn forget_transport_media_rewrites(
    rewrites: &mut HashMap<LocalSendRewriteKey, LocalSendRewriteState>,
    transport_media_id: TransportMediaId,
) {
    rewrites.retain(|key, _| key.transport_media_id != transport_media_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewritten_sequence_numbers_start_in_initial_roc_and_increment_per_stream() {
        let source_seq: SeqNo = 131_072.into();
        let transport_media_id = TransportMediaId::new(41);
        let mut rewrites = HashMap::new();

        let first = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        let second = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );

        assert_eq!(source_seq.roc(), 2);
        assert_eq!(first.seq_no().roc(), 0);
        assert!(first.seq_no().is_next(second.seq_no()));
    }

    #[test]
    fn rewritten_sequence_numbers_are_scoped_by_consumer_stream() {
        let transport_media_id = TransportMediaId::new(42);
        let mut rewrites = HashMap::new();

        let first = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        let second = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        let other = next_rewritten_rtp_identity(
            &mut rewrites,
            TransportMediaId::new(43),
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );

        assert_eq!(first.seq_no().roc(), 0);
        assert!(first.seq_no().is_next(second.seq_no()));
        assert_eq!(other.seq_no().roc(), 0);
    }

    #[test]
    fn forgetting_transport_media_rewrites_drops_all_stream_state_for_that_consumer() {
        let transport_media_id = TransportMediaId::new(44);
        let mut rewrites = HashMap::new();

        let first = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        let _ = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );

        forget_transport_media_rewrites(&mut rewrites, transport_media_id);

        let reset = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        assert_eq!(first.seq_no().roc(), 0);
        assert_eq!(reset.seq_no().roc(), 0);
        assert_eq!(reset.seq_no().roc(), first.seq_no().roc());
    }

    #[test]
    fn rewritten_timestamps_preserve_source_deltas_on_one_ssrc() {
        let transport_media_id = TransportMediaId::new(45);
        let mut rewrites = HashMap::new();

        let first = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            10_000,
            Vp8PayloadIdentity::default(),
        );
        let second = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            13_000,
            Vp8PayloadIdentity::default(),
        );

        assert_eq!(first.rtp_timestamp(), 10_000);
        assert_eq!(second.rtp_timestamp(), 13_000);
        assert!(!first.source_switched());
        assert!(!second.source_switched());
    }

    #[test]
    fn rewritten_timestamps_stay_monotonic_across_simulcast_ssrc_switches() {
        let transport_media_id = TransportMediaId::new(46);
        let mut rewrites = HashMap::new();

        let low = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            90_000,
            Vp8PayloadIdentity::default(),
        );
        let high = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(222),
            1_000,
            Vp8PayloadIdentity::default(),
        );
        let high_next = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(222),
            4_000,
            Vp8PayloadIdentity::default(),
        );

        assert_eq!(low.rtp_timestamp(), 90_000);
        assert_eq!(high.rtp_timestamp(), 90_001);
        assert_eq!(high_next.rtp_timestamp(), 93_001);
        assert!(high.source_switched());
        assert_eq!(high.previous_source_ssrc(), Some(Ssrc::from(111)));
    }

    #[test]
    fn rewritten_vp8_picture_ids_stay_continuous_across_simulcast_ssrc_switches() {
        let transport_media_id = TransportMediaId::new(47);
        let mut rewrites = HashMap::new();

        let low = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(111),
            90_000,
            Vp8PayloadIdentity {
                picture_id: Some(32_760),
                tl0_pic_idx: Some(250),
            },
        );
        let high = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(222),
            1_000,
            Vp8PayloadIdentity {
                picture_id: Some(12),
                tl0_pic_idx: Some(4),
            },
        );
        let high_next = next_rewritten_rtp_identity(
            &mut rewrites,
            transport_media_id,
            Ssrc::from(222),
            4_000,
            Vp8PayloadIdentity {
                picture_id: Some(14),
                tl0_pic_idx: Some(5),
            },
        );

        assert_eq!(low.vp8_payload().picture_id, Some(32_760));
        assert_eq!(low.vp8_payload().tl0_pic_idx, Some(250));
        assert_eq!(high.vp8_payload().picture_id, Some(32_761));
        assert_eq!(high.vp8_payload().tl0_pic_idx, Some(251));
        assert_eq!(high_next.vp8_payload().picture_id, Some(32_763));
        assert_eq!(high_next.vp8_payload().tl0_pic_idx, Some(252));
    }
}
