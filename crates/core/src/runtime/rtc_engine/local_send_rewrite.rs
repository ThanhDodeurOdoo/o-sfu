//! when switching between different quality levels (simulcast), the source packets
//! change their sequence numbers and timestamps. this module hides those
//! jumps from the browser by mapping them into one continuous, monotonic stream.
//! for vp8, it also handles the picture identifiers to prevent playback glitches.

use std::collections::HashMap;

use str0m::rtp::{SeqNo, Ssrc};

use crate::runtime::media_transport::TransportMediaId;

const VP8_LONG_PICTURE_ID_MODULUS: u16 = 1 << 15;

/// key for one consumer stream
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ConsumerStreamKey {
    transport_media_id: TransportMediaId,
}

/// state for one downstream video stream
///
/// it tracks the counters and offsets needed to keep the stream continuous for
/// the browser even when the publisher source changes.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ConsumerStream {
    /// the next sequence number to assign
    next_seq_no: SeqNo,
    /// the current publisher source
    source_ssrc: Option<Ssrc>,
    /// the publisher timestamp used as the start of the current projection
    source_timestamp_anchor: u32,
    /// the consumer timestamp used as the start of the current projection
    projected_timestamp_anchor: u32,
    /// the last timestamp we sent to the browser
    last_projected_timestamp: Option<u32>,
    source_picture_id_anchor: Option<u16>,
    projected_picture_id_anchor: Option<u16>,
    last_projected_picture_id: Option<u16>,
    source_tl0_pic_idx_anchor: Option<u8>,
    projected_tl0_pic_idx_anchor: Option<u8>,
    last_projected_tl0_pic_idx: Option<u8>,
}

impl ConsumerStream {
    fn take_seq_no(&mut self) -> SeqNo {
        self.next_seq_no.inc()
    }

    /// assigns a sequential id to a packet so it follows the previous one
    ///
    /// if we switch to a different quality level (ssrc), we calculate a new
    /// offset so the browser doesn't see a jump in timestamps or sequence numbers.
    fn project(
        &mut self,
        source_ssrc: Ssrc,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> ProjectedIdentity {
        let seq_no = self.take_seq_no();
        let previous_source_ssrc = self.source_ssrc;
        if previous_source_ssrc != Some(source_ssrc) {
            // when the source ssrc changes, we pick up the timestamp right after the
            // last one we sent to keep the timeline continuous
            let rtp_timestamp = self
                .last_projected_timestamp
                .map_or(source_timestamp, |timestamp| timestamp.wrapping_add(1));
            self.source_ssrc = Some(source_ssrc);
            self.source_timestamp_anchor = source_timestamp;
            self.projected_timestamp_anchor = rtp_timestamp;
            self.last_projected_timestamp = Some(rtp_timestamp);
            // we must also re-anchor vp8 identifiers on the next packet
            self.reset_vp8_source_anchors();
            let projected_vp8_payload = self.project_vp8_payload(vp8_payload);
            return ProjectedIdentity {
                seq_no,
                rtp_timestamp,
                vp8_payload: projected_vp8_payload,
                previous_source_ssrc,
                source_switched: previous_source_ssrc.is_some(),
            };
        }
        // if the source is the same, we just apply the offset we calculated when
        // we first anchored this ssrc
        let rtp_timestamp = self
            .projected_timestamp_anchor
            .wrapping_add(source_timestamp.wrapping_sub(self.source_timestamp_anchor));
        self.last_projected_timestamp = Some(rtp_timestamp);
        let projected_vp8_payload = self.project_vp8_payload(vp8_payload);
        ProjectedIdentity {
            seq_no,
            rtp_timestamp,
            vp8_payload: projected_vp8_payload,
            previous_source_ssrc,
            source_switched: false,
        }
    }

    /// clears the vp8 offsets so they are recalculated on the next packet
    fn reset_vp8_source_anchors(&mut self) {
        self.source_picture_id_anchor = None;
        self.projected_picture_id_anchor = None;
        self.source_tl0_pic_idx_anchor = None;
        self.projected_tl0_pic_idx_anchor = None;
    }

    /// maps vp8 identifiers to be continuous with the previous ones
    fn project_vp8_payload(&mut self, vp8_payload: Vp8PayloadIdentity) -> Vp8PayloadIdentity {
        Vp8PayloadIdentity {
            picture_id: vp8_payload
                .picture_id
                .map(|picture_id| self.project_picture_id(picture_id)),
            tl0_pic_idx: vp8_payload
                .tl0_pic_idx
                .map(|tl0_pic_idx| self.project_tl0_pic_idx(tl0_pic_idx)),
        }
    }

    /// maps a vp8 picture id to a continuous value
    fn project_picture_id(&mut self, source_picture_id: u16) -> u16 {
        let projected_picture_id = match (
            self.source_picture_id_anchor,
            self.projected_picture_id_anchor,
        ) {
            (Some(source_anchor), Some(projected_anchor)) => {
                // vp8 picture ids use a 15-bit space as per rfc 7741
                let source_delta =
                    source_picture_id.wrapping_sub(source_anchor) % VP8_LONG_PICTURE_ID_MODULUS;
                projected_anchor.wrapping_add(source_delta) % VP8_LONG_PICTURE_ID_MODULUS
            }
            _ => self
                .last_projected_picture_id
                .map_or(source_picture_id, |last| {
                    last.wrapping_add(1) % VP8_LONG_PICTURE_ID_MODULUS
                }),
        };
        self.source_picture_id_anchor = Some(source_picture_id);
        self.projected_picture_id_anchor = Some(projected_picture_id);
        self.last_projected_picture_id = Some(projected_picture_id);
        projected_picture_id
    }

    /// maps a vp8 temporal index to a continuous value
    fn project_tl0_pic_idx(&mut self, source_tl0_pic_idx: u8) -> u8 {
        let projected_tl0_pic_idx = match (
            self.source_tl0_pic_idx_anchor,
            self.projected_tl0_pic_idx_anchor,
        ) {
            (Some(source_anchor), Some(projected_anchor)) => {
                projected_anchor.wrapping_add(source_tl0_pic_idx.wrapping_sub(source_anchor))
            }
            _ => self
                .last_projected_tl0_pic_idx
                .map_or(source_tl0_pic_idx, |last| last.wrapping_add(1)),
        };
        self.source_tl0_pic_idx_anchor = Some(source_tl0_pic_idx);
        self.projected_tl0_pic_idx_anchor = Some(projected_tl0_pic_idx);
        self.last_projected_tl0_pic_idx = Some(projected_tl0_pic_idx);
        projected_tl0_pic_idx
    }
}

/// the projected identity for a single packet
///
/// contains the continuous sequence number, timestamp, and any codec-specific
/// identifiers that the browser expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProjectedIdentity {
    /// the continuous sequence number for the packet
    seq_no: SeqNo,
    /// the continuous timestamp for the packet
    rtp_timestamp: u32,
    /// smooth vp8 identifiers if the packet is vp8
    vp8_payload: Vp8PayloadIdentity,
    /// the ssrc that was active before this packet
    previous_source_ssrc: Option<Ssrc>,
    /// true if this packet is the first one from a new source
    source_switched: bool,
}

/// vp8-specific identifiers that need smoothing
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Vp8PayloadIdentity {
    /// frame identifier used for loss detection and reordering
    pub(super) picture_id: Option<u16>,
    /// temporal layer index used for layer decoding
    pub(super) tl0_pic_idx: Option<u8>,
}

impl ProjectedIdentity {
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

/// calculates the next sequential identity for a packet
///
/// fresh consumers must start from a clean counter because the browser
/// is not aware of the publisher's history. it also ensures that switching
/// between quality levels is invisible to the browser.
pub(super) fn next_projected_rtp_identity(
    streams: &mut HashMap<ConsumerStreamKey, ConsumerStream>,
    transport_media_id: TransportMediaId,
    source_ssrc: Ssrc,
    source_timestamp: u32,
    vp8_payload: Vp8PayloadIdentity,
) -> ProjectedIdentity {
    streams
        .entry(ConsumerStreamKey { transport_media_id })
        .or_default()
        .project(source_ssrc, source_timestamp, vp8_payload)
}

/// clears any stored stream state for a consumer
pub(super) fn forget_transport_media_streams(
    streams: &mut HashMap<ConsumerStreamKey, ConsumerStream>,
    transport_media_id: TransportMediaId,
) {
    streams.retain(|key, _| key.transport_media_id != transport_media_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projected_sequence_numbers_start_in_initial_roc_and_increment_per_stream() {
        let source_seq: SeqNo = 131_072.into();
        let transport_media_id = TransportMediaId::new(41);
        let mut streams = HashMap::new();

        let first = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        let second = next_projected_rtp_identity(
            &mut streams,
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
    fn projected_sequence_numbers_are_scoped_by_consumer_stream() {
        let transport_media_id = TransportMediaId::new(42);
        let mut streams = HashMap::new();

        let first = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        let second = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        let other = next_projected_rtp_identity(
            &mut streams,
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
    fn forgetting_transport_media_streams_drops_all_stream_state_for_that_consumer() {
        let transport_media_id = TransportMediaId::new(44);
        let mut streams = HashMap::new();

        let first = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );
        let _ = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        );

        forget_transport_media_streams(&mut streams, transport_media_id);

        let reset = next_projected_rtp_identity(
            &mut streams,
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
    fn projected_timestamps_preserve_source_deltas_on_one_ssrc() {
        let transport_media_id = TransportMediaId::new(45);
        let mut streams = HashMap::new();

        let first = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(111),
            10_000,
            Vp8PayloadIdentity::default(),
        );
        let second = next_projected_rtp_identity(
            &mut streams,
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
    fn projected_timestamps_stay_monotonic_across_simulcast_ssrc_switches() {
        let transport_media_id = TransportMediaId::new(46);
        let mut streams = HashMap::new();

        let low = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(111),
            90_000,
            Vp8PayloadIdentity::default(),
        );
        let high = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(222),
            1_000,
            Vp8PayloadIdentity::default(),
        );
        let high_next = next_projected_rtp_identity(
            &mut streams,
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
    fn projected_vp8_picture_ids_stay_continuous_across_simulcast_ssrc_switches() {
        let transport_media_id = TransportMediaId::new(47);
        let mut streams = HashMap::new();

        let low = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(111),
            90_000,
            Vp8PayloadIdentity {
                picture_id: Some(32_760),
                tl0_pic_idx: Some(250),
            },
        );
        let high = next_projected_rtp_identity(
            &mut streams,
            transport_media_id,
            Ssrc::from(222),
            1_000,
            Vp8PayloadIdentity {
                picture_id: Some(12),
                tl0_pic_idx: Some(4),
            },
        );
        let high_next = next_projected_rtp_identity(
            &mut streams,
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
