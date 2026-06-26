//! receiver-side RTP identity projection for local egress
//!
//! consumer media declaration allocates a [`ConsumerStreamHandle`] in the
//! destination session
//! routes carry the handle until teardown releases it
//!
//! ```text
//! publisher ssrc/timestamp/vp8 -> [`ConsumerStreamStore::project_identity`]
//!                            -> consumer sequence/timestamp/vp8
//! ```

use o_sfu_rfc::rtp::vp8::LONG_PICTURE_ID_MODULUS;
use str0m::rtp::{SeqNo, Ssrc};

use super::slots::{ConsumerStreamHandle, ConsumerStreamSlot, SlotStore};

/// per-route RTP and VP8 projection state for one destination
///
/// one stream maps a changing publisher source into one browser-facing RTP line
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ConsumerStream {
    next_seq_no: SeqNo,
    source_ssrc: Option<Ssrc>,
    source_timestamp_anchor: u32,
    projected_timestamp_anchor: u32,
    last_projected_timestamp: Option<u32>,
    source_picture_id_anchor: Option<u16>,
    projected_picture_id_anchor: Option<u16>,
    last_projected_picture_id: Option<u16>,
    source_tl0_pic_idx_anchor: Option<u8>,
    projected_tl0_pic_idx_anchor: Option<u8>,
    last_projected_tl0_pic_idx: Option<u8>,
}

/// destination-session table for live RTP projection streams
///
/// route destinations keep [`ConsumerStreamHandle`] values
/// every access validates the slot generation before packet data can mutate
/// receiver-local RTP state
#[derive(Default)]
pub(super) struct ConsumerStreamStore {
    streams: SlotStore<ConsumerStream, ConsumerStreamSlot>,
}

impl ConsumerStreamStore {
    /// allocates one receiver projection stream for a consumer route
    pub(super) fn allocate(&mut self) -> ConsumerStreamHandle {
        self.streams.insert(ConsumerStream::default())
    }

    /// invalidates a route destination's stream handle
    ///
    /// stale or already released handles are ignored
    pub(super) fn release(&mut self, handle: ConsumerStreamHandle) {
        let _ = self.streams.remove(handle);
    }

    /// projects one publisher packet through `stream_handle`
    ///
    /// returns `None` when the handle no longer names a live stream
    /// same-source packets preserve source timestamp and VP8 deltas
    /// source switches resume from the last projected destination identity
    pub(super) fn project_identity(
        &mut self,
        stream_handle: ConsumerStreamHandle,
        source_ssrc: Ssrc,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> Option<ProjectedIdentity> {
        self.streams
            .get_mut(stream_handle)
            .map(|stream| stream.project(source_ssrc, source_timestamp, vp8_payload))
    }
}

impl ConsumerStream {
    /// projects one packet through the receiver stream state
    ///
    /// publisher sequence numbers are not forwarded
    /// the receiver stream owns sequence number continuity independently from
    /// timestamp and vp8 projection
    fn project(
        &mut self,
        source_ssrc: Ssrc,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> ProjectedIdentity {
        let seq_no = self.next_seq_no.inc();
        let previous_source_ssrc = self.source_ssrc;
        if previous_source_ssrc != Some(source_ssrc) {
            let rtp_timestamp = self
                .last_projected_timestamp
                .map_or(source_timestamp, |timestamp| timestamp.wrapping_add(1));
            self.source_ssrc = Some(source_ssrc);
            self.source_timestamp_anchor = source_timestamp;
            self.projected_timestamp_anchor = rtp_timestamp;
            self.last_projected_timestamp = Some(rtp_timestamp);
            self.reset_vp8_source_anchors();
            return ProjectedIdentity {
                seq_no,
                rtp_timestamp,
                vp8_payload: self.project_vp8_payload(vp8_payload),
                previous_source_ssrc,
                source_switched: previous_source_ssrc.is_some(),
            };
        }

        let rtp_timestamp = self
            .projected_timestamp_anchor
            .wrapping_add(source_timestamp.wrapping_sub(self.source_timestamp_anchor));
        self.last_projected_timestamp = Some(rtp_timestamp);
        ProjectedIdentity {
            seq_no,
            rtp_timestamp,
            vp8_payload: self.project_vp8_payload(vp8_payload),
            previous_source_ssrc,
            source_switched: false,
        }
    }

    fn reset_vp8_source_anchors(&mut self) {
        self.source_picture_id_anchor = None;
        self.projected_picture_id_anchor = None;
        self.source_tl0_pic_idx_anchor = None;
        self.projected_tl0_pic_idx_anchor = None;
    }

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

    fn project_picture_id(&mut self, source_picture_id: u16) -> u16 {
        let projected_picture_id = match (
            self.source_picture_id_anchor,
            self.projected_picture_id_anchor,
        ) {
            (Some(source_anchor), Some(projected_anchor)) => {
                let source_delta =
                    source_picture_id.wrapping_sub(source_anchor) % LONG_PICTURE_ID_MODULUS;
                projected_anchor.wrapping_add(source_delta) % LONG_PICTURE_ID_MODULUS
            }
            _ => self
                .last_projected_picture_id
                .map_or(source_picture_id, |last| {
                    last.wrapping_add(1) % LONG_PICTURE_ID_MODULUS
                }),
        };
        self.source_picture_id_anchor = Some(source_picture_id);
        self.projected_picture_id_anchor = Some(projected_picture_id);
        self.last_projected_picture_id = Some(projected_picture_id);
        projected_picture_id
    }

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

/// receiver-side identity for one forwarded RTP packet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProjectedIdentity {
    /// rtp sequence number in the receiver stream
    pub(super) seq_no: SeqNo,
    /// rtp timestamp after source-switch smoothing
    pub(super) rtp_timestamp: u32,
    /// vp8 descriptor counters after source-switch smoothing
    pub(super) vp8_payload: Vp8PayloadIdentity,
    /// source SSRC active before this packet
    pub(super) previous_source_ssrc: Option<Ssrc>,
    /// true when this packet starts a new projection segment after a prior source
    pub(super) source_switched: bool,
}

/// vp8 descriptor counters that can be patched without copying payload bytes
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Vp8PayloadIdentity {
    /// picture id from `Vp8Descriptor` when present
    pub(super) picture_id: Option<u16>,
    /// tl0 picture index from `Vp8Descriptor` when present
    pub(super) tl0_pic_idx: Option<u8>,
}

#[cfg(test)]
#[path = "TESTS/local_send_rewrite.rs"]
mod tests;
