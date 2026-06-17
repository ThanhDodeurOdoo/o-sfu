//! when switching between different quality levels (simulcast), the source packets
//! change their sequence numbers and timestamps. this module hides those
//! jumps from the browser by mapping them into one continuous, monotonic stream.
//! for vp8, it also handles the picture identifiers to prevent playback glitches.

use str0m::rtp::{SeqNo, Ssrc};

use super::slots::{ConsumerStreamHandle, ConsumerStreamSlot, SlotStore};

const VP8_LONG_PICTURE_ID_MODULUS: u16 = 1 << 15;

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

/// slot-backed ownership table for downstream RTP rewrite state
///
/// route destinations store `ConsumerStreamHandle` instead of replaying a
/// consumer-stream key lookup for every forwarded packet
/// releasing the handle on route teardown invalidates stale destinations before
/// they can rewrite packets for a reused stream
#[derive(Default)]
pub(super) struct ConsumerStreamStore {
    streams: SlotStore<ConsumerStream, ConsumerStreamSlot>,
}

impl ConsumerStreamStore {
    /// allocate rewrite state for one route destination
    pub(super) fn allocate(&mut self) -> ConsumerStreamHandle {
        self.streams.insert(ConsumerStream::default())
    }

    /// release rewrite state when the route destination is removed
    ///
    /// `None` means the destination already held a stale or released handle
    pub(super) fn release(&mut self, handle: ConsumerStreamHandle) -> Option<ConsumerStream> {
        self.streams.remove(handle)
    }

    /// return rewrite state only while the destination handle is still live
    fn get_mut(&mut self, handle: ConsumerStreamHandle) -> Option<&mut ConsumerStream> {
        self.streams.get_mut(handle)
    }
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
    pub(super) seq_no: SeqNo,
    /// the continuous timestamp for the packet
    pub(super) rtp_timestamp: u32,
    /// smooth vp8 identifiers if the packet is vp8
    pub(super) vp8_payload: Vp8PayloadIdentity,
    /// the ssrc that was active before this packet
    pub(super) previous_source_ssrc: Option<Ssrc>,
    /// true if this packet is the first one from a new source
    pub(super) source_switched: bool,
}

/// vp8-specific identifiers that need smoothing
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Vp8PayloadIdentity {
    /// frame identifier used for loss detection and reordering
    pub(super) picture_id: Option<u16>,
    /// temporal layer index used for layer decoding
    pub(super) tl0_pic_idx: Option<u8>,
}

/// calculates the next sequential identity for a packet
///
/// fresh consumers must start from a clean counter because the browser
/// is not aware of the publisher's history. it also ensures that switching
/// between quality levels is invisible to the browser.
pub(super) fn next_projected_rtp_identity(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    source_ssrc: Ssrc,
    source_timestamp: u32,
    vp8_payload: Vp8PayloadIdentity,
) -> Option<ProjectedIdentity> {
    streams
        .get_mut(stream_handle)
        .map(|stream| stream.project(source_ssrc, source_timestamp, vp8_payload))
}

pub(super) fn forget_transport_media_stream(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
) {
    let _ = streams.release(stream_handle);
}

#[cfg(test)]
#[path = "TESTS/local_send_rewrite.rs"]
mod tests;
