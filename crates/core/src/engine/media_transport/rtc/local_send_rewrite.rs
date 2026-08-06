//! destination RTP identity projection for local egress
//!
//! each consumer route maps to one receiver-visible RTP line
//! publisher identity can change when source selection or simulcast switches
//!
//! ```text
//! publisher SSRC/seq/timestamp/VP8 -> [`ConsumerStreamStore::project_identity`]
//!                                  -> receiver seq/timestamp/VP8
//! ```
//!
//! state is scoped to the destination session so every consumer can rewrite the
//! same source packet without copying payload bytes

use std::cmp::Ordering;

use o_sfu_rfc::rtp::vp8::LONG_PICTURE_ID_MODULUS;
use str0m::rtp::{SeqNo, Ssrc};

use super::{
    slots::{ConsumerStreamHandle, ConsumerStreamSlot, SlotStore},
    source_route::SourceFilterGeneration,
};

/// receiver identity state for one consumer route
///
/// all publisher SSRCs feeding the route are projected into one browser-facing
/// sequence, timestamp and VP8 counter space
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ConsumerStream {
    delivery_epoch: u64,
    source_filter_generation: SourceFilterGeneration,
    sequence: SequenceProjection,
    rtp: RtpTimeline,
    vp8: Vp8Projection,
}

/// generation-checked table of live consumer rewrite streams
///
/// route setup allocates a [`ConsumerStreamHandle`]
/// route removal releases it
/// stale handles make [`Self::project_identity`] return `None`
#[derive(Default)]
pub(super) struct ConsumerStreamStore {
    streams: SlotStore<ConsumerStream, ConsumerStreamSlot>,
}

impl ConsumerStreamStore {
    /// creates the stream handle stored on one local route destination
    pub(super) fn allocate(&mut self) -> ConsumerStreamHandle {
        self.streams.insert(ConsumerStream::default())
    }

    /// releases a route destination handle
    pub(super) fn release(&mut self, handle: ConsumerStreamHandle) {
        let _ = self.streams.remove(handle);
    }

    /// rewrites one source packet through `stream_handle`
    ///
    /// returns `None` when the handle is stale or the packet conflicts with the
    /// active delivery epoch
    /// same-source packets preserve RTP and VP8 deltas
    /// a new delivery epoch compacts packets intentionally filtered by routing
    /// a new source filter generation compacts only the receiver sequence gap
    /// source switches continue from the last projected receiver identity
    pub(super) fn project_identity(
        &mut self,
        stream_handle: ConsumerStreamHandle,
        delivery: DeliveryGenerations,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> Option<ProjectedIdentity> {
        self.streams.get_mut(stream_handle)?.project(
            delivery,
            source_ssrc,
            source_seq_no,
            source_timestamp,
            vp8_payload,
        )
    }
}

impl ConsumerStream {
    /// projects one packet after handle validation
    ///
    /// sequence numbers are receiver-local and reject packets before the epoch anchor
    /// a different source SSRC requires a new delivery epoch
    /// epochs use half-range serial arithmetic so stale planned packets cannot re-anchor
    /// stale source filter generations are rejected before projection state changes
    /// timestamp and VP8 counters follow source deltas until the publisher SSRC
    /// changes
    fn project(
        &mut self,
        delivery: DeliveryGenerations,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> Option<ProjectedIdentity> {
        let epoch_delta = delivery.epoch.wrapping_sub(self.delivery_epoch);
        let reanchor = match epoch_delta {
            0 => false,
            delta if delta <= u64::MAX / 2 => true,
            _ => return None,
        };
        let filter_reanchor = match delivery.source_filter.cmp(&self.source_filter_generation) {
            Ordering::Less => return None,
            Ordering::Equal => false,
            Ordering::Greater => true,
        };
        let mut sequence_projection = self.sequence;
        if reanchor || filter_reanchor {
            sequence_projection.reset_source_anchor();
        }
        let sequence = sequence_projection.project(source_ssrc, source_seq_no)?;
        let (rtp_timestamp, transition) = match &mut self.rtp {
            RtpTimeline::Active {
                ssrc,
                src_anchor,
                dst_anchor,
                highest,
            } if *ssrc == source_ssrc && !reanchor => {
                let rtp_timestamp =
                    dst_anchor.wrapping_add(source_timestamp.wrapping_sub(*src_anchor));
                if sequence.advances_high_water {
                    *highest = rtp_timestamp;
                }
                (rtp_timestamp, SourceTransition::Unchanged)
            }
            RtpTimeline::Active {
                ssrc: previous_ssrc,
                highest,
                ..
            } => {
                let previous_ssrc = *previous_ssrc;
                let rtp_timestamp = highest.wrapping_add(1);
                self.rtp = RtpTimeline::Active {
                    ssrc: source_ssrc,
                    src_anchor: source_timestamp,
                    dst_anchor: rtp_timestamp,
                    highest: rtp_timestamp,
                };
                let transition = if previous_ssrc == source_ssrc {
                    SourceTransition::Unchanged
                } else {
                    SourceTransition::Switched { previous_ssrc }
                };
                (rtp_timestamp, transition)
            }
            RtpTimeline::Empty => {
                self.rtp = RtpTimeline::Active {
                    ssrc: source_ssrc,
                    src_anchor: source_timestamp,
                    dst_anchor: source_timestamp,
                    highest: source_timestamp,
                };
                (source_timestamp, SourceTransition::Unchanged)
            }
        };
        let reanchor_vp8 = reanchor || matches!(transition, SourceTransition::Switched { .. });
        let vp8_payload = if sequence.advances_high_water {
            self.vp8.project_with_reanchor(vp8_payload, reanchor_vp8)
        } else {
            let mut projection = self.vp8;
            projection.project_with_reanchor(vp8_payload, reanchor_vp8)
        };
        self.delivery_epoch = delivery.epoch;
        self.source_filter_generation = delivery.source_filter;
        self.sequence = sequence_projection;
        Some(ProjectedIdentity {
            seq_no: sequence.seq_no,
            rtp_timestamp,
            vp8_payload,
            transition,
        })
    }
}

/// route generations that distinguish intentional delivery gaps
#[derive(Debug, Clone, Copy)]
pub(super) struct DeliveryGenerations {
    epoch: u64,
    source_filter: SourceFilterGeneration,
}

impl DeliveryGenerations {
    pub(super) const fn new(epoch: u64, source_filter: SourceFilterGeneration) -> Self {
        Self {
            epoch,
            source_filter,
        }
    }
}

/// sequence mapping for the active publisher source
///
/// same-source packets keep source sequence deltas so receiver NACKs describe
/// the packet loss observed at ingress
/// packets before the delivery epoch anchor cannot reuse an emitted identity
/// one epoch accepts one publisher SSRC
#[derive(Debug, Clone, Copy, Default)]
struct SequenceProjection {
    next_seq_no: SeqNo,
    timeline: SequenceTimeline,
}

impl SequenceProjection {
    fn reset_source_anchor(&mut self) {
        self.timeline = SequenceTimeline::Empty;
    }

    fn project(&mut self, source_ssrc: Ssrc, source_seq_no: SeqNo) -> Option<ProjectedSequence> {
        match &mut self.timeline {
            SequenceTimeline::Active {
                ssrc,
                src_anchor,
                dst_anchor,
                highest_src,
            } if *ssrc == source_ssrc => {
                if source_seq_no < *src_anchor {
                    return None;
                }
                let seq_no: SeqNo = (**dst_anchor + (*source_seq_no - **src_anchor)).into();
                let advances_high_water = source_seq_no > *highest_src;
                if advances_high_water {
                    *highest_src = source_seq_no;
                    self.next_seq_no = (*seq_no + 1).into();
                }
                Some(ProjectedSequence {
                    seq_no,
                    advances_high_water,
                })
            }
            SequenceTimeline::Active { .. } => None,
            SequenceTimeline::Empty => {
                let seq_no = self.next_seq_no.inc();
                self.timeline = SequenceTimeline::Active {
                    ssrc: source_ssrc,
                    src_anchor: source_seq_no,
                    dst_anchor: seq_no,
                    highest_src: source_seq_no,
                };
                Some(ProjectedSequence {
                    seq_no,
                    advances_high_water: true,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProjectedSequence {
    seq_no: SeqNo,
    advances_high_water: bool,
}

#[derive(Debug, Clone, Copy, Default)]
enum SequenceTimeline {
    #[default]
    Empty,
    Active {
        ssrc: Ssrc,
        src_anchor: SeqNo,
        dst_anchor: SeqNo,
        highest_src: SeqNo,
    },
}

/// rtp timestamp mapping for the active publisher source
///
/// same-source packets use source deltas without moving the high-water mark backward
/// source switches and delivery epochs start at `highest + 1`
#[derive(Debug, Clone, Copy, Default)]
enum RtpTimeline {
    #[default]
    Empty,
    Active {
        ssrc: Ssrc,
        src_anchor: u32,
        dst_anchor: u32,
        highest: u32,
    },
}

/// vp8 descriptor counter mapping for one receiver stream
///
/// absent descriptor fields do not advance their counters
/// source switches re-anchor only fields present on the wire
#[derive(Debug, Clone, Copy, Default)]
struct Vp8Projection {
    picture_id: CounterProjection<PictureId>,
    tl0_pic_idx: CounterProjection<Tl0PicIdx>,
}

const NORMAL_VP8_PROJECTION: bool = false;
const REANCHOR_VP8_PROJECTION: bool = true;

impl Vp8Projection {
    fn project_with_reanchor(
        &mut self,
        vp8_payload: Vp8PayloadIdentity,
        reanchor: bool,
    ) -> Vp8PayloadIdentity {
        if reanchor {
            self.project::<REANCHOR_VP8_PROJECTION>(vp8_payload)
        } else {
            self.project::<NORMAL_VP8_PROJECTION>(vp8_payload)
        }
    }

    fn project<const REANCHOR: bool>(
        &mut self,
        vp8_payload: Vp8PayloadIdentity,
    ) -> Vp8PayloadIdentity {
        let picture_id = if let Some(picture_id) = vp8_payload.picture_id {
            Some(self.picture_id.project::<REANCHOR>(PictureId(picture_id)).0)
        } else {
            if REANCHOR {
                self.picture_id.reset_anchor();
            }
            None
        };
        let tl0_pic_idx = if let Some(tl0_pic_idx) = vp8_payload.tl0_pic_idx {
            Some(
                self.tl0_pic_idx
                    .project::<REANCHOR>(Tl0PicIdx(tl0_pic_idx))
                    .0,
            )
        } else {
            if REANCHOR {
                self.tl0_pic_idx.reset_anchor();
            }
            None
        };
        Vp8PayloadIdentity {
            picture_id,
            tl0_pic_idx,
        }
    }
}

/// vp8 `PictureID` in long-picture-id space
#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
struct PictureId(u16);

/// vp8 `TL0PICIDX`
#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
struct Tl0PicIdx(u8);

/// projection state for one optional VP8 descriptor counter
///
/// missing wire fields suspend anchoring without inventing a projected value
#[derive(Debug, Clone, Copy, Default)]
enum CounterProjection<T> {
    #[default]
    /// no descriptor value has been seen for this stream
    Empty,
    /// last projected value kept while the descriptor field is absent
    LastOnly(T),
    /// source delta to receiver delta mapping for the current publisher source
    Anchored {
        src_anchor: T,
        dst_anchor: T,
        last: T,
    },
}

impl<T: Copy> CounterProjection<T> {
    /// suspends source-delta tracking after a switch with a missing wire field
    fn reset_anchor(&mut self) {
        if let Self::Anchored { last, .. } = *self {
            *self = Self::LastOnly(last);
        }
    }
}

impl CounterProjection<PictureId> {
    /// projects `src` into the receiver `PictureID` space
    ///
    /// `REANCHOR` starts after `last` for a publisher switch
    fn project<const REANCHOR: bool>(&mut self, src: PictureId) -> PictureId {
        let dst = match *self {
            Self::Anchored { last, .. } if REANCHOR => {
                PictureId(last.0.wrapping_add(1) % LONG_PICTURE_ID_MODULUS)
            }
            Self::Anchored {
                src_anchor,
                dst_anchor,
                ..
            } => {
                let delta = src.0.wrapping_sub(src_anchor.0) % LONG_PICTURE_ID_MODULUS;
                PictureId(dst_anchor.0.wrapping_add(delta) % LONG_PICTURE_ID_MODULUS)
            }
            Self::LastOnly(last) => PictureId(last.0.wrapping_add(1) % LONG_PICTURE_ID_MODULUS),
            Self::Empty => src,
        };
        *self = Self::Anchored {
            src_anchor: src,
            dst_anchor: dst,
            last: dst,
        };
        dst
    }
}

impl CounterProjection<Tl0PicIdx> {
    /// projects `src` into the receiver `TL0PICIDX` space
    ///
    /// `REANCHOR` starts after `last` for a publisher switch
    fn project<const REANCHOR: bool>(&mut self, src: Tl0PicIdx) -> Tl0PicIdx {
        let dst = match *self {
            Self::Anchored { last, .. } if REANCHOR => Tl0PicIdx(last.0.wrapping_add(1)),
            Self::Anchored {
                src_anchor,
                dst_anchor,
                ..
            } => Tl0PicIdx(dst_anchor.0.wrapping_add(src.0.wrapping_sub(src_anchor.0))),
            Self::LastOnly(last) => Tl0PicIdx(last.0.wrapping_add(1)),
            Self::Empty => src,
        };
        *self = Self::Anchored {
            src_anchor: src,
            dst_anchor: dst,
            last: dst,
        };
        dst
    }
}

/// receiver-facing RTP identity for one successful local write
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProjectedIdentity {
    /// receiver-local RTP sequence number
    pub(super) seq_no: SeqNo,
    /// receiver-local RTP timestamp after SSRC switch smoothing
    pub(super) rtp_timestamp: u32,
    /// vp8 descriptor counters to patch before serialization
    pub(super) vp8_payload: Vp8PayloadIdentity,
    /// source switch observed by local forwarding
    pub(super) transition: SourceTransition,
}

/// source identity transition observed during projection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourceTransition {
    /// packet continued the current projected source
    Unchanged,
    /// packet switched from the previous publisher SSRC
    Switched {
        /// publisher SSRC used by the previous projected packet
        previous_ssrc: Ssrc,
    },
}

/// optional VP8 descriptor counters carried by one source packet
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Vp8PayloadIdentity {
    /// `PictureID` when present on the wire
    pub(super) picture_id: Option<u16>,
    /// `TL0PICIDX` when present on the wire
    pub(super) tl0_pic_idx: Option<u8>,
}

#[cfg(test)]
#[path = "TESTS/local_send_rewrite.rs"]
mod tests;
