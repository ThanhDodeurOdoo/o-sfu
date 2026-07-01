//! destination RTP identity projection for local egress
//!
//! each consumer route maps to one receiver-visible RTP line
//! publisher identity can change when source selection or simulcast switches
//!
//! ```text
//! publisher SSRC/timestamp/VP8 -> [`ConsumerStreamStore::project_identity`]
//!                              -> receiver seq/timestamp/VP8
//! ```
//!
//! state is scoped to the destination session so every consumer can rewrite the
//! same source packet without copying payload bytes

use o_sfu_rfc::rtp::vp8::LONG_PICTURE_ID_MODULUS;
use str0m::rtp::{SeqNo, Ssrc};

use super::slots::{ConsumerStreamHandle, ConsumerStreamSlot, SlotStore};

/// receiver identity state for one consumer route
///
/// all publisher SSRCs feeding the route are projected into one browser-facing
/// sequence, timestamp and VP8 counter space
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ConsumerStream {
    next_seq_no: SeqNo,
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
    /// returns `None` when the handle is stale or released
    /// same-source packets preserve RTP and VP8 deltas
    /// source switches continue from the last projected receiver identity
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
    /// projects one packet after handle validation
    ///
    /// sequence numbers are always receiver-local
    /// timestamp and VP8 counters follow source deltas until the publisher SSRC
    /// changes
    fn project(
        &mut self,
        source_ssrc: Ssrc,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> ProjectedIdentity {
        let seq_no = self.next_seq_no.inc();
        let (rtp_timestamp, vp8_payload, transition) = match &mut self.rtp {
            RtpTimeline::Active {
                ssrc,
                src_anchor,
                dst_anchor,
                last,
            } if *ssrc == source_ssrc => {
                let rtp_timestamp =
                    dst_anchor.wrapping_add(source_timestamp.wrapping_sub(*src_anchor));
                *last = rtp_timestamp;
                (
                    rtp_timestamp,
                    self.vp8.project::<NORMAL_VP8_PROJECTION>(vp8_payload),
                    SourceTransition::Unchanged,
                )
            }
            RtpTimeline::Active {
                ssrc: previous_ssrc,
                last,
                ..
            } => {
                let previous_ssrc = *previous_ssrc;
                let rtp_timestamp = last.wrapping_add(1);
                self.rtp = RtpTimeline::Active {
                    ssrc: source_ssrc,
                    src_anchor: source_timestamp,
                    dst_anchor: rtp_timestamp,
                    last: rtp_timestamp,
                };
                (
                    rtp_timestamp,
                    self.vp8.project::<REANCHOR_VP8_PROJECTION>(vp8_payload),
                    SourceTransition::Switched { previous_ssrc },
                )
            }
            RtpTimeline::Empty => {
                self.rtp = RtpTimeline::Active {
                    ssrc: source_ssrc,
                    src_anchor: source_timestamp,
                    dst_anchor: source_timestamp,
                    last: source_timestamp,
                };
                (
                    source_timestamp,
                    self.vp8.project::<NORMAL_VP8_PROJECTION>(vp8_payload),
                    SourceTransition::Unchanged,
                )
            }
        };
        ProjectedIdentity {
            seq_no,
            rtp_timestamp,
            vp8_payload,
            transition,
        }
    }
}

/// rtp timestamp mapping for the active publisher source
///
/// same-source packets use source deltas
/// source switches start at `last + 1`
#[derive(Debug, Clone, Copy, Default)]
enum RtpTimeline {
    #[default]
    Empty,
    Active {
        ssrc: Ssrc,
        src_anchor: u32,
        dst_anchor: u32,
        last: u32,
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
