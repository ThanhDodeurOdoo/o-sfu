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

use o_sfu_rfc::rtp::vp8::LONG_PICTURE_ID_MODULUS;
use str0m::rtp::{SeqNo, Ssrc};

use super::slots::{ConsumerStreamHandle, ConsumerStreamSlot, SlotStore};

/// receiver identity state for one consumer route
///
/// all publisher SSRCs feeding the route are projected into one browser-facing
/// sequence, timestamp and VP8 counter space
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ConsumerStream {
    delivery_generation: u64,
    rtp: RtpProjection,
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

    /// Projects one source packet into receiver RTP and VP8 identity.
    ///
    /// ```text
    /// source loss or reordering -> preserve the source delta
    /// new delivery generation   -> compact the SFU-filtered gap
    /// source SSRC switch        -> continue receiver identity
    /// ```
    ///
    /// Returns `None` for a stale handle, an older delivery generation or a
    /// source sequence outside the representable projection range.
    pub(super) fn project_identity(
        &mut self,
        stream_handle: ConsumerStreamHandle,
        delivery_generation: u64,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> Option<ProjectedIdentity> {
        self.streams.get_mut(stream_handle)?.project(
            delivery_generation,
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
    /// sequence numbers are always receiver-local
    /// timestamp and VP8 counters follow source deltas until the publisher SSRC
    /// changes
    fn project(
        &mut self,
        delivery_generation: u64,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> Option<ProjectedIdentity> {
        if delivery_generation == self.delivery_generation {
            let RtpProjection {
                next_seq_no,
                timeline,
            } = &mut self.rtp;
            if let RtpTimeline::Active {
                ssrc,
                highest_src_seq,
                src_timestamp_anchor,
                dst_timestamp_anchor,
                highest_timestamp,
                ..
            } = timeline
            {
                if *ssrc == source_ssrc {
                    if highest_src_seq.is_next(source_seq_no) {
                        let seq_no = next_seq_no.inc();
                        *highest_src_seq = source_seq_no;
                        let rtp_timestamp = dst_timestamp_anchor
                            .wrapping_add(source_timestamp.wrapping_sub(*src_timestamp_anchor));
                        *highest_timestamp = rtp_timestamp;
                        return Some(ProjectedIdentity {
                            seq_no,
                            rtp_timestamp,
                            vp8_payload: self.vp8.project::<NORMAL_VP8_PROJECTION>(vp8_payload),
                            transition: SourceTransition::Unchanged,
                        });
                    }
                } else {
                    let previous_ssrc = *ssrc;
                    let seq_no = next_seq_no.inc();
                    let rtp_timestamp = highest_timestamp.wrapping_add(1);
                    *timeline = RtpTimeline::Active {
                        ssrc: source_ssrc,
                        src_seq_anchor: source_seq_no,
                        dst_seq_anchor: seq_no,
                        highest_src_seq: source_seq_no,
                        src_timestamp_anchor: source_timestamp,
                        dst_timestamp_anchor: rtp_timestamp,
                        highest_timestamp: rtp_timestamp,
                    };
                    return Some(ProjectedIdentity {
                        seq_no,
                        rtp_timestamp,
                        vp8_payload: self.vp8.project::<REANCHOR_VP8_PROJECTION>(vp8_payload),
                        transition: SourceTransition::Switched { previous_ssrc },
                    });
                }
            }
        }
        self.project_discontinuity(
            delivery_generation,
            source_ssrc,
            source_seq_no,
            source_timestamp,
            vp8_payload,
        )
    }

    #[cold]
    #[inline(never)]
    fn project_discontinuity(
        &mut self,
        delivery_generation: u64,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
        source_timestamp: u32,
        vp8_payload: Vp8PayloadIdentity,
    ) -> Option<ProjectedIdentity> {
        let generation_delta = delivery_generation.wrapping_sub(self.delivery_generation);
        if generation_delta > u64::MAX / 2 {
            return None;
        }
        let reanchor = generation_delta != 0;
        let mut rtp = self.rtp;
        let projected = rtp.project(source_ssrc, source_seq_no, source_timestamp, reanchor)?;
        let reanchor_vp8 =
            reanchor || matches!(projected.transition, SourceTransition::Switched { .. });
        let vp8_payload = if projected.advances_high_water {
            self.vp8.project_with_reanchor(vp8_payload, reanchor_vp8)
        } else {
            let mut projection = self.vp8;
            projection.project_with_reanchor(vp8_payload, reanchor_vp8)
        };
        self.delivery_generation = delivery_generation;
        self.rtp = rtp;
        Some(ProjectedIdentity {
            seq_no: projected.seq_no,
            rtp_timestamp: projected.rtp_timestamp,
            vp8_payload,
            transition: projected.transition,
        })
    }
}

/// receiver RTP mapping for the active publisher source
///
/// source gaps remain gaps so receiver loss reflects ingress loss
/// resetting the source anchor compacts packets deliberately filtered by the SFU
#[derive(Debug, Clone, Copy, Default)]
struct RtpProjection {
    next_seq_no: SeqNo,
    timeline: RtpTimeline,
}

impl RtpProjection {
    #[cfg(test)]
    const fn new(next_seq_no: SeqNo) -> Self {
        Self {
            next_seq_no,
            timeline: RtpTimeline::Empty,
        }
    }

    #[cfg(test)]
    fn next_source_seq(&self, source_ssrc: Ssrc) -> Option<SeqNo> {
        match self.timeline {
            RtpTimeline::Active {
                ssrc,
                highest_src_seq,
                ..
            } if ssrc == source_ssrc => Some((*highest_src_seq + 1).into()),
            RtpTimeline::Active { .. } => Some(SeqNo::default()),
            RtpTimeline::Empty => None,
        }
    }

    fn project(
        &mut self,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
        source_timestamp: u32,
        reanchor: bool,
    ) -> Option<ProjectedRtp> {
        match &mut self.timeline {
            RtpTimeline::Active {
                ssrc,
                src_seq_anchor,
                dst_seq_anchor,
                highest_src_seq,
                src_timestamp_anchor,
                dst_timestamp_anchor,
                highest_timestamp,
            } if *ssrc == source_ssrc && !reanchor => {
                let source_delta = source_seq_no.checked_sub(**src_seq_anchor)?;
                let seq_no: SeqNo = (**dst_seq_anchor).checked_add(source_delta)?.into();
                let rtp_timestamp = dst_timestamp_anchor
                    .wrapping_add(source_timestamp.wrapping_sub(*src_timestamp_anchor));
                let advances_high_water = source_seq_no > *highest_src_seq;
                if advances_high_water {
                    *highest_src_seq = source_seq_no;
                    *highest_timestamp = rtp_timestamp;
                    self.next_seq_no = (*seq_no).checked_add(1)?.into();
                }
                Some(ProjectedRtp {
                    seq_no,
                    rtp_timestamp,
                    advances_high_water,
                    transition: SourceTransition::Unchanged,
                })
            }
            RtpTimeline::Active {
                ssrc: previous_ssrc,
                highest_timestamp,
                ..
            } => {
                let previous_ssrc = *previous_ssrc;
                let seq_no = self.next_seq_no.inc();
                let rtp_timestamp = highest_timestamp.wrapping_add(1);
                self.timeline = RtpTimeline::Active {
                    ssrc: source_ssrc,
                    src_seq_anchor: source_seq_no,
                    dst_seq_anchor: seq_no,
                    highest_src_seq: source_seq_no,
                    src_timestamp_anchor: source_timestamp,
                    dst_timestamp_anchor: rtp_timestamp,
                    highest_timestamp: rtp_timestamp,
                };
                let transition = if previous_ssrc == source_ssrc {
                    SourceTransition::Unchanged
                } else {
                    SourceTransition::Switched { previous_ssrc }
                };
                Some(ProjectedRtp {
                    seq_no,
                    rtp_timestamp,
                    advances_high_water: true,
                    transition,
                })
            }
            RtpTimeline::Empty => {
                let seq_no = self.next_seq_no.inc();
                self.timeline = RtpTimeline::Active {
                    ssrc: source_ssrc,
                    src_seq_anchor: source_seq_no,
                    dst_seq_anchor: seq_no,
                    highest_src_seq: source_seq_no,
                    src_timestamp_anchor: source_timestamp,
                    dst_timestamp_anchor: source_timestamp,
                    highest_timestamp: source_timestamp,
                };
                Some(ProjectedRtp {
                    seq_no,
                    rtp_timestamp: source_timestamp,
                    advances_high_water: true,
                    transition: SourceTransition::Unchanged,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProjectedRtp {
    seq_no: SeqNo,
    rtp_timestamp: u32,
    advances_high_water: bool,
    transition: SourceTransition,
}

/// source and receiver anchors for one projected RTP line
#[derive(Debug, Clone, Copy, Default)]
enum RtpTimeline {
    #[default]
    Empty,
    Active {
        ssrc: Ssrc,
        src_seq_anchor: SeqNo,
        dst_seq_anchor: SeqNo,
        highest_src_seq: SeqNo,
        src_timestamp_anchor: u32,
        dst_timestamp_anchor: u32,
        highest_timestamp: u32,
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
