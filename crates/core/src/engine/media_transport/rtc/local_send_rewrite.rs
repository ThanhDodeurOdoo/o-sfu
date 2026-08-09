//! destination RTP identity projection for local egress
//!
//! each consumer route maps to one receiver-visible RTP line
//! publisher identity can change when source selection or simulcast switches
//!
//! ```text
//! publisher SSRC/seq/timestamp/codec -> [`ConsumerStreamStore::project_identity`]
//!                                    -> receiver seq/timestamp/codec
//! ```
//!
//! state is scoped to the destination session so every consumer can rewrite the
//! same source packet without copying payload bytes

use str0m::rtp::{SeqNo, Ssrc};

use super::{
    codec,
    slots::{ConsumerStreamHandle, ConsumerStreamSlot, SlotStore},
};

/// receiver identity state for one consumer route
///
/// all publisher SSRCs feeding the route are projected into one browser-facing
/// sequence, timestamp and encoded payload identity
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ConsumerStream {
    delivery_generation: u64,
    rtp: RtpProjection,
    codec: codec::Projection,
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

    /// Projects one source packet into receiver RTP and codec identity.
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
        codec_identity: codec::PacketIdentity,
    ) -> Option<ProjectedIdentity> {
        self.streams.get_mut(stream_handle)?.project(
            delivery_generation,
            source_ssrc,
            source_seq_no,
            source_timestamp,
            codec_identity,
        )
    }
}

impl ConsumerStream {
    /// projects one packet after handle validation
    ///
    /// sequence numbers are always receiver-local
    /// timestamp and encoded payload identity follow source deltas until the publisher SSRC
    /// changes
    fn project(
        &mut self,
        delivery_generation: u64,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
        source_timestamp: u32,
        codec_identity: codec::PacketIdentity,
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
                            codec: self.codec.project(codec_identity, false),
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
                        codec: self.codec.project(codec_identity, true),
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
            codec_identity,
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
        codec_identity: codec::PacketIdentity,
    ) -> Option<ProjectedIdentity> {
        let generation_delta = delivery_generation.wrapping_sub(self.delivery_generation);
        if generation_delta > u64::MAX / 2 {
            return None;
        }
        let reanchor = generation_delta != 0;
        let mut rtp = self.rtp;
        let projected = rtp.project(source_ssrc, source_seq_no, source_timestamp, reanchor)?;
        let reanchor_codec =
            reanchor || matches!(projected.transition, SourceTransition::Switched { .. });
        let mut observed_codec = self.codec;
        let codec_projection = if projected.advances_high_water {
            &mut self.codec
        } else {
            &mut observed_codec
        };
        let codec = codec_projection.project(codec_identity, reanchor_codec);
        self.delivery_generation = delivery_generation;
        self.rtp = rtp;
        Some(ProjectedIdentity {
            seq_no: projected.seq_no,
            rtp_timestamp: projected.rtp_timestamp,
            codec,
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

/// receiver-facing RTP identity for one successful local write
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProjectedIdentity {
    /// receiver-local RTP sequence number
    pub(super) seq_no: SeqNo,
    /// receiver-local RTP timestamp after SSRC switch smoothing
    pub(super) rtp_timestamp: u32,
    /// projected encoded payload identity used by the codec rewrite boundary
    pub(super) codec: codec::ProjectedPacket,
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

#[cfg(test)]
#[path = "TESTS/local_send_rewrite.rs"]
mod tests;
