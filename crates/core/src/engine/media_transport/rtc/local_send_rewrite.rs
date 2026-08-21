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

use std::{
    collections::HashMap,
    mem,
    time::{Duration, Instant},
};

use str0m::{
    Rtc,
    media::Mid,
    rtp::{SeqNo, Ssrc},
};

use super::{
    codec,
    slots::{ConsumerStreamHandle, ConsumerStreamSlot, SlotStore},
    state::{RtcSessionState, muxed_rtp_ssrc},
};

// These mirror pinned str0m `Config::send_buffer_video`,
// `DEFAULT_RTX_CACHE_DURATION` and `DEFAULT_RTX_RATIO_CAP`. Recheck them on
// str0m upgrades so cache policy cannot drift.
pub(super) const RTX_CACHE_MAX_PACKETS: usize = 1_000;
pub(super) const RTX_CACHE_LIFETIME: Duration = Duration::from_secs(3);
const RTX_RATIO_CAP: Option<f32> = Some(0.15);

/// receiver identity state for one consumer route
///
/// all publisher SSRCs feeding the route are projected into one browser-facing
/// sequence, timestamp and encoded payload identity
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ConsumerStream {
    mid: Mid,
    delivery_generation: u64,
    primary_ssrc: Option<Ssrc>,
    queued_primary_writes: usize,
    stale_primary_writes: usize,
    rtx_cache_deadline: Option<Instant>,
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
    repair_streams_by_primary: HashMap<Ssrc, ConsumerStreamHandle>,
    retired_repair_streams: HashMap<Ssrc, RetiredRepairStream>,
    next_rtx_deadline: Option<Instant>,
}

struct RetiredRepairStream {
    mid: Mid,
    queued_primary_writes: usize,
}

impl ConsumerStreamStore {
    /// creates the stream handle stored on one local route destination
    pub(super) fn allocate(&mut self, mid: Mid) -> ConsumerStreamHandle {
        self.streams.insert(ConsumerStream {
            mid,
            ..ConsumerStream::default()
        })
    }

    /// releases a route destination handle
    pub(super) fn release(&mut self, handle: ConsumerStreamHandle) {
        let Some(stream) = self.streams.remove(handle) else {
            return;
        };
        if let Some(ssrc) = stream.primary_ssrc
            && self.repair_streams_by_primary.get(&ssrc) == Some(&handle)
        {
            self.repair_streams_by_primary.remove(&ssrc);
            if stream.queued_primary_writes > 0 {
                let retired =
                    self.retired_repair_streams
                        .entry(ssrc)
                        .or_insert(RetiredRepairStream {
                            mid: stream.mid,
                            queued_primary_writes: 0,
                        });
                retired.mid = stream.mid;
                retired.queued_primary_writes = retired
                    .queued_primary_writes
                    .saturating_add(stream.queued_primary_writes);
            }
        }
    }

    pub(super) fn queue_repairable_write(&mut self, handle: ConsumerStreamHandle, ssrc: Ssrc) {
        let Some(stream) = self.streams.get_mut(handle) else {
            return;
        };
        if stream.primary_ssrc == Some(ssrc) {
            stream.queued_primary_writes = stream.queued_primary_writes.saturating_add(1);
            return;
        }
        let previous_ssrc = stream.primary_ssrc.replace(ssrc);
        let mid = stream.mid;
        let previous_writes = mem::take(&mut stream.queued_primary_writes);
        stream.stale_primary_writes = 0;

        if let Some(previous_ssrc) = previous_ssrc
            && self.repair_streams_by_primary.get(&previous_ssrc) == Some(&handle)
        {
            self.repair_streams_by_primary.remove(&previous_ssrc);
            if previous_writes > 0 {
                let retired = self.retired_repair_streams.entry(previous_ssrc).or_insert(
                    RetiredRepairStream {
                        mid,
                        queued_primary_writes: 0,
                    },
                );
                retired.mid = mid;
                retired.queued_primary_writes = retired
                    .queued_primary_writes
                    .saturating_add(previous_writes);
            }
        }
        if let Some(retired) = self.retired_repair_streams.remove(&ssrc) {
            stream.queued_primary_writes = retired.queued_primary_writes;
            stream.stale_primary_writes = retired.queued_primary_writes;
        }
        stream.queued_primary_writes = stream.queued_primary_writes.saturating_add(1);
        self.repair_streams_by_primary.insert(ssrc, handle);
    }

    fn note_repairable_transmit(&mut self, ssrc: Ssrc, now: Instant) -> Option<Ssrc> {
        if let Some(handle) = self.repair_streams_by_primary.get(&ssrc).copied() {
            let stream = self.streams.get_mut(handle)?;
            stream.queued_primary_writes = stream.queued_primary_writes.checked_sub(1)?;
            if stream.stale_primary_writes > 0 {
                stream.stale_primary_writes -= 1;
                stream.rtx_cache_deadline = None;
                return Some(ssrc);
            }
            let deadline = now + RTX_CACHE_LIFETIME;
            stream.rtx_cache_deadline = Some(deadline);
            self.next_rtx_deadline = Some(
                self.next_rtx_deadline
                    .map_or(deadline, |current| current.min(deadline)),
            );
            return None;
        }
        let retired = self.retired_repair_streams.get_mut(&ssrc)?;
        retired.queued_primary_writes = retired.queued_primary_writes.checked_sub(1)?;
        if retired.queued_primary_writes == 0 {
            self.retired_repair_streams.remove(&ssrc);
        }
        Some(ssrc)
    }

    fn invalidate_rtx_stream(&mut self, handle: ConsumerStreamHandle) -> Option<Ssrc> {
        let stream = self.streams.get_mut(handle)?;
        stream.rtx_cache_deadline = None;
        stream.stale_primary_writes = stream.queued_primary_writes;
        stream.primary_ssrc
    }

    fn purge_removed_rtx_streams(&mut self, rtc: &mut Rtc) {
        let mut api = rtc.direct_api();
        self.retired_repair_streams
            .retain(|ssrc, _| api.stream_tx(ssrc).is_some());
    }

    fn reset_rtx_streams(&mut self, mid: Mid) {
        for stream in self.streams.values_mut().filter(|stream| stream.mid == mid) {
            stream.queued_primary_writes = 0;
            stream.stale_primary_writes = 0;
            stream.rtx_cache_deadline = None;
        }
        self.retired_repair_streams
            .retain(|_, stream| stream.mid != mid);
    }

    fn expire_rtx_streams(&mut self, rtc: &mut Rtc, now: Instant) {
        if !matches!(self.next_rtx_deadline, Some(deadline) if deadline <= now) {
            return;
        }
        self.next_rtx_deadline = None;
        for stream in self.streams.values_mut() {
            let Some(deadline) = stream.rtx_cache_deadline else {
                continue;
            };
            if deadline <= now {
                if let Some(primary_ssrc) = stream.primary_ssrc {
                    rotate_rtx_cache(rtc, primary_ssrc);
                }
                stream.rtx_cache_deadline = None;
            } else {
                self.next_rtx_deadline = Some(
                    self.next_rtx_deadline
                        .map_or(deadline, |current| current.min(deadline)),
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) fn rtx_cache_is_armed(&self, handle: ConsumerStreamHandle) -> bool {
        self.streams
            .get(handle)
            .is_some_and(|stream| stream.rtx_cache_deadline.is_some())
    }

    #[cfg(test)]
    pub(super) fn rtx_write_counts(&self, handle: ConsumerStreamHandle) -> Option<(usize, usize)> {
        self.streams
            .get(handle)
            .map(|stream| (stream.queued_primary_writes, stream.stale_primary_writes))
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
        source: SourceRtpIdentity,
        codec_identity: codec::PacketIdentity,
    ) -> Option<ProjectedIdentity> {
        self.streams.get_mut(stream_handle)?.project(
            source.delivery_generation,
            source.ssrc,
            source.seq_no,
            source.timestamp,
            codec_identity,
            source.was_repair,
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SourceRtpIdentity {
    pub(super) delivery_generation: u64,
    pub(super) ssrc: Ssrc,
    pub(super) seq_no: SeqNo,
    pub(super) timestamp: u32,
    pub(super) was_repair: bool,
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
        was_repair: bool,
    ) -> Option<ProjectedIdentity> {
        if was_repair && !self.accepts_repair(delivery_generation, source_ssrc, source_seq_no) {
            return None;
        }
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
                            resets_rtx_cache: false,
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
                        resets_rtx_cache: false,
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

    fn accepts_repair(
        &self,
        delivery_generation: u64,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
    ) -> bool {
        // str0m restores the RFC 4588 OSN before this boundary. O-SFU accepts
        // repair only for a primary RTP gap in the same SSRC and delivery epoch.
        // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
        delivery_generation == self.delivery_generation
            && matches!(
                self.rtp.timeline,
                RtpTimeline::Active {
                    ssrc,
                    src_seq_anchor,
                    highest_src_seq,
                    ..
                } if ssrc == source_ssrc
                    && source_seq_no >= src_seq_anchor
                    && source_seq_no < highest_src_seq
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
        // Compare wrapping generations as serial numbers. A delta in the upper half
        // of `u64` is older and must not roll receiver identity back after route resume.
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
            resets_rtx_cache: reanchor,
        })
    }
}

/// Receiver RTP mapping for the active publisher source.
///
/// O-SFU preserves source gaps so receiver loss reports identify the same
/// missing source sequence numbers. RFC 4588 section 3 calls this sequence
/// number preservation. str0m restores the section 4 OSN before projection.
/// Resetting the source anchor compacts only packets deliberately filtered by
/// O-SFU.
///
/// References:
/// - <https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1>
/// - <https://www.rfc-editor.org/rfc/rfc4588.html#section-3>
/// - <https://www.rfc-editor.org/rfc/rfc4588.html#section-4>
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
    pub(super) resets_rtx_cache: bool,
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

impl RtcSessionState {
    pub(super) fn note_repairable_transmit(&mut self, contents: &[u8], now: Instant) {
        let Some(ssrc) = muxed_rtp_ssrc(contents) else {
            return;
        };
        if let Some(primary_ssrc) = self.consumer_streams.note_repairable_transmit(ssrc, now) {
            rotate_rtx_cache(&mut self.rtc, primary_ssrc);
        }
    }

    pub(super) fn invalidate_rtx_stream(&mut self, handle: ConsumerStreamHandle) {
        let Some(primary_ssrc) = self.consumer_streams.invalidate_rtx_stream(handle) else {
            return;
        };
        rotate_rtx_cache(&mut self.rtc, primary_ssrc);
    }

    pub(super) fn expire_rtx_streams(&mut self, now: Instant) {
        self.consumer_streams.expire_rtx_streams(&mut self.rtc, now);
    }

    pub(super) fn purge_removed_rtx_streams(&mut self) {
        self.consumer_streams
            .purge_removed_rtx_streams(&mut self.rtc);
    }

    pub(super) fn reset_rtx_streams(&mut self, mid: Mid) {
        self.consumer_streams.reset_rtx_streams(mid);
    }

    /// Retires the destination `StreamTx` and host RTX state for one consumer MID.
    pub(super) fn remove_consumer_stream_tx(&mut self, mid: Mid) {
        self.reset_rtx_streams(mid);
        {
            let mut api = self.rtc.direct_api();
            if let Some(ssrc) = api.stream_tx_by_mid(mid, None).map(|stream| stream.ssrc()) {
                api.remove_stream_tx(ssrc);
            }
        }
        self.purge_removed_rtx_streams();
    }
}

fn rotate_rtx_cache(rtc: &mut Rtc, primary_ssrc: Ssrc) {
    let mut api = rtc.direct_api();
    // Primary-SSRC lookup avoids a MID scan for every cache expired in this drain.
    let Some(stream) = api.stream_tx(&primary_ssrc) else {
        return;
    };
    if stream.rtx().is_some() {
        stream.set_rtx_cache(RTX_CACHE_MAX_PACKETS, RTX_CACHE_LIFETIME, RTX_RATIO_CAP);
    }
}

#[cfg(test)]
#[path = "TESTS/local_send_rewrite.rs"]
mod tests;
