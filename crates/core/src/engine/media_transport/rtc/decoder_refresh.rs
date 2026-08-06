use std::{
    collections::{HashMap, VecDeque},
    mem,
    ops::Range,
    time::{Duration, Instant},
};

use o_sfu_rfc::rtp::vp8::{self, KEYFRAME_PREFIX_LEN};
use str0m::{
    media::{Pt, Rid},
    rtp::{SeqNo, Ssrc},
};

use super::{
    forwarded_packet::{ForwardedPacket, ForwardedPacketSource},
    source_route::PacketCodec,
};
use crate::engine::{RoomInstanceId, media_transport::TransportMediaId};

pub(super) const MAX_REFRESH_FRAME_PACKETS: usize = 512;
pub(super) const MAX_REFRESH_FRAME_BYTES: usize = 4 * 1_024 * 1_024;
pub(super) const MAX_PENDING_REFRESH_FRAMES: usize = 64;
pub(super) const MAX_PENDING_REFRESH_PACKETS: usize = 4_096;
pub(super) const MAX_PENDING_REFRESH_BYTES: usize = 16 * 1_024 * 1_024;
pub(super) const MAX_RELEASED_REFRESH_PACKETS_PER_TURN: usize = 64;
pub(super) const MAX_PENDING_REFRESH_FRAMES_PER_SOURCE: usize = 4;
pub(super) const MAX_PENDING_REFRESH_PACKETS_PER_SOURCE: usize = 512;
pub(super) const MAX_PENDING_REFRESH_BYTES_PER_SOURCE: usize = 4 * 1_024 * 1_024;
const MAX_REFRESH_FRAME_AGE: Duration = Duration::from_secs(2);
const RELEASE_BATCH_QUANTUM: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DecoderRefreshNeed {
    None,
    PendingDestination,
    SourceTransition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DecoderRefreshEvidence {
    H264 { starts_idr: bool },
    Vp8(Vp8RefreshFragment),
}

impl DecoderRefreshEvidence {
    fn starts_plausible_refresh(self) -> bool {
        match self {
            Self::H264 { starts_idr } => starts_idr,
            Self::Vp8(fragment) => fragment.starts_plausible_keyframe(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Vp8RefreshFragment {
    bytes: [u8; KEYFRAME_PREFIX_LEN],
    len: u8,
    starts_frame: bool,
}

impl Vp8RefreshFragment {
    pub(super) fn parse(payload: &[u8]) -> Option<Self> {
        let descriptor = vp8::payload_descriptor(payload)?;
        let payload = descriptor.frame_payload(payload)?;
        let len = payload.len().min(KEYFRAME_PREFIX_LEN);
        let mut bytes = [0; KEYFRAME_PREFIX_LEN];
        bytes.get_mut(..len)?.copy_from_slice(payload.get(..len)?);
        Some(Self {
            bytes,
            len: u8::try_from(len).ok()?,
            starts_frame: descriptor.starts_frame(),
        })
    }

    fn starts_plausible_keyframe(self) -> bool {
        self.starts_frame
            && self
                .bytes
                .first()
                .is_some_and(|frame_tag| frame_tag & vp8::INTERFRAME_BIT == 0)
            && self.len != 0
    }

    fn append_to(self, prefix: &mut [u8; KEYFRAME_PREFIX_LEN], len: usize) -> Option<usize> {
        let fragment = self.bytes.get(..usize::from(self.len))?;
        let remaining = prefix.get_mut(len..)?;
        let appended = remaining.len().min(fragment.len());
        remaining
            .get_mut(..appended)?
            .copy_from_slice(fragment.get(..appended)?);
        Some(len + appended)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DecoderRefreshPacket {
    pub(super) sequence_number: SeqNo,
    pub(super) timestamp: u32,
    pub(super) marker: bool,
    pub(super) ssrc: Ssrc,
    pub(super) payload_type: Pt,
    pub(super) codec: Option<PacketCodec>,
    pub(super) evidence: Option<DecoderRefreshEvidence>,
}

pub(super) enum DecoderRefreshAdmission {
    Held,
    Ready { packet_range: Range<usize> },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DecoderRefreshAdmissionInput {
    pub(super) room_instance_id: RoomInstanceId,
    pub(super) source_id: TransportMediaId,
    pub(super) rid: Option<Rid>,
    pub(super) need: DecoderRefreshNeed,
    pub(super) packet: DecoderRefreshPacket,
    pub(super) observed_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamIdentity {
    ssrc: Ssrc,
    payload_type: Pt,
    codec: PacketCodec,
}

impl StreamIdentity {
    fn from_packet(packet: DecoderRefreshPacket) -> Option<Self> {
        Some(Self {
            ssrc: packet.ssrc,
            payload_type: packet.payload_type,
            codec: packet.codec?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrameIdentity {
    stream: StreamIdentity,
    timestamp: u32,
}

impl FrameIdentity {
    fn from_packet(packet: DecoderRefreshPacket) -> Option<Self> {
        Some(Self {
            stream: StreamIdentity::from_packet(packet)?,
            timestamp: packet.timestamp,
        })
    }
}

#[derive(Debug)]
enum Vp8KeyframeState {
    Empty,
    Prefix {
        bytes: [u8; KEYFRAME_PREFIX_LEN],
        len: usize,
    },
    Invalid,
}

#[derive(Debug)]
enum DecoderRefreshState {
    H264 { starts_idr: bool },
    Vp8(Vp8KeyframeState),
}

impl DecoderRefreshState {
    fn new(codec: PacketCodec, evidence: Option<DecoderRefreshEvidence>) -> Self {
        let mut state = match codec {
            PacketCodec::H264(_) => Self::H264 { starts_idr: false },
            PacketCodec::Vp8 => Self::Vp8(Vp8KeyframeState::Empty),
        };
        state.observe(evidence);
        state
    }

    fn observe(&mut self, evidence: Option<DecoderRefreshEvidence>) {
        match (self, evidence) {
            (Self::H264 { starts_idr }, Some(DecoderRefreshEvidence::H264 { starts_idr: idr })) => {
                *starts_idr |= idr;
            }
            (Self::Vp8(state), Some(DecoderRefreshEvidence::Vp8(fragment))) => {
                state.observe(Some(fragment));
            }
            (Self::Vp8(state), _) => state.observe(None),
            (Self::H264 { .. }, _) => {}
        }
    }

    fn complete(&self) -> bool {
        match self {
            Self::H264 { starts_idr } => *starts_idr,
            Self::Vp8(state) => state.complete(),
        }
    }
}

impl Vp8KeyframeState {
    fn observe(&mut self, evidence: Option<Vp8RefreshFragment>) {
        let Some(fragment) = evidence else {
            *self = Self::Invalid;
            return;
        };
        match self {
            Self::Empty if fragment.starts_frame => {
                let mut bytes = [0; KEYFRAME_PREFIX_LEN];
                *self = fragment
                    .append_to(&mut bytes, 0)
                    .map_or(Self::Invalid, |len| Self::Prefix { bytes, len });
            }
            Self::Prefix { bytes, len } if !fragment.starts_frame => {
                if let Some(new_len) = fragment.append_to(bytes, *len) {
                    *len = new_len;
                } else {
                    *self = Self::Invalid;
                }
            }
            Self::Empty | Self::Prefix { .. } | Self::Invalid => *self = Self::Invalid,
        }
    }

    fn complete(&self) -> bool {
        matches!(
            self,
            Self::Prefix { bytes, len }
                if *len == KEYFRAME_PREFIX_LEN && vp8::valid_keyframe_prefix(bytes)
        )
    }
}

#[derive(Debug)]
struct DecoderRefreshCandidate {
    identity: FrameIdentity,
    last_sequence_number: SeqNo,
    started_at: Instant,
    payload_bytes: usize,
    refresh: DecoderRefreshState,
    packets: Vec<ForwardedPacket>,
}

impl DecoderRefreshCandidate {
    fn new(
        identity: FrameIdentity,
        packet: DecoderRefreshPacket,
        forwarded: ForwardedPacket,
        observed_at: Instant,
    ) -> Self {
        Self {
            identity,
            last_sequence_number: packet.sequence_number,
            started_at: observed_at,
            payload_bytes: forwarded.payload().len(),
            refresh: DecoderRefreshState::new(identity.stream.codec, packet.evidence),
            packets: vec![forwarded],
        }
    }

    fn accepts(&self, packet: DecoderRefreshPacket) -> bool {
        FrameIdentity::from_packet(packet) == Some(self.identity)
            && self.last_sequence_number.is_next(packet.sequence_number)
    }
}

#[derive(Debug)]
enum DecoderRefreshLaneState {
    Vacant,
    Holding(DecoderRefreshCandidate),
    Dropping {
        identity: FrameIdentity,
        expires_at: Instant,
    },
}

#[derive(Debug)]
struct DecoderRefreshLane {
    source_id: TransportMediaId,
    rid: Option<Rid>,
    state: DecoderRefreshLaneState,
    last_observed_at: Instant,
}

impl DecoderRefreshLane {
    fn new(source_id: TransportMediaId, rid: Option<Rid>, observed_at: Instant) -> Self {
        Self {
            source_id,
            rid,
            state: DecoderRefreshLaneState::Vacant,
            last_observed_at: observed_at,
        }
    }

    fn deadline(&self) -> Instant {
        match &self.state {
            DecoderRefreshLaneState::Holding(candidate) => {
                candidate.started_at + MAX_REFRESH_FRAME_AGE
            }
            DecoderRefreshLaneState::Dropping { expires_at, .. } => *expires_at,
            DecoderRefreshLaneState::Vacant => self.last_observed_at + MAX_REFRESH_FRAME_AGE,
        }
    }
}

#[derive(Debug)]
struct ReleasedDecoderRefreshBatch {
    room_instance_id: RoomInstanceId,
    source_id: TransportMediaId,
    rid: Option<Rid>,
    stream: StreamIdentity,
    released_at: Instant,
    activation: Option<DecoderRefreshActivation>,
    packets: VecDeque<ForwardedPacket>,
}

#[derive(Debug, Clone)]
pub(super) struct DecoderRefreshActivation {
    pub(super) source: ForwardedPacketSource,
    pub(super) source_id: TransportMediaId,
    pub(super) rid: Option<Rid>,
    pub(super) ssrc: Ssrc,
    pub(super) observed_at: Instant,
}

pub(super) struct DecoderRefreshRelease {
    pub(super) source_id: TransportMediaId,
    pub(super) packet_range: Range<usize>,
    pub(super) activation: Option<DecoderRefreshActivation>,
}

#[derive(Debug, Clone, Copy, Default)]
struct RetainedUsage {
    frames: usize,
    packets: usize,
    bytes: usize,
}

#[derive(Debug, Default)]
pub(super) struct PendingDecoderRefreshes {
    lanes: Vec<DecoderRefreshLane>,
    released_batches: VecDeque<ReleasedDecoderRefreshBatch>,
    released_source_packets: HashMap<TransportMediaId, usize>,
    source_usage: HashMap<TransportMediaId, RetainedUsage>,
    payload_bytes: usize,
    retained_packets: usize,
    next_lane_deadline: Option<Instant>,
    last_released_room: Option<RoomInstanceId>,
}

impl PendingDecoderRefreshes {
    pub(super) fn admit(
        &mut self,
        input: DecoderRefreshAdmissionInput,
        packet: ForwardedPacket,
        ready: &mut Vec<ForwardedPacket>,
    ) -> DecoderRefreshAdmission {
        self.drop_conflicting_lane(input);
        let lane_index = self.lane_index(input.source_id, input.rid);
        match lane_index {
            Some(index) => self.admit_to_lane(index, input, packet, ready),
            None => self.admit_without_lane(input, packet, ready),
        }
    }

    pub(super) fn expire(&mut self, now: Instant) {
        if self
            .next_lane_deadline
            .is_none_or(|deadline| deadline > now)
        {
            return;
        }
        let mut index = 0;
        while index < self.lanes.len() {
            if self
                .lanes
                .get(index)
                .is_some_and(|lane| lane.deadline() <= now)
            {
                self.expire_lane(index, now);
            } else {
                index += 1;
            }
        }
        self.recompute_lane_deadline();
    }

    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.next_lane_deadline
            .into_iter()
            .chain(self.released_batches.iter().map(|batch| batch.released_at))
            .min()
    }

    pub(super) fn remove_source(&mut self, source_id: TransportMediaId) {
        let mut index = 0;
        while index < self.lanes.len() {
            if self
                .lanes
                .get(index)
                .is_some_and(|lane| lane.source_id == source_id)
            {
                self.drop_lane(index);
            } else {
                index += 1;
            }
        }
        while let Some(index) = self
            .released_batches
            .iter()
            .position(|batch| batch.source_id == source_id)
        {
            if let Some(batch) = self.remove_released_batch(index) {
                let packet_count = batch.packets.len();
                let bytes = batch
                    .packets
                    .iter()
                    .map(|packet| packet.payload().len())
                    .sum();
                self.release_retained(batch.source_id, packet_count, bytes);
            }
        }
        self.released_source_packets.remove(&source_id);
    }

    pub(super) fn drain_released(
        &mut self,
        ready: &mut Vec<ForwardedPacket>,
        limit: usize,
        releases: &mut Vec<DecoderRefreshRelease>,
    ) -> usize {
        let mut remaining = limit;
        while remaining > 0 {
            let Some(index) = self.next_release_batch() else {
                break;
            };
            let packet_count = self.released_batches.get(index).map_or(0, |batch| {
                remaining
                    .min(RELEASE_BATCH_QUANTUM)
                    .min(batch.packets.len())
            });
            if packet_count == 0 {
                self.remove_released_batch(index);
                continue;
            }
            let start = ready.len();
            let Some((room_instance_id, source_id, activation, drained_bytes, batch_empty)) =
                self.released_batches.get_mut(index).map(|batch| {
                    let room_instance_id = batch.room_instance_id;
                    let source_id = batch.source_id;
                    let activation = batch.activation.take();
                    let mut drained_bytes = 0;
                    for packet in batch.packets.drain(..packet_count) {
                        drained_bytes += packet.payload().len();
                        ready.push(packet);
                    }
                    (
                        room_instance_id,
                        source_id,
                        activation,
                        drained_bytes,
                        batch.packets.is_empty(),
                    )
                })
            else {
                break;
            };
            self.last_released_room = Some(room_instance_id);
            self.release_retained(source_id, packet_count, drained_bytes);
            self.remove_released_source_packets(source_id, packet_count);
            releases.push(DecoderRefreshRelease {
                source_id,
                packet_range: start..ready.len(),
                activation,
            });
            if batch_empty {
                self.remove_released_batch(index);
            } else if let Some(batch) = self.released_batches.remove(index) {
                self.released_batches.push_back(batch);
            }
            remaining -= packet_count;
        }
        limit - remaining
    }

    #[cfg(test)]
    pub(super) fn has_released_packets(&self) -> bool {
        !self.released_batches.is_empty()
    }

    pub(super) fn has_matching_release(
        &self,
        room_instance_id: RoomInstanceId,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        packet: DecoderRefreshPacket,
    ) -> bool {
        let Some(stream) = StreamIdentity::from_packet(packet) else {
            return false;
        };
        self.release_batch_index(room_instance_id, source_id, rid, stream)
            .is_some()
    }

    pub(super) fn defer_behind_release(
        &mut self,
        room_instance_id: RoomInstanceId,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        packet_identity: DecoderRefreshPacket,
        packet: ForwardedPacket,
    ) -> bool {
        let Some(stream) = StreamIdentity::from_packet(packet_identity) else {
            return false;
        };
        self.queue_packet(room_instance_id, source_id, rid, stream, packet)
    }

    fn admit_without_lane(
        &mut self,
        input: DecoderRefreshAdmissionInput,
        packet: ForwardedPacket,
        ready: &mut Vec<ForwardedPacket>,
    ) -> DecoderRefreshAdmission {
        let Some(identity) = FrameIdentity::from_packet(input.packet) else {
            return push_ready(packet, ready);
        };
        if input.need == DecoderRefreshNeed::None
            || !input
                .packet
                .evidence
                .is_some_and(DecoderRefreshEvidence::starts_plausible_refresh)
            || !self.lane_fits(input.source_id)
            || !self.packet_fits(input.source_id, 0, 0, packet.payload().len())
        {
            return push_ready(packet, ready);
        }
        let index = self.push_lane(DecoderRefreshLane::new(
            input.source_id,
            input.rid,
            input.observed_at,
        ));
        self.retain_packet(input.source_id, packet.payload().len());
        let candidate =
            DecoderRefreshCandidate::new(identity, input.packet, packet, input.observed_at);
        if input.packet.marker {
            self.finish_candidate(index, candidate, input);
        } else {
            self.set_lane_state(index, DecoderRefreshLaneState::Holding(candidate));
        }
        DecoderRefreshAdmission::Held
    }

    fn admit_to_lane(
        &mut self,
        index: usize,
        input: DecoderRefreshAdmissionInput,
        packet: ForwardedPacket,
        ready: &mut Vec<ForwardedPacket>,
    ) -> DecoderRefreshAdmission {
        let Some(state) = self.take_lane_state(index, input.observed_at) else {
            return self.admit_without_lane(input, packet, ready);
        };
        if lane_state_stream(&state)
            .is_some_and(|stream| Some(stream) != StreamIdentity::from_packet(input.packet))
        {
            self.set_lane_state(index, state);
            if input
                .packet
                .evidence
                .is_some_and(DecoderRefreshEvidence::starts_plausible_refresh)
                && input.need != DecoderRefreshNeed::None
            {
                self.drop_lane(index);
                return self.admit_without_lane(input, packet, ready);
            }
            return push_ready(packet, ready);
        }
        match state {
            DecoderRefreshLaneState::Holding(candidate) => {
                if input.need == DecoderRefreshNeed::None {
                    self.abort_candidate(index, &candidate, input, packet, ready)
                } else {
                    self.admit_holding(index, candidate, input, packet, ready)
                }
            }
            DecoderRefreshLaneState::Dropping {
                identity,
                expires_at,
            } => self.admit_dropping(index, identity, expires_at, input, packet, ready),
            DecoderRefreshLaneState::Vacant => {
                self.drop_lane(index);
                self.admit_without_lane(input, packet, ready)
            }
        }
    }

    fn admit_holding(
        &mut self,
        index: usize,
        mut candidate: DecoderRefreshCandidate,
        input: DecoderRefreshAdmissionInput,
        packet: ForwardedPacket,
        ready: &mut Vec<ForwardedPacket>,
    ) -> DecoderRefreshAdmission {
        if !candidate.accepts(input.packet) {
            return self.abort_candidate(index, &candidate, input, packet, ready);
        }
        let payload_len = packet.payload().len();
        if !self.packet_fits(
            input.source_id,
            candidate.packets.len(),
            candidate.payload_bytes,
            payload_len,
        ) {
            return self.abort_candidate(index, &candidate, input, packet, ready);
        }
        candidate.last_sequence_number = input.packet.sequence_number;
        candidate.payload_bytes += payload_len;
        candidate.refresh.observe(input.packet.evidence);
        candidate.packets.push(packet);
        self.retain_packet(input.source_id, payload_len);
        if input.packet.marker {
            self.finish_candidate(index, candidate, input);
        } else {
            self.set_lane_state(index, DecoderRefreshLaneState::Holding(candidate));
        }
        DecoderRefreshAdmission::Held
    }

    fn abort_candidate(
        &mut self,
        index: usize,
        candidate: &DecoderRefreshCandidate,
        input: DecoderRefreshAdmissionInput,
        packet: ForwardedPacket,
        ready: &mut Vec<ForwardedPacket>,
    ) -> DecoderRefreshAdmission {
        let same_frame = FrameIdentity::from_packet(input.packet) == Some(candidate.identity);
        self.release_retained(
            input.source_id,
            candidate.packets.len(),
            candidate.payload_bytes,
        );
        if same_frame {
            if input.packet.marker {
                self.drop_lane(index);
            } else {
                self.set_lane_state(
                    index,
                    DecoderRefreshLaneState::Dropping {
                        identity: candidate.identity,
                        expires_at: input.observed_at + MAX_REFRESH_FRAME_AGE,
                    },
                );
            }
            return DecoderRefreshAdmission::Held;
        }
        self.drop_lane(index);
        self.admit_without_lane(input, packet, ready)
    }

    fn admit_dropping(
        &mut self,
        index: usize,
        dropped: FrameIdentity,
        expires_at: Instant,
        input: DecoderRefreshAdmissionInput,
        packet: ForwardedPacket,
        ready: &mut Vec<ForwardedPacket>,
    ) -> DecoderRefreshAdmission {
        if FrameIdentity::from_packet(input.packet) == Some(dropped) {
            if input.packet.marker {
                self.drop_lane(index);
            } else {
                self.set_lane_state(
                    index,
                    DecoderRefreshLaneState::Dropping {
                        identity: dropped,
                        expires_at,
                    },
                );
            }
            return DecoderRefreshAdmission::Held;
        }
        self.drop_lane(index);
        self.admit_without_lane(input, packet, ready)
    }

    fn finish_candidate(
        &mut self,
        index: usize,
        candidate: DecoderRefreshCandidate,
        input: DecoderRefreshAdmissionInput,
    ) {
        if candidate.refresh.complete() {
            let activation = candidate
                .packets
                .first()
                .map(|packet| DecoderRefreshActivation {
                    source: packet.source().clone(),
                    source_id: input.source_id,
                    rid: input.rid,
                    ssrc: candidate.identity.stream.ssrc,
                    observed_at: input.observed_at,
                });
            let packets = VecDeque::from(candidate.packets);
            if !packets.is_empty() {
                self.retain_frame(input.source_id);
                self.add_released_source_packets(input.source_id, packets.len());
                self.released_batches
                    .push_back(ReleasedDecoderRefreshBatch {
                        room_instance_id: input.room_instance_id,
                        source_id: input.source_id,
                        rid: input.rid,
                        stream: candidate.identity.stream,
                        released_at: input.observed_at,
                        activation,
                        packets,
                    });
            }
        } else {
            self.release_retained(
                input.source_id,
                candidate.packets.len(),
                candidate.payload_bytes,
            );
        }
        self.drop_lane(index);
    }

    fn lane_fits(&self, source_id: TransportMediaId) -> bool {
        self.lanes.len().saturating_add(self.released_batches.len()) < MAX_PENDING_REFRESH_FRAMES
            && self
                .source_usage
                .get(&source_id)
                .is_none_or(|usage| usage.frames < MAX_PENDING_REFRESH_FRAMES_PER_SOURCE)
    }

    fn packet_fits(
        &self,
        source_id: TransportMediaId,
        frame_packets: usize,
        frame_bytes: usize,
        packet_bytes: usize,
    ) -> bool {
        frame_packets < MAX_REFRESH_FRAME_PACKETS
            && frame_bytes
                .checked_add(packet_bytes)
                .is_some_and(|bytes| bytes <= MAX_REFRESH_FRAME_BYTES)
            && self.pending_packet_fits(source_id, packet_bytes)
    }

    fn pending_packet_fits(&self, source_id: TransportMediaId, packet_bytes: usize) -> bool {
        self.retained_packets < MAX_PENDING_REFRESH_PACKETS
            && self
                .payload_bytes
                .checked_add(packet_bytes)
                .is_some_and(|bytes| bytes <= MAX_PENDING_REFRESH_BYTES)
            && usage_packet_fits(
                self.source_usage
                    .get(&source_id)
                    .copied()
                    .unwrap_or_default(),
                packet_bytes,
                MAX_PENDING_REFRESH_PACKETS_PER_SOURCE,
                MAX_PENDING_REFRESH_BYTES_PER_SOURCE,
            )
    }

    fn drop_conflicting_lane(&mut self, input: DecoderRefreshAdmissionInput) {
        if let Some(index) = self.lanes.iter().position(|lane| {
            lane.source_id == input.source_id
                && lane.rid != input.rid
                && lane_ssrc(lane) == Some(input.packet.ssrc)
        }) {
            self.abort_lane(index, input.observed_at);
        }
    }

    fn push_lane(&mut self, lane: DecoderRefreshLane) -> usize {
        let index = self.lanes.len();
        self.retain_frame(lane.source_id);
        self.next_lane_deadline = Some(
            self.next_lane_deadline
                .map_or_else(|| lane.deadline(), |deadline| deadline.min(lane.deadline())),
        );
        self.lanes.push(lane);
        index
    }

    fn set_lane_state(&mut self, index: usize, state: DecoderRefreshLaneState) {
        let Some(deadline) = self.lanes.get_mut(index).map(|lane| {
            lane.state = state;
            lane.deadline()
        }) else {
            return;
        };
        self.lower_lane_deadline(deadline);
    }

    fn take_lane_state(
        &mut self,
        index: usize,
        observed_at: Instant,
    ) -> Option<DecoderRefreshLaneState> {
        let (deadline, state) = {
            let lane = self.lanes.get_mut(index)?;
            lane.last_observed_at = observed_at;
            let state = mem::replace(&mut lane.state, DecoderRefreshLaneState::Vacant);
            (lane.deadline(), state)
        };
        self.lower_lane_deadline(deadline);
        Some(state)
    }

    fn queue_packet(
        &mut self,
        room_instance_id: RoomInstanceId,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        stream: StreamIdentity,
        packet: ForwardedPacket,
    ) -> bool {
        let Some(batch_index) = self.release_batch_index(room_instance_id, source_id, rid, stream)
        else {
            return false;
        };
        if !self.pending_packet_fits(source_id, packet.payload().len()) {
            return false;
        }
        let packet_bytes = packet.payload().len();
        let Some(batch) = self.released_batches.get_mut(batch_index) else {
            return false;
        };
        batch.packets.push_back(packet);
        self.retain_packet(source_id, packet_bytes);
        self.add_released_source_packets(source_id, 1);
        true
    }

    fn release_batch_index(
        &self,
        room_instance_id: RoomInstanceId,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        stream: StreamIdentity,
    ) -> Option<usize> {
        if !self.released_source_packets.contains_key(&source_id) {
            return None;
        }
        self.released_batches.iter().position(|batch| {
            batch.room_instance_id == room_instance_id
                && batch.source_id == source_id
                && batch.rid == rid
                && batch.stream == stream
        })
    }

    fn next_release_batch(&self) -> Option<usize> {
        let queue_head = (!self.released_batches.is_empty()).then_some(0)?;
        self.last_released_room
            .and_then(|last_room| {
                self.released_batches
                    .iter()
                    .position(|batch| batch.room_instance_id != last_room)
            })
            .or(Some(queue_head))
    }

    fn add_released_source_packets(&mut self, source_id: TransportMediaId, count: usize) {
        *self.released_source_packets.entry(source_id).or_default() += count;
    }

    fn remove_released_source_packets(&mut self, source_id: TransportMediaId, count: usize) {
        let Some(retained) = self.released_source_packets.get_mut(&source_id) else {
            return;
        };
        *retained = retained.saturating_sub(count);
        if *retained == 0 {
            self.released_source_packets.remove(&source_id);
        }
    }

    fn retain_packet(&mut self, source_id: TransportMediaId, packet_bytes: usize) {
        self.retained_packets += 1;
        self.payload_bytes += packet_bytes;
        let usage = self.source_usage.entry(source_id).or_default();
        usage.packets += 1;
        usage.bytes += packet_bytes;
    }

    fn release_retained(&mut self, source_id: TransportMediaId, packet_count: usize, bytes: usize) {
        self.retained_packets = self.retained_packets.saturating_sub(packet_count);
        self.payload_bytes = self.payload_bytes.saturating_sub(bytes);
        let Some(usage) = self.source_usage.get_mut(&source_id) else {
            return;
        };
        usage.packets = usage.packets.saturating_sub(packet_count);
        usage.bytes = usage.bytes.saturating_sub(bytes);
        if usage.frames == 0 && usage.packets == 0 {
            self.source_usage.remove(&source_id);
        }
    }

    fn retain_frame(&mut self, source_id: TransportMediaId) {
        self.source_usage.entry(source_id).or_default().frames += 1;
    }

    fn release_frame(&mut self, source_id: TransportMediaId) {
        let Some(usage) = self.source_usage.get_mut(&source_id) else {
            return;
        };
        usage.frames = usage.frames.saturating_sub(1);
        if usage.frames == 0 && usage.packets == 0 {
            self.source_usage.remove(&source_id);
        }
    }

    fn remove_released_batch(&mut self, index: usize) -> Option<ReleasedDecoderRefreshBatch> {
        let batch = self.released_batches.remove(index)?;
        self.release_frame(batch.source_id);
        Some(batch)
    }

    fn lower_lane_deadline(&mut self, current: Instant) {
        self.next_lane_deadline = Some(
            self.next_lane_deadline
                .map_or(current, |deadline| deadline.min(current)),
        );
    }

    fn recompute_lane_deadline(&mut self) {
        self.next_lane_deadline = self.lanes.iter().map(DecoderRefreshLane::deadline).min();
    }

    fn drop_lane(&mut self, index: usize) {
        let Some(lane) = self.swap_remove_lane(index) else {
            return;
        };
        if let DecoderRefreshLaneState::Holding(candidate) = lane.state {
            self.release_retained(
                lane.source_id,
                candidate.packets.len(),
                candidate.payload_bytes,
            );
        }
    }

    fn expire_lane(&mut self, index: usize, now: Instant) {
        let Some(source_id) = self.lanes.get(index).map(|lane| lane.source_id) else {
            return;
        };
        let Some(state) = self.take_lane_state(index, now) else {
            return;
        };
        match state {
            DecoderRefreshLaneState::Holding(candidate) => {
                self.release_retained(source_id, candidate.packets.len(), candidate.payload_bytes);
                self.set_lane_state(
                    index,
                    DecoderRefreshLaneState::Dropping {
                        identity: candidate.identity,
                        expires_at: now + MAX_REFRESH_FRAME_AGE,
                    },
                );
            }
            DecoderRefreshLaneState::Dropping { .. } | DecoderRefreshLaneState::Vacant => {
                self.drop_lane(index);
            }
        }
    }

    fn abort_lane(&mut self, index: usize, now: Instant) {
        let Some(source_id) = self.lanes.get(index).map(|lane| lane.source_id) else {
            return;
        };
        let Some(state) = self.take_lane_state(index, now) else {
            return;
        };
        match state {
            DecoderRefreshLaneState::Holding(candidate) => {
                self.release_retained(source_id, candidate.packets.len(), candidate.payload_bytes);
                self.set_lane_state(
                    index,
                    DecoderRefreshLaneState::Dropping {
                        identity: candidate.identity,
                        expires_at: now + MAX_REFRESH_FRAME_AGE,
                    },
                );
            }
            DecoderRefreshLaneState::Dropping {
                identity,
                expires_at,
            } => {
                self.set_lane_state(
                    index,
                    DecoderRefreshLaneState::Dropping {
                        identity,
                        expires_at,
                    },
                );
            }
            DecoderRefreshLaneState::Vacant => self.drop_lane(index),
        }
    }

    fn swap_remove_lane(&mut self, index: usize) -> Option<DecoderRefreshLane> {
        if index >= self.lanes.len() {
            return None;
        }
        let lane = self.lanes.swap_remove(index);
        self.release_frame(lane.source_id);
        Some(lane)
    }

    fn lane_index(&self, source_id: TransportMediaId, rid: Option<Rid>) -> Option<usize> {
        self.lanes
            .iter()
            .position(|lane| lane.source_id == source_id && lane.rid == rid)
    }

    #[cfg(test)]
    pub(super) fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    #[cfg(test)]
    pub(super) const fn buffered_bytes(&self) -> usize {
        self.payload_bytes
    }

    #[cfg(test)]
    pub(super) const fn retained_packet_count(&self) -> usize {
        self.retained_packets
    }
}

fn lane_ssrc(lane: &DecoderRefreshLane) -> Option<Ssrc> {
    match lane.state {
        DecoderRefreshLaneState::Holding(ref candidate) => Some(candidate.identity.stream.ssrc),
        DecoderRefreshLaneState::Dropping { identity, .. } => Some(identity.stream.ssrc),
        DecoderRefreshLaneState::Vacant => None,
    }
}

fn lane_state_stream(state: &DecoderRefreshLaneState) -> Option<StreamIdentity> {
    match state {
        DecoderRefreshLaneState::Holding(candidate) => Some(candidate.identity.stream),
        DecoderRefreshLaneState::Dropping { identity, .. } => Some(identity.stream),
        DecoderRefreshLaneState::Vacant => None,
    }
}

fn push_ready(
    packet: ForwardedPacket,
    ready: &mut Vec<ForwardedPacket>,
) -> DecoderRefreshAdmission {
    let start = ready.len();
    ready.push(packet);
    DecoderRefreshAdmission::Ready {
        packet_range: start..ready.len(),
    }
}

fn usage_packet_fits(
    usage: RetainedUsage,
    packet_bytes: usize,
    max_packets: usize,
    max_bytes: usize,
) -> bool {
    usage.packets < max_packets
        && usage
            .bytes
            .checked_add(packet_bytes)
            .is_some_and(|bytes| bytes <= max_bytes)
}
