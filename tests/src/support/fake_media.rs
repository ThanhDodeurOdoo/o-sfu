#![allow(
    dead_code,
    reason = "deterministic media fixtures are shared across the protocol integration and fake-stream scenarios"
)]

use std::{fmt, time::Duration};

use o_sfu_protocol::wire::StreamType;
use o_sfu_rfc::rtp::{self, CodecName, frame_marking};
use o_sfu_router::MediaKind;
use str0m::rtp::ExtensionValues;

const AUDIO_FRAME_INTERVAL: Duration = Duration::from_millis(20);
const AUDIO_TIMESTAMP_STEP: u32 = rtp::opus::RTP_CLOCK_RATE_HZ / 50;
const AUDIO_PACKET_PAYLOAD_LEN: usize = 160;
const OPUS_FRAME_BODY_LEN: usize = AUDIO_PACKET_PAYLOAD_LEN - 1;
pub const SYNTHETIC_OPUS_ONE_FRAME_TOC: u8 =
    (rtp::opus::toc_config::SILK_WIDEBAND_20_MS << 3) | rtp::opus::frame_count_code::ONE_FRAME;
const SYNTHETIC_H264_FUA_IDR_CONTINUATION_HEADER: u8 = rtp::h264::NAL_UNIT_TYPE_IDR;

const VIDEO_FRAME_INTERVAL: Duration = Duration::from_millis(33);
const VIDEO_TIMESTAMP_STEP: u32 = 2_970;
const VIDEO_PAYLOAD_BODY_LEN: usize = 1_200;

const AUDIO_PAYLOAD_SEED: u8 = 0x11;
const VP8_PAYLOAD_SEED: u8 = 0x41;
const H264_PAYLOAD_SEED: u8 = 0x51;
const UNSUPPORTED_PAYLOAD_SEED: u8 = 0x71;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FakeClock {
    now: Duration,
}

impl FakeClock {
    #[must_use]
    pub fn now(self) -> Duration {
        self.now
    }

    pub fn advance(&mut self, delta: Duration) -> Duration {
        self.now += delta;
        self.now
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticRtpPacket {
    pub emitted_at: Duration,
    pub rtp_timestamp: u32,
    pub sequence_number: u16,
    pub marker: bool,
    pub codec: CodecName,
    pub media_kind: MediaKind,
    pub rid: Option<String>,
    pub extension_values: ExtensionValues,
    pub payload: Vec<u8>,
}

pub type FakeMediaFrame = SyntheticRtpPacket;

pub trait SyntheticRtpStream: fmt::Debug {
    fn stream_type(&self) -> StreamType;

    fn codec(&self) -> CodecName;

    fn next_packet(&mut self, clock: &mut FakeClock) -> SyntheticRtpPacket;

    fn media_kind(&self) -> MediaKind {
        media_kind_for_stream_type(self.stream_type())
    }
}

#[derive(Debug)]
pub struct FakeMediaSource {
    stream: Box<dyn SyntheticRtpStream>,
}

impl FakeMediaSource {
    #[must_use]
    pub fn new(stream: impl SyntheticRtpStream + 'static) -> Self {
        Self {
            stream: Box::new(stream),
        }
    }

    #[must_use]
    pub fn audio() -> Self {
        Self::new(SyntheticOpusStream::default())
    }

    #[must_use]
    pub fn camera() -> Self {
        Self::vp8_camera_high()
    }

    #[must_use]
    pub fn vp8_camera_high() -> Self {
        Self::new(SyntheticVp8Stream::high())
    }

    #[must_use]
    pub fn vp8_camera_with_rid(rid: impl Into<String>) -> Self {
        Self::new(SyntheticVp8Stream::new(Some(rid.into())))
    }

    #[must_use]
    pub fn h264_camera_high() -> Self {
        Self::new(SyntheticH264Stream::high())
    }

    #[must_use]
    pub fn unsupported_camera_codec() -> Self {
        Self::new(SyntheticUnsupportedStream::camera())
    }

    #[must_use]
    pub fn codec(&self) -> CodecName {
        self.stream.codec()
    }

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.stream.media_kind()
    }

    #[must_use]
    pub fn stream_type(&self) -> StreamType {
        self.stream.stream_type()
    }

    pub fn next_frame(&mut self, clock: &mut FakeClock) -> FakeMediaFrame {
        self.stream.next_packet(clock)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticOpusStream {
    timing: SyntheticTiming,
    audio_level_dbov: i8,
    voice_activity: bool,
}

impl Default for SyntheticOpusStream {
    fn default() -> Self {
        Self {
            timing: SyntheticTiming::new(
                AUDIO_FRAME_INTERVAL,
                AUDIO_TIMESTAMP_STEP,
                AUDIO_PAYLOAD_SEED,
            ),
            audio_level_dbov: -32,
            voice_activity: true,
        }
    }
}

impl SyntheticOpusStream {
    #[must_use]
    pub fn with_audio_activity(audio_level_dbov: i8, voice_activity: bool) -> Self {
        Self {
            audio_level_dbov,
            voice_activity,
            ..Self::default()
        }
    }
}

impl SyntheticRtpStream for SyntheticOpusStream {
    fn stream_type(&self) -> StreamType {
        StreamType::Audio
    }

    fn codec(&self) -> CodecName {
        CodecName::Opus
    }

    fn next_packet(&mut self, clock: &mut FakeClock) -> SyntheticRtpPacket {
        let timing = self.timing.next(clock);
        let frame_body = deterministic_payload(
            OPUS_FRAME_BODY_LEN,
            self.timing.payload_seed,
            timing.sequence_number,
            timing.rtp_timestamp,
        );
        SyntheticRtpPacket {
            emitted_at: timing.emitted_at,
            rtp_timestamp: timing.rtp_timestamp,
            sequence_number: timing.sequence_number,
            marker: false,
            codec: CodecName::Opus,
            media_kind: MediaKind::Audio,
            rid: None,
            extension_values: ExtensionValues {
                audio_level: Some(self.audio_level_dbov),
                voice_activity: Some(self.voice_activity),
                ..ExtensionValues::default()
            },
            payload: synthetic_opus_one_frame_packet(&frame_body),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticVp8Stream {
    timing: SyntheticTiming,
    rid: Option<String>,
    picture_id: u16,
    tl0_pic_idx: u8,
    temporal_layer_id: u8,
    next_keyframe: bool,
    keyframe_after_next: bool,
}

impl SyntheticVp8Stream {
    #[must_use]
    pub fn new(rid: Option<String>) -> Self {
        Self {
            timing: SyntheticTiming::new(
                VIDEO_FRAME_INTERVAL,
                VIDEO_TIMESTAMP_STEP,
                VP8_PAYLOAD_SEED,
            ),
            rid,
            picture_id: 1,
            tl0_pic_idx: 1,
            temporal_layer_id: frame_marking::BASE_LAYER_ID,
            next_keyframe: true,
            keyframe_after_next: false,
        }
    }

    #[must_use]
    pub fn high() -> Self {
        Self::new(Some("hi".to_owned()))
    }

    #[must_use]
    pub fn with_next_keyframe(next_keyframe: bool) -> Self {
        Self {
            next_keyframe,
            keyframe_after_next: true,
            ..Self::high()
        }
    }
}

impl SyntheticRtpStream for SyntheticVp8Stream {
    fn stream_type(&self) -> StreamType {
        StreamType::Camera
    }

    fn codec(&self) -> CodecName {
        CodecName::Vp8
    }

    fn next_packet(&mut self, clock: &mut FakeClock) -> SyntheticRtpPacket {
        let timing = self.timing.next(clock);
        let keyframe = self.next_keyframe;
        self.next_keyframe = self.keyframe_after_next;
        let body = deterministic_payload(
            VIDEO_PAYLOAD_BODY_LEN,
            self.timing.payload_seed,
            timing.sequence_number,
            timing.rtp_timestamp,
        );
        let payload = synthetic_vp8_payload_with_long_picture_id(
            self.picture_id,
            self.tl0_pic_idx,
            self.temporal_layer_id,
            keyframe,
            &body,
        )
        .unwrap_or_default();
        self.picture_id = self.picture_id.wrapping_add(1);
        self.tl0_pic_idx = self.tl0_pic_idx.wrapping_add(1);
        SyntheticRtpPacket {
            emitted_at: timing.emitted_at,
            rtp_timestamp: timing.rtp_timestamp,
            sequence_number: timing.sequence_number,
            marker: true,
            codec: CodecName::Vp8,
            media_kind: MediaKind::Video,
            rid: self.rid.clone(),
            extension_values: ExtensionValues {
                frame_mark: Some(frame_marking_value(self.temporal_layer_id, keyframe)),
                ..ExtensionValues::default()
            },
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticH264Stream {
    timing: SyntheticTiming,
    rid: Option<String>,
    next_idr: bool,
    idr_after_next: bool,
}

impl SyntheticH264Stream {
    #[must_use]
    pub fn new(rid: Option<String>) -> Self {
        Self {
            timing: SyntheticTiming::new(
                VIDEO_FRAME_INTERVAL,
                VIDEO_TIMESTAMP_STEP,
                H264_PAYLOAD_SEED,
            ),
            rid,
            next_idr: true,
            idr_after_next: false,
        }
    }

    #[must_use]
    pub fn high() -> Self {
        Self::new(Some("hi".to_owned()))
    }

    #[must_use]
    pub fn with_idr(next_idr: bool) -> Self {
        Self {
            next_idr,
            idr_after_next: true,
            ..Self::high()
        }
    }
}

impl SyntheticRtpStream for SyntheticH264Stream {
    fn stream_type(&self) -> StreamType {
        StreamType::Camera
    }

    fn codec(&self) -> CodecName {
        CodecName::H264
    }

    fn next_packet(&mut self, clock: &mut FakeClock) -> SyntheticRtpPacket {
        let timing = self.timing.next(clock);
        let idr = self.next_idr;
        self.next_idr = self.idr_after_next;
        let body = deterministic_payload(
            VIDEO_PAYLOAD_BODY_LEN,
            self.timing.payload_seed,
            timing.sequence_number,
            timing.rtp_timestamp,
        );
        SyntheticRtpPacket {
            emitted_at: timing.emitted_at,
            rtp_timestamp: timing.rtp_timestamp,
            sequence_number: timing.sequence_number,
            marker: true,
            codec: CodecName::H264,
            media_kind: MediaKind::Video,
            rid: self.rid.clone(),
            extension_values: ExtensionValues {
                frame_mark: Some(frame_marking_value(frame_marking::BASE_LAYER_ID, idr)),
                ..ExtensionValues::default()
            },
            payload: synthetic_h264_payload(&body, idr),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticUnsupportedStream {
    timing: SyntheticTiming,
    rid: Option<String>,
}

impl SyntheticUnsupportedStream {
    fn camera() -> Self {
        Self {
            timing: SyntheticTiming::new(
                VIDEO_FRAME_INTERVAL,
                VIDEO_TIMESTAMP_STEP,
                UNSUPPORTED_PAYLOAD_SEED,
            ),
            rid: Some("hi".to_owned()),
        }
    }
}

impl SyntheticRtpStream for SyntheticUnsupportedStream {
    fn stream_type(&self) -> StreamType {
        StreamType::Camera
    }

    fn codec(&self) -> CodecName {
        CodecName::Other("synthetic-unsupported".to_owned())
    }

    fn next_packet(&mut self, clock: &mut FakeClock) -> SyntheticRtpPacket {
        let timing = self.timing.next(clock);
        SyntheticRtpPacket {
            emitted_at: timing.emitted_at,
            rtp_timestamp: timing.rtp_timestamp,
            sequence_number: timing.sequence_number,
            marker: true,
            codec: CodecName::Other("synthetic-unsupported".to_owned()),
            media_kind: MediaKind::Video,
            rid: self.rid.clone(),
            extension_values: ExtensionValues::default(),
            payload: deterministic_payload(
                VIDEO_PAYLOAD_BODY_LEN,
                self.timing.payload_seed,
                timing.sequence_number,
                timing.rtp_timestamp,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyntheticTiming {
    frame_interval: Duration,
    timestamp_step: u32,
    payload_seed: u8,
    next_rtp_timestamp: u32,
    next_sequence_number: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PacketTiming {
    emitted_at: Duration,
    rtp_timestamp: u32,
    sequence_number: u16,
}

impl SyntheticTiming {
    const fn new(frame_interval: Duration, timestamp_step: u32, payload_seed: u8) -> Self {
        Self {
            frame_interval,
            timestamp_step,
            payload_seed,
            next_rtp_timestamp: 0,
            next_sequence_number: 0,
        }
    }

    fn next(&mut self, clock: &mut FakeClock) -> PacketTiming {
        let emitted_at = clock.advance(self.frame_interval);
        let sequence_number = self.next_sequence_number;
        self.next_sequence_number = self.next_sequence_number.wrapping_add(1);
        let rtp_timestamp = self.next_rtp_timestamp;
        self.next_rtp_timestamp = self.next_rtp_timestamp.wrapping_add(self.timestamp_step);
        PacketTiming {
            emitted_at,
            rtp_timestamp,
            sequence_number,
        }
    }
}

fn media_kind_for_stream_type(stream_type: StreamType) -> MediaKind {
    match stream_type {
        StreamType::Audio => MediaKind::Audio,
        StreamType::Camera | StreamType::Screen => MediaKind::Video,
    }
}

fn deterministic_payload(
    len: usize,
    seed: u8,
    sequence_number: u16,
    rtp_timestamp: u32,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(len);
    for byte in sequence_number
        .to_be_bytes()
        .into_iter()
        .chain(rtp_timestamp.to_be_bytes())
    {
        if payload.len() == len {
            return payload;
        }
        payload.push(byte);
    }
    while payload.len() < len {
        let next_byte = seed.wrapping_add(u8::try_from(payload.len()).unwrap_or(u8::MAX));
        payload.push(next_byte);
    }
    payload
}

fn frame_marking_value(temporal_layer_id: u8, keyframe: bool) -> u32 {
    let independent = if keyframe {
        frame_marking::INDEPENDENT_FRAME_MASK
    } else {
        0
    };
    let first_octet = frame_marking::START_OF_FRAME_MASK
        | frame_marking::END_OF_FRAME_MASK
        | independent
        | (temporal_layer_id & frame_marking::TEMPORAL_LAYER_ID_MASK);
    u32::from(first_octet) << 24
}

fn synthetic_opus_one_frame_packet(frame_body: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(frame_body.len() + 1);
    packet.push(SYNTHETIC_OPUS_ONE_FRAME_TOC);
    packet.extend_from_slice(frame_body);
    packet
}

fn synthetic_vp8_payload_with_long_picture_id(
    picture_id: u16,
    tl0_pic_idx: u8,
    temporal_layer_id: u8,
    keyframe: bool,
    body: &[u8],
) -> Option<Vec<u8>> {
    if temporal_layer_id > rtp::vp8::TEMPORAL_LAYER_ID_MASK {
        return None;
    }
    let picture_id = picture_id & rtp::vp8::LONG_PICTURE_ID_MASK;
    let mut payload = Vec::with_capacity(body.len() + 7);
    payload.push(rtp::vp8::X_BIT | rtp::vp8::S_BIT);
    payload.push(rtp::vp8::I_BIT | rtp::vp8::L_BIT | rtp::vp8::T_BIT);
    payload.push(rtp::vp8::LONG_PICTURE_ID_BIT | u8::try_from(picture_id >> 8).ok()?);
    payload.push(u8::try_from(picture_id & 0xff).ok()?);
    payload.push(tl0_pic_idx);
    payload.push(temporal_layer_id << 6);
    payload.push(if keyframe {
        0
    } else {
        rtp::vp8::INTERFRAME_BIT
    });
    payload.extend_from_slice(body);
    Some(payload)
}

fn synthetic_h264_single_nal_unit_idr_payload(nal_body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(nal_body.len() + 1);
    payload.push(rtp::h264::NAL_REF_IDC_HIGH | rtp::h264::NAL_UNIT_TYPE_IDR);
    payload.extend_from_slice(nal_body);
    payload
}

fn synthetic_h264_payload(nal_body: &[u8], idr: bool) -> Vec<u8> {
    if idr {
        return synthetic_h264_single_nal_unit_idr_payload(nal_body);
    }
    synthetic_h264_fua_idr_continuation_payload(nal_body)
}

fn synthetic_h264_fua_idr_continuation_payload(fragment_body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(fragment_body.len() + 2);
    payload.push(rtp::h264::NAL_REF_IDC_HIGH | rtp::h264::NAL_UNIT_TYPE_FU_A);
    payload.push(SYNTHETIC_H264_FUA_IDR_CONTINUATION_HEADER);
    payload.extend_from_slice(fragment_body);
    payload
}

#[cfg(test)]
fn synthetic_h264_stap_a_payload(nal_units: &[&[u8]]) -> Option<Vec<u8>> {
    let mut payload = Vec::new();
    payload.push(rtp::h264::NAL_REF_IDC_HIGH | rtp::h264::NAL_UNIT_TYPE_STAP_A);
    for nal_unit in nal_units {
        if nal_unit.is_empty() {
            return None;
        }
        let len = u16::try_from(nal_unit.len()).ok()?;
        payload.extend_from_slice(&len.to_be_bytes());
        payload.extend_from_slice(nal_unit);
    }
    Some(payload)
}

#[cfg(test)]
fn synthetic_h264_fua_idr_start_payload(fragment_body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(fragment_body.len() + 2);
    payload.push(rtp::h264::NAL_REF_IDC_HIGH | rtp::h264::NAL_UNIT_TYPE_FU_A);
    payload.push(rtp::h264::FU_START_BIT | rtp::h264::NAL_UNIT_TYPE_IDR);
    payload.extend_from_slice(fragment_body);
    payload
}

#[cfg(test)]
mod tests {
    use o_sfu_rfc::rtp::{h264, vp8};

    use super::*;

    #[test]
    fn synthetic_opus_one_frame_packet_uses_local_toc_byte() {
        let packet = synthetic_opus_one_frame_packet(&[0x11, 0x22, 0x33]);

        assert_eq!(packet.first().copied(), Some(SYNTHETIC_OPUS_ONE_FRAME_TOC));
        assert_eq!(packet.get(1..), Some([0x11, 0x22, 0x33].as_slice()));
        assert_eq!(rtp::opus::RTP_CLOCK_RATE_HZ / 50, AUDIO_TIMESTAMP_STEP);
    }

    #[test]
    fn synthetic_opus_audio_activity_builder_sets_extensions() {
        let mut clock = FakeClock::default();
        let mut stream = SyntheticOpusStream::with_audio_activity(-12, false);
        let packet = stream.next_packet(&mut clock);

        assert_eq!(packet.extension_values.audio_level, Some(-12));
        assert_eq!(packet.extension_values.voice_activity, Some(false));
    }

    #[test]
    fn synthetic_vp8_payload_round_trips_descriptor_fields() {
        let payload =
            synthetic_vp8_payload_with_long_picture_id(0x1234, 42, 2, true, &[0xaa, 0xbb]);
        assert!(payload.is_some());
        let Some(payload) = payload else {
            return;
        };

        let descriptor = vp8::payload_descriptor(&payload);
        assert!(descriptor.is_some());
        let Some(descriptor) = descriptor else {
            return;
        };

        assert_eq!(descriptor.picture_id(), Some(0x1234));
        assert_eq!(descriptor.tl0_pic_idx(), Some(42));
        assert!(vp8::payload_starts_keyframe(&payload));
    }

    #[test]
    fn synthetic_vp8_builder_can_start_with_interframe_then_keyframe() {
        let mut clock = FakeClock::default();
        let mut stream = SyntheticVp8Stream::with_next_keyframe(false);
        let interframe = stream.next_packet(&mut clock);
        let keyframe = stream.next_packet(&mut clock);

        assert!(!vp8::payload_starts_keyframe(&interframe.payload));
        assert!(vp8::payload_starts_keyframe(&keyframe.payload));
    }

    #[test]
    fn synthetic_h264_idr_payloads_are_detected() {
        let single = synthetic_h264_single_nal_unit_idr_payload(&[0x01, 0x02]);
        assert!(h264::payload_starts_idr(&single));

        let stap_a = synthetic_h264_stap_a_payload(&[single.as_slice()]);
        assert!(stap_a.is_some());
        let Some(stap_a) = stap_a else {
            return;
        };
        assert!(h264::payload_starts_idr(&stap_a));

        let fua = synthetic_h264_fua_idr_start_payload(&[0x03, 0x04]);
        assert!(h264::payload_starts_idr(&fua));
    }

    #[test]
    fn synthetic_h264_builder_can_start_without_idr_then_emit_idr() {
        let mut clock = FakeClock::default();
        let mut stream = SyntheticH264Stream::with_idr(false);
        let non_idr = stream.next_packet(&mut clock);
        let idr = stream.next_packet(&mut clock);

        assert!(!h264::payload_starts_idr(&non_idr.payload));
        assert!(h264::payload_starts_idr(&idr.payload));
    }
}
