#![allow(
    dead_code,
    reason = "deterministic media fixtures are shared across the protocol integration and fake-stream scenarios"
)]

use std::time::Duration;

use o_sfu::signaling::shared::StreamType;
use o_sfu_router::MediaKind;

const AUDIO_FRAME_INTERVAL: Duration = Duration::from_millis(20);
const AUDIO_TIMESTAMP_STEP: u32 = 960;
const AUDIO_PAYLOAD_LEN: usize = 160;

const VIDEO_FRAME_INTERVAL: Duration = Duration::from_millis(33);
const VIDEO_TIMESTAMP_STEP: u32 = 2_970;
const VIDEO_PAYLOAD_LEN: usize = 1_200;

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
pub struct FakeMediaFrame {
    pub emitted_at: Duration,
    pub rtp_timestamp: u32,
    pub sequence_number: u16,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeMediaSource {
    stream_type: StreamType,
    media_kind: MediaKind,
    frame_interval: Duration,
    timestamp_step: u32,
    payload_len: usize,
    payload_seed: u8,
    next_rtp_timestamp: u32,
    next_sequence_number: u16,
}

impl FakeMediaSource {
    #[must_use]
    pub fn audio() -> Self {
        Self {
            stream_type: StreamType::Audio,
            media_kind: MediaKind::Audio,
            frame_interval: AUDIO_FRAME_INTERVAL,
            timestamp_step: AUDIO_TIMESTAMP_STEP,
            payload_len: AUDIO_PAYLOAD_LEN,
            payload_seed: 0x11,
            next_rtp_timestamp: 0,
            next_sequence_number: 0,
        }
    }

    #[must_use]
    pub fn camera() -> Self {
        Self {
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            frame_interval: VIDEO_FRAME_INTERVAL,
            timestamp_step: VIDEO_TIMESTAMP_STEP,
            payload_len: VIDEO_PAYLOAD_LEN,
            payload_seed: 0x41,
            next_rtp_timestamp: 0,
            next_sequence_number: 0,
        }
    }

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[must_use]
    pub fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub fn next_frame(&mut self, clock: &mut FakeClock) -> FakeMediaFrame {
        let emitted_at = clock.advance(self.frame_interval);
        let sequence_number = self.next_sequence_number;
        self.next_sequence_number = self.next_sequence_number.wrapping_add(1);
        let rtp_timestamp = self.next_rtp_timestamp;
        self.next_rtp_timestamp = self.next_rtp_timestamp.wrapping_add(self.timestamp_step);

        let mut payload = Vec::with_capacity(self.payload_len);
        payload.extend_from_slice(&sequence_number.to_be_bytes());
        payload.extend_from_slice(&rtp_timestamp.to_be_bytes());
        while payload.len() < self.payload_len {
            let next_byte = self
                .payload_seed
                .wrapping_add(u8::try_from(payload.len()).unwrap_or(u8::MAX));
            payload.push(next_byte);
        }

        FakeMediaFrame {
            emitted_at,
            rtp_timestamp,
            sequence_number,
            payload,
        }
    }
}
