//! Packet-level active-speaker observation state.
//!
//! The RTC adapter can observe RFC 6464 audio-level and VAD metadata while it
//! routes packets. This module turns those packet facts into a bounded
//! transport-owned active-speaker signal and a packet gate for silent audio
//! sources. Channel policy later maps the active audio sources into room layout
//! intent.

use std::time::{Duration, Instant};

use super::packet_gate::PacketLayerGate;

const ACTIVE_SPEAKER_HOLD_WINDOW: Duration = Duration::from_millis(250);
const ACTIVE_SPEAKER_AUDIO_LEVEL_THRESHOLD_DBOV: i8 = -48;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct SourceAudioPolicyState {
    active_until: Option<Instant>,
    last_spoke_at: Option<Instant>,
    packet_gate: PacketLayerGate,
}

impl SourceAudioPolicyState {
    pub(super) fn observe_packet(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) {
        let speech_detected = match voice_activity {
            Some(true) => true,
            Some(false) => false,
            None => audio_level_dbov
                .is_some_and(|level| level >= ACTIVE_SPEAKER_AUDIO_LEVEL_THRESHOLD_DBOV),
        };
        if speech_detected {
            self.active_until = Some(now + ACTIVE_SPEAKER_HOLD_WINDOW);
            self.last_spoke_at = Some(now);
            self.packet_gate = PacketLayerGate::Open;
            return;
        }
        if self.active_until.is_some_and(|deadline| now < deadline) {
            self.packet_gate = PacketLayerGate::Open;
            return;
        }
        self.active_until = None;
        self.packet_gate = PacketLayerGate::Block;
    }

    pub(super) fn packet_gate(&self) -> PacketLayerGate {
        self.packet_gate.clone()
    }

    pub(super) fn active_speaker_observed_at(&self, now: Instant) -> Option<Instant> {
        self.last_spoke_at
            .filter(|_| self.active_until.is_some_and(|deadline| now < deadline))
    }

    pub(super) fn active_deadline_after(&self, now: Instant) -> Option<Instant> {
        self.active_until.filter(|deadline| *deadline > now)
    }

    pub(super) fn expired_at(&self, now: Instant) -> bool {
        self.active_until.is_some_and(|deadline| deadline <= now)
    }
}
