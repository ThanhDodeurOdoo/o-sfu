//! Worker-local route-control state.
//!
//! `RouteControlState` composes source-local packet gates, relay-target gates,
//! active-speaker packet policy, and keyframe coalescing. It remains
//! transport-facing: callers provide transport media ids and packet metadata,
//! and room policy stays outside this module.

use std::{cmp::Reverse, collections::BTreeMap, time::Instant};

use str0m::media::Rid;
use tracing::debug;

use super::{
    active_speaker::SourceAudioPolicyState,
    keyframe::{KeyframeRequestDecision, KeyframeRequestWindow},
    packet_gate::{
        PacketLayerGate, PacketLayerMetadata, PacketRouteDecision, aggregate_packet_gates,
        intersect_packet_gates,
    },
};
use crate::runtime::{
    media_transport::{ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportMediaId},
    rtc_engine::{packet_loop::time::PacketLoopTime, relay_registry::RelayTargetId},
};

#[derive(Debug, Default)]
pub(in crate::runtime::rtc_engine) struct RouteControlState {
    sources: BTreeMap<TransportMediaId, SourceRouteControl>,
}

impl RouteControlState {
    #[cfg(test)]
    pub(in crate::runtime::rtc_engine) fn decide_keyframe_request(
        &mut self,
        source_transport_media_id: TransportMediaId,
        now: PacketLoopTime,
    ) -> KeyframeRequestDecision {
        self.decide_keyframe_request_for_rid(source_transport_media_id, None, now)
    }

    pub(in crate::runtime::rtc_engine) fn decide_keyframe_request_for_rid(
        &mut self,
        source_transport_media_id: TransportMediaId,
        rid: Option<Rid>,
        now: PacketLoopTime,
    ) -> KeyframeRequestDecision {
        let source_control = self.sources.entry(source_transport_media_id).or_default();
        source_control.decide_keyframe_request(rid, now)
    }

    pub(in crate::runtime::rtc_engine) fn decide_packet_route(
        &self,
        source_transport_media_id: TransportMediaId,
        metadata: PacketLayerMetadata,
    ) -> PacketRouteDecision {
        let Some(source_control) = self.sources.get(&source_transport_media_id) else {
            return PacketRouteDecision::Forward;
        };
        match source_control
            .effective_packet_gate()
            .unwrap_or(PacketLayerGate::Open)
        {
            gate if gate.permits(metadata) => PacketRouteDecision::Forward,
            _gate => PacketRouteDecision::Drop,
        }
    }

    pub(in crate::runtime::rtc_engine) fn set_local_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        let should_remove = if let Some(source_control) =
            self.sources.get_mut(&source_transport_media_id)
        {
            source_control.local_packet_gate = packet_gate;
            source_control.is_empty()
        } else {
            let Some(packet_gate) = packet_gate else {
                return;
            };
            self.sources.insert(
                source_transport_media_id,
                SourceRouteControl {
                    local_packet_gate: Some(packet_gate),
                    ..Default::default()
                },
            );
            debug!(
                ?source_transport_media_id,
                effective_packet_gate = ?self.effective_packet_gate_for_log(source_transport_media_id),
                "updated source packet gate"
            );
            return;
        };
        if should_remove {
            self.sources.remove(&source_transport_media_id);
        }
        debug!(
            ?source_transport_media_id,
            effective_packet_gate = ?self.effective_packet_gate_for_log(source_transport_media_id),
            "updated source packet gate"
        );
    }

    pub(in crate::runtime::rtc_engine) fn observe_audio_activity(
        &mut self,
        source_transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        let should_remove =
            if let Some(source_control) = self.sources.get_mut(&source_transport_media_id) {
                if !source_control.observe_audio_activity(voice_activity, audio_level_dbov, now) {
                    return false;
                }
                source_control.is_empty()
            } else {
                if voice_activity.is_none() && audio_level_dbov.is_none() {
                    return false;
                }
                let mut source_control = SourceRouteControl::default();
                if !source_control.observe_audio_activity(voice_activity, audio_level_dbov, now) {
                    return false;
                }
                if source_control.is_empty() {
                    return false;
                }
                self.sources
                    .insert(source_transport_media_id, source_control);
                return true;
            };
        if should_remove {
            self.sources.remove(&source_transport_media_id);
        }
        true
    }

    pub(in crate::runtime::rtc_engine) fn set_relay_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    ) {
        self.sources
            .entry(source_transport_media_id)
            .or_default()
            .relay_packet_gates
            .insert(target_id, packet_gate);
    }

    pub(in crate::runtime::rtc_engine) fn relay_packet_gate(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> Option<&PacketLayerGate> {
        self.sources
            .get(&source_transport_media_id)
            .and_then(|source_control| source_control.relay_packet_gates.get(&target_id))
    }

    pub(in crate::runtime::rtc_engine) fn forget_relay_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) {
        let Some(source_control) = self.sources.get_mut(&source_transport_media_id) else {
            return;
        };
        source_control.relay_packet_gates.remove(&target_id);
        if source_control.is_empty() {
            self.sources.remove(&source_transport_media_id);
        }
    }

    pub(in crate::runtime::rtc_engine) fn forget_source(
        &mut self,
        source_transport_media_id: TransportMediaId,
    ) {
        self.sources.remove(&source_transport_media_id);
    }

    pub(in crate::runtime::rtc_engine) fn retain_sources<F>(&mut self, mut keep: F)
    where
        F: FnMut(&TransportMediaId) -> bool,
    {
        self.sources
            .retain(|source_transport_media_id, _source_control| keep(source_transport_media_id));
    }

    pub(in crate::runtime::rtc_engine) fn active_speaker_sources(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSource> {
        let mut sources = self
            .sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control.active_speaker_source(*source_transport_media_id, now)
            })
            .collect::<Vec<_>>();
        sources.sort_by_key(|source| {
            (
                Reverse(source.observed_at()),
                source.transport_media_id().as_u64(),
            )
        });
        sources
    }

    pub(in crate::runtime::rtc_engine) fn active_speaker_diagnostics(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control.active_speaker_diagnostic(*source_transport_media_id, now)
            })
            .collect()
    }

    pub(in crate::runtime::rtc_engine) fn next_active_speaker_deadline(
        &self,
        now: Instant,
    ) -> Option<Instant> {
        self.sources
            .values()
            .filter_map(|source_control| {
                source_control
                    .source_audio_policy
                    .as_ref()
                    .and_then(|source_audio_policy| source_audio_policy.active_deadline_after(now))
            })
            .min()
    }

    pub(in crate::runtime::rtc_engine) fn expired_active_speaker_source_ids(
        &self,
        now: Instant,
    ) -> Vec<TransportMediaId> {
        self.sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control
                    .source_audio_policy
                    .as_ref()
                    .is_some_and(|source_audio_policy| source_audio_policy.expired_at(now))
                    .then_some(*source_transport_media_id)
            })
            .collect()
    }

    #[cfg(test)]
    pub(in crate::runtime::rtc_engine) fn set_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        self.set_local_packet_gate(source_transport_media_id, Some(packet_gate));
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::rtc_engine) fn effective_packet_gate(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.sources
            .get(&source_transport_media_id)
            .and_then(SourceRouteControl::effective_packet_gate)
    }

    fn effective_packet_gate_for_log(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.sources
            .get(&source_transport_media_id)
            .and_then(SourceRouteControl::effective_packet_gate)
    }
}

#[derive(Debug, Default)]
struct SourceRouteControl {
    keyframe_requests: Vec<KeyframeRequestState>,
    source_audio_policy: Option<SourceAudioPolicyState>,
    local_packet_gate: Option<PacketLayerGate>,
    relay_packet_gates: BTreeMap<RelayTargetId, PacketLayerGate>,
}

#[derive(Debug, Clone, Copy)]
struct KeyframeRequestState {
    rid: Option<Rid>,
    window: KeyframeRequestWindow,
}

impl SourceRouteControl {
    fn decide_keyframe_request(
        &mut self,
        rid: Option<Rid>,
        now: PacketLoopTime,
    ) -> KeyframeRequestDecision {
        let Some(request_state) = self
            .keyframe_requests
            .iter_mut()
            .find(|request_state| request_state.rid == rid)
        else {
            self.keyframe_requests.push(KeyframeRequestState {
                rid,
                window: KeyframeRequestWindow::new(now),
            });
            return KeyframeRequestDecision::Forward;
        };
        if request_state.window.is_open(now) {
            return KeyframeRequestDecision::Absorb;
        }
        request_state.window = KeyframeRequestWindow::new(now);
        KeyframeRequestDecision::Forward
    }

    fn active_speaker_source(
        &self,
        source_transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSource> {
        self.source_audio_policy
            .as_ref()
            .and_then(|source_audio_policy| source_audio_policy.active_speaker_observed_at(now))
            .map(|observed_at| ActiveSpeakerSource::new(source_transport_media_id, observed_at))
    }

    fn active_speaker_diagnostic(
        &self,
        source_transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSourceDiagnostic> {
        self.source_audio_policy
            .as_ref()
            .map(|source_audio_policy| {
                source_audio_policy.diagnostic(source_transport_media_id, now)
            })
    }

    fn observe_audio_activity(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        let previous = self.source_audio_policy.clone();
        let Some(mut source_policy) = self.source_audio_policy.take().or_else(|| {
            (voice_activity.is_some() || audio_level_dbov.is_some())
                .then(SourceAudioPolicyState::default)
        }) else {
            return false;
        };
        source_policy.observe_packet(voice_activity, audio_level_dbov, now);
        self.source_audio_policy = Some(source_policy);
        self.source_audio_policy != previous
    }

    fn effective_packet_gate(&self) -> Option<PacketLayerGate> {
        intersect_packet_gates(
            aggregate_packet_gates(
                self.local_packet_gate
                    .iter()
                    .chain(self.relay_packet_gates.values()),
            ),
            self.source_audio_policy
                .as_ref()
                .map(SourceAudioPolicyState::packet_gate),
        )
    }

    fn is_empty(&self) -> bool {
        self.keyframe_requests.is_empty()
            && self.source_audio_policy.is_none()
            && self.local_packet_gate.is_none()
            && self.relay_packet_gates.is_empty()
    }
}
