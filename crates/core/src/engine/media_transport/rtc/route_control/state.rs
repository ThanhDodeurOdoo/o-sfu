//! worker-local route-control state for source packet forwarding
//!
//! route control sits between worker-local media routes and packet planning
//! callers address it with transport media ids plus packet metadata
//! room policy reaches this layer only after it has been projected into
//! [`PacketLayerGate`] values
//!
//! each source entry is sparse and exists only while some transport-side
//! control state is installed
//! packet-level audio policy, local route gates and relay-target gates are kept
//! together because the packet loop needs one source-wide answer before it fans
//! out to destinations
//!
//! packet gating is composed in two stages
//! local and relay gates are unioned first so a source packet survives when any
//! downstream route can still use it
//! active-speaker audio policy is intersected afterward because a silent source
//! policy must be able to block every downstream route

use std::{
    cmp::Reverse,
    collections::{BTreeMap, btree_map::Entry},
    time::Instant,
};

use tracing::debug;

use super::{
    active_speaker::SourceAudioPolicyState,
    packet_gate::{
        PacketLayerGate, PacketLayerMetadata, PacketRouteDecision, aggregate_packet_gates,
        intersect_packet_gates,
    },
};
use crate::engine::media_transport::{
    ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportMediaId,
    rtc::relay_registry::RelayTargetId,
};

/// worker-local route-control state keyed by source transport media id
///
/// this state is not room authority
/// it stores packet-path facts that let the worker make cheap forwarding and
/// feedback decisions while room state remains the owner of source policy,
/// receiver selection and route lifecycle
#[derive(Debug, Default)]
pub(in crate::engine::media_transport::rtc) struct RouteControlState {
    /// sparse per-source control entries
    ///
    /// entries may be created by packet gates or audio observations
    sources: BTreeMap<TransportMediaId, SourceRouteControl>,
}

impl RouteControlState {
    /// decides whether the packet may enter destination planning for a source
    ///
    /// missing source state means no route-control restriction is installed
    /// the packet is then forwarded to downstream planning where relay and local
    /// destination gates still get their own checks
    pub fn decide_packet_route(
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

    /// installs or clears the source-wide gate derived from local destinations
    ///
    /// `None` means no local route gate is installed
    /// it is different from [`PacketLayerGate::Block`], which is an explicit
    /// source-level deny
    /// callers refresh this value after local route destinations are added,
    /// removed or retargeted
    pub fn set_local_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        let should_remove = if let Some(source_control) =
            self.sources.get_mut(&source_transport_media_id)
        {
            source_control.set_local_packet_gate(packet_gate);
            source_control.is_empty()
        } else {
            let Some(packet_gate) = packet_gate else {
                return;
            };
            let mut source_control = SourceRouteControl::default();
            source_control.set_local_packet_gate(Some(packet_gate));
            self.sources
                .insert(source_transport_media_id, source_control);
            debug!(
                ?source_transport_media_id,
                effective_packet_gate = ?self.effective_packet_gate_for_source(source_transport_media_id),
                "updated source packet gate"
            );
            return;
        };
        if should_remove {
            self.sources.remove(&source_transport_media_id);
        }
        debug!(
            ?source_transport_media_id,
            effective_packet_gate = ?self.effective_packet_gate_for_source(source_transport_media_id),
            "updated source packet gate"
        );
    }

    /// observes packet-level audio activity for active-speaker policy
    ///
    /// the return value tells the packet loop whether room-facing active-speaker
    /// state may have changed and needs a policy refresh
    /// unknown sources without audio metadata are ignored so empty control
    /// entries are not created for ordinary packets
    pub fn observe_audio_activity(
        &mut self,
        source_transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        if let Entry::Vacant(entry) = self.sources.entry(source_transport_media_id) {
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
            entry.insert(source_control);
            return true;
        }
        let previous_active_speakers = self.room_policy_ranked_active_speaker_sources(now);
        let previous_packet_gate = self.effective_packet_gate_for_source(source_transport_media_id);
        let Some(source_control) = self.sources.get_mut(&source_transport_media_id) else {
            return false;
        };
        if !source_control.observe_audio_activity(voice_activity, audio_level_dbov, now) {
            return false;
        }
        let should_remove = source_control.is_empty();
        if should_remove {
            self.sources.remove(&source_transport_media_id);
        }
        previous_packet_gate != self.effective_packet_gate_for_source(source_transport_media_id)
            || !same_active_speaker_order(
                &previous_active_speakers,
                &self.room_policy_ranked_active_speaker_sources(now),
            )
    }

    /// installs the packet gate requested by one relay target
    ///
    /// relay gates contribute to the source-wide union so the source worker
    /// keeps packets that at least one remote target still needs
    /// the forwarding planner also checks the same target gate later before
    /// enqueueing that relay destination
    pub fn set_relay_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    ) {
        self.sources
            .entry(source_transport_media_id)
            .or_default()
            .set_relay_packet_gate(target_id, packet_gate);
    }

    /// returns the relay-specific packet gate for destination planning
    ///
    /// `None` means the relay target has no extra layer restriction beyond the
    /// source-wide gate
    pub fn relay_packet_gate(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> Option<&PacketLayerGate> {
        self.sources
            .get(&source_transport_media_id)
            .and_then(|source_control| source_control.relay_packet_gates.get(&target_id))
    }

    /// removes the packet gate owned by one relay target
    ///
    /// relay cleanup uses this when a target is released while the source itself
    /// may still have local routes or audio policy
    pub fn forget_relay_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) {
        let Some(source_control) = self.sources.get_mut(&source_transport_media_id) else {
            return;
        };
        source_control.forget_relay_packet_gate(target_id);
        if source_control.is_empty() {
            self.sources.remove(&source_transport_media_id);
        }
    }

    /// drops all route-control state for a source
    ///
    /// source teardown must use this instead of clearing individual gates so
    /// audio policy does not outlive the media handle it describes
    pub fn forget_source(&mut self, source_transport_media_id: TransportMediaId) {
        self.sources.remove(&source_transport_media_id);
    }

    /// retains only sources accepted by the caller's registry snapshot
    ///
    /// this is used by media-registry reconciliation to discard control state
    /// for sources that no longer have live media ownership
    pub fn retain_sources<F>(&mut self, mut keep: F)
    where
        F: FnMut(&TransportMediaId) -> bool,
    {
        self.sources
            .retain(|source_transport_media_id, _source_control| keep(source_transport_media_id));
    }

    /// returns active-speaker sources currently inside their hold window
    ///
    /// results are ordered by most recent speech observation first
    /// transport media id is used as a deterministic tie-breaker so transport
    /// snapshots stay stable when packets share the same timestamp
    pub fn active_speaker_sources(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        let mut sources = self.active_speaker_sources_unsorted(now);
        sources.sort_unstable_by_key(|source| {
            (
                Reverse(source.observed_at()),
                source.transport_media_id().as_u64(),
            )
        });
        sources
    }

    fn active_speaker_sources_unsorted(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        self.sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control.active_speaker_source(*source_transport_media_id, now)
            })
            .collect()
    }

    fn room_policy_ranked_active_speaker_sources(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        let mut sources = self.active_speaker_sources_unsorted(now);
        sources.sort_unstable_by_key(|source| {
            (
                Reverse(source.observed_at()),
                Reverse(source.last_audio_level_dbov().unwrap_or(i8::MIN)),
                source.transport_media_id().as_u64(),
            )
        });
        sources
    }

    /// returns audio-policy diagnostics for sources with observed audio policy
    ///
    /// diagnostics include blocked and recently expired states because those are
    /// useful for debugging why audio did not affect active-speaker policy
    pub fn active_speaker_diagnostics(&self, now: Instant) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control.active_speaker_diagnostic(*source_transport_media_id, now)
            })
            .collect()
    }

    /// returns the next deadline where an active-speaker source may expire
    ///
    /// the runtime uses this as a timer hint
    /// returning `None` means there is no active audio hold window to wake for
    pub fn next_active_speaker_deadline(&self, now: Instant) -> Option<Instant> {
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

    /// returns sources whose active-speaker hold window has expired
    ///
    /// callers use these ids to trigger room policy refresh after timer wakeup
    /// the audio diagnostic state is kept so later diagnostics can still explain
    /// why the source became inactive
    pub fn expired_active_speaker_source_ids(&self, now: Instant) -> Vec<TransportMediaId> {
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

    /// test helper that installs a local packet gate directly
    #[cfg(test)]
    pub fn set_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        self.set_local_packet_gate(source_transport_media_id, Some(packet_gate));
    }

    /// returns the composed source-wide packet gate for tests and diagnostics
    ///
    /// `None` means no gate is installed
    /// [`PacketLayerGate::Open`] means an explicit allow-all gate exists
    #[cfg(any(test, feature = "testing-transport", feature = "internal-benchmarks"))]
    pub fn effective_packet_gate(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.effective_packet_gate_for_source(source_transport_media_id)
    }

    /// computes the gate installed for one route-control source
    fn effective_packet_gate_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.sources
            .get(&source_transport_media_id)
            .and_then(SourceRouteControl::effective_packet_gate)
    }
}

fn same_active_speaker_order(left: &[ActiveSpeakerSource], right: &[ActiveSpeakerSource]) -> bool {
    left.iter()
        .map(|source| source.transport_media_id())
        .eq(right.iter().map(|source| source.transport_media_id()))
}

/// all route-control state known for one source media id
///
/// the source entry composes independent concerns that have to agree before a
/// packet reaches fanout:
///
/// * local and relay gates preserve downstream selected-layer needs
/// * audio policy can block the source when packet metadata says it is inactive
#[derive(Debug, Default)]
struct SourceRouteControl {
    /// packet-derived audio policy for active-speaker state and audio gating
    source_audio_policy: Option<SourceAudioPolicyState>,
    /// source gate aggregated from local consumer destinations
    local_packet_gate: Option<PacketLayerGate>,
    /// target-specific gates installed for active relay targets
    relay_packet_gates: BTreeMap<RelayTargetId, PacketLayerGate>,
    /// cached source-wide gate used before destination planning
    effective_packet_gate: Option<PacketLayerGate>,
}

impl SourceRouteControl {
    /// projects audio policy into the active-speaker source list
    fn active_speaker_source(
        &self,
        source_transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSource> {
        self.source_audio_policy
            .as_ref()
            .and_then(|source_audio_policy| {
                source_audio_policy.active_speaker_source(source_transport_media_id, now)
            })
    }

    /// builds diagnostics for the source audio policy when one exists
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

    /// updates packet-level audio policy and reports whether it changed
    ///
    /// policy is created lazily only after a packet exposes voice-activity or
    /// audio-level metadata
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
        let changed = self.source_audio_policy != previous;
        if changed {
            self.recompute_effective_packet_gate();
        }
        changed
    }

    /// replaces the source gate derived from local destinations
    fn set_local_packet_gate(&mut self, packet_gate: Option<PacketLayerGate>) {
        self.local_packet_gate = packet_gate;
        self.recompute_effective_packet_gate();
    }

    /// replaces the packet gate requested by one relay target
    fn set_relay_packet_gate(&mut self, target_id: RelayTargetId, packet_gate: PacketLayerGate) {
        self.relay_packet_gates.insert(target_id, packet_gate);
        self.recompute_effective_packet_gate();
    }

    /// removes the packet gate requested by one relay target
    fn forget_relay_packet_gate(&mut self, target_id: RelayTargetId) {
        self.relay_packet_gates.remove(&target_id);
        self.recompute_effective_packet_gate();
    }

    /// computes the source-wide packet gate used by early packet filtering
    ///
    /// local and relay gates are unioned because source filtering must preserve
    /// every downstream route that may need the packet
    /// the packet-level audio policy is then intersected so inactive audio can
    /// block all downstream fanout
    fn effective_packet_gate(&self) -> Option<PacketLayerGate> {
        self.effective_packet_gate
    }

    /// refreshes the cached packet gate after a source policy mutation
    fn recompute_effective_packet_gate(&mut self) {
        self.effective_packet_gate = self.compute_effective_packet_gate();
    }

    /// computes the source-wide packet gate from source policy state
    fn compute_effective_packet_gate(&self) -> Option<PacketLayerGate> {
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

    /// reports whether the source entry can be pruned from route-control state
    fn is_empty(&self) -> bool {
        self.source_audio_policy.is_none()
            && self.local_packet_gate.is_none()
            && self.relay_packet_gates.is_empty()
    }
}
