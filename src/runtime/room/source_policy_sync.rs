//! Async synchronization bridge for room-owned video source policy.
//!
//! This file connects `Room` to the pure source-selection policy in
//! `state::video_policy`. Room state decides which source-domain selector
//! each receiver should use, while `SourcePolicyEffectPlan` applies the
//! resulting transport gates after the room lock is released.
//!
//! The observations consumed here are best-effort transport snapshots. They
//! guide quality policy, but they do not become authoritative room state until
//! the effect plan has applied the transport work and committed only the updates
//! that still match the live room routes.
//!
//! # Concurrency model
//!
//! This module must not hold a room lock across observability or media-port
//! awaits. It takes short state snapshots, builds a cold-path effect plan and
//! lets the effect layer revalidate connection and media handles before any
//! selector state is stored.

use super::{Room, effects::SourcePolicyEffectPlan};
use crate::runtime::transport_adapter::{ActiveSpeakerSource, MediaPort, ObservabilityPort};

impl Room {
    /// Refreshes source packet policy from live transport observability.
    ///
    /// Normal room transitions call this after publish, subscribe or user
    /// membership changes may have altered route pressure. If no observability
    /// port exists, the runtime has no active-speaker or receiver-bandwidth
    /// signal to consume, so the refresh is intentionally a no-op.
    pub(super) async fn sync_source_packet_selection_policy(
        &self,
        observability_port: Option<&impl ObservabilityPort>,
        media_port: &impl MediaPort,
    ) {
        let Some(observability_port) = observability_port else {
            return;
        };
        let active_speaker_sources = observability_port.active_speaker_source_snapshot().await;
        self.sync_source_packet_selection_policy_from_observations(
            &active_speaker_sources,
            observability_port,
            media_port,
        )
        .await;
    }

    /// Refreshes source packet policy from a caller-provided active-speaker snapshot.
    ///
    /// This variant exists so manager-level fanout and tests can reuse the same
    /// policy path after they already have an active-speaker observation. The
    /// method still asks the observability port for receiver bandwidth using
    /// the current transport users, because bandwidth estimates must be
    /// scoped to the users that are still attached to this room
    ///
    /// The state is read twice on purpose. The first read gathers transport
    /// user keys for the bandwidth query, then the lock is released before
    /// consulting observability. The second read builds the effect plan from
    /// the latest room state. Any change between the two snapshots is handled
    /// by the effect plan's stale-update checks.
    pub(super) async fn sync_source_packet_selection_policy_from_observations(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) {
        let session_keys = {
            let state = self.state.read().await;
            state
                .transport_user_entries()
                .into_iter()
                .map(|(user_id, connection_id)| self.transport_user_key(&user_id, connection_id))
                .collect::<Vec<_>>()
        };
        let receiver_bandwidth_snapshot =
            observability_port.receiver_bandwidth_snapshot(&session_keys);
        let effect_plan = {
            let state = self.state.read().await;
            SourcePolicyEffectPlan::from_state(
                &state,
                active_speaker_sources,
                &receiver_bandwidth_snapshot,
            )
        };
        if effect_plan.is_empty() {
            return;
        }
        effect_plan.execute(self, media_port).await;
    }
}
