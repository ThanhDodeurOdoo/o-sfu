//! Async synchronization bridge for room-owned video source policy.
//!
//! This file connects `Room` to the pure source-selection policy in
//! `room::source_policy`. Room state decides which source-domain selector
//! each receiver should use, while `SourcePolicyEffectPlan` applies the
//! resulting transport gates after the room lock is released.
//!
//! The observations consumed here are best-effort transport snapshots. They
//! guide quality policy, but they do not become authoritative room state until
//! the effect plan has applied the transport work and committed only the updates
//! that still match the live room routes.
//!
//! # Concurrency
//!
//! This module must not hold a room lock across media transport
//! awaits. It takes short state snapshots, builds a cold-path effect plan and
//! lets the effect layer revalidate connection and media handles before any
//! selector state is stored.

use super::SourcePolicyEffectPlan;
use crate::{
    RoomSpilloverMode,
    runtime::{
        media_transport::{ActiveSpeakerSource, MediaTransport},
        room::Room,
        sync::lock_unpoisoned,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    clippy::enum_variant_names,
    reason = "source-policy events intentionally name the changed room dimension at call sites"
)]
pub(in crate::runtime::room) enum SourcePolicyEvent {
    RouteGraphChanged,
    ReceiverIntentChanged,
    FanoutPressureChanged,
}

impl Room {
    pub(in crate::runtime::room) async fn handle_source_policy_event(
        &self,
        event: SourcePolicyEvent,
        media_transport: Option<&MediaTransport>,
    ) {
        match event {
            SourcePolicyEvent::RouteGraphChanged => {
                self.observe_source_fanout_pressure().await;
                if let Some(media_transport) = media_transport {
                    self.sync_source_packet_selection_policy(media_transport)
                        .await;
                }
            }
            SourcePolicyEvent::ReceiverIntentChanged => {
                if let Some(media_transport) = media_transport {
                    self.sync_source_packet_selection_policy(media_transport)
                        .await;
                }
            }
            SourcePolicyEvent::FanoutPressureChanged => {
                self.observe_source_fanout_pressure().await;
            }
        }
    }

    async fn observe_source_fanout_pressure(&self) {
        let RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) =
            self.room_worker_policy().spillover()
        else {
            return;
        };
        let policy = policy.parts();
        let pressured = self.state.read().await.source_fanout_pressure(
            policy.max_fanout_per_source,
            |connection_id| {
                self.placement_state
                    .media_worker_id_for_connection(connection_id)
            },
        );
        lock_unpoisoned(&self.load_triggered_placement).set_source_fanout_pressure(pressured);
    }

    /// Refreshes source packet policy from live transport observability.
    ///
    /// Normal room transitions call this after publish, subscribe or user
    /// membership changes may have altered route pressure.
    pub(in crate::runtime::room) async fn sync_source_packet_selection_policy(
        &self,
        media_transport: &MediaTransport,
    ) {
        let active_speaker_sources = media_transport.active_speaker_source_snapshot().await;
        self.sync_source_packet_selection_policy_from_observations(
            &active_speaker_sources,
            media_transport,
        )
        .await;
    }

    /// Refreshes source packet policy from a caller-provided active-speaker snapshot.
    ///
    /// This variant exists so manager-level fanout and tests can reuse the same
    /// policy path after they already have an active-speaker observation. The
    /// method still asks the same `MediaTransport` for receiver bandwidth using the
    /// current transport users, because bandwidth estimates must be
    /// scoped to the users that are still attached to this room
    ///
    /// The state is read twice on purpose. The first read gathers transport
    /// user keys for the bandwidth query, then the lock is released before
    /// consulting observability. The second read builds the effect plan from
    /// the latest room state. Any change between the two snapshots is handled
    /// by the effect plan's stale-update checks.
    pub(in crate::runtime::room) async fn sync_source_packet_selection_policy_from_observations(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
        media_transport: &MediaTransport,
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
            media_transport.receiver_bandwidth_snapshot(&session_keys);
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
        effect_plan.execute(self, media_transport).await;
    }
}
