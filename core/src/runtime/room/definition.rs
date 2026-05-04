//! Immutable room identity and runtime placement.
//!
//! A room has two identities:
//!
//! ```text
//! Odoo-facing identity        Runtime-local placement
//! issuer, uuid, key           instance, router, media worker
//! ```
//!
//! The first identity is visible at the HTTP and websocket edge. The second
//! identity is process-local and drives transport ownership, diagnostics and
//! room topology. Keeping them together in `RoomDefinition` gives the room
//! facade one immutable source of truth after creation.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

use uuid::Uuid;

use super::{LocalRoomRouterPlacements, RoomConfig, RoomRuntimeContext, RoomRuntimePolicy};
use crate::{
    RoomShardingPolicy, RuntimeFeatureFlags,
    runtime::{
        AvailableFeatures, ConnectionId, RoomInstanceId, UserId,
        media_transport::TransportSessionKey,
    },
};

#[derive(Debug, Clone)]
struct RoomIdentity {
    uuid: String,
    issuer: String,
    key: Option<String>,
}

impl RoomIdentity {
    fn new(issuer: String, key: Option<String>) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            issuer,
            key,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RoomDefinition {
    /// Live room instance id used by runtime diagnostics and transport keys.
    ///
    /// Recreating a room for the same issuer allocates a fresh instance id so
    /// stale transport work cannot be confused with the new room lifetime.
    instance_id: RoomInstanceId,
    /// Primary media worker selected for the room.
    ///
    /// Strict single-router rooms use this for all transport sessions.
    /// Spillover rooms use it as the fallback when a connection does not map
    /// onto one of the reserved local placements.
    media_worker_id: usize,
    /// Immutable local router placements reserved for this room.
    ///
    /// This vector is shared with `RoomTopology` through room construction.
    /// `transport_user_key` uses the same placement order so transport worker
    /// addressing and topology home-router placement cannot drift.
    local_routers: LocalRoomRouterPlacements,
    /// Policy used to interpret `local_routers` for transport placement.
    ///
    /// The policy is stored here because transport session keys are derived
    /// outside the topology lock. The room definition can answer that cold-path
    /// routing question without borrowing mutable room state.
    room_sharding_policy: RoomShardingPolicy,
    transport_worker_by_connection: Arc<Mutex<BTreeMap<ConnectionId, usize>>>,
    identity: RoomIdentity,
    config: RoomConfig,
    feature_flags: RuntimeFeatureFlags,
}

impl RoomDefinition {
    #[must_use]
    pub(crate) fn new(
        runtime_context: &RoomRuntimeContext,
        runtime_policy: &RoomRuntimePolicy,
        issuer: String,
        key: Option<String>,
        config: RoomConfig,
    ) -> Self {
        Self {
            instance_id: runtime_context.instance(),
            media_worker_id: runtime_context.media_worker(),
            local_routers: runtime_context.local_routers().clone(),
            room_sharding_policy: runtime_policy.room_sharding_policy,
            transport_worker_by_connection: Arc::new(Mutex::new(BTreeMap::new())),
            identity: RoomIdentity::new(issuer, key),
            config,
            feature_flags: runtime_policy.feature_flags,
        }
    }

    #[must_use]
    pub(crate) fn uuid(&self) -> &str {
        &self.identity.uuid
    }

    #[must_use]
    pub(crate) fn issuer(&self) -> &str {
        &self.identity.issuer
    }

    #[must_use]
    pub(crate) fn key(&self) -> Option<&str> {
        self.identity.key.as_deref()
    }

    #[must_use]
    pub(crate) fn available_features(&self) -> AvailableFeatures {
        AvailableFeatures {
            rtc: self.config.web_rtc_enabled,
            transcription: self.feature_flags.transcription,
            audio_recording: self.feature_flags.audio_recording,
            video_recording: self.feature_flags.video_recording,
        }
    }

    #[must_use]
    pub(crate) fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        TransportSessionKey::new(
            self.instance_id,
            self.media_worker_id_for_connection(connection_id),
            connection_id,
            user_id.clone(),
        )
    }

    pub(crate) fn register_transport_worker(
        &self,
        connection_id: ConnectionId,
        media_worker_id: usize,
    ) {
        self.transport_worker_by_connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(connection_id, media_worker_id);
    }

    pub(crate) fn unregister_transport_worker(&self, connection_id: ConnectionId) {
        self.transport_worker_by_connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&connection_id);
    }

    /// Resolve the media worker that owns one user's transport session.
    ///
    /// This mirrors `RoomTopology` home-router placement for local spillover.
    /// The input is the runtime connection id because reconnects should receive
    /// a fresh deterministic placement even when the same Odoo user id rejoins.
    ///
    /// The method is cold-path control-plane work. It is called while building
    /// transport commands and diagnostics, not from packet forwarding loops.
    pub(crate) fn media_worker_id_for_connection(&self, connection_id: ConnectionId) -> usize {
        if let Some(media_worker_id) = self
            .transport_worker_by_connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&connection_id)
            .copied()
        {
            return media_worker_id;
        }
        let placement_count = self
            .room_sharding_policy
            .allowed_local_router_count(self.local_routers.len());
        let placement_index =
            usize::try_from(connection_id.as_u64()).unwrap_or(0) % placement_count;
        self.local_routers
            .get(placement_index)
            .map_or(self.media_worker_id, |placement| placement.media_worker)
    }

    #[must_use]
    pub(crate) const fn web_rtc_enabled(&self) -> bool {
        self.config.web_rtc_enabled
    }

    #[must_use]
    pub(crate) const fn recording_enabled(&self) -> bool {
        self.config.recording_address.is_some()
    }

    #[must_use]
    pub(crate) const fn feature_flags(&self) -> RuntimeFeatureFlags {
        self.feature_flags
    }

    #[must_use]
    pub(crate) const fn media_worker_id(&self) -> usize {
        self.media_worker_id
    }

    #[must_use]
    pub(crate) const fn instance_id(&self) -> RoomInstanceId {
        self.instance_id
    }
}
