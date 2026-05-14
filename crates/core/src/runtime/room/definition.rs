//! Room identity and dynamic runtime placement.
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
//! room topology. The room is created before a media worker is selected, so
//! `RoomDefinition` records the worker placement resolved for each committed
//! connection.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

use uuid::Uuid;

use super::{
    LocalRoomRouterPlacements, LocalRouterRuntimeContext, RoomConfig, RoomRuntimeContext,
    RoomRuntimePolicy,
};
use crate::{
    RoomShardingPolicy, RuntimeFeatureFlags,
    runtime::{
        AvailableFeatures, ConnectionId, RoomInstanceId, UserId,
        media_transport::TransportSessionKey,
    },
};

/// Central gate for exposing recording as a production room capability.
///
/// This must become true only when accepted recording controls can produce the
/// promised persistent artifact or handoff.
const fn persistent_recording_backend_available() -> bool {
    false
}

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
    /// Media worker currently assigned to the primary router.
    ///
    /// Empty rooms keep the creation placeholder. The first placed session
    /// updates this value when it lands on the primary router.
    media_worker_id: Arc<Mutex<usize>>,
    /// Local router placements assigned to this room.
    ///
    /// The room starts with only its primary router. Spillover placements are
    /// inserted after the placement planner selects a worker for a session.
    local_routers: Arc<Mutex<LocalRoomRouterPlacements>>,
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
            media_worker_id: Arc::new(Mutex::new(runtime_context.media_worker())),
            local_routers: Arc::new(Mutex::new(runtime_context.local_routers().clone())),
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
        let recording_available = self.recording_available();
        AvailableFeatures {
            rtc: self.config.web_rtc_enabled,
            transcription: recording_available && self.feature_flags.transcription,
            audio_recording: recording_available && self.feature_flags.audio_recording,
            video_recording: recording_available && self.feature_flags.video_recording,
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

    pub(crate) fn register_transport_placement(
        &self,
        connection_id: ConnectionId,
        placement: LocalRouterRuntimeContext,
    ) {
        let is_primary = {
            let mut local_routers = self
                .local_routers
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            local_routers.upsert(placement);
            local_routers.primary().router == placement.router
        };
        if is_primary {
            *self
                .media_worker_id
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = placement.media_worker;
        }
        self.transport_worker_by_connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(connection_id, placement.media_worker);
    }

    pub(crate) fn unregister_transport_worker(&self, connection_id: ConnectionId) {
        self.transport_worker_by_connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&connection_id);
    }

    /// Resolve the media worker that owns one committed connection.
    ///
    /// Dynamic placement has no deterministic fallback from the connection id.
    /// Live transport keys must therefore use the committed mapping recorded
    /// during join finalization. If the mapping has already been removed, the
    /// primary worker is returned only as a stale-key fallback.
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
        self.media_worker_id()
    }

    #[must_use]
    pub(crate) const fn web_rtc_enabled(&self) -> bool {
        self.config.web_rtc_enabled
    }

    #[must_use]
    pub(crate) const fn recording_available(&self) -> bool {
        if self.config.recording_address.is_none() {
            return false;
        }
        persistent_recording_backend_available()
    }

    #[must_use]
    pub(crate) const fn feature_flags(&self) -> RuntimeFeatureFlags {
        self.feature_flags
    }

    #[must_use]
    pub(crate) fn room_sharding_policy(&self) -> RoomShardingPolicy {
        self.room_sharding_policy
    }

    pub(crate) fn media_worker_id(&self) -> usize {
        *self
            .media_worker_id
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    #[must_use]
    pub(crate) const fn instance_id(&self) -> RoomInstanceId {
        self.instance_id
    }
}
