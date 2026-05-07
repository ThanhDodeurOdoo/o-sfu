//! Media-node cache for room topology snapshots.
//!
//! This cache is the local degraded-mode boundary for already-owned rooms. It
//! stores the last accepted owner snapshot per room and rejects replayed or
//! stale updates. The cache is not an authority. It never assigns owners and it
//! cannot override the cluster room directory.
//!
//! The store is cold-path state. It is meant for admission, reconnect and
//! topology update handling, not RTP forwarding.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::{ClusterRoomId, OwnerNodeId, RoomOwnershipEpoch, TopologySnapshot};

/// Rejections produced by the topology snapshot cache.
///
/// Cache rejections protect a media node from acting on older control-plane
/// views. A stale owner or epoch should normally close or refuse the local
/// topology mutation. A missing snapshot means the node has no degraded-mode
/// basis for claiming ownership of that room.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TopologyCacheError {
    /// Snapshot epoch is older than the cached epoch or a validation request
    /// used an epoch different from the cached snapshot.
    #[error("topology snapshot has a stale ownership epoch")]
    StaleEpoch,
    /// Snapshot version did not move forward within the same epoch.
    #[error("topology snapshot has a stale version")]
    StaleVersion,
    /// Validation was attempted by a node that is not the cached owner.
    #[error("cached topology belongs to another owner")]
    StaleOwner,
    /// No snapshot exists for the requested room.
    #[error("topology snapshot is missing")]
    MissingSnapshot,
}

/// Last-known topology snapshots accepted by one media node.
///
/// The store contains pure map state and does not lock internally. Runtime code
/// should put it behind the service boundary that owns topology updates. This
/// keeps the cache easy to test and avoids leaking lock handling into ingress
/// or room orchestration.
#[derive(Debug, Default, Clone)]
pub struct CachedTopologyStore {
    snapshots: BTreeMap<ClusterRoomId, TopologySnapshot>,
}

impl CachedTopologyStore {
    /// Accepts a newer snapshot for a room.
    ///
    /// A higher epoch always replaces an older epoch. Within the same epoch the
    /// version must move forward. This makes replayed topology watch messages
    /// harmless and keeps owner failover from being overwritten by an older
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Returns a stale epoch or version error when `snapshot` is older than the
    /// cached snapshot for the same room.
    pub fn update(&mut self, snapshot: TopologySnapshot) -> Result<(), TopologyCacheError> {
        if let Some(current) = self.snapshots.get(&snapshot.room_id) {
            if snapshot.epoch < current.epoch {
                return Err(TopologyCacheError::StaleEpoch);
            }
            if snapshot.epoch == current.epoch && snapshot.version <= current.version {
                return Err(TopologyCacheError::StaleVersion);
            }
        }
        self.snapshots.insert(snapshot.room_id.clone(), snapshot);
        Ok(())
    }

    /// Returns the cached snapshot for a room.
    ///
    /// This is a local best-effort view. It is useful for diagnostics and
    /// degraded operation, but it is not the authoritative room directory.
    #[must_use]
    pub fn snapshot(&self, room_id: &ClusterRoomId) -> Option<&TopologySnapshot> {
        self.snapshots.get(room_id)
    }

    /// Checks whether a local operation is still fenced by the cached owner
    /// term.
    ///
    /// This method is for media-node self-protection during degraded operation.
    /// It confirms that the caller's owner and epoch match the last accepted
    /// snapshot before local room work continues.
    ///
    /// # Errors
    ///
    /// Returns a stale-owner, stale-epoch or missing-snapshot error when a local
    /// room operation is no longer fenced by the cached owner term.
    pub fn validate_owner(
        &self,
        room_id: &ClusterRoomId,
        owner: &OwnerNodeId,
        epoch: RoomOwnershipEpoch,
    ) -> Result<&TopologySnapshot, TopologyCacheError> {
        let Some(snapshot) = self.snapshots.get(room_id) else {
            return Err(TopologyCacheError::MissingSnapshot);
        };
        if &snapshot.owner != owner {
            return Err(TopologyCacheError::StaleOwner);
        }
        if snapshot.epoch != epoch {
            return Err(TopologyCacheError::StaleEpoch);
        }
        Ok(snapshot)
    }
}
