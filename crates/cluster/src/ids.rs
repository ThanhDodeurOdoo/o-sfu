//! Strong identity and version types for cluster ownership.
//!
//! These wrappers prevent the control plane from mixing identifiers that are
//! all strings or counters at the wire boundary but mean different things in
//! the domain. They are small, serializable and cheap to clone
//! because they live on the control path rather than the media hot path.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Rejection returned when a cluster identity wrapper would otherwise contain
/// no usable value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdentifierError {
    /// Empty or whitespace-only input is rejected at construction time so map
    /// keys and serialized DTOs never carry ambiguous blank identities.
    #[error("cluster identifier must not be empty")]
    Empty,
}

/// Stable identity of one cluster node.
///
/// A node id names a process-level participant in the cluster control plane.
/// It does not imply the node is currently healthy, leased or allowed to own
/// rooms. Those facts belong to [`crate::NodeAdvertisement`] and
/// [`crate::ClusterNodeRegistry`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    /// Builds a node id from operator or control-plane input.
    ///
    /// The value is preserved as provided after validation. The constructor
    /// only rejects blank values. It does not normalize case or impose a naming
    /// scheme because deployments may use hostnames, UUIDs or scheduler ids.
    ///
    /// # Errors
    ///
    /// Returns [`IdentifierError::Empty`] when `value` is empty after trimming.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        non_empty(value).map(Self)
    }

    /// Returns the stored node id exactly as accepted by [`Self::new`].
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Node identity in the specific role of room owner.
///
/// This wrapper keeps assignment and validation APIs explicit. A caller cannot
/// accidentally pass an arbitrary [`NodeId`] where the API expects the current
/// authority for a room.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OwnerNodeId(NodeId);

impl OwnerNodeId {
    /// Marks an existing node id as the owner identity carried by room
    /// assignments, resolutions and topology snapshots.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self(node_id)
    }

    /// Returns the underlying node id used to look up the owner advertisement.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.0
    }
}

impl From<NodeId> for OwnerNodeId {
    fn from(value: NodeId) -> Self {
        Self::new(value)
    }
}

/// Region label advertised by a node.
///
/// Region is metadata for placement policy. The current scheduler does not use
/// it yet, but keeping it in the registration contract prevents topology work
/// from inventing a second node identity shape.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeRegion(String);

impl NodeRegion {
    /// Builds a region label from deployment input.
    ///
    /// The control-plane model treats the value as opaque. Geographic meaning,
    /// failure-domain grouping and scheduler affinity stay above this type.
    ///
    /// # Errors
    ///
    /// Returns [`IdentifierError::Empty`] when `value` is empty after trimming.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        non_empty(value).map(Self)
    }

    /// Returns the region label exactly as accepted by [`Self::new`].
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Cluster-facing room identity.
///
/// This is the room key used by the distributed ownership directory. It is
/// separate from process-local runtime ids and from media router
/// ids. The media server should translate compatibility room strings into this
/// type at the control-plane boundary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClusterRoomId(String);

impl ClusterRoomId {
    /// Builds a cluster room id from an already authenticated room reference.
    ///
    /// # Errors
    ///
    /// Returns [`IdentifierError::Empty`] when `value` is empty after trimming.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        non_empty(value).map(Self)
    }

    /// Returns the room id exactly as accepted by [`Self::new`].
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Monotonic freshness term for a node lease.
///
/// A renewed lease must move to a strictly greater term. The term is a fencing
/// value, not a wall-clock timestamp. Real expiry policy belongs to the
/// production control-plane storage and scheduler layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeLeaseTerm(u64);

impl NodeLeaseTerm {
    /// First usable lease term for a newly advertised node.
    pub const INITIAL: Self = Self(1);

    /// Wraps an already chosen lease term.
    ///
    /// This constructor does not reject zero because tests and storage adapters
    /// may need to model externally supplied terms. Registry mutation methods
    /// enforce ordering.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw term for storage, metrics or wire projection.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Single-writer fence for one room ownership term.
///
/// Every room starts at [`Self::INITIAL`]. Failover advances the epoch so the
/// old owner cannot keep reporting health or applying topology changes after a
/// replacement owner has been chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RoomOwnershipEpoch(u64);

impl RoomOwnershipEpoch {
    /// First epoch for a newly assigned room owner.
    pub const INITIAL: Self = Self(1);

    /// Wraps an epoch value that came from storage or a validated request.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw epoch for storage, metrics or wire projection.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next epoch or `None` when the counter cannot advance.
    ///
    /// Callers must treat overflow as an authority failure. Reusing the current
    /// epoch would remove the stale-owner fence.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Monotonic version of the topology snapshot for a room.
///
/// Topology versions let media nodes ignore replayed or delayed snapshots
/// within the same ownership epoch. They do not replace
/// [`RoomOwnershipEpoch`]. A newer epoch always represents a new owner term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TopologyVersion(u64);

impl TopologyVersion {
    /// First snapshot version for a newly assigned room owner.
    pub const INITIAL: Self = Self(1);

    /// Wraps a version value that came from storage or a validated request.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw version for storage, metrics or wire projection.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next snapshot version or `None` when the counter cannot
    /// advance.
    ///
    /// Version overflow means the authority can no longer publish replay-safe
    /// updates for the room.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

pub(crate) fn non_empty(value: impl Into<String>) -> Result<String, IdentifierError> {
    let value = value.into();
    if value.trim().is_empty() {
        return Err(IdentifierError::Empty);
    }
    Ok(value)
}
