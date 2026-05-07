//! Room resolution and topology snapshot DTOs.
//!
//! A room resolution is the answer ingress needs before admitting a client. A
//! topology snapshot is the durable subset a media node caches so an already
//! owned room can keep operating while fresh control-plane reads are degraded.

use serde::{Deserialize, Serialize};

use crate::{
    ClusterRoomId, NodeAddress, NodeLeaseTerm, OwnerNodeId, RoomOwnershipEpoch, TopologyVersion,
};

/// Owner resolution returned for one room.
///
/// This value is authoritative at the instant it is produced by
/// [`crate::ClusterRoomDirectory`] or [`crate::ClusterControlPlane`]. Callers
/// should cache its [`TopologySnapshot`] form before admitting local work that
/// must survive a temporary control-plane outage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomResolution {
    /// Room whose owner was resolved.
    pub room_id: ClusterRoomId,
    /// Node that currently owns the room.
    pub owner: OwnerNodeId,
    /// Owner node lease term used to fence stale reports from older node state.
    pub owner_lease: NodeLeaseTerm,
    /// Room ownership epoch for this owner term.
    pub epoch: RoomOwnershipEpoch,
    /// Version of the topology snapshot represented by this resolution.
    pub topology_version: TopologyVersion,
    /// Address clients or ingress proxies should use to reach the owner.
    pub owner_ingress_address: NodeAddress,
    /// Address other media nodes should use for relay traffic to the owner.
    pub owner_relay_address: NodeAddress,
}

impl RoomResolution {
    /// Projects this resolution into the form cached by media nodes.
    ///
    /// The snapshot keeps the same owner, lease, epoch and version. It drops
    /// the assignment-source context because cache validation only cares about
    /// fencing and freshness.
    #[must_use]
    pub fn snapshot(&self) -> TopologySnapshot {
        TopologySnapshot {
            room_id: self.room_id.clone(),
            owner: self.owner.clone(),
            owner_lease: self.owner_lease,
            epoch: self.epoch,
            version: self.topology_version,
            owner_ingress_address: self.owner_ingress_address.clone(),
            owner_relay_address: self.owner_relay_address.clone(),
        }
    }
}

/// Cached view of one room's owner topology.
///
/// # Degraded mode
///
/// Media nodes use this shape for owner-led operation when the control plane is
/// temporarily unavailable. A snapshot is not a permission to accept new
/// authority after failover. It is a local fence for work the node already owns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologySnapshot {
    /// Room covered by the snapshot.
    pub room_id: ClusterRoomId,
    /// Owner node recorded by the snapshot.
    pub owner: OwnerNodeId,
    /// Owner lease term recorded when the snapshot was produced.
    pub owner_lease: NodeLeaseTerm,
    /// Ownership epoch recorded by the snapshot.
    pub epoch: RoomOwnershipEpoch,
    /// Monotonic version within the ownership epoch.
    pub version: TopologyVersion,
    /// Cached ingress address of the owner.
    pub owner_ingress_address: NodeAddress,
    /// Cached relay address of the owner.
    pub owner_relay_address: NodeAddress,
}
