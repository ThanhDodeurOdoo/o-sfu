//! Authoritative room-to-owner assignment state.
//!
//! # Boundary role
//!
//! The distributed room directory answers "which node owns this room right
//! now". It is deliberately separate from the process-local `RoomDirectory` in
//! `o-sfu-core`, which only tracks rooms already live inside one media-server
//! process.
//!
//! This module owns ownership assignment, stale-owner rejection and failover
//! epoch advancement. It does not mutate media rooms, move WebRTC sessions or
//! send reconnect instructions. Runtime ingress and later event delivery layers
//! consume its decisions.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ClusterRoomId, NodeLeaseTerm, OwnerNodeId, RoomOwnershipEpoch, TopologyVersion,
    node::{ClusterNodeRegistry, NodeAdvertisement},
    topology::RoomResolution,
};

/// How a room-resolution result was produced.
///
/// The source lets ingress and metrics distinguish first assignment from reuse
/// of an existing authoritative owner. It is not part of the room ownership
/// fence. The fence is the owner plus epoch carried by [`RoomAssignment`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoomAssignmentSource {
    /// The room already had an authoritative owner.
    Existing,
    /// The directory selected an owner for a previously unassigned room.
    Assigned,
    /// Ownership moved to a replacement owner after failover.
    Failover,
}

/// Authoritative owner term for one room.
///
/// This is the compact state the directory must persist in a durable
/// production authority. It is enough to reject stale topology writers and to
/// reconstruct a [`RoomResolution`] when combined with the current owner node
/// advertisement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomAssignment {
    /// Node that currently has single-writer authority for the room.
    pub owner: OwnerNodeId,
    /// Owner lease term observed when the assignment was created or handed off.
    ///
    /// Reports carrying older owner lease terms are stale even when the owner
    /// id and room epoch still match.
    pub owner_lease: NodeLeaseTerm,
    /// Room ownership epoch used to fence delayed or restarted owners.
    pub epoch: RoomOwnershipEpoch,
    /// Latest topology snapshot version published for this owner epoch.
    pub topology_version: TopologyVersion,
}

/// Rejections produced by the authoritative room directory.
///
/// # Error handling guidance
///
/// Most variants are expected conflicts at the control-plane boundary. They
/// should not crash the media server. `NoSchedulableOwner` is an admission
/// outage, stale variants are fencing rejections and overflow variants mean the
/// authority cannot safely advance the room term.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RoomDirectoryError {
    /// A new room could not be assigned because no media node is currently
    /// eligible.
    #[error("no schedulable media node is available")]
    NoSchedulableOwner,
    /// A caller claimed authority for a room owned by another node.
    #[error("room is owned by another node")]
    StaleOwner,
    /// A caller used an old room ownership epoch.
    #[error("room ownership epoch is stale")]
    StaleEpoch,
    /// A caller used an owner lease older than the directory assignment.
    #[error("room owner lease is stale")]
    StaleLease,
    /// The room ownership epoch could not advance during failover.
    #[error("room ownership epoch overflowed")]
    EpochOverflow,
    /// The topology version could not advance during failover.
    #[error("topology version overflowed")]
    TopologyVersionOverflow,
    /// The directory references an owner that is not present in the node
    /// registry view supplied by the caller.
    #[error("owner node is not registered")]
    UnknownOwner,
}

/// In-memory authoritative mapping from room id to owner assignment.
///
/// # Concurrency model
///
/// The type owns pure state and does not lock internally. The control-plane
/// service serializes access around it. This keeps the ownership rules usable
/// in unit tests and leaves storage, replication and leader election outside
/// the domain model.
///
/// # Invariant
///
/// A room has at most one assignment. A successful failover changes the owner,
/// advances the room epoch and advances the topology version in the same
/// mutation.
#[derive(Debug, Default, Clone)]
pub struct ClusterRoomDirectory {
    rooms: BTreeMap<ClusterRoomId, RoomAssignment>,
}

impl ClusterRoomDirectory {
    /// Resolves a room to the current owner or assigns a new owner.
    ///
    /// Existing assignments are reused and returned with
    /// [`RoomAssignmentSource::Existing`]. New assignments choose the
    /// deterministic lowest node id from the schedulable media nodes exposed by
    /// the supplied registry. That simple rule keeps the current behavior
    /// predictable while leaving load-aware policy for the production scheduler.
    ///
    /// # Errors
    ///
    /// Returns [`RoomDirectoryError::NoSchedulableOwner`] when a new room has no
    /// eligible media node.
    pub fn resolve_room(
        &mut self,
        room_id: ClusterRoomId,
        registry: &ClusterNodeRegistry,
    ) -> Result<(RoomResolution, RoomAssignmentSource), RoomDirectoryError> {
        if let Some(assignment) = self.rooms.get(&room_id) {
            let owner = registry
                .owner_node(&assignment.owner)
                .ok_or(RoomDirectoryError::UnknownOwner)?;
            let resolution = resolution_from_assignment(&room_id, assignment, owner);
            return Ok((resolution, RoomAssignmentSource::Existing));
        }
        let owner = select_owner(registry)?;
        let assignment = RoomAssignment {
            owner: owner.node_id.clone().into(),
            owner_lease: owner.lease.term,
            epoch: RoomOwnershipEpoch::INITIAL,
            topology_version: TopologyVersion::INITIAL,
        };
        let resolution = resolution_from_assignment(&room_id, &assignment, owner);
        self.rooms.insert(room_id, assignment);
        Ok((resolution, RoomAssignmentSource::Assigned))
    }

    /// Validates that a caller still owns a room for the claimed epoch.
    ///
    /// This is the central fencing check for owner-authored messages such as
    /// health reports and topology mutations. The check intentionally
    /// uses the directory's assignment as the authority rather than trusting a
    /// topology snapshot supplied by the caller.
    ///
    /// # Errors
    ///
    /// Returns a stale-owner, stale-epoch, stale-lease or unknown-owner error
    /// when the candidate owner no longer matches the authoritative assignment.
    pub fn validate_owner(
        &self,
        room_id: &ClusterRoomId,
        owner: &OwnerNodeId,
        epoch: RoomOwnershipEpoch,
        lease: NodeLeaseTerm,
    ) -> Result<(), RoomDirectoryError> {
        let Some(assignment) = self.rooms.get(room_id) else {
            return Err(RoomDirectoryError::UnknownOwner);
        };
        if &assignment.owner != owner {
            return Err(RoomDirectoryError::StaleOwner);
        }
        if assignment.epoch != epoch {
            return Err(RoomDirectoryError::StaleEpoch);
        }
        if assignment.owner_lease > lease {
            return Err(RoomDirectoryError::StaleLease);
        }
        Ok(())
    }

    /// Moves a room from a failed owner to a replacement owner.
    ///
    /// Failover is accepted only when the caller observed the current owner and
    /// epoch. On success the replacement owner becomes authoritative, the epoch
    /// advances and the topology version advances so cached topology watchers
    /// can distinguish the new owner term.
    ///
    /// # Errors
    ///
    /// Returns an error when the observed owner or epoch is stale, the
    /// replacement is not registered or the epoch/version cannot advance.
    pub fn declare_failover(
        &mut self,
        room_id: &ClusterRoomId,
        failed_owner: &OwnerNodeId,
        observed_epoch: RoomOwnershipEpoch,
        replacement: &NodeAdvertisement,
    ) -> Result<RoomResolution, RoomDirectoryError> {
        let Some(assignment) = self.rooms.get_mut(room_id) else {
            return Err(RoomDirectoryError::UnknownOwner);
        };
        if &assignment.owner != failed_owner {
            return Err(RoomDirectoryError::StaleOwner);
        }
        if assignment.epoch != observed_epoch {
            return Err(RoomDirectoryError::StaleEpoch);
        }
        assignment.owner = replacement.node_id.clone().into();
        assignment.owner_lease = replacement.lease.term;
        assignment.epoch = assignment
            .epoch
            .next()
            .ok_or(RoomDirectoryError::EpochOverflow)?;
        assignment.topology_version = assignment
            .topology_version
            .next()
            .ok_or(RoomDirectoryError::TopologyVersionOverflow)?;
        Ok(resolution_from_assignment(room_id, assignment, replacement))
    }

    /// Returns the current authoritative assignment for a room.
    ///
    /// The returned assignment is exact for this in-memory view. It does not
    /// include owner addresses because those remain node-registry metadata.
    #[must_use]
    pub fn assignment(&self, room_id: &ClusterRoomId) -> Option<&RoomAssignment> {
        self.rooms.get(room_id)
    }
}

fn select_owner(registry: &ClusterNodeRegistry) -> Result<&NodeAdvertisement, RoomDirectoryError> {
    registry
        .schedulable_media_nodes()
        .min_by(|left, right| left.node_id.cmp(&right.node_id))
        .ok_or(RoomDirectoryError::NoSchedulableOwner)
}

fn resolution_from_assignment(
    room_id: &ClusterRoomId,
    assignment: &RoomAssignment,
    owner: &NodeAdvertisement,
) -> RoomResolution {
    RoomResolution {
        room_id: room_id.clone(),
        owner: assignment.owner.clone(),
        owner_lease: assignment.owner_lease,
        epoch: assignment.epoch,
        topology_version: assignment.topology_version,
        owner_ingress_address: owner.ingress_address.clone(),
        owner_relay_address: owner.relay_address.clone(),
    }
}
