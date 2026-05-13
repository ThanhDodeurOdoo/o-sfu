//! In-memory control-plane service model for room ownership.
//!
//! [`ClusterControlPlane`] composes the node registry, room directory, topology
//! snapshots and room-health reports behind one synchronous API. The deployable
//! HTTP binary is only a transport wrapper around this model.
//!
//! This type is authoritative only for the process that owns it. It gives the
//! project a precise contract for node registration, room resolution, failover
//! and topology watch behavior. Durable storage or an HA consensus layer must
//! preserve the same authority and fencing rules.
//!
//! # Concurrency
//!
//! The service owns plain maps and has no internal locking. A caller must
//! serialize mutation externally. The current binary uses a Tokio mutex. A
//! storage-backed implementation can keep the same method contracts while
//! moving serialization into transactions.
//!
//! This service is never part of RTP forwarding. Media workers consume cached
//! topology decisions that were produced here on admission or topology-update
//! paths.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ClusterNodeRegistry, ClusterRoomDirectory, ClusterRoomId, NodeAdvertisement, NodeId, NodeLease,
    NodeLeaseTerm, OwnerNodeId, RoomAssignmentSource, RoomDirectoryError, RoomOwnershipEpoch,
    TopologySnapshot, TopologyVersion, node::NodeRegistryError, topology::RoomResolution,
};

/// Request to register or replace one node advertisement.
///
/// The advertisement is accepted only when its lease term is newer than the
/// current registry entry for the same node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterNodeRequest {
    /// Full node state the registry should store if the lease term is fresh.
    pub advertisement: NodeAdvertisement,
}

/// Request to advance one registered node lease.
///
/// Lease renewal is a liveness signal for the existing advertisement. It does
/// not update address, role, region, health or capacity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewLeaseRequest {
    /// Node whose current lease should advance.
    pub node_id: NodeId,
    /// New lease term. It must be strictly greater than the stored term.
    pub term: NodeLeaseTerm,
}

/// Request to resolve the authoritative owner for one room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveRoomRequest {
    /// Cluster room id to look up or assign.
    pub room_id: ClusterRoomId,
}

/// Request to hand room ownership from a failed owner to a replacement owner.
///
/// The observed owner and epoch are part of the request so delayed failover
/// decisions cannot overwrite a newer owner term.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclareFailoverRequest {
    /// Room whose owner should be replaced.
    pub room_id: ClusterRoomId,
    /// Owner the caller believes has failed.
    pub failed_owner: OwnerNodeId,
    /// Epoch observed when the failover decision was made.
    pub observed_epoch: RoomOwnershipEpoch,
    /// Registered node that should become the new owner.
    pub replacement_owner: OwnerNodeId,
}

/// Owner-authored room health report.
///
/// Health reports are accepted only from the current owner, epoch and owner
/// lease term. This keeps stale owners from updating control-plane state after
/// failover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportRoomHealthRequest {
    /// Room covered by the report.
    pub room_id: ClusterRoomId,
    /// Owner that produced the report.
    pub owner: OwnerNodeId,
    /// Ownership epoch observed by the reporting owner.
    pub epoch: RoomOwnershipEpoch,
    /// Owner lease term observed by the reporting owner.
    pub owner_lease: NodeLeaseTerm,
    /// Number of users the owner currently sees in the room.
    ///
    /// Stored as control-plane metadata only. Scheduling policy and metrics can
    /// consume it without changing the health-report fence.
    pub users: u32,
}

/// Result of watching a room topology after an optional known version.
///
/// The model exposes a polling-friendly shape rather than an async stream so
/// the domain rules stay independent from the transport used by the deployable
/// service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyWatchUpdate {
    /// The caller already has the latest known snapshot.
    Unchanged,
    /// A newer or first known snapshot is available.
    Snapshot(TopologySnapshot),
}

/// Errors returned by the composed control-plane model.
///
/// # Error handling
///
/// `NodeRegistry` and `RoomDirectory` wrap lower-level domain rejections.
/// `StaleHealthReporter` hides which owner fence failed because
/// callers should treat all stale health reports the same. `MissingTopology`
/// means the room has not been resolved or assigned in this authority.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ControlPlaneError {
    /// Node registration or lease renewal failed.
    #[error(transparent)]
    NodeRegistry(#[from] NodeRegistryError),
    /// Room assignment, owner validation or failover failed.
    #[error(transparent)]
    RoomDirectory(#[from] RoomDirectoryError),
    /// Room health was reported by a stale owner, epoch or lease term.
    #[error("reported room health came from a stale owner")]
    StaleHealthReporter,
    /// No topology snapshot is known for the requested room.
    #[error("topology snapshot is not known")]
    MissingTopology,
}

/// Synchronous authority for room ownership decisions.
#[derive(Debug, Default, Clone)]
pub struct ClusterControlPlane {
    nodes: ClusterNodeRegistry,
    rooms: ClusterRoomDirectory,
    topology_snapshots: BTreeMap<ClusterRoomId, TopologySnapshot>,
    room_health: BTreeMap<ClusterRoomId, ReportRoomHealthRequest>,
}

impl ClusterControlPlane {
    /// Registers a node advertisement with the node authority.
    ///
    /// A successful call does not assign rooms by itself. It only makes the
    /// node visible to later room resolution if the advertisement is a
    /// schedulable media node.
    ///
    /// # Errors
    ///
    /// Returns [`NodeRegistryError::StaleLease`] when registration does not move
    /// the node lease forward.
    pub fn register_node(&mut self, request: RegisterNodeRequest) -> Result<(), ControlPlaneError> {
        self.nodes.register(request.advertisement)?;
        Ok(())
    }

    /// Advances the lease for an already registered node.
    ///
    /// Renewing a lease proves the caller has a fresher term than the stored
    /// advertisement. It does not update scheduling metadata.
    ///
    /// # Errors
    ///
    /// Returns [`NodeRegistryError::UnknownNode`] when the node is not
    /// registered or [`NodeRegistryError::StaleLease`] when the term is stale.
    pub fn renew_lease(
        &mut self,
        request: &RenewLeaseRequest,
    ) -> Result<NodeLease, ControlPlaneError> {
        Ok(self.nodes.renew_lease(&request.node_id, request.term)?)
    }

    /// Resolves a room owner and records the matching topology snapshot.
    ///
    /// Existing room assignments are reused. Unassigned rooms are placed on a
    /// schedulable media node through [`ClusterRoomDirectory`]. The returned
    /// snapshot is stored so topology watchers can replay the latest owner term.
    ///
    /// # Errors
    ///
    /// Returns [`RoomDirectoryError::NoSchedulableOwner`] when an unowned room
    /// cannot be assigned to a media node.
    pub fn resolve_room(
        &mut self,
        request: ResolveRoomRequest,
    ) -> Result<(RoomResolution, RoomAssignmentSource), ControlPlaneError> {
        let (resolution, source) = self.rooms.resolve_room(request.room_id, &self.nodes)?;
        self.topology_snapshots
            .insert(resolution.room_id.clone(), resolution.snapshot());
        Ok((resolution, source))
    }

    /// Declares owner failover and publishes the replacement snapshot.
    ///
    /// The request must match the current owner and epoch. This makes failover
    /// replay-safe and prevents an older failure detector from replacing a
    /// newer owner.
    ///
    /// # Errors
    ///
    /// Returns stale-owner or stale-epoch errors when the failover request no
    /// longer matches the authoritative assignment.
    pub fn declare_failover(
        &mut self,
        request: &DeclareFailoverRequest,
    ) -> Result<RoomResolution, ControlPlaneError> {
        let Some(replacement) = self.nodes.owner_node(&request.replacement_owner) else {
            return Err(RoomDirectoryError::UnknownOwner.into());
        };
        let resolution = self.rooms.declare_failover(
            &request.room_id,
            &request.failed_owner,
            request.observed_epoch,
            replacement,
        )?;
        self.topology_snapshots
            .insert(resolution.room_id.clone(), resolution.snapshot());
        Ok(resolution)
    }

    /// Stores an owner-authored room health report.
    ///
    /// Health is accepted only if the report still matches the authoritative
    /// owner, epoch and owner lease. The current model stores the last report
    /// so metrics and scheduling policy can consume it later.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneError::StaleHealthReporter`] when the health report
    /// does not match the current room owner, epoch or owner lease.
    pub fn report_room_health(
        &mut self,
        request: ReportRoomHealthRequest,
    ) -> Result<(), ControlPlaneError> {
        self.rooms
            .validate_owner(
                &request.room_id,
                &request.owner,
                request.epoch,
                request.owner_lease,
            )
            .map_err(|_error| ControlPlaneError::StaleHealthReporter)?;
        self.room_health.insert(request.room_id.clone(), request);
        Ok(())
    }

    /// Returns the latest topology snapshot when it is newer than `after`.
    ///
    /// This is the domain shape behind topology-watch transports. The result is
    /// authoritative for this control-plane instance. It is not a durable audit
    /// log, so callers that miss updates should request the latest snapshot
    /// again rather than expecting replay of every intermediate version.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneError::MissingTopology`] when no topology is known
    /// for the requested room.
    pub fn watch_room_topology(
        &self,
        room_id: &ClusterRoomId,
        after: Option<TopologyVersion>,
    ) -> Result<TopologyWatchUpdate, ControlPlaneError> {
        let Some(snapshot) = self.topology_snapshots.get(room_id) else {
            return Err(ControlPlaneError::MissingTopology);
        };
        if after.is_some_and(|version| version >= snapshot.version) {
            return Ok(TopologyWatchUpdate::Unchanged);
        }
        Ok(TopologyWatchUpdate::Snapshot(snapshot.clone()))
    }

    /// Returns the node registry view owned by this control-plane instance.
    ///
    /// This is primarily for diagnostics, tests and server wrappers that need
    /// read-only access without bypassing mutation methods.
    #[must_use]
    pub fn nodes(&self) -> &ClusterNodeRegistry {
        &self.nodes
    }

    /// Returns the room directory view owned by this control-plane instance.
    ///
    /// This is read-only access to the authoritative assignments held by this
    /// in-memory model.
    #[must_use]
    pub fn rooms(&self) -> &ClusterRoomDirectory {
        &self.rooms
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::{
        NodeAddress, NodeCapacity, NodeHealth, NodeLease, NodeRegion, NodeRole, TopologyCacheError,
        cache::CachedTopologyStore,
    };

    fn text_id(value: &str) -> Result<NodeId, crate::IdentifierError> {
        NodeId::new(value)
    }

    fn room_id(value: &str) -> Result<ClusterRoomId, crate::IdentifierError> {
        ClusterRoomId::new(value)
    }

    fn address(value: &str) -> Result<NodeAddress, crate::IdentifierError> {
        NodeAddress::new(value)
    }

    fn region(value: &str) -> Result<NodeRegion, crate::IdentifierError> {
        NodeRegion::new(value)
    }

    fn media_node(
        id: &str,
        lease: u64,
        room_slots: u32,
    ) -> Result<NodeAdvertisement, Box<dyn Error>> {
        Ok(NodeAdvertisement {
            node_id: text_id(id)?,
            role: NodeRole::Media,
            region: region("local")?,
            ingress_address: address(&format!("https://{id}.example.test"))?,
            relay_address: address(&format!("relay://{id}.example.test"))?,
            health: NodeHealth::Schedulable,
            capacity: NodeCapacity::new(room_slots),
            lease: NodeLease::new(NodeLeaseTerm::new(lease)),
        })
    }

    #[test]
    fn lease_terms_must_move_forward() -> Result<(), Box<dyn Error>> {
        let mut control_plane = ClusterControlPlane::default();
        let node = media_node("node-a", 10, 10)?;
        let node_id = node.node_id.clone();
        let registered = control_plane.register_node(RegisterNodeRequest {
            advertisement: node,
        });
        assert_eq!(registered, Ok(()));

        let stale = control_plane.renew_lease(&RenewLeaseRequest {
            node_id: node_id.clone(),
            term: NodeLeaseTerm::new(10),
        });
        assert!(matches!(
            stale,
            Err(ControlPlaneError::NodeRegistry(
                NodeRegistryError::StaleLease
            ))
        ));

        let renewed = control_plane.renew_lease(&RenewLeaseRequest {
            node_id,
            term: NodeLeaseTerm::new(11),
        });
        assert_eq!(renewed, Ok(NodeLease::new(NodeLeaseTerm::new(11))));
        Ok(())
    }

    #[test]
    fn room_resolution_assigns_exactly_one_owner() -> Result<(), Box<dyn Error>> {
        let mut control_plane = ClusterControlPlane::default();
        for node in [media_node("node-b", 1, 10)?, media_node("node-a", 1, 10)?] {
            assert_eq!(
                control_plane.register_node(RegisterNodeRequest {
                    advertisement: node,
                }),
                Ok(())
            );
        }
        let room_id = room_id("room-1")?;
        let (first, first_source) = control_plane.resolve_room(ResolveRoomRequest {
            room_id: room_id.clone(),
        })?;
        let (second, second_source) = control_plane.resolve_room(ResolveRoomRequest { room_id })?;

        assert_eq!(first.owner, OwnerNodeId::from(text_id("node-a")?));
        assert_eq!(first.owner, second.owner);
        assert_eq!(first.epoch, second.epoch);
        assert_eq!(first_source, RoomAssignmentSource::Assigned);
        assert_eq!(second_source, RoomAssignmentSource::Existing);
        Ok(())
    }

    #[test]
    fn failover_advances_epoch_and_rejects_stale_owner() -> Result<(), Box<dyn Error>> {
        let mut control_plane = ClusterControlPlane::default();
        for node in [media_node("node-a", 1, 10)?, media_node("node-b", 1, 10)?] {
            assert_eq!(
                control_plane.register_node(RegisterNodeRequest {
                    advertisement: node,
                }),
                Ok(())
            );
        }
        let room_id = room_id("room-1")?;
        let (resolution, _source) = control_plane.resolve_room(ResolveRoomRequest {
            room_id: room_id.clone(),
        })?;
        let failed_owner = resolution.owner.clone();
        let replacement_owner = OwnerNodeId::from(text_id("node-b")?);
        let failover = control_plane.declare_failover(&DeclareFailoverRequest {
            room_id: room_id.clone(),
            failed_owner: failed_owner.clone(),
            observed_epoch: resolution.epoch,
            replacement_owner: replacement_owner.clone(),
        })?;
        assert_eq!(failover.owner, replacement_owner);
        assert_eq!(failover.epoch, RoomOwnershipEpoch::new(2));

        let stale = control_plane.report_room_health(ReportRoomHealthRequest {
            room_id,
            owner: failed_owner,
            epoch: resolution.epoch,
            owner_lease: resolution.owner_lease,
            users: 1,
        });
        assert_eq!(stale, Err(ControlPlaneError::StaleHealthReporter));
        Ok(())
    }

    #[test]
    fn cached_topology_rejects_stale_snapshots_and_owner_terms() -> Result<(), Box<dyn Error>> {
        let mut control_plane = ClusterControlPlane::default();
        for node in [media_node("node-a", 1, 10)?, media_node("node-b", 1, 10)?] {
            assert_eq!(
                control_plane.register_node(RegisterNodeRequest {
                    advertisement: node,
                }),
                Ok(())
            );
        }
        let room_id = room_id("room-1")?;
        let (resolution, _source) = control_plane.resolve_room(ResolveRoomRequest {
            room_id: room_id.clone(),
        })?;
        let mut cache = CachedTopologyStore::default();
        assert_eq!(cache.update(resolution.snapshot()), Ok(()));
        assert_eq!(
            cache.update(resolution.snapshot()),
            Err(TopologyCacheError::StaleVersion)
        );

        let failover = control_plane.declare_failover(&DeclareFailoverRequest {
            room_id: room_id.clone(),
            failed_owner: resolution.owner.clone(),
            observed_epoch: resolution.epoch,
            replacement_owner: OwnerNodeId::from(text_id("node-b")?),
        })?;
        assert!(
            cache
                .validate_owner(&room_id, &resolution.owner, resolution.epoch)
                .is_ok()
        );
        assert_eq!(cache.update(failover.snapshot()), Ok(()));
        assert_eq!(
            cache.validate_owner(&room_id, &resolution.owner, resolution.epoch),
            Err(TopologyCacheError::StaleOwner)
        );
        Ok(())
    }
}
