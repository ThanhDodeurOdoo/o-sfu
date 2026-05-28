//! Cluster node advertisement and lease registry.
//!
//! This module models node-level authority facts for the control plane. A node
//! can advertise how clients should reach it, how relays should reach it, what
//! role it wants to serve and whether it can receive new room ownership.
//!
//! The registry is an in-memory authority. It validates lease ordering and
//! exposes schedulable media nodes for room assignment.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ids::{NodeId, NodeLeaseTerm, NodeRegion, non_empty};

/// Role a node advertises to the cluster authority.
///
/// The role is a scheduling hint and a capability boundary. The current owner
/// scheduler assigns room owners only to [`Self::Media`] nodes. Other roles are
/// present so ingress, recorder and control-plane separation can share the same
/// identity model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NodeRole {
    /// Node can own rooms and host participant media sessions.
    Media,
    /// Node can terminate external ingress before forwarding or resolving to
    /// the current owner.
    Ingress,
    /// Node can host recording work once recorder placement becomes topology
    /// aware.
    Recorder,
    /// Node participates in the ownership authority rather than serving media.
    ControlPlane,
}

/// Scheduler-visible health state for a registered node.
///
/// This is an admission signal, not a liveness detector. The component that
/// renews leases or receives probes decides when to change the advertised
/// health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeHealth {
    /// New room ownership may be assigned to this node.
    Schedulable,
    /// Existing ownership may remain, but new rooms should avoid this node.
    Draining,
    /// The node must not receive ownership and should be considered failed by
    /// failover policy.
    Unreachable,
}

/// Network address advertised for cluster ingress or relay traffic.
///
/// The value is opaque to this crate. HTTP URLs, host and port pairs, internal
/// service names and relay endpoints can all fit behind the same type.
/// the edge that knows the scheme validates the address.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeAddress(String);

impl NodeAddress {
    /// Builds an address wrapper from deployment input.
    ///
    /// # Errors
    ///
    /// Returns [`crate::IdentifierError::Empty`] when `value` is empty after trimming.
    pub fn new(value: impl Into<String>) -> Result<Self, crate::IdentifierError> {
        non_empty(value).map(Self)
    }

    /// Returns the address string exactly as accepted by [`Self::new`].
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Capacity signal used by room-owner selection.
///
/// The current owner scheduler only needs to know whether the node can accept
/// another room. The field is still explicit because schedulers can derive it
/// from CPU, memory, media-worker pressure or operator policy before
/// advertising it here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapacity {
    /// Number of additional room ownership slots the node is willing to accept.
    ///
    /// A zero value keeps the node registered but removes it from new room
    /// assignment.
    pub room_slots: u32,
}

impl NodeCapacity {
    /// Creates a capacity advertisement from an already computed slot count.
    #[must_use]
    pub const fn new(room_slots: u32) -> Self {
        Self { room_slots }
    }

    /// Returns whether this capacity permits new room ownership.
    #[must_use]
    pub const fn can_accept_room(self) -> bool {
        self.room_slots > 0
    }
}

/// Full node advertisement stored by the cluster node registry.
///
/// The advertisement is the authority's current view of one node. It combines
/// stable identity with mutable scheduling facts such as health, capacity and
/// lease term. Re-registering a node replaces the advertisement only when the
/// lease term moves forward.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAdvertisement {
    /// Stable node identity used by ownership assignments and registry lookups.
    pub node_id: NodeId,
    /// Role that controls which scheduling decisions may use this node.
    pub role: NodeRole,
    /// Region metadata for placement and failover policy.
    pub region: NodeRegion,
    /// Address clients or ingress proxies can use to reach this node.
    pub ingress_address: NodeAddress,
    /// Address other media nodes can use for relay traffic.
    pub relay_address: NodeAddress,
    /// Scheduler-visible admission state.
    pub health: NodeHealth,
    /// Advertised room ownership capacity.
    pub capacity: NodeCapacity,
    /// Freshness fence for this advertisement.
    pub lease: NodeLease,
}

/// Current lease fence for a node advertisement.
///
/// A lease term is monotonic. It is not a timer by itself. The control-plane
/// service decides when a term expires, then uses newer terms to reject stale
/// registrations or renewals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLease {
    /// Monotonic term associated with the advertisement.
    pub term: NodeLeaseTerm,
}

impl NodeLease {
    /// Creates a lease value around an already chosen term.
    #[must_use]
    pub const fn new(term: NodeLeaseTerm) -> Self {
        Self { term }
    }
}

/// Rejections produced by the node registry.
///
/// # Error
///
/// These are expected control-plane conflicts, not media-server failures. An
/// edge should map `StaleLease` to a conflict response and `UnknownNode` to a
/// missing registration or bootstrap problem.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NodeRegistryError {
    /// Registration or renewal did not move the node lease term forward.
    #[error("node is already registered with a newer or equal lease term")]
    StaleLease,
    /// A lease renewal referenced a node that has not been registered.
    #[error("node is not registered")]
    UnknownNode,
}

/// In-memory node authority for the control plane.
///
/// The type itself is synchronous and owns no locks. The deployable
/// control-plane process decides how to serialize access. This keeps the domain
/// rules testable without a runtime and lets storage-backed implementations
/// reuse the same transition semantics.
#[derive(Debug, Default, Clone)]
pub struct ClusterNodeRegistry {
    nodes: BTreeMap<NodeId, NodeAdvertisement>,
}

impl ClusterNodeRegistry {
    /// Registers or replaces one node advertisement.
    ///
    /// Replacement is accepted only when the advertised lease term is newer
    /// than the current one. This prevents a restarted or delayed process from
    /// overwriting fresher node state.
    ///
    /// # Errors
    ///
    /// Returns [`NodeRegistryError::StaleLease`] when the node is already
    /// registered with a lease term newer than or equal to the advertised term.
    pub fn register(&mut self, advertisement: NodeAdvertisement) -> Result<(), NodeRegistryError> {
        if let Some(current) = self.nodes.get(&advertisement.node_id)
            && current.lease.term >= advertisement.lease.term
        {
            return Err(NodeRegistryError::StaleLease);
        }
        self.nodes
            .insert(advertisement.node_id.clone(), advertisement);
        Ok(())
    }

    /// Advances the lease term of an existing node.
    ///
    /// Renewals update only the lease. A node that changes address, role,
    /// region, health or capacity should register a fresh advertisement with a
    /// newer lease term instead.
    ///
    /// # Errors
    ///
    /// Returns [`NodeRegistryError::UnknownNode`] when the node is not registered,
    /// or [`NodeRegistryError::StaleLease`] when the renewed term does not move
    /// forward.
    pub fn renew_lease(
        &mut self,
        node_id: &NodeId,
        term: NodeLeaseTerm,
    ) -> Result<NodeLease, NodeRegistryError> {
        let Some(node) = self.nodes.get_mut(node_id) else {
            return Err(NodeRegistryError::UnknownNode);
        };
        if node.lease.term >= term {
            return Err(NodeRegistryError::StaleLease);
        }
        node.lease = NodeLease::new(term);
        Ok(node.lease)
    }

    /// Returns the current advertisement for a node if it is registered.
    ///
    /// This is an authoritative registry lookup, but it is only as fresh as the
    /// caller's serialized view of the in-memory control-plane state.
    #[must_use]
    pub fn node(&self, node_id: &NodeId) -> Option<&NodeAdvertisement> {
        self.nodes.get(node_id)
    }

    /// Resolves an owner id back to its node advertisement.
    ///
    /// Room assignment stores owners as [`crate::OwnerNodeId`] so this helper
    /// is the narrow bridge back into node metadata such as ingress and relay
    /// addresses.
    #[must_use]
    pub fn owner_node(&self, owner: &crate::OwnerNodeId) -> Option<&NodeAdvertisement> {
        self.node(owner.node_id())
    }

    /// Iterates over nodes eligible for new room ownership.
    ///
    /// The iterator filters by role, health and advertised capacity. It does
    /// not reserve capacity. The caller must still commit any room assignment
    /// through the room directory.
    pub fn schedulable_media_nodes(&self) -> impl Iterator<Item = &NodeAdvertisement> {
        self.nodes.values().filter(|node| {
            node.role == NodeRole::Media
                && node.health == NodeHealth::Schedulable
                && node.capacity.can_accept_room()
        })
    }
}
