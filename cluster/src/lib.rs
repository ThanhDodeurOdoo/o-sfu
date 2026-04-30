//! Cluster
//!
//! This crate owns the shared language for distributed room ownership. It is
//! intentionally smaller than a full cluster runtime. The media server, the
//! control-plane binary and integration tests can depend on these types
//! without importing HTTP, WebSocket, Tokio task orchestration or media routing
//! code.
//!
//! The crate models the cold path only:
//!
//! - node registration
//! - lease renewal
//! - room owner resolution
//! - ownership epoch fencing
//! - topology snapshot caching
//! - failover ownership handoff
//!
//! Packet forwarding, room membership mutation and WebRTC transport work stay
//! outside this crate. A packet loop must never call into the control-plane
//! authority to decide where a packet goes.
//!
//! # Current scope
//!
//! The in-memory service model is authoritative only for one process and one
//! storage image. A clustered deployment needs a durable or highly available
//! authority around the same domain rules.
//!
//! # Ownership model
//!
//! One room maps to one owner node for one [`RoomOwnershipEpoch`]. Any
//! topology-changing message must carry the current owner and epoch so stale
//! owners can be rejected after restart, failover or delayed delivery.

pub mod cache;
pub mod control_plane;
pub mod directory;
pub mod ids;
pub mod node;
pub mod topology;

pub use cache::{CachedTopologyStore, TopologyCacheError};
pub use control_plane::{
    ClusterControlPlane, ControlPlaneError, DeclareFailoverRequest, RegisterNodeRequest,
    RenewLeaseRequest, ReportRoomHealthRequest, ResolveRoomRequest, TopologyWatchUpdate,
};
pub use directory::{
    ClusterRoomDirectory, RoomAssignment, RoomAssignmentSource, RoomDirectoryError,
};
pub use ids::{
    ClusterRoomId, IdentifierError, NodeId, NodeLeaseTerm, NodeRegion, OwnerNodeId,
    RoomOwnershipEpoch, TopologyVersion,
};
pub use node::{
    ClusterNodeRegistry, NodeAddress, NodeAdvertisement, NodeCapacity, NodeHealth, NodeLease,
    NodeRegistryError, NodeRole,
};
pub use topology::{RoomResolution, TopologySnapshot};
