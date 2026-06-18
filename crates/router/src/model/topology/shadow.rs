//! Cross-router receiver shadow ownership for routing topology.
//!
//! `RoutingTopology` creates a receiver shadow session on a producer's source
//! router when a receiver's home router is different. The pure router owns the
//! real session, transport, producer and consumer maps. This module contains only
//! the derived question that the pure router cannot answer by itself:
//! Which receiver shadows are still justified by live routed consumer edges?
//!
//! The tracker is cold-path. Packet forwarding never consults it.
//! It returns shadow sessions that should be pruned after a topology mutation
//! removes the last routed consumer edge for that receiver on that source
//! router.

#[cfg(not(kani))]
use std::collections::{BTreeMap, BTreeSet};

use o_sfu_model::UserId;

use super::RoutedConsumerId;
use crate::model::RouterId;
#[cfg(kani)]
use crate::model::proof_storage::{BTreeMap, BTreeSet};

/// Identity of one receiver shadow session on one source router.
///
/// The router id is the source router that hosts the consumer edge. The user id
/// is the receiver whose real home session may live on a different router. This
/// key is not a placement decision. It is a cleanup target that becomes valid
/// only after the last routed edge using the shadow has been released.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ShadowSessionKey {
    /// Source router that owns the shadow session.
    router_id: RouterId,
    /// Receiver user represented by the shadow on the source router.
    user_id: UserId,
}

impl ShadowSessionKey {
    #[must_use]
    pub(super) fn new(router_id: RouterId, user_id: UserId) -> Self {
        Self { router_id, user_id }
    }

    #[must_use]
    pub(super) const fn router_id(&self) -> RouterId {
        self.router_id
    }

    #[must_use]
    pub(super) fn user_id(&self) -> &UserId {
        &self.user_id
    }
}

/// Tracks which routed consumer edges keep cross-router shadows alive.
///
/// The tracker does not call into `RouterAdapterState` and it does not decide
/// placement. `RoutingTopology` registers routed producers and consumers after the
/// pure router accepts them. On teardown, the tracker releases its derived
/// ownership and returns the shadow sessions whose reference count reached
/// zero. The caller then removes those sessions from the relevant router.
///
/// # Invariants
///
/// Only cross-router consumers are tracked. A same-router consumer has no
/// shadow session and is ignored here. Each tracked consumer points to exactly
/// one producer and at most one shadow. Each shadow reference count is the
/// number of live routed consumers that still need that receiver session on the
/// source router.
#[derive(Debug, Clone, Default)]
pub(super) struct ShadowSessionTracker {
    /// Consumer-to-shadow link for cross-router receiver cleanup.
    consumer_shadows: BTreeMap<RoutedConsumerId, ShadowSessionKey>,
    /// Live routed edge count per receiver shadow.
    shadow_refcounts: BTreeMap<ShadowSessionKey, usize>,
}

impl ShadowSessionTracker {
    /// Register a routed consumer after the source router accepted it.
    ///
    /// `shadow_key` is `None` for same-router consumers. Those edges are still
    /// owned by the pure router but they do not require topology-level shadow
    /// bookkeeping.
    pub(super) fn register_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
        shadow_key: Option<ShadowSessionKey>,
    ) {
        let Some(shadow_key) = shadow_key else {
            return;
        };
        self.consumer_shadows
            .insert(consumer_id, shadow_key.clone());
        if let Some(count) = self.shadow_refcounts.get_mut(&shadow_key) {
            *count = count.saturating_add(1);
            return;
        }
        self.shadow_refcounts.insert(shadow_key, 1);
    }

    /// Return whether a shadow currently has any tracked routed edge.
    ///
    /// `RoutingTopology` uses this before finishing consumer creation. If the pure
    /// router later rejects the consumer, the topology can remove a newly
    /// materialized shadow without touching an older shadow that still belongs
    /// to another live consumer.
    #[must_use]
    pub(super) fn contains_shadow_session(&self, shadow_key: &ShadowSessionKey) -> bool {
        self.shadow_refcounts.contains_key(shadow_key)
    }

    /// Release all routed consumer edges supplied by the caller.
    ///
    /// The caller owns the producer and consumer graph. The topology tracker
    /// only keeps the shadow reference counts needed after router teardown
    /// accepts the mutation.
    #[must_use]
    pub(super) fn unregister_consumers(
        &mut self,
        consumer_ids: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> BTreeSet<ShadowSessionKey> {
        let mut prune = BTreeSet::new();
        for consumer_id in consumer_ids {
            self.release_consumer(consumer_id, &mut prune);
        }
        prune
    }

    fn release_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
        prune: &mut BTreeSet<ShadowSessionKey>,
    ) {
        let Some(shadow_key) = self.consumer_shadows.remove(&consumer_id) else {
            return;
        };
        if self.decrement_shadow_refcount(&shadow_key) {
            prune.insert(shadow_key);
        }
    }

    fn decrement_shadow_refcount(&mut self, shadow_key: &ShadowSessionKey) -> bool {
        let Some(count) = self.shadow_refcounts.get_mut(shadow_key) else {
            return false;
        };
        if *count > 1 {
            *count -= 1;
            return false;
        }
        self.shadow_refcounts.remove(shadow_key);
        true
    }
}
