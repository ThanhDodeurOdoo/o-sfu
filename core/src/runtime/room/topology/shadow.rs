//! Cross-router receiver shadow ownership for room topology.
//!
//! `RoomTopology` creates a receiver shadow session on a producer's source
//! router when a receiver's home router is different. The pure router owns the
//! real session, transport, producer and consumer maps. This module owns only
//! the derived question that the pure router cannot answer by itself:
//! Which receiver shadows are still justified by live routed consumer edges?
//!
//! The tracker is cold-path. Packet forwarding never consults it.
//! It returns shadow sessions that should be pruned after a topology mutation
//! removes the last routed consumer edge for that receiver on that source
//! router.

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::RouterId;

use super::{RoutedConsumerId, RoutedProducerId};
use crate::runtime::UserId;

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
/// # Boundary role
///
/// The tracker does not call into `RoomRouterState` and it does not decide
/// placement. `RoomTopology` registers routed producers and consumers after the
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
    /// Producer owner lookup used when a room session leaves.
    producer_owners: BTreeMap<RoutedProducerId, UserId>,
    /// Reverse producer lookup by room user.
    producers_by_user: BTreeMap<UserId, BTreeSet<RoutedProducerId>>,
    /// Consumer-to-producer link for releasing one consumer edge.
    consumer_producers: BTreeMap<RoutedConsumerId, RoutedProducerId>,
    /// Producer-to-consumer reverse index for source teardown.
    consumers_by_producer: BTreeMap<RoutedProducerId, BTreeSet<RoutedConsumerId>>,
    /// Consumer-to-shadow link for cross-router receiver cleanup.
    consumer_shadows: BTreeMap<RoutedConsumerId, ShadowSessionKey>,
    /// Live routed edge count per receiver shadow.
    shadow_refcounts: BTreeMap<ShadowSessionKey, usize>,
}

impl ShadowSessionTracker {
    /// Register a producer after the source router accepted it.
    ///
    /// This lets source-session teardown release every cross-router consumer
    /// shadow derived from the user's producers without scanning unrelated
    /// consumers.
    pub(super) fn register_producer(&mut self, user_id: &UserId, producer_id: RoutedProducerId) {
        self.producer_owners.insert(producer_id, user_id.clone());
        self.producers_by_user
            .entry(user_id.clone())
            .or_default()
            .insert(producer_id);
    }

    /// Register a routed consumer after the source router accepted it.
    ///
    /// `shadow_key` is `None` for same-router consumers. Those edges are still
    /// owned by the pure router but they do not require topology-level shadow
    /// bookkeeping.
    pub(super) fn register_consumer(
        &mut self,
        producer_id: RoutedProducerId,
        consumer_id: RoutedConsumerId,
        shadow_key: Option<ShadowSessionKey>,
    ) {
        let Some(shadow_key) = shadow_key else {
            return;
        };
        self.consumer_producers.insert(consumer_id, producer_id);
        self.consumers_by_producer
            .entry(producer_id)
            .or_default()
            .insert(consumer_id);
        self.consumer_shadows
            .insert(consumer_id, shadow_key.clone());
        self.shadow_refcounts
            .entry(shadow_key)
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
    }

    /// Return whether a shadow currently has any tracked routed edge.
    ///
    /// `RoomTopology` uses this before finishing consumer creation. If the pure
    /// router later rejects the consumer, the topology can remove a newly
    /// materialized shadow without touching an older shadow that still belongs
    /// to another live consumer.
    #[must_use]
    pub(super) fn contains_shadow_session(&self, shadow_key: &ShadowSessionKey) -> bool {
        self.shadow_refcounts.contains_key(shadow_key)
    }

    /// Release one consumer edge and return shadows that became orphaned.
    ///
    /// The returned keys are cleanup instructions for `RoomTopology`. The
    /// tracker has already forgotten its ownership for those shadows.
    #[must_use]
    pub(super) fn unregister_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
    ) -> BTreeSet<ShadowSessionKey> {
        let mut prune = BTreeSet::new();
        self.release_consumer(consumer_id, &mut prune);
        prune
    }

    /// Release one producer and every consumer edge that depends on it.
    ///
    /// This mirrors pure-router producer teardown from the topology side. It is
    /// the path that handles source users leaving before cross-router receivers.
    #[must_use]
    pub(super) fn unregister_producer(
        &mut self,
        producer_id: RoutedProducerId,
    ) -> BTreeSet<ShadowSessionKey> {
        let mut prune = BTreeSet::new();
        self.release_producer(producer_id, &mut prune);
        prune
    }

    /// Release all shadow ownership tied to a room user.
    ///
    /// When the user is a source, this releases every cross-router consumer of
    /// that user's producers. When the user is a receiver, this releases every
    /// shadow created for that receiver on other routers.
    #[must_use]
    pub(super) fn unregister_session(&mut self, user_id: &UserId) -> BTreeSet<ShadowSessionKey> {
        let mut prune = BTreeSet::new();
        let producer_ids = self.producers_by_user.remove(user_id).unwrap_or_default();
        for producer_id in producer_ids {
            self.release_producer_consumers(producer_id, &mut prune);
            self.producer_owners.remove(&producer_id);
        }

        let consumer_ids = self
            .consumer_shadows
            .iter()
            .filter_map(|(consumer_id, shadow_key)| {
                (shadow_key.user_id() == user_id).then_some(*consumer_id)
            })
            .collect::<Vec<_>>();
        for consumer_id in consumer_ids {
            self.release_consumer(consumer_id, &mut prune);
        }
        prune
    }

    fn release_producer(
        &mut self,
        producer_id: RoutedProducerId,
        prune: &mut BTreeSet<ShadowSessionKey>,
    ) {
        self.release_producer_consumers(producer_id, prune);
        if let Some(owner) = self.producer_owners.remove(&producer_id) {
            remove_from_index_set(&mut self.producers_by_user, &owner, &producer_id);
        }
    }

    fn release_producer_consumers(
        &mut self,
        producer_id: RoutedProducerId,
        prune: &mut BTreeSet<ShadowSessionKey>,
    ) {
        let consumer_ids = self
            .consumers_by_producer
            .remove(&producer_id)
            .unwrap_or_default();
        for consumer_id in consumer_ids {
            self.release_consumer(consumer_id, prune);
        }
    }

    fn release_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
        prune: &mut BTreeSet<ShadowSessionKey>,
    ) {
        if let Some(producer_id) = self.consumer_producers.remove(&consumer_id) {
            remove_from_index_set(&mut self.consumers_by_producer, &producer_id, &consumer_id);
        }
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

fn remove_from_index_set<K, V>(index: &mut BTreeMap<K, BTreeSet<V>>, key: &K, value: &V)
where
    K: Ord,
    V: Ord,
{
    let should_remove_key = index.get_mut(key).is_some_and(|values| {
        values.remove(value);
        values.is_empty()
    });
    if should_remove_key {
        index.remove(key);
    }
}
