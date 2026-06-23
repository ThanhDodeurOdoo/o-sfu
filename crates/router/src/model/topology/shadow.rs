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

use super::{RoutedConsumerId, RoutedProducerId};
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
/// one producer and one receiver shadow. Each shadow reference count is the
/// number of live routed consumers that still need that receiver session on the
/// source router.
#[derive(Debug, Clone, Default)]
pub(super) struct ShadowSessionTracker {
    consumer_edges: BTreeMap<RoutedConsumerId, ShadowConsumerEdge>,
    shadow_refcounts: BTreeMap<ShadowSessionKey, usize>,
    producer_owners: BTreeMap<RoutedProducerId, UserId>,
}

impl ShadowSessionTracker {
    pub(super) fn register_producer(&mut self, producer_id: RoutedProducerId, owner: UserId) {
        self.producer_owners.insert(producer_id, owner);
    }

    pub(super) fn register_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
        producer_id: RoutedProducerId,
        shadow_key: Option<ShadowSessionKey>,
    ) {
        let Some(shadow_key) = shadow_key else {
            return;
        };
        if let Some(count) = self.shadow_refcounts.get_mut(&shadow_key) {
            *count = count.saturating_add(1);
        } else {
            self.shadow_refcounts.insert(shadow_key.clone(), 1);
        }
        self.consumer_edges.insert(
            consumer_id,
            ShadowConsumerEdge {
                producer_id,
                shadow_key,
            },
        );
    }

    #[must_use]
    pub(super) fn contains_shadow_session(&self, shadow_key: &ShadowSessionKey) -> bool {
        self.shadow_refcounts.contains_key(shadow_key)
    }

    #[must_use]
    pub(super) fn unregister_producer(
        &mut self,
        producer_id: RoutedProducerId,
    ) -> BTreeSet<ShadowSessionKey> {
        self.producer_owners.remove(&producer_id);
        self.release_matching(|consumer| consumer.producer_id == producer_id)
    }

    #[must_use]
    pub(super) fn unregister_user(&mut self, user_id: &UserId) -> BTreeSet<ShadowSessionKey> {
        let mut producers = BTreeSet::new();
        for (producer_id, owner) in &self.producer_owners {
            if owner == user_id {
                producers.insert(*producer_id);
            }
        }
        for producer_id in &producers {
            self.producer_owners.remove(producer_id);
        }
        self.release_matching(|consumer| {
            consumer.shadow_key.user_id() == user_id || producers.contains(&consumer.producer_id)
        })
    }

    #[must_use]
    pub(super) fn release_consumers(
        &mut self,
        consumer_ids: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> BTreeSet<ShadowSessionKey> {
        let mut prune = BTreeSet::new();
        for consumer_id in consumer_ids {
            self.release_consumer(consumer_id, &mut prune);
        }
        prune
    }

    fn release_matching(
        &mut self,
        mut should_release: impl FnMut(&ShadowConsumerEdge) -> bool,
    ) -> BTreeSet<ShadowSessionKey> {
        let mut consumer_ids = BTreeSet::new();
        for (consumer_id, consumer) in &self.consumer_edges {
            if should_release(consumer) {
                consumer_ids.insert(*consumer_id);
            }
        }
        self.release_consumers(consumer_ids)
    }

    fn release_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
        prune: &mut BTreeSet<ShadowSessionKey>,
    ) {
        let Some(consumer) = self.consumer_edges.remove(&consumer_id) else {
            return;
        };
        if self.decrement_shadow_refcount(&consumer.shadow_key) {
            prune.insert(consumer.shadow_key);
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

#[derive(Debug, Clone)]
struct ShadowConsumerEdge {
    producer_id: RoutedProducerId,
    shadow_key: ShadowSessionKey,
}
