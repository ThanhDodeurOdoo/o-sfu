//! cross-router receiver shadow refcounts

#[cfg(not(kani))]
use std::collections::{BTreeMap, BTreeSet};

use o_sfu_model::UserId;

use super::{RoutedConsumerId, RoutedProducerId};
use crate::model::RouterId;
#[cfg(kani)]
use crate::model::proof_storage::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ShadowSessionKey {
    router_id: RouterId,
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

/// Tracks routed consumer edges that keep cross-router shadows alive.
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
        Self::increment_refcount(&mut self.shadow_refcounts, &shadow_key);
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
        let producers = self.producers_for(user_id);
        for producer_id in &producers {
            self.producer_owners.remove(producer_id);
        }
        self.release_matching(|consumer| {
            consumer.shadow_key.user_id() == user_id || producers.contains(&consumer.producer_id)
        })
    }

    #[must_use]
    pub(super) fn prunable_for_user(&self, user_id: &UserId) -> BTreeSet<ShadowSessionKey> {
        let producers = self.producers_for(user_id);
        let mut released_counts = BTreeMap::new();
        for consumer in self.consumer_edges.values() {
            if consumer.shadow_key.user_id() == user_id || producers.contains(&consumer.producer_id)
            {
                Self::increment_refcount(&mut released_counts, &consumer.shadow_key);
            }
        }
        released_counts
            .iter()
            .filter_map(|(key, released_count)| {
                self.shadow_refcounts
                    .get(key)
                    .filter(|count| **count <= *released_count)
                    .map(|_| key.clone())
            })
            .collect()
    }

    fn increment_refcount(counts: &mut BTreeMap<ShadowSessionKey, usize>, key: &ShadowSessionKey) {
        match counts.get_mut(key) {
            Some(count) => *count = count.saturating_add(1),
            None => _ = counts.insert(key.clone(), 1),
        }
    }

    fn producers_for(&self, user_id: &UserId) -> BTreeSet<RoutedProducerId> {
        self.producer_owners
            .iter()
            .filter_map(|(producer_id, owner)| (owner == user_id).then_some(*producer_id))
            .collect()
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
