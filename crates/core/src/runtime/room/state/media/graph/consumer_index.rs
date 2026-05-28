use std::collections::{BTreeMap, BTreeSet};

use super::{
    ConsumerKey, ConsumerSourceSelection, ConsumerState, TransportMediaRemoval,
    remove_from_index_set,
};
use crate::runtime::{
    ConnectionId, UserId, room::topology::RoutedConsumerId, source_model::PublishedSourceId,
};

#[derive(Debug, Default)]
pub(super) struct ConsumerIndex {
    consumer_source_selections: BTreeMap<ConsumerKey, ConsumerSourceSelection>,
    committed_consumers: BTreeMap<ConsumerKey, ConsumerState>,
    pending_consumer_bootstraps: BTreeSet<ConsumerKey>,
    consumer_keys_by_user: BTreeMap<UserId, BTreeSet<ConsumerKey>>,
    consumer_keys_by_source: BTreeMap<PublishedSourceId, BTreeSet<ConsumerKey>>,
}

impl ConsumerIndex {
    pub(super) fn subscription_count(&self) -> usize {
        self.committed_consumers
            .len()
            .saturating_add(self.pending_consumer_bootstraps.len())
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn consumer_count(&self) -> usize {
        self.committed_consumers.len()
    }

    pub(super) fn set_source_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.consumer_source_selections
            .entry(key.clone())
            .and_modify(|selection| selection.set_active(active))
            .or_insert_with(|| ConsumerSourceSelection::open(active));
        self.register_key(key);
    }

    pub(super) fn ensure_source_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        if self.committed_consumers.contains_key(key) {
            self.consumer_source_selections
                .entry(key.clone())
                .or_insert(selection);
        } else {
            self.consumer_source_selections
                .insert(key.clone(), selection);
        }
        self.register_key(key);
    }

    pub(super) fn selection_mut_or_open(
        &mut self,
        key: ConsumerKey,
    ) -> &mut ConsumerSourceSelection {
        self.register_key(&key);
        self.consumer_source_selections
            .entry(key)
            .or_insert_with(|| ConsumerSourceSelection::open(true))
    }

    pub(super) fn reserve_bootstrap(&mut self, key: ConsumerKey) {
        self.register_key(&key);
        self.pending_consumer_bootstraps.insert(key);
    }

    pub(super) fn remove_pending_bootstrap(&mut self, key: &ConsumerKey) {
        self.pending_consumer_bootstraps.remove(key);
        self.prune_key_if_unused(key);
    }

    pub(super) fn commit(
        &mut self,
        key: ConsumerKey,
        state: ConsumerState,
        selection: ConsumerSourceSelection,
    ) -> bool {
        if self.committed_consumers.contains_key(&key) {
            return false;
        }
        self.consumer_source_selections
            .insert(key.clone(), selection);
        self.register_key(&key);
        self.committed_consumers.insert(key, state);
        true
    }

    pub(super) fn source_selection(&self, key: &ConsumerKey) -> Option<ConsumerSourceSelection> {
        self.consumer_source_selections.get(key).copied()
    }

    pub(super) fn consumer_state(&self, key: &ConsumerKey) -> Option<ConsumerState> {
        self.committed_consumers.get(key).copied()
    }

    pub(super) fn committed_consumer_transport_entries(
        &self,
    ) -> impl Iterator<Item = (UserId, ConnectionId)> + '_ {
        self.committed_consumers
            .iter()
            .map(|(key, state)| (key.consumer_user_id.clone(), state.consumer_connection_id))
    }

    pub(super) fn pending_consumer_user_ids(&self) -> impl Iterator<Item = &UserId> {
        self.pending_consumer_bootstraps
            .iter()
            .map(|key| &key.consumer_user_id)
    }

    pub(super) fn committed_entries(&self) -> impl Iterator<Item = (&ConsumerKey, ConsumerState)> {
        self.committed_consumers
            .iter()
            .map(|(key, state)| (key, *state))
    }

    pub(super) fn pending_keys_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = &ConsumerKey> {
        self.consumer_keys_by_user
            .get(user_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter(move |key| {
                self.pending_consumer_bootstraps.contains(*key)
                    && !self.committed_consumers.contains_key(*key)
            })
    }

    pub(super) fn remove_key_state(&mut self, key: &ConsumerKey) {
        self.committed_consumers.remove(key);
        self.pending_consumer_bootstraps.remove(key);
        self.consumer_source_selections.remove(key);
        remove_from_index_set(&mut self.consumer_keys_by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.consumer_keys_by_source, &key.source_id, key);
    }

    pub(super) fn keys_for_user(&self, user_id: &UserId) -> Vec<ConsumerKey> {
        self.consumer_keys_by_user
            .get(user_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn keys_for_source(&self, source_id: PublishedSourceId) -> Vec<ConsumerKey> {
        self.consumer_keys_by_source
            .get(&source_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn affected_keys_for_user(
        &self,
        user_id: &UserId,
        user_source_ids: impl IntoIterator<Item = PublishedSourceId>,
    ) -> BTreeSet<ConsumerKey> {
        let mut keys = self
            .consumer_keys_by_user
            .get(user_id)
            .cloned()
            .unwrap_or_default();
        for source_id in user_source_ids {
            if let Some(source_keys) = self.consumer_keys_by_source.get(&source_id) {
                keys.extend(source_keys.iter().cloned());
            }
        }
        keys
    }

    pub(super) fn routed_consumer_ids_for_keys(
        &self,
        keys: impl IntoIterator<Item = ConsumerKey>,
    ) -> Vec<RoutedConsumerId> {
        keys.into_iter()
            .filter_map(|key| self.committed_consumers.get(&key))
            .map(|consumer_state| consumer_state.routed_consumer_id)
            .collect()
    }

    pub(super) fn routed_consumer_ids_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Vec<RoutedConsumerId> {
        self.consumer_keys_by_source
            .get(&source_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|key| self.committed_consumers.get(key))
            .map(|consumer_state| consumer_state.routed_consumer_id)
            .collect()
    }

    pub(super) fn transport_removals_for_keys(
        &self,
        keys: impl IntoIterator<Item = ConsumerKey>,
    ) -> Vec<TransportMediaRemoval> {
        keys.into_iter()
            .filter_map(|key| {
                let consumer_state = self.committed_consumers.get(&key)?;
                Some(TransportMediaRemoval::new(
                    key.consumer_user_id,
                    consumer_state.consumer_connection_id,
                    consumer_state.consumer_media,
                ))
            })
            .collect()
    }

    pub(super) fn transport_removals_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Vec<TransportMediaRemoval> {
        self.consumer_keys_by_source
            .get(&source_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|key| {
                let consumer_state = self.committed_consumers.get(key)?;
                Some(TransportMediaRemoval::new(
                    key.consumer_user_id.clone(),
                    consumer_state.consumer_connection_id,
                    consumer_state.consumer_media,
                ))
            })
            .collect()
    }

    pub(super) fn bootstrap_exists(&self, consumer_key: &ConsumerKey) -> bool {
        self.committed_consumers.contains_key(consumer_key)
            || self.pending_consumer_bootstraps.contains(consumer_key)
    }

    pub(super) fn contains_consumer(&self, key: &ConsumerKey) -> bool {
        self.committed_consumers.contains_key(key)
    }

    fn prune_key_if_unused(&mut self, key: &ConsumerKey) {
        if self.committed_consumers.contains_key(key)
            || self.pending_consumer_bootstraps.contains(key)
            || self.consumer_source_selections.contains_key(key)
        {
            return;
        }
        remove_from_index_set(&mut self.consumer_keys_by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.consumer_keys_by_source, &key.source_id, key);
    }

    #[cfg(test)]
    pub(super) fn contains_pending_bootstrap(&self, key: &ConsumerKey) -> bool {
        self.pending_consumer_bootstraps.contains(key)
    }

    fn register_key(&mut self, key: &ConsumerKey) {
        self.consumer_keys_by_user
            .entry(key.consumer_user_id.clone())
            .or_default()
            .insert(key.clone());
        self.consumer_keys_by_source
            .entry(key.source_id)
            .or_default()
            .insert(key.clone());
    }
}
