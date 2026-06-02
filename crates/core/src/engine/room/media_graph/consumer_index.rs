use std::collections::{BTreeMap, BTreeSet};

use super::{
    ConsumerKey, ConsumerSourceSelection, ConsumerState, TransportMediaRemoval,
    remove_from_index_set,
};
use crate::engine::{
    ConnectionId, UserId, room::topology::RoutedConsumerId, source_model::PublishedSourceId,
};

#[derive(Debug, Default)]
pub(super) struct ConsumerIndex {
    pub(super) selections: BTreeMap<ConsumerKey, ConsumerSourceSelection>,
    committed: BTreeMap<ConsumerKey, ConsumerState>,
    pub(super) pending_bootstraps: BTreeSet<ConsumerKey>,
    by_user: BTreeMap<UserId, BTreeSet<ConsumerKey>>,
    by_source: BTreeMap<PublishedSourceId, BTreeSet<ConsumerKey>>,
}

impl ConsumerIndex {
    pub(super) fn subscription_count(&self) -> usize {
        self.committed
            .len()
            .saturating_add(self.pending_bootstraps.len())
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn count(&self) -> usize {
        self.committed.len()
    }

    pub(super) fn set_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.selections
            .entry(key.clone())
            .and_modify(|selection| selection.set_active(active))
            .or_insert_with(|| ConsumerSourceSelection::open(active));
        self.register_key(key);
    }

    pub(super) fn ensure_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        if self.committed.contains_key(key) {
            self.selections.entry(key.clone()).or_insert(selection);
        } else {
            self.selections.insert(key.clone(), selection);
        }
        self.register_key(key);
    }

    pub(super) fn selection_mut_or_open(
        &mut self,
        key: ConsumerKey,
    ) -> &mut ConsumerSourceSelection {
        self.register_key(&key);
        self.selections
            .entry(key)
            .or_insert_with(|| ConsumerSourceSelection::open(true))
    }

    pub(super) fn reserve_bootstrap(&mut self, key: ConsumerKey) {
        self.register_key(&key);
        self.pending_bootstraps.insert(key);
    }

    pub(super) fn remove_pending_bootstrap(&mut self, key: &ConsumerKey) {
        self.pending_bootstraps.remove(key);
        self.prune_key_if_unused(key);
    }

    pub(super) fn commit(
        &mut self,
        key: ConsumerKey,
        state: ConsumerState,
        selection: ConsumerSourceSelection,
    ) -> bool {
        if self.committed.contains_key(&key) {
            return false;
        }
        self.selections.insert(key.clone(), selection);
        self.register_key(&key);
        self.committed.insert(key, state);
        true
    }

    pub(super) fn selection(&self, key: &ConsumerKey) -> Option<ConsumerSourceSelection> {
        self.selections.get(key).copied()
    }

    pub(super) fn consumer_state(&self, key: &ConsumerKey) -> Option<ConsumerState> {
        self.committed.get(key).copied()
    }

    pub(super) fn committed_consumer_transport_entries(
        &self,
    ) -> impl Iterator<Item = (UserId, ConnectionId)> + '_ {
        self.committed
            .iter()
            .map(|(key, state)| (key.consumer_user_id.clone(), state.consumer_connection_id))
    }

    pub(super) fn pending_consumer_user_ids(&self) -> impl Iterator<Item = &UserId> {
        self.pending_bootstraps
            .iter()
            .map(|key| &key.consumer_user_id)
    }

    pub(super) fn committed_entries(&self) -> impl Iterator<Item = (&ConsumerKey, ConsumerState)> {
        self.committed.iter().map(|(key, state)| (key, *state))
    }

    pub(super) fn pending_keys_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = &ConsumerKey> {
        self.by_user
            .get(user_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter(move |key| {
                self.pending_bootstraps.contains(*key) && !self.committed.contains_key(*key)
            })
    }

    pub(super) fn remove_key_state(&mut self, key: &ConsumerKey) {
        self.committed.remove(key);
        self.pending_bootstraps.remove(key);
        self.selections.remove(key);
        remove_from_index_set(&mut self.by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.by_source, &key.source_id, key);
    }

    pub(super) fn keys_for_user(&self, user_id: &UserId) -> Vec<ConsumerKey> {
        self.by_user
            .get(user_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn keys_for_source(&self, source_id: PublishedSourceId) -> Vec<ConsumerKey> {
        self.by_source
            .get(&source_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn affected_keys_for_user(
        &self,
        user_id: &UserId,
        user_source_ids: impl IntoIterator<Item = PublishedSourceId>,
    ) -> BTreeSet<ConsumerKey> {
        let mut keys = self.by_user.get(user_id).cloned().unwrap_or_default();
        for source_id in user_source_ids {
            if let Some(source_keys) = self.by_source.get(&source_id) {
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
            .filter_map(|key| self.committed.get(&key))
            .map(|state| state.routed_consumer_id)
            .collect()
    }

    pub(super) fn routed_consumer_ids_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Vec<RoutedConsumerId> {
        self.by_source
            .get(&source_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|key| self.committed.get(key))
            .map(|state| state.routed_consumer_id)
            .collect()
    }

    pub(super) fn transport_removals_for_keys(
        &self,
        keys: impl IntoIterator<Item = ConsumerKey>,
    ) -> Vec<TransportMediaRemoval> {
        keys.into_iter()
            .filter_map(|key| {
                let state = self.committed.get(&key)?;
                Some(TransportMediaRemoval::new(
                    key.consumer_user_id,
                    state.consumer_connection_id,
                    state.consumer_media,
                ))
            })
            .collect()
    }

    pub(super) fn transport_removals_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Vec<TransportMediaRemoval> {
        self.by_source
            .get(&source_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|key| {
                let state = self.committed.get(key)?;
                Some(TransportMediaRemoval::new(
                    key.consumer_user_id.clone(),
                    state.consumer_connection_id,
                    state.consumer_media,
                ))
            })
            .collect()
    }

    pub(super) fn has_bootstrap(&self, consumer_key: &ConsumerKey) -> bool {
        self.committed.contains_key(consumer_key) || self.pending_bootstraps.contains(consumer_key)
    }

    pub(super) fn contains(&self, key: &ConsumerKey) -> bool {
        self.committed.contains_key(key)
    }

    fn prune_key_if_unused(&mut self, key: &ConsumerKey) {
        if self.committed.contains_key(key)
            || self.pending_bootstraps.contains(key)
            || self.selections.contains_key(key)
        {
            return;
        }
        remove_from_index_set(&mut self.by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.by_source, &key.source_id, key);
    }

    fn register_key(&mut self, key: &ConsumerKey) {
        self.by_user
            .entry(key.consumer_user_id.clone())
            .or_default()
            .insert(key.clone());
        self.by_source
            .entry(key.source_id)
            .or_default()
            .insert(key.clone());
    }
}
