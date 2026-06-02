use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use super::{
    ConsumerKey, ConsumerSourceSelection, ConsumerState, TransportMediaRemoval,
    remove_from_index_set, subscription::PendingConsumerBootstrapTarget,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{RelayRouteActivity, TransportMediaId, TransportRelayRouteAction},
    room::topology::RoutedConsumerId,
    source_model::PublishedSourceId,
};

/// receiver-source route graph
///
/// every entry tracks the receiver selection, bootstrap state, committed
/// consumer route and relay owner for one source relationship
///
/// relay effects are returned as transport commands so callers can mutate graph
/// state under room authority and execute transport work after releasing the lock
#[derive(Debug, Default)]
pub(super) struct RouteGraph {
    entries: BTreeMap<ConsumerKey, RouteEntry>,
    by_user: BTreeMap<UserId, BTreeSet<ConsumerKey>>,
    by_source: BTreeMap<PublishedSourceId, BTreeSet<ConsumerKey>>,
    relays: BTreeMap<RelayRouteKey, RelayOwners>,
    pending: usize,
    committed: usize,
}

type RelayOwners = BTreeMap<ConsumerKey, RelayRouteActivity>;

/// state for one receiver-source relationship
///
/// selection may exist before a transport consumer, state records whether
/// bootstrap or consumer transport exists and relay links pending relay transport
/// work to the same cleanup key
#[derive(Debug, Default)]
struct RouteEntry {
    selection: Option<ConsumerSourceSelection>,
    state: RouteState,
    relay: Option<RouteRelay>,
}

/// lifecycle stage that contributes to subscription counts
///
/// stored preserves receiver intent only, pending reserves accepted bootstrap
/// work and committed carries the transport consumer handle
#[derive(Debug, Default)]
enum RouteState {
    #[default]
    Stored,
    Pending,
    Committed(ConsumerState),
}

/// relay owner attached to one receiver-source route
///
/// the connection field rejects stale release or activity updates from replaced
/// sockets while the route key identifies the transport relay aggregate
#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteRelay {
    route: RelayRouteKey,
    connection: ConnectionId,
    activity: RelayRouteActivity,
}

/// relay transport change required after a graph mutation
///
/// graph methods return these values while still under room state authority so
/// callers can execute transport work after the lock is released
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::engine::room) struct RelayRouteEffect {
    pub route: RelayRouteKey,
    pub action: TransportRelayRouteAction,
}

/// aggregate key for one source media forwarded to one target worker
///
/// multiple consumer routes may share this key, so effects are emitted only when
/// the aggregate changes between no routes, inactive routes and active routes
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::engine::room) struct RelayRouteKey {
    pub source_user: UserId,
    pub source_connection: ConnectionId,
    pub source_media: TransportMediaId,
    pub target_worker: MediaWorkerId,
}

impl RouteGraph {
    pub(super) fn subscription_count(&self) -> usize {
        self.pending + self.committed
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn count(&self) -> usize {
        self.committed
    }

    pub(super) fn set_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.entry(key.clone()).selection().set_active(active);
    }

    pub(super) fn ensure_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        let entry = self.entry(key.clone());
        if entry.state.is_committed() {
            entry.selection.get_or_insert(selection);
        } else {
            entry.selection = Some(selection);
        }
    }

    pub(super) fn selection_mut_or_open(
        &mut self,
        key: ConsumerKey,
    ) -> &mut ConsumerSourceSelection {
        self.entry(key).selection()
    }

    pub(super) fn reserve_bootstrap(&mut self, key: ConsumerKey) {
        let reserved = {
            let entry = self.entry(key);
            if matches!(entry.state, RouteState::Stored) {
                entry.state = RouteState::Pending;
                true
            } else {
                false
            }
        };
        if reserved {
            self.pending += 1;
        }
    }

    pub(super) fn remove_pending_bootstrap(&mut self, key: &ConsumerKey) {
        let removed = if let Some(entry) = self.entries.get_mut(key)
            && matches!(entry.state, RouteState::Pending)
        {
            entry.state = RouteState::Stored;
            true
        } else {
            false
        };
        if removed {
            self.pending -= 1;
        }
        self.prune(key);
    }

    pub(super) fn commit(
        &mut self,
        key: ConsumerKey,
        state: ConsumerState,
        selection: ConsumerSourceSelection,
    ) -> bool {
        let was_pending = {
            let entry = self.entry(key);
            if entry.state.is_committed() {
                return false;
            }
            let was_pending = matches!(entry.state, RouteState::Pending);
            entry.selection = Some(selection);
            entry.state = RouteState::Committed(state);
            was_pending
        };
        if was_pending {
            self.pending -= 1;
        }
        self.committed += 1;
        true
    }

    pub(super) fn selection(&self, key: &ConsumerKey) -> Option<ConsumerSourceSelection> {
        self.entries.get(key).and_then(|entry| entry.selection)
    }

    pub(super) fn consumer_state(&self, key: &ConsumerKey) -> Option<ConsumerState> {
        self.entries.get(key)?.state.consumer()
    }

    pub(super) fn committed_consumer_transport_entries(
        &self,
    ) -> impl Iterator<Item = (UserId, ConnectionId)> + '_ {
        self.committed_entries()
            .map(|(key, state)| (key.consumer_user_id.clone(), state.consumer_connection_id))
    }

    pub(super) fn pending_consumer_user_ids(&self) -> impl Iterator<Item = &UserId> {
        self.entries.iter().filter_map(|(key, entry)| {
            matches!(entry.state, RouteState::Pending).then_some(&key.consumer_user_id)
        })
    }

    pub(super) fn committed_entries(&self) -> impl Iterator<Item = (&ConsumerKey, ConsumerState)> {
        self.entries
            .iter()
            .filter_map(|(key, entry)| Some((key, entry.state.consumer()?)))
    }

    pub(super) fn pending_keys_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = &ConsumerKey> {
        self.by_user
            .get(user_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter(|key| {
                self.entries
                    .get(*key)
                    .is_some_and(|entry| matches!(entry.state, RouteState::Pending))
            })
    }

    pub(super) fn remove_key_state(&mut self, key: &ConsumerKey) -> Vec<RelayRouteEffect> {
        let Some(entry) = self.entries.remove(key) else {
            return Vec::new();
        };
        self.drop_count(&entry.state);
        remove_from_index_set(&mut self.by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.by_source, &key.source_id, key);
        entry
            .relay
            .map_or_else(Vec::new, |relay| self.release_relay(key, &relay))
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
            .filter_map(|key| self.consumer_state(&key))
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
            .filter_map(|key| self.consumer_state(key))
            .map(|state| state.routed_consumer_id)
            .collect()
    }

    pub(super) fn transport_removals_for_keys(
        &self,
        keys: impl IntoIterator<Item = ConsumerKey>,
    ) -> Vec<TransportMediaRemoval> {
        keys.into_iter()
            .filter_map(|key| {
                let state = self.consumer_state(&key)?;
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
                let state = self.consumer_state(key)?;
                Some(TransportMediaRemoval::new(
                    key.consumer_user_id.clone(),
                    state.consumer_connection_id,
                    state.consumer_media,
                ))
            })
            .collect()
    }

    pub(super) fn has_bootstrap(&self, key: &ConsumerKey) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.state.has_bootstrap())
    }

    pub(super) fn contains(&self, key: &ConsumerKey) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.state.is_committed())
    }

    pub(super) fn reserve_relay(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        source_connection: ConnectionId,
        source_media: TransportMediaId,
        target_worker: MediaWorkerId,
        active: bool,
    ) -> Vec<RelayRouteEffect> {
        let key = ConsumerKey::new(target.consumer_user_id(), target.source_id());
        let relay = RouteRelay {
            route: RelayRouteKey {
                source_user: target.producer_user_id().clone(),
                source_connection,
                source_media,
                target_worker,
            },
            connection: target.consumer_connection_id(),
            activity: RelayRouteActivity::from_active(active),
        };
        let previous = {
            let entry = self.entry(key.clone());
            if entry.relay.as_ref() == Some(&relay) {
                return Vec::new();
            }
            entry.relay.replace(relay.clone())
        };
        self.replace_relay(&key, previous, &relay)
    }

    pub(super) fn set_relay_active(
        &mut self,
        consumer_user_id: &UserId,
        consumer_connection_id: ConnectionId,
        source_id: PublishedSourceId,
        activity: RelayRouteActivity,
    ) -> Vec<RelayRouteEffect> {
        let key = ConsumerKey::new(consumer_user_id, source_id);
        let Some(entry) = self.entries.get_mut(&key) else {
            return Vec::new();
        };
        let Some(relay) = entry.set_relay_activity(consumer_connection_id, activity) else {
            return Vec::new();
        };
        self.set_relay_owner(&key, &relay, false)
    }

    pub(super) fn release_target(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Vec<RelayRouteEffect> {
        let key = ConsumerKey::new(target.consumer_user_id(), target.source_id());
        let Some(relay) = self
            .entries
            .get_mut(&key)
            .and_then(|entry| entry.take_relay(target.consumer_connection_id()))
        else {
            return Vec::new();
        };
        let effects = self.release_relay(&key, &relay);
        self.prune(&key);
        effects
    }

    fn entry(&mut self, key: ConsumerKey) -> &mut RouteEntry {
        self.by_user
            .entry(key.consumer_user_id.clone())
            .or_default()
            .insert(key.clone());
        self.by_source
            .entry(key.source_id)
            .or_default()
            .insert(key.clone());
        self.entries.entry(key).or_default()
    }

    fn prune(&mut self, key: &ConsumerKey) {
        if self.entries.get(key).is_some_and(RouteEntry::is_used) {
            return;
        }
        self.entries.remove(key);
        remove_from_index_set(&mut self.by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.by_source, &key.source_id, key);
    }

    fn drop_count(&mut self, state: &RouteState) {
        match state {
            RouteState::Stored => {}
            RouteState::Pending => self.pending -= 1,
            RouteState::Committed(_) => self.committed -= 1,
        }
    }

    fn replace_relay(
        &mut self,
        key: &ConsumerKey,
        previous: Option<RouteRelay>,
        relay: &RouteRelay,
    ) -> Vec<RelayRouteEffect> {
        match previous {
            None => self.set_relay_owner(key, relay, true),
            Some(previous)
                if previous.route == relay.route && previous.connection == relay.connection =>
            {
                self.set_relay_owner(key, relay, false)
            }
            Some(previous) => {
                let mut effects = self.release_relay(key, &previous);
                effects.extend(self.set_relay_owner(key, relay, true));
                effects
            }
        }
    }

    fn set_relay_owner(
        &mut self,
        key: &ConsumerKey,
        relay: &RouteRelay,
        insert_missing: bool,
    ) -> Vec<RelayRouteEffect> {
        let route = relay.route.clone();
        let owners = match self.relays.entry(route.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) if insert_missing => entry.insert(RelayOwners::default()),
            Entry::Vacant(_) => return Vec::new(),
        };
        let before = relay_aggregate(owners);
        owners.insert(key.clone(), relay.activity);
        relay_effects_for(route, before, relay_aggregate(owners))
    }

    fn release_relay(&mut self, key: &ConsumerKey, relay: &RouteRelay) -> Vec<RelayRouteEffect> {
        let route = relay.route.clone();
        let Some(owners) = self.relays.get_mut(&route) else {
            return Vec::new();
        };
        let before = relay_aggregate(owners);
        if owners.remove(key).is_none() {
            return Vec::new();
        }
        let after = relay_aggregate(owners);
        if after.is_none() {
            self.relays.remove(&route);
        }
        relay_effects_for(route, before, after)
    }
}

impl RouteEntry {
    fn selection(&mut self) -> &mut ConsumerSourceSelection {
        self.selection
            .get_or_insert_with(|| ConsumerSourceSelection::open(true))
    }

    fn is_used(&self) -> bool {
        self.selection.is_some()
            || !matches!(self.state, RouteState::Stored)
            || self.relay.is_some()
    }

    fn set_relay_activity(
        &mut self,
        connection: ConnectionId,
        activity: RelayRouteActivity,
    ) -> Option<RouteRelay> {
        let relay = self.relay.as_mut()?;
        if relay.connection != connection || relay.activity == activity {
            return None;
        }
        relay.activity = activity;
        Some(relay.clone())
    }

    fn take_relay(&mut self, connection: ConnectionId) -> Option<RouteRelay> {
        if self
            .relay
            .as_ref()
            .is_some_and(|relay| relay.connection == connection)
        {
            self.relay.take()
        } else {
            None
        }
    }
}

impl RouteState {
    const fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }

    const fn has_bootstrap(&self) -> bool {
        matches!(self, Self::Pending | Self::Committed(_))
    }

    const fn consumer(&self) -> Option<ConsumerState> {
        match self {
            Self::Committed(state) => Some(*state),
            Self::Stored | Self::Pending => None,
        }
    }
}

fn relay_aggregate(owners: &RelayOwners) -> Option<RelayRouteActivity> {
    (!owners.is_empty()).then(|| {
        RelayRouteActivity::from_active(owners.values().copied().any(RelayRouteActivity::is_active))
    })
}

fn relay_effects_for(
    route: RelayRouteKey,
    before: Option<RelayRouteActivity>,
    after: Option<RelayRouteActivity>,
) -> Vec<RelayRouteEffect> {
    let Some(activity) = after else {
        return vec![RelayRouteEffect {
            route,
            action: TransportRelayRouteAction::Release,
        }];
    };
    let mut effects = Vec::new();
    if before.is_none() {
        effects.push(RelayRouteEffect {
            route: route.clone(),
            action: TransportRelayRouteAction::Install,
        });
    }
    if before.unwrap_or(RelayRouteActivity::Inactive) != activity {
        effects.push(RelayRouteEffect {
            route,
            action: TransportRelayRouteAction::SetActivity(activity),
        });
    }
    effects
}
