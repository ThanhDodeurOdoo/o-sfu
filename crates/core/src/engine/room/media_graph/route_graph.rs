use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use super::{
    ConsumerKey, ConsumerSourceSelection, ConsumerState, TransportMediaRemoval,
    consumer_setup::ConsumerSetupTarget, remove_from_index_set,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{
        RelayRouteActivity, TransportMediaId, TransportRelayRouteAction, TransportSessionKey,
    },
    source_model::PublishedSourceId,
};

#[derive(Debug, Default)]
pub(super) struct RouteGraph {
    entries: BTreeMap<ConsumerKey, RouteSlot>,
    by_user: BTreeMap<UserId, BTreeSet<ConsumerKey>>,
    by_source: BTreeMap<PublishedSourceId, BTreeSet<ConsumerKey>>,
    relays: BTreeMap<RelayRouteKey, RelayOwners>,
    next_reservation: RouteReservationId,
}

type RelayOwners = BTreeMap<ConsumerKey, RelayRouteActivity>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RouteReservationId(u64);

/// reservation token for one pending consumer setup slot
///
/// stale reservations must not commit or release a newer pending route for the
/// same [`ConsumerKey`]
#[derive(Debug)]
pub struct ConsumerRouteReservation {
    key: ConsumerKey,
    selection: ConsumerSourceSelection,
    id: RouteReservationId,
}

#[derive(Debug)]
enum RouteSlot {
    Intent(ConsumerSourceSelection),
    Pending(
        ConsumerSourceSelection,
        RouteReservationId,
        Option<RouteRelay>,
    ),
    Committed(ConsumerSourceSelection, ConsumerState, Option<RouteRelay>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteRelay {
    route: RelayRouteKey,
    connection: ConnectionId,
    activity: RelayRouteActivity,
}

#[derive(Debug)]
struct TakenPendingRoute {
    relay: Option<RouteRelay>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayRouteEffect {
    pub route: RelayRouteKey,
    pub action: TransportRelayRouteAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRelayRouteEffect {
    pub route: RelayRouteKey,
    pub source_session_key: TransportSessionKey,
    pub action: TransportRelayRouteAction,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelayRouteKey {
    pub source_user: UserId,
    pub source_connection: ConnectionId,
    pub source_media: TransportMediaId,
    pub target_worker: MediaWorkerId,
}

impl RouteGraph {
    pub(super) fn subscription_count(&self) -> usize {
        self.entries
            .values()
            .filter(|slot| slot.has_consumer_setup_or_route())
            .count()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn count(&self) -> usize {
        self.entries
            .values()
            .filter(|slot| slot.is_committed())
            .count()
    }

    pub(super) fn set_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.entry(key.clone()).selection().set_active(active);
    }

    #[cfg(test)]
    pub(super) fn ensure_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        let entry = self.entry(key.clone());
        if !entry.is_committed() {
            entry.set_selection(selection);
        }
    }

    pub(super) fn selection_mut_or_open(
        &mut self,
        key: ConsumerKey,
    ) -> &mut ConsumerSourceSelection {
        self.entry(key).selection()
    }

    pub(super) fn reserve_consumer_setup(
        &mut self,
        key: ConsumerKey,
        selection: ConsumerSourceSelection,
    ) -> Option<ConsumerRouteReservation> {
        if self
            .entries
            .get(&key)
            .is_some_and(RouteSlot::has_consumer_setup_or_route)
        {
            return None;
        }
        let id = self.next_reservation();
        let entry = self.entry(key.clone());
        *entry = RouteSlot::Pending(selection, id, None);
        Some(ConsumerRouteReservation { key, selection, id })
    }

    pub(super) fn release_consumer_setup(
        &mut self,
        reservation: ConsumerRouteReservation,
    ) -> Vec<RelayRouteEffect> {
        let key = reservation.key;
        let Some(entry) = self.entries.get_mut(&key) else {
            return Vec::new();
        };
        let Some(pending) = entry.take_pending(reservation.id) else {
            return Vec::new();
        };
        pending
            .relay
            .map_or_else(Vec::new, |relay| self.release_relay(&key, &relay))
    }

    pub(super) fn commit(
        &mut self,
        reservation: &ConsumerRouteReservation,
        state: ConsumerState,
        selection: ConsumerSourceSelection,
    ) -> bool {
        let Some(entry) = self.entries.get_mut(&reservation.key) else {
            return false;
        };
        let Some(pending) = entry.take_pending(reservation.id) else {
            return false;
        };
        *entry = RouteSlot::Committed(selection, state, pending.relay);
        true
    }

    pub(super) fn selection(&self, key: &ConsumerKey) -> Option<ConsumerSourceSelection> {
        self.entries.get(key).map(RouteSlot::selection_value)
    }

    pub(super) fn consumer_state(&self, key: &ConsumerKey) -> Option<&ConsumerState> {
        self.entries.get(key)?.consumer()
    }

    pub(super) fn committed_consumer_connection_ids(
        &self,
    ) -> impl Iterator<Item = ConnectionId> + '_ {
        self.committed_entries()
            .map(|(_, state)| state.consumer_connection_id)
    }

    pub(super) fn pending_consumer_user_ids(&self) -> impl Iterator<Item = &UserId> {
        self.entries
            .iter()
            .filter_map(|(key, entry)| entry.is_pending().then_some(&key.consumer_user_id))
    }

    pub(super) fn committed_entries(&self) -> impl Iterator<Item = (&ConsumerKey, &ConsumerState)> {
        self.entries
            .iter()
            .filter_map(|(key, entry)| entry.consumer().map(|state| (key, state)))
    }

    pub(super) fn pending_keys_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = &ConsumerKey> {
        self.by_user
            .get(user_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter(|key| self.entries.get(*key).is_some_and(RouteSlot::is_pending))
    }

    pub(super) fn committed_entries_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = (&ConsumerKey, &ConsumerState)> {
        self.by_user
            .get(user_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|key| self.entries.get(key)?.consumer().map(|state| (key, state)))
    }

    pub(super) fn remove_key_state(&mut self, key: &ConsumerKey) -> Vec<RelayRouteEffect> {
        let Some(entry) = self.entries.remove(key) else {
            return Vec::new();
        };
        remove_from_index_set(&mut self.by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.by_source, &key.source_id, key);
        entry
            .into_relay()
            .map_or_else(Vec::new, |relay| self.release_relay(key, &relay))
    }

    pub(super) fn keys_for_user(&self, user_id: &UserId) -> Vec<ConsumerKey> {
        self.by_user
            .get(user_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn keys_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> impl Iterator<Item = &ConsumerKey> {
        self.by_source
            .get(&source_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
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

    pub(super) fn has_consumer_setup_or_route(&self, key: &ConsumerKey) -> bool {
        self.entries
            .get(key)
            .is_some_and(RouteSlot::has_consumer_setup_or_route)
    }

    pub(super) fn contains(&self, key: &ConsumerKey) -> bool {
        self.entries.get(key).is_some_and(RouteSlot::is_committed)
    }

    pub(super) fn reserve_relay(
        &mut self,
        reservation: &ConsumerRouteReservation,
        target: &ConsumerSetupTarget,
        target_worker: MediaWorkerId,
        active: bool,
    ) -> Vec<RelayRouteEffect> {
        let key = &reservation.key;
        let (previous, relay) = {
            let Some(entry) = self.entries.get_mut(key) else {
                return Vec::new();
            };
            let RouteSlot::Pending(_, id, pending_relay) = entry else {
                return Vec::new();
            };
            if *id != reservation.id {
                return Vec::new();
            }
            let relay = RouteRelay {
                route: target.relay_route_key(target_worker),
                connection: target.connection,
                activity: RelayRouteActivity::from_active(active),
            };
            if pending_relay.as_ref() == Some(&relay) {
                return Vec::new();
            }
            (pending_relay.replace(relay.clone()), relay)
        };
        self.replace_relay(key, previous, &relay)
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

    fn entry(&mut self, key: ConsumerKey) -> &mut RouteSlot {
        self.by_user
            .entry(key.consumer_user_id.clone())
            .or_default()
            .insert(key.clone());
        self.by_source
            .entry(key.source_id)
            .or_default()
            .insert(key.clone());
        self.entries
            .entry(key)
            .or_insert_with(|| RouteSlot::Intent(ConsumerSourceSelection::open(true)))
    }

    fn next_reservation(&mut self) -> RouteReservationId {
        self.next_reservation.0 += 1;
        self.next_reservation
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

impl ConsumerRouteReservation {
    pub(super) const fn key(&self) -> &ConsumerKey {
        &self.key
    }

    pub const fn selection(&self) -> ConsumerSourceSelection {
        self.selection
    }
}

impl RouteSlot {
    const fn selection_value(&self) -> ConsumerSourceSelection {
        match self {
            Self::Intent(selection)
            | Self::Pending(selection, _, _)
            | Self::Committed(selection, _, _) => *selection,
        }
    }

    fn selection(&mut self) -> &mut ConsumerSourceSelection {
        match self {
            Self::Intent(selection)
            | Self::Pending(selection, _, _)
            | Self::Committed(selection, _, _) => selection,
        }
    }

    #[cfg(test)]
    fn set_selection(&mut self, selection: ConsumerSourceSelection) {
        *self.selection() = selection;
    }

    fn take_pending(&mut self, expected_id: RouteReservationId) -> Option<TakenPendingRoute> {
        let Self::Pending(selection, id, relay) = self else {
            return None;
        };
        if *id != expected_id {
            return None;
        }
        let selection = *selection;
        let relay = relay.take();
        *self = Self::Intent(selection);
        Some(TakenPendingRoute { relay })
    }

    const fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(..))
    }

    const fn is_pending(&self) -> bool {
        matches!(self, Self::Pending(..))
    }

    const fn has_consumer_setup_or_route(&self) -> bool {
        matches!(self, Self::Pending(..) | Self::Committed(..))
    }

    const fn consumer(&self) -> Option<&ConsumerState> {
        match self {
            Self::Committed(_, state, _) => Some(state),
            Self::Intent(_) | Self::Pending(_, _, _) => None,
        }
    }

    fn set_relay_activity(
        &mut self,
        connection: ConnectionId,
        activity: RelayRouteActivity,
    ) -> Option<RouteRelay> {
        let relay = match self {
            Self::Intent(_) => return None,
            Self::Pending(_, _, relay) | Self::Committed(_, _, relay) => relay.as_mut()?,
        };
        if relay.connection != connection || relay.activity == activity {
            return None;
        }
        relay.activity = activity;
        Some(relay.clone())
    }

    fn into_relay(self) -> Option<RouteRelay> {
        match self {
            Self::Intent(_) => None,
            Self::Pending(_, _, relay) | Self::Committed(_, _, relay) => relay,
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
