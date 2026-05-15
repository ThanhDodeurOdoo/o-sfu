use std::collections::BTreeMap;

use super::subscription::PendingConsumerBootstrapTarget;
use crate::{
    runtime::{
        ConnectionId, UserId, media_transport::TransportMediaId, source_model::PublishedSourceId,
    },
    transport::TransportRelayRouteAction,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct RelayRouteEffect {
    pub route: RelayRouteKey,
    pub action: TransportRelayRouteAction,
}

#[derive(Debug, Default)]
pub(in crate::runtime::room::state) struct RoomRelayRoutes {
    routes: BTreeMap<RelayRouteKey, RelayRouteOwners>,
}

type RelayRouteOwners = BTreeMap<RelayOwnerKey, bool>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::runtime::room) struct RelayRouteKey {
    pub source_user: UserId,
    pub source_connection: ConnectionId,
    pub source_media: TransportMediaId,
    pub target_worker: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RelayOwnerKey(UserId, ConnectionId, PublishedSourceId);

impl RoomRelayRoutes {
    pub(super) fn reserve_consumer(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        source_connection_id: ConnectionId,
        source_transport_media_id: TransportMediaId,
        target_media_worker_id: usize,
        active: bool,
    ) -> Vec<RelayRouteEffect> {
        let route_key = RelayRouteKey {
            source_user: target.producer_user_id().clone(),
            source_connection: source_connection_id,
            source_media: source_transport_media_id,
            target_worker: target_media_worker_id,
        };
        let owner_key = RelayOwnerKey::from_target(target);
        let owners = self.routes.entry(route_key.clone()).or_default();
        let before = aggregate(owners);
        if owners.insert(owner_key, active) == Some(active) {
            return Vec::new();
        }
        relay_effects_for(route_key, before, aggregate(owners))
    }

    pub(super) fn set_consumer_active(
        &mut self,
        consumer_user_id: &UserId,
        consumer_connection_id: ConnectionId,
        source_id: PublishedSourceId,
        active: bool,
    ) -> Vec<RelayRouteEffect> {
        let owner_key = RelayOwnerKey(consumer_user_id.clone(), consumer_connection_id, source_id);
        let Some(route_key) = self.route_key_for(&owner_key) else {
            return Vec::new();
        };
        let Some(owners) = self.routes.get_mut(&route_key) else {
            return Vec::new();
        };
        let before = aggregate(owners);
        let Some(owner_active) = owners.get_mut(&owner_key) else {
            return Vec::new();
        };
        if *owner_active == active {
            return Vec::new();
        }
        *owner_active = active;
        relay_effects_for(route_key, before, aggregate(owners))
    }

    pub(super) fn release_target(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Vec<RelayRouteEffect> {
        self.release_owner(&RelayOwnerKey::from_target(target))
    }

    pub fn release_consumer_key(
        &mut self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> Vec<RelayRouteEffect> {
        let owner_keys = self
            .routes
            .values()
            .flat_map(|owners| owners.keys())
            .filter(|owner| owner.0 == *consumer_user_id && owner.2 == source_id)
            .cloned()
            .collect::<Vec<_>>();
        owner_keys
            .into_iter()
            .flat_map(|owner| self.release_owner(&owner))
            .collect()
    }

    fn release_owner(&mut self, owner_key: &RelayOwnerKey) -> Vec<RelayRouteEffect> {
        let Some(route_key) = self.route_key_for(owner_key) else {
            return Vec::new();
        };
        let Some(owners) = self.routes.get_mut(&route_key) else {
            return Vec::new();
        };
        let before = aggregate(owners);
        owners.remove(owner_key);
        let after = aggregate(owners);
        if after.0 {
            self.routes.remove(&route_key);
        }
        relay_effects_for(route_key, before, after)
    }

    fn route_key_for(&self, owner_key: &RelayOwnerKey) -> Option<RelayRouteKey> {
        self.routes
            .iter()
            .find_map(|(key, owners)| owners.contains_key(owner_key).then(|| key.clone()))
    }
}

impl RelayOwnerKey {
    fn from_target(target: &PendingConsumerBootstrapTarget) -> Self {
        Self(
            target.consumer_user_id().clone(),
            target.consumer_connection_id(),
            target.source_id(),
        )
    }
}

fn aggregate(owners: &RelayRouteOwners) -> (bool, bool) {
    (owners.is_empty(), owners.values().any(|active| *active))
}

fn relay_effect(route: RelayRouteKey, action: TransportRelayRouteAction) -> RelayRouteEffect {
    RelayRouteEffect { route, action }
}

fn relay_effects_for(
    route: RelayRouteKey,
    before: (bool, bool),
    after: (bool, bool),
) -> Vec<RelayRouteEffect> {
    if after.0 {
        return vec![relay_effect(route, TransportRelayRouteAction::Release)];
    }
    let mut effects = Vec::new();
    if before.0 {
        effects.push(relay_effect(
            route.clone(),
            TransportRelayRouteAction::Install,
        ));
    }
    if before.1 != after.1 {
        effects.push(relay_effect(
            route,
            TransportRelayRouteAction::SetActive(after.1),
        ));
    }
    effects
}
