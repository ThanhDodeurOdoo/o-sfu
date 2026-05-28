use std::collections::{BTreeMap, BTreeSet};

use super::{
    ProducerRouteTarget, ProducerRuntimeId, PublishedProducer, PublishedSourceInstall, SourceKey,
    SourceTransportMediaIndexEntry, TransportMediaRemoval, remove_from_index_set,
};
use crate::runtime::{
    ConnectionId, UserId,
    media_transport::TransportMediaId,
    source_model::{
        ActiveSpeakerGroup, ActiveSpeakerSourceRole, PublishedSourceDescriptor, PublishedSourceId,
        UserStreamId,
    },
};

#[derive(Debug, Default)]
pub(super) struct SourceIndex {
    pub(super) descriptors: BTreeMap<PublishedSourceId, PublishedSourceDescriptor>,
    pub(super) source_ids_by_owner_stream: BTreeMap<SourceKey, PublishedSourceId>,
    pub(super) source_ids_by_owner: BTreeMap<UserId, BTreeSet<PublishedSourceId>>,
    pub(super) producer_id_by_source_id: BTreeMap<PublishedSourceId, ProducerRuntimeId>,
    pub(super) producer_ids_by_owner: BTreeMap<UserId, BTreeSet<ProducerRuntimeId>>,
    pub(super) producers: BTreeMap<ProducerRuntimeId, PublishedProducer>,
    pub(super) source_transport_media_index:
        BTreeMap<TransportMediaId, SourceTransportMediaIndexEntry>,
}

impl SourceIndex {
    pub(super) fn publication_count(&self) -> usize {
        self.descriptors.len()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn producer_count(&self) -> usize {
        self.producers.len()
    }

    pub(super) fn sources(&self) -> impl Iterator<Item = &PublishedSourceDescriptor> {
        self.descriptors.values()
    }

    pub(super) fn producers(
        &self,
    ) -> impl Iterator<Item = (ProducerRuntimeId, &PublishedProducer)> {
        self.producers
            .iter()
            .map(|(producer_id, producer)| (*producer_id, producer))
    }

    pub(super) fn active_producer_stream_owners(
        &self,
    ) -> impl Iterator<Item = (&UserStreamId, &UserId)> {
        self.producers
            .values()
            .filter(|producer| producer.active)
            .map(|producer| (&producer.stream_id, &producer.owner_user_id))
    }

    pub(super) fn source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSourceDescriptor> {
        self.descriptors.get(&source_id)
    }

    pub(super) fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.source_transport_media_index.get(&transport_media_id)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn first_published_transport_media_id(&self) -> Option<TransportMediaId> {
        self.producers
            .values()
            .find_map(|producer| producer.transport_media_id)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<TransportMediaId> {
        let producer_id = self.producer_id_for_source_key(&SourceKey::new(user_id, stream_id))?;
        let producer = self.producers.get(&producer_id)?;
        if producer.owner_connection_id == connection_id {
            producer.transport_media_id
        } else {
            None
        }
    }

    pub(super) fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.source_ids_by_owner_stream
            .get(&SourceKey::new(owner_user_id, stream_id))
            .copied()
    }

    pub(super) fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        let producer_id =
            self.producer_id_for_source_key(&SourceKey::new(owner_user_id, stream_id))?;
        let producer = self.producers.get(&producer_id)?;
        if producer.owner_connection_id != owner_connection_id {
            return None;
        }
        Some(ProducerRouteTarget {
            source_id: producer.source_id,
            producer_id,
            owner_connection_id: producer.owner_connection_id,
            routed_producer_id: producer.routed_producer_id,
            transport_media_id: producer.transport_media_id?,
        })
    }

    pub(super) fn producer_for_route_target(
        &self,
        target: &ProducerRouteTarget,
        current_connection_id: Option<ConnectionId>,
    ) -> Option<&PublishedProducer> {
        let producer = self.producers.get(&target.producer_id)?;
        if !target.matches_producer(producer)
            || Some(producer.owner_connection_id) != current_connection_id
        {
            return None;
        }
        Some(producer)
    }

    pub(super) fn set_producer_active(
        &mut self,
        target: &ProducerRouteTarget,
        active: bool,
    ) -> bool {
        let Some(producer) = self.producers.get_mut(&target.producer_id) else {
            return false;
        };
        if !target.matches_producer(producer) {
            return false;
        }
        producer.active = active;
        true
    }

    pub(super) fn publications_for_user_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> impl Iterator<Item = (&PublishedSourceDescriptor, &PublishedProducer)> {
        self.producer_ids_by_owner
            .get(user_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(move |producer_id| {
                let producer = self.producers.get(producer_id)?;
                if producer.owner_user_id != *user_id
                    || producer.owner_connection_id != connection_id
                {
                    return None;
                }
                let source = self.descriptors.get(&producer.source_id)?;
                Some((source, producer))
            })
    }

    pub(super) fn owner_has_promotable_source_in_group(
        &self,
        owner_user_id: &UserId,
        group: ActiveSpeakerGroup,
    ) -> bool {
        self.source_ids_by_owner
            .get(owner_user_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|source_id| self.descriptors.get(source_id))
            .any(|source| {
                source.policy().active_speaker().is_some_and(|policy| {
                    policy.group() == group && policy.role() == ActiveSpeakerSourceRole::Promotable
                })
            })
    }

    pub(super) fn install_source(&mut self, install: PublishedSourceInstall) {
        let PublishedSourceInstall {
            source_key,
            source_descriptor,
            source_encoding_ids,
            producer_id,
            producer,
            transport_media_id,
        } = install;
        let source_id = source_descriptor.source_id();
        let owner_user_id = producer.owner_user_id.clone();
        let owner_connection_id = producer.owner_connection_id;
        let stream_id = producer.stream_id.clone();
        self.producers.insert(producer_id, producer);
        self.descriptors.insert(source_id, source_descriptor);
        self.source_ids_by_owner_stream
            .insert(source_key, source_id);
        self.producer_id_by_source_id.insert(source_id, producer_id);
        self.register_source_owner(&owner_user_id, source_id);
        self.register_producer_owner(&owner_user_id, producer_id);
        self.source_transport_media_index.insert(
            transport_media_id,
            SourceTransportMediaIndexEntry::new(
                source_id,
                source_encoding_ids,
                owner_user_id,
                owner_connection_id,
                stream_id,
            ),
        );
    }

    pub(super) fn source_ids_for_owner(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = PublishedSourceId> + '_ {
        self.source_ids_by_owner
            .get(user_id)
            .into_iter()
            .flat_map(|source_ids| source_ids.iter().copied())
    }

    #[cfg(test)]
    pub(super) fn producer_ids_for_user(&self, user_id: &UserId) -> Vec<ProducerRuntimeId> {
        self.producer_ids_by_owner
            .get(user_id)
            .map(|producer_ids| producer_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(super) fn remove_source(
        &mut self,
        source_id: PublishedSourceId,
    ) -> Option<PublishedProducer> {
        let source = self.descriptors.remove(&source_id)?;
        let source_key = SourceKey::new(source.owner().user_id(), source.stream_id());
        self.source_ids_by_owner_stream.remove(&source_key);
        self.unregister_source_owner(source.owner().user_id(), source_id);
        let producer_id = self.producer_id_by_source_id.remove(&source_id)?;
        let producer = self.producers.remove(&producer_id)?;
        self.unregister_producer_owner(&producer.owner_user_id, producer_id);
        if let Some(transport_media_id) = producer.transport_media_id {
            self.source_transport_media_index
                .remove(&transport_media_id);
        }
        Some(producer)
    }

    pub(super) fn producer_transport_removals_for_users(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        departing_user_ids
            .iter()
            .filter_map(|user_id| self.producer_ids_by_owner.get(user_id))
            .flat_map(|producer_ids| producer_ids.iter())
            .filter_map(|producer_id| {
                let producer = self.producers.get(producer_id)?;
                let transport_media = producer.transport_media_id?;
                Some(TransportMediaRemoval::new(
                    producer.owner_user_id.clone(),
                    producer.owner_connection_id,
                    transport_media,
                ))
            })
            .collect()
    }

    pub(super) fn producer_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedProducer> {
        let producer_id = self.producer_id_by_source_id.get(&source_id)?;
        self.producers.get(producer_id)
    }

    pub(super) fn producer(&self, producer_id: ProducerRuntimeId) -> Option<&PublishedProducer> {
        self.producers.get(&producer_id)
    }

    fn producer_id_for_source_key(&self, source_key: &SourceKey) -> Option<ProducerRuntimeId> {
        let source_id = self.source_ids_by_owner_stream.get(source_key)?;
        self.producer_id_by_source_id.get(source_id).copied()
    }

    fn register_source_owner(&mut self, user_id: &UserId, source_id: PublishedSourceId) {
        self.source_ids_by_owner
            .entry(user_id.clone())
            .or_default()
            .insert(source_id);
    }

    fn unregister_source_owner(&mut self, user_id: &UserId, source_id: PublishedSourceId) {
        remove_from_index_set(&mut self.source_ids_by_owner, user_id, &source_id);
    }

    fn register_producer_owner(&mut self, user_id: &UserId, producer_id: ProducerRuntimeId) {
        self.producer_ids_by_owner
            .entry(user_id.clone())
            .or_default()
            .insert(producer_id);
    }

    fn unregister_producer_owner(&mut self, user_id: &UserId, producer_id: ProducerRuntimeId) {
        remove_from_index_set(&mut self.producer_ids_by_owner, user_id, &producer_id);
    }
}
