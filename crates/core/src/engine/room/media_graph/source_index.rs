use std::collections::{BTreeMap, BTreeSet};

use super::{
    ProducerRouteTarget, ProducerRuntimeId, PublishedProducer, PublishedSourceInstall, SourceKey,
    SourceTransportMediaIndexEntry, SourceView, TransportMediaRemoval, remove_from_index_set,
};
use crate::engine::{
    ConnectionId, UserId,
    media_transport::TransportMediaId,
    source_model::{
        ActiveSpeakerGroup, ActiveSpeakerSourceRole, PublishedSourceDescriptor, PublishedSourceId,
        UserStreamId,
    },
};

#[derive(Debug, Default)]
pub(super) struct SourceIndex {
    records: BTreeMap<PublishedSourceId, SourceRecord>,
    id_by_key: BTreeMap<SourceKey, PublishedSourceId>,
    ids_by_owner: BTreeMap<UserId, BTreeSet<PublishedSourceId>>,
    source_by_producer: BTreeMap<ProducerRuntimeId, PublishedSourceId>,
    by_transport_media: BTreeMap<TransportMediaId, SourceTransportMediaIndexEntry>,
}

#[derive(Debug)]
struct SourceRecord {
    descriptor: PublishedSourceDescriptor,
    producer_id: ProducerRuntimeId,
    producer: PublishedProducer,
}

impl SourceIndex {
    pub(super) fn publication_count(&self) -> usize {
        self.records.len()
    }

    pub(super) fn source_views(&self) -> impl Iterator<Item = SourceView<'_>> {
        self.records.values().map(|record| SourceView {
            source: &record.descriptor,
            producer: &record.producer,
        })
    }

    pub(super) fn source_view(&self, source_id: PublishedSourceId) -> Option<SourceView<'_>> {
        self.records.get(&source_id).map(|record| SourceView {
            source: &record.descriptor,
            producer: &record.producer,
        })
    }

    pub(super) fn producers(
        &self,
    ) -> impl Iterator<Item = (ProducerRuntimeId, &PublishedProducer)> {
        self.records
            .values()
            .map(|record| (record.producer_id, &record.producer))
    }

    pub(super) fn source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSourceDescriptor> {
        self.records
            .get(&source_id)
            .map(|record| &record.descriptor)
    }

    pub(super) fn transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.by_transport_media.get(&transport_media_id)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn first_published_transport_media_id(&self) -> Option<TransportMediaId> {
        self.records
            .values()
            .find_map(|record| record.producer.transport_media_id)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<TransportMediaId> {
        let producer = &self
            .record_for_key(&SourceKey::new(user_id, stream_id))?
            .producer;
        if producer.owner_connection_id == connection_id {
            producer.transport_media_id
        } else {
            None
        }
    }

    pub(super) fn id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.id_by_key
            .get(&SourceKey::new(owner_user_id, stream_id))
            .copied()
    }

    pub(super) fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        let record = self.record_for_key(&SourceKey::new(owner_user_id, stream_id))?;
        let producer = &record.producer;
        if producer.owner_connection_id != owner_connection_id {
            return None;
        }
        Some(ProducerRouteTarget {
            source_id: producer.source_id,
            producer_id: record.producer_id,
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
        let producer = self.producer(target.producer_id)?;
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
        let Some(producer) = self.producer_mut(target.producer_id) else {
            return false;
        };
        if !target.matches_producer(producer) {
            return false;
        }
        producer.active = active;
        true
    }

    pub(super) fn owner_has_promotable_source_in_group(
        &self,
        owner_user_id: &UserId,
        group: ActiveSpeakerGroup,
    ) -> bool {
        self.ids_for_owner(owner_user_id)
            .filter_map(|source_id| self.source(source_id))
            .any(|source| {
                source.policy().active_speaker().is_some_and(|policy| {
                    policy.group() == group && policy.role() == ActiveSpeakerSourceRole::Promotable
                })
            })
    }

    pub(super) fn install_source(&mut self, install: PublishedSourceInstall) {
        let PublishedSourceInstall {
            source_descriptor,
            producer_id,
            producer,
            transport_media_id,
        } = install;
        let source_id = source_descriptor.source_id();
        let source_key = SourceKey::new(
            source_descriptor.owner().user_id(),
            source_descriptor.stream_id(),
        );
        let owner_user_id = source_descriptor.owner().user_id().clone();
        let stream_id = source_descriptor.stream_id().clone();
        self.id_by_key.insert(source_key, source_id);
        self.ids_by_owner
            .entry(owner_user_id.clone())
            .or_default()
            .insert(source_id);
        self.source_by_producer.insert(producer_id, source_id);
        self.by_transport_media.insert(
            transport_media_id,
            SourceTransportMediaIndexEntry::new(source_id, owner_user_id, stream_id),
        );
        self.records.insert(
            source_id,
            SourceRecord {
                descriptor: source_descriptor,
                producer_id,
                producer,
            },
        );
    }

    pub(super) fn ids_for_owner(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = PublishedSourceId> + '_ {
        self.ids_by_owner
            .get(user_id)
            .into_iter()
            .flat_map(|source_ids| source_ids.iter().copied())
    }

    pub(super) fn remove_source(
        &mut self,
        source_id: PublishedSourceId,
    ) -> Option<PublishedProducer> {
        let record = self.records.remove(&source_id)?;
        let source_key = SourceKey::new(
            record.descriptor.owner().user_id(),
            record.descriptor.stream_id(),
        );
        self.id_by_key.remove(&source_key);
        remove_from_index_set(
            &mut self.ids_by_owner,
            record.descriptor.owner().user_id(),
            &source_id,
        );
        self.source_by_producer.remove(&record.producer_id);
        if let Some(transport_media_id) = record.producer.transport_media_id {
            self.by_transport_media.remove(&transport_media_id);
        }
        Some(record.producer)
    }

    pub(super) fn producer_transport_removals_for_users(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        departing_user_ids
            .iter()
            .flat_map(|user_id| self.ids_for_owner(user_id))
            .filter_map(|source_id| {
                let producer = &self.records.get(&source_id)?.producer;
                Some(TransportMediaRemoval::new(
                    producer.owner_user_id.clone(),
                    producer.owner_connection_id,
                    producer.transport_media_id?,
                ))
            })
            .collect()
    }

    pub(super) fn producer(&self, producer_id: ProducerRuntimeId) -> Option<&PublishedProducer> {
        self.records
            .get(self.source_by_producer.get(&producer_id)?)
            .map(|record| &record.producer)
    }

    fn producer_mut(&mut self, producer_id: ProducerRuntimeId) -> Option<&mut PublishedProducer> {
        let source_id = *self.source_by_producer.get(&producer_id)?;
        self.records
            .get_mut(&source_id)
            .map(|record| &mut record.producer)
    }

    fn record_for_key(&self, source_key: &SourceKey) -> Option<&SourceRecord> {
        self.records.get(self.id_by_key.get(source_key)?)
    }
}
