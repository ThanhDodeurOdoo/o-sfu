use o_sfu_router::{rtp::MediaStream, topology::RoutedProducerId};

use super::{
    super::{ConsumerKey, ProducerRuntimeId, PublishedProducer, PublishedSourceInstall},
    RoomTopology,
};
use crate::engine::{
    ConnectionId,
    media_transport::TransportMediaId,
    source_model::{ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId},
};

impl RoomTopology {
    pub(in crate::engine::room::media_graph) fn ensure_selection_for_test(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        self.route_graph.ensure_selection(key, selection);
    }

    pub(in crate::engine::room::media_graph) fn remove_source_for_test(
        &mut self,
        source_id: PublishedSourceId,
    ) -> bool {
        self.remove_source(source_id).is_some()
    }

    pub(in crate::engine::room::media_graph) fn remove_route_graph_entry_for_test(
        &mut self,
        key: &ConsumerKey,
    ) {
        self.route_graph.remove_key_state(key);
    }

    pub(in crate::engine::room::media_graph) fn install_missing_router_producer_for_test(
        &mut self,
        source_descriptor: PublishedSourceDescriptor,
        producer_id: ProducerRuntimeId,
        owner_connection_id: ConnectionId,
        routed_producer_id: RoutedProducerId,
        consumable_rtp_parameters: MediaStream,
        transport_media_id: TransportMediaId,
    ) {
        let source_id = source_descriptor.source_id();
        let owner_user_id = source_descriptor.owner().user_id().clone();
        let stream_id = source_descriptor.stream_id().clone();
        let media_kind = source_descriptor.media_kind();
        self.sources.install_source(PublishedSourceInstall {
            source_descriptor,
            producer_id,
            producer: PublishedProducer {
                source_id,
                owner_user_id,
                owner_connection_id,
                stream_id,
                media_kind,
                consumable_rtp_parameters,
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                active: true,
            },
            transport_media_id,
        });
    }

    pub(in crate::engine::room::media_graph) fn routed_consumer_count_for_test(&self) -> usize {
        self.routing.consumer_count_for_test()
    }
}
