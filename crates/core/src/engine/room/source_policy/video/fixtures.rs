use o_sfu_router::{MediaKind, Rid};

use super::{input::ReceiverVideoRouteInput, layout::ReceiverVideoLayoutIntent};
use crate::{
    Bitrate,
    engine::{
        ConnectionId, UserId,
        media_transport::TransportMediaId,
        room::media_graph::ConsumerRouteTransportRef,
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceDescriptorParts,
            PublishedSourceId, PublishedSourceOwner, SourceAdaptationPolicy,
            SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
            SourceLayoutPolicy, SourceModelError, SourcePolicy, SourceRoomPolicySelector,
            UploadLayerPolicyRole, UserStreamId,
        },
    },
};

pub(super) fn role_encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: &str,
    role: UploadLayerPolicyRole,
) -> SourceEncodingDescriptor {
    encoding(source_id, encoding_id, Some(rid), None, Some(role))
}

pub(super) fn bitrate_encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: &str,
    max_bitrate: Bitrate,
    role: UploadLayerPolicyRole,
) -> SourceEncodingDescriptor {
    encoding(
        source_id,
        encoding_id,
        Some(rid),
        Some(max_bitrate),
        Some(role),
    )
}

pub(super) fn ridless_encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
) -> SourceEncodingDescriptor {
    encoding(
        source_id,
        encoding_id,
        None,
        Some(Bitrate::from_kbps(900)),
        None,
    )
}

fn encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: Option<&str>,
    max_bitrate: Option<Bitrate>,
    role: Option<UploadLayerPolicyRole>,
) -> SourceEncodingDescriptor {
    SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
        encoding_id,
        source_id,
        rid: rid.map(Rid::new),
        primary_ssrc: None,
        repair_ssrc: None,
        max_bitrate,
        resolution_scale: None,
        max_framerate: None,
        policy_role: role,
        max_temporal_layer_id: None,
        negotiated_format: None,
    })
}

pub(super) fn scalable_source(
    encodings: Vec<SourceEncodingDescriptor>,
) -> Result<PublishedSourceDescriptor, SourceModelError> {
    scalable_source_with(
        PublishedSourceId::from_raw(7),
        UserId::Integer(41),
        SourceRoomPolicySelector::VisibleThumbnail,
        encodings,
    )
}

pub(super) fn scalable_source_with(
    source_id: PublishedSourceId,
    owner_user_id: UserId,
    visible_selector: SourceRoomPolicySelector,
    encodings: Vec<SourceEncodingDescriptor>,
) -> Result<PublishedSourceDescriptor, SourceModelError> {
    PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(owner_user_id),
        stream_id: UserStreamId::new("camera"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::new(
            Some(SourceLayoutPolicy::new(
                visible_selector,
                Some(SourceRoomPolicySelector::ActiveSpeaker),
            )),
            SourceAdaptationPolicy::ScalableVideo,
            None,
        ),
        mid: None,
        encodings,
    })
}

pub(super) fn route(
    source: &PublishedSourceDescriptor,
    current_selection: ConsumerSourceSelection,
) -> ReceiverVideoRouteInput<'_> {
    route_with_layout(
        source,
        current_selection,
        SourceRoomPolicySelector::VisibleThumbnail,
    )
}

pub(super) fn route_with_layout(
    source: &PublishedSourceDescriptor,
    current_selection: ConsumerSourceSelection,
    layout_selector: SourceRoomPolicySelector,
) -> ReceiverVideoRouteInput<'_> {
    ReceiverVideoRouteInput {
        user_count: 3,
        source,
        transport_ref: ConsumerRouteTransportRef::from_parts(
            UserId::Integer(42),
            ConnectionId::from_raw(10),
            TransportMediaId::new(20),
            source.owner().user_id().clone(),
            ConnectionId::from_raw(11),
            TransportMediaId::new(source.source_id().as_u64().saturating_add(20)),
        ),
        current_selection,
        layout_intent: ReceiverVideoLayoutIntent::new(layout_selector),
        visible_scalable_route_count: 2,
        active_speaker_rank: None,
        receiver_bandwidth: None,
    }
}
