use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use super::shared::{ConsumerKey, RoomState};
use crate::{
    Bitrate,
    runtime::{
        ConnectionId, UserId,
        diagnostics::{
            DiagnosticsActiveSpeaker, DiagnosticsActiveSpeakerReason,
            DiagnosticsActiveSpeakerState, DiagnosticsIncomingBitrate, DiagnosticsMediaKind,
            DiagnosticsOverBudgetExceptionReason, DiagnosticsPolicyPauseReason,
            DiagnosticsPublication, DiagnosticsQualitySummary, DiagnosticsRouteState,
            DiagnosticsSource, DiagnosticsSourceEncoding, DiagnosticsSourceSelection,
            DiagnosticsSourceSelectionReason, DiagnosticsSourceSelector, DiagnosticsSubscription,
            DiagnosticsTemporalLayerMetadata, DiagnosticsTemporalLayerSelection,
            DiagnosticsUserTransport, DiagnosticsUserView, DiagnosticsVideoLayoutRole,
            DiagnosticsVideoRoutePriority,
        },
        media_transport::{
            ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSourceDiagnostic,
            TransportMediaId,
        },
        source_model::{
            ConsumerSourceSelection, OverBudgetExceptionReason, PolicyPauseReason,
            PublishedSourceDescriptor, PublishedSourceId, SourceEncodingDescriptor,
            SourceEncodingId, SourceRoomPolicySelector, SourceRoutePriority, SourceSelector,
            SourceTemporalLayerId,
        },
    },
};

impl RoomState {
    pub fn diagnostics_incoming_bitrate_by_session(
        &self,
        per_media: &[(TransportMediaId, Bitrate)],
    ) -> BTreeMap<UserId, DiagnosticsIncomingBitrate> {
        let mut incoming_bitrate = BTreeMap::new();
        for (transport_media_id, bits) in per_media {
            let Some(entry) = self.source_transport_media_entry(*transport_media_id) else {
                continue;
            };
            let session_bitrate: &mut DiagnosticsIncomingBitrate = incoming_bitrate
                .entry(entry.owner_user_id().clone())
                .or_default();
            session_bitrate.total = session_bitrate.total.saturating_add(bits.as_bps());
            let stream_bitrate = session_bitrate
                .by_stream_bps
                .entry(entry.stream_id().to_string())
                .or_default();
            *stream_bitrate = stream_bitrate.saturating_add(bits.as_bps());
        }
        incoming_bitrate
    }

    pub fn diagnostics_incoming_bitrate_by_source(
        &self,
        per_media: &[(TransportMediaId, Bitrate)],
    ) -> BTreeMap<PublishedSourceId, u64> {
        let mut incoming_bitrate: BTreeMap<PublishedSourceId, u64> = BTreeMap::new();
        for (transport_media_id, bits) in per_media {
            let Some(entry) = self.source_transport_media_entry(*transport_media_id) else {
                continue;
            };
            let source_bitrate = incoming_bitrate.entry(entry.source_id()).or_default();
            *source_bitrate = (*source_bitrate).saturating_add(bits.as_bps());
        }
        incoming_bitrate
    }

    pub fn diagnostics_sources(
        &self,
        incoming_bitrate_by_source: &BTreeMap<PublishedSourceId, u64>,
        active_speaker_diagnostics_by_media: &BTreeMap<
            TransportMediaId,
            ActiveSpeakerSourceDiagnostic,
        >,
    ) -> Vec<DiagnosticsSource> {
        self.sources
            .values()
            .map(|source| {
                let producer = self
                    .producer_id_by_source_id
                    .get(&source.source_id())
                    .and_then(|producer_id| self.producers.get(producer_id));
                let encodings = source
                    .encodings()
                    .map(diagnostics_source_encoding)
                    .collect();
                let transport_media_id = producer.and_then(|producer| producer.transport_media_id);
                DiagnosticsSource {
                    active: producer.is_some_and(|producer| producer.active),
                    active_speaker: diagnostics_active_speaker(
                        source,
                        transport_media_id,
                        active_speaker_diagnostics_by_media,
                    ),
                    current_incoming_bitrate_bps: incoming_bitrate_by_source
                        .get(&source.source_id())
                        .copied()
                        .unwrap_or_default(),
                    encodings,
                    media_kind: diagnostics_media_kind(source.media_kind()),
                    mid: source.mid().map(|mid| mid.as_str().to_owned()),
                    owner_user_id: source.owner().user_id().clone(),
                    source_id: source.source_id().as_u64(),
                    stream_id: source.stream_id().to_string(),
                    transport_media_id: transport_media_id.map(TransportMediaId::as_u64),
                }
            })
            .collect()
    }

    pub fn diagnostics_user_views(
        &self,
        media_worker_id: usize,
        transport_by_session: &BTreeMap<UserId, DiagnosticsUserTransport>,
    ) -> Vec<DiagnosticsUserView> {
        self.users
            .iter()
            .filter_map(|(user_id, user)| {
                let user_info = self.user_info_snapshot(user_id)?.1;
                let transport = transport_by_session
                    .get(user_id)
                    .cloned()
                    .unwrap_or_else(|| DiagnosticsUserTransport {
                        connection_id: user.connection_id.as_u64(),
                        health: None,
                        media_worker_id,
                        quality_summary: DiagnosticsQualitySummary {
                            current_incoming_bitrate: DiagnosticsIncomingBitrate::default(),
                            sampled_metrics_available: false,
                        },
                    });
                Some(DiagnosticsUserView {
                    publications: self.diagnostics_publications(user_id, user.connection_id),
                    user_id: user_id.clone(),
                    user_info,
                    subscriptions: self.diagnostics_subscriptions(user_id, user.connection_id),
                    transport,
                })
            })
            .collect()
    }

    fn diagnostics_publications(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<DiagnosticsPublication> {
        self.producers
            .values()
            .filter(|producer| {
                producer.owner_user_id == *user_id && producer.owner_connection_id == connection_id
            })
            .filter_map(|producer| {
                let source = self.sources.get(&producer.source_id)?;
                Some(DiagnosticsPublication {
                    active: producer.active,
                    encoding_ids: source
                        .encodings()
                        .map(|encoding| encoding.encoding_id().as_u64())
                        .collect(),
                    media_kind: diagnostics_media_kind(producer.media_kind),
                    source_id: producer.source_id.as_u64(),
                    stream_id: producer.stream_id.to_string(),
                    transport_media_id: producer.transport_media_id.map(TransportMediaId::as_u64),
                })
            })
            .collect()
    }

    fn diagnostics_subscriptions(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<DiagnosticsSubscription> {
        let mut subscriptions = self
            .consumer_index
            .iter()
            .filter_map(|(key, consumer_state)| {
                if key.consumer_user_id != *user_id
                    || consumer_state.consumer_connection_id != connection_id
                {
                    return None;
                }
                let source = self.sources.get(&key.source_id)?;
                let selection = self
                    .consumer_source_selections
                    .get(key)
                    .copied()
                    .unwrap_or_else(|| ConsumerSourceSelection::open(true));
                let route_state = self.consumer_route_state(
                    user_id,
                    source.owner().user_id(),
                    source.stream_id(),
                )?;
                let layout_intent = self.diagnostics_video_layout_intent(user_id, source);
                Some(DiagnosticsSubscription {
                    consumer_transport_media_id: Some(consumer_state.consumer_media.as_u64()),
                    layout_priority: layout_intent
                        .map(|intent| diagnostics_video_route_priority(intent.priority())),
                    layout_role: layout_intent
                        .map(|intent| diagnostics_video_layout_role(intent.role())),
                    producer_user_id: source.owner().user_id().clone(),
                    selection: diagnostics_source_selection(source, selection),
                    source_id: source.source_id().as_u64(),
                    source_transport_media_id: Some(consumer_state.source_media.as_u64()),
                    state: match route_state {
                        super::ConsumerRouteState::Active => DiagnosticsRouteState::Active,
                        super::ConsumerRouteState::Inactive => DiagnosticsRouteState::Inactive,
                        super::ConsumerRouteState::Absent => return None,
                    },
                    stream_id: source.stream_id().to_string(),
                })
            })
            .collect::<Vec<_>>();

        let pending_keys = self
            .pending_consumer_bootstraps
            .iter()
            .filter(|key| key.consumer_user_id == *user_id)
            .cloned()
            .collect::<BTreeSet<ConsumerKey>>();
        let existing_keys = self
            .consumer_index
            .keys()
            .filter(|key| key.consumer_user_id == *user_id)
            .cloned()
            .collect::<BTreeSet<ConsumerKey>>();
        subscriptions.extend(pending_keys.difference(&existing_keys).filter_map(|key| {
            let source = self.sources.get(&key.source_id)?;
            let selection = self
                .consumer_source_selections
                .get(key)
                .copied()
                .unwrap_or_else(|| ConsumerSourceSelection::open(true));
            let layout_intent = self.diagnostics_video_layout_intent(user_id, source);
            Some(DiagnosticsSubscription {
                consumer_transport_media_id: None,
                layout_priority: layout_intent
                    .map(|intent| diagnostics_video_route_priority(intent.priority())),
                layout_role: layout_intent
                    .map(|intent| diagnostics_video_layout_role(intent.role())),
                producer_user_id: source.owner().user_id().clone(),
                selection: diagnostics_source_selection(source, selection),
                source_id: source.source_id().as_u64(),
                source_transport_media_id: self
                    .producer_id_by_source_id
                    .get(&key.source_id)
                    .and_then(|producer_id| self.producers.get(producer_id))
                    .and_then(|producer| producer.transport_media_id)
                    .map(TransportMediaId::as_u64),
                state: DiagnosticsRouteState::Pending,
                stream_id: source.stream_id().to_string(),
            })
        }));
        subscriptions
    }
}

fn diagnostics_source_encoding(encoding: &SourceEncodingDescriptor) -> DiagnosticsSourceEncoding {
    let negotiated_format = encoding.negotiated_format();
    let max_temporal_layer_id = encoding
        .max_temporal_layer_id()
        .map(SourceTemporalLayerId::as_u8);
    DiagnosticsSourceEncoding {
        codec: negotiated_format.map(|format| format.codec_name().to_owned()),
        encoding_id: encoding.encoding_id().as_u64(),
        max_bitrate_bps: encoding.max_bitrate().map(Bitrate::as_bps),
        resolution_scale: encoding.resolution_scale(),
        max_framerate: encoding.max_framerate(),
        policy_role: encoding
            .policy_role()
            .map(|role| role.as_wire_value().to_owned()),
        max_temporal_layer_id,
        payload_type: negotiated_format.map(o_sfu_router::MediaFormat::payload_type),
        primary_ssrc: encoding.primary_ssrc().map(o_sfu_router::Ssrc::value),
        repair_ssrc: encoding.repair_ssrc().map(o_sfu_router::Ssrc::value),
        rid: encoding.rid().map(|rid| rid.as_str().to_owned()),
        temporal_layer_metadata: if max_temporal_layer_id.is_some() {
            DiagnosticsTemporalLayerMetadata::Advertised
        } else {
            DiagnosticsTemporalLayerMetadata::Absent
        },
    }
}

fn diagnostics_source_selection(
    source: &PublishedSourceDescriptor,
    selection: ConsumerSourceSelection,
) -> DiagnosticsSourceSelection {
    let selected_encoding_id = selection.selector().selected_encoding();
    let selected_temporal_layer_id = selection
        .selector()
        .selected_operating_point()
        .map(|operating_point| operating_point.max_temporal_layer_id().as_u8());
    let temporal_layer_selection = if selected_temporal_layer_id.is_some() {
        DiagnosticsTemporalLayerSelection::Selected
    } else {
        DiagnosticsTemporalLayerSelection::NotSelected
    };
    let budget = selection.budget();
    DiagnosticsSourceSelection {
        active: selection.active(),
        active_video_route_count: budget.active_video_route_count(),
        latest_receiver_bandwidth_estimate_bps: budget
            .latest_receiver_bandwidth()
            .map(Bitrate::as_bps),
        over_budget_exception_reason: budget
            .over_budget_exception_reason()
            .map(diagnostics_over_budget_exception_reason),
        policy_allows_delivery: selection.policy_allows_delivery(),
        policy_pause_reason: selection
            .policy_pause_reason()
            .map(diagnostics_policy_pause_reason),
        pressure_observations: selection.pressure_observations(),
        selection_reason: diagnostics_source_selection_reason(selection.selector()),
        selector: diagnostics_source_selector(selection.selector()),
        selected_estimated_bitrate_bps: selected_encoding_id
            .and_then(|encoding_id| source.encoding(encoding_id))
            .and_then(SourceEncodingDescriptor::max_bitrate)
            .map(Bitrate::as_bps),
        selected_video_bitrate_bps: budget.selected_video_bitrate().as_bps(),
        selected_video_budget_bps: budget.selected_video_budget().map(Bitrate::as_bps),
        selected_encoding_id: selected_encoding_id.map(SourceEncodingId::as_u64),
        selected_rid: selected_encoding_id
            .and_then(|encoding_id| source.encoding(encoding_id))
            .and_then(SourceEncodingDescriptor::rid)
            .map(|rid| rid.as_str().to_owned()),
        selected_temporal_layer_id,
        temporal_layer_selection,
        upgrade_observations: selection.upgrade_observations(),
    }
}

fn diagnostics_active_speaker(
    source: &PublishedSourceDescriptor,
    transport_media_id: Option<TransportMediaId>,
    active_speaker_diagnostics_by_media: &BTreeMap<TransportMediaId, ActiveSpeakerSourceDiagnostic>,
) -> Option<DiagnosticsActiveSpeaker> {
    if source.media_kind() != o_sfu_router::MediaKind::Audio {
        return None;
    }
    let Some(transport_media_id) = transport_media_id else {
        return Some(DiagnosticsActiveSpeaker::idle());
    };
    Some(
        active_speaker_diagnostics_by_media
            .get(&transport_media_id)
            .copied()
            .map_or_else(
                DiagnosticsActiveSpeaker::idle,
                diagnostics_active_speaker_snapshot,
            ),
    )
}

fn diagnostics_media_kind(value: o_sfu_router::MediaKind) -> DiagnosticsMediaKind {
    match value {
        o_sfu_router::MediaKind::Audio => DiagnosticsMediaKind::Audio,
        o_sfu_router::MediaKind::Video => DiagnosticsMediaKind::Video,
    }
}

fn diagnostics_source_selector(value: SourceSelector) -> DiagnosticsSourceSelector {
    match value {
        SourceSelector::Open => DiagnosticsSourceSelector::Open,
        SourceSelector::Encoding(_) => DiagnosticsSourceSelector::Encoding,
        SourceSelector::OperatingPoint(_) => DiagnosticsSourceSelector::OperatingPoint,
        SourceSelector::RoomPolicy(SourceRoomPolicySelector::Pinned) => {
            DiagnosticsSourceSelector::RoomPolicyPinned
        }
        SourceSelector::RoomPolicy(SourceRoomPolicySelector::Featured) => {
            DiagnosticsSourceSelector::RoomPolicyFeatured
        }
        SourceSelector::RoomPolicy(SourceRoomPolicySelector::ReadableDetail) => {
            DiagnosticsSourceSelector::RoomPolicyReadableDetail
        }
        SourceSelector::RoomPolicy(SourceRoomPolicySelector::ActiveSpeaker) => {
            DiagnosticsSourceSelector::RoomPolicyActiveSpeaker
        }
        SourceSelector::RoomPolicy(SourceRoomPolicySelector::VisibleThumbnail) => {
            DiagnosticsSourceSelector::RoomPolicyVisibleThumbnail
        }
        SourceSelector::RoomPolicy(SourceRoomPolicySelector::Hidden) => {
            DiagnosticsSourceSelector::RoomPolicyHidden
        }
        SourceSelector::RoomPolicy(SourceRoomPolicySelector::Overflow) => {
            DiagnosticsSourceSelector::RoomPolicyOverflow
        }
    }
}

fn diagnostics_source_selection_reason(value: SourceSelector) -> DiagnosticsSourceSelectionReason {
    match value {
        SourceSelector::Open => DiagnosticsSourceSelectionReason::Open,
        SourceSelector::Encoding(_) | SourceSelector::OperatingPoint(_) => {
            DiagnosticsSourceSelectionReason::ReceiverAdaptation
        }
        SourceSelector::RoomPolicy(_) => DiagnosticsSourceSelectionReason::RoomPolicy,
    }
}

fn diagnostics_over_budget_exception_reason(
    value: OverBudgetExceptionReason,
) -> DiagnosticsOverBudgetExceptionReason {
    match value {
        OverBudgetExceptionReason::ProtectedRoute => {
            DiagnosticsOverBudgetExceptionReason::ProtectedRoute
        }
    }
}

fn diagnostics_policy_pause_reason(value: PolicyPauseReason) -> DiagnosticsPolicyPauseReason {
    match value {
        PolicyPauseReason::BudgetPressure => DiagnosticsPolicyPauseReason::BudgetPressure,
        PolicyPauseReason::HiddenTile => DiagnosticsPolicyPauseReason::HiddenTile,
        PolicyPauseReason::OverflowTile => DiagnosticsPolicyPauseReason::OverflowTile,
        PolicyPauseReason::MissingUsableLayer => DiagnosticsPolicyPauseReason::MissingUsableLayer,
    }
}

fn diagnostics_video_layout_role(value: SourceRoomPolicySelector) -> DiagnosticsVideoLayoutRole {
    match value {
        SourceRoomPolicySelector::Pinned => DiagnosticsVideoLayoutRole::Pinned,
        SourceRoomPolicySelector::Featured => DiagnosticsVideoLayoutRole::Featured,
        SourceRoomPolicySelector::ReadableDetail => DiagnosticsVideoLayoutRole::ReadableDetail,
        SourceRoomPolicySelector::ActiveSpeaker => DiagnosticsVideoLayoutRole::ActiveSpeaker,
        SourceRoomPolicySelector::VisibleThumbnail => DiagnosticsVideoLayoutRole::VisibleThumbnail,
        SourceRoomPolicySelector::Hidden => DiagnosticsVideoLayoutRole::Hidden,
        SourceRoomPolicySelector::Overflow => DiagnosticsVideoLayoutRole::Overflow,
    }
}

fn diagnostics_video_route_priority(value: SourceRoutePriority) -> DiagnosticsVideoRoutePriority {
    match value {
        SourceRoutePriority::PinnedOrFeatured => DiagnosticsVideoRoutePriority::PinnedOrFeatured,
        SourceRoutePriority::ReadableDetail => DiagnosticsVideoRoutePriority::ReadableDetail,
        SourceRoutePriority::ActiveSpeaker => DiagnosticsVideoRoutePriority::ActiveSpeaker,
        SourceRoutePriority::VisibleThumbnail => DiagnosticsVideoRoutePriority::VisibleThumbnail,
        SourceRoutePriority::HiddenOrOverflow => DiagnosticsVideoRoutePriority::HiddenOrOverflow,
    }
}

fn diagnostics_active_speaker_snapshot(
    diagnostic: ActiveSpeakerSourceDiagnostic,
) -> DiagnosticsActiveSpeaker {
    DiagnosticsActiveSpeaker {
        state: diagnostics_active_speaker_state(diagnostic.state()),
        reason: diagnostics_active_speaker_reason(diagnostic.reason()),
        last_audio_level_dbov: diagnostic.last_audio_level_dbov(),
        confidence_observations: diagnostic.confidence_observations(),
        hold_remaining_ms: diagnostic.hold_remaining().map(duration_millis),
    }
}

fn diagnostics_active_speaker_state(
    state: ActiveSpeakerActivityState,
) -> DiagnosticsActiveSpeakerState {
    match state {
        ActiveSpeakerActivityState::Active => DiagnosticsActiveSpeakerState::Active,
        ActiveSpeakerActivityState::Idle => DiagnosticsActiveSpeakerState::Idle,
        ActiveSpeakerActivityState::Blocked => DiagnosticsActiveSpeakerState::Blocked,
        ActiveSpeakerActivityState::RecentlyExpired => {
            DiagnosticsActiveSpeakerState::RecentlyExpired
        }
    }
}

fn diagnostics_active_speaker_reason(
    reason: ActiveSpeakerActivityReason,
) -> DiagnosticsActiveSpeakerReason {
    match reason {
        ActiveSpeakerActivityReason::Vad => DiagnosticsActiveSpeakerReason::Vad,
        ActiveSpeakerActivityReason::AudioLevel => DiagnosticsActiveSpeakerReason::AudioLevel,
        ActiveSpeakerActivityReason::AudioLevelWarmup => {
            DiagnosticsActiveSpeakerReason::AudioLevelWarmup
        }
        ActiveSpeakerActivityReason::VadFalse => DiagnosticsActiveSpeakerReason::VadFalse,
        ActiveSpeakerActivityReason::LowNoise => DiagnosticsActiveSpeakerReason::LowNoise,
        ActiveSpeakerActivityReason::BelowSpeechThreshold => {
            DiagnosticsActiveSpeakerReason::BelowSpeechThreshold
        }
        ActiveSpeakerActivityReason::MissingAudioMetadata => {
            DiagnosticsActiveSpeakerReason::MissingAudioMetadata
        }
        ActiveSpeakerActivityReason::Expired => DiagnosticsActiveSpeakerReason::Expired,
        ActiveSpeakerActivityReason::NoMetadata => DiagnosticsActiveSpeakerReason::NoMetadata,
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "diagnostics fixtures should fail loudly when they build invalid source graphs"
    )]

    use o_sfu_router::{MediaKind, Rid};

    use super::*;
    use crate::runtime::source_model::{
        PublishedSourceDescriptorParts, PublishedSourceOwner, SourceEncodingDescriptorParts,
        SourceOperatingPoint, SourcePolicy, SourceSelector, UserStreamId,
    };

    fn encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        max_temporal_layer_id: Option<u8>,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: Some(Rid::new("hi")),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: None,
            resolution_scale: None,
            max_framerate: None,
            policy_role: None,
            max_temporal_layer_id: max_temporal_layer_id.and_then(SourceTemporalLayerId::new),
            negotiated_format: None,
        })
    }

    fn source_with_encoding(encoding: SourceEncodingDescriptor) -> PublishedSourceDescriptor {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id: encoding.source_id(),
            owner: PublishedSourceOwner::new(UserId::Integer(1)),
            stream_id: UserStreamId::new("main-video"),
            media_kind: MediaKind::Video,
            policy: SourcePolicy::hidden(),
            mid: None,
            encodings: vec![encoding],
        })
        .expect("test source graph should be valid")
    }

    #[test]
    fn source_encoding_diagnostics_distinguish_absent_temporal_metadata_from_base_layer() {
        let source_id = PublishedSourceId::from_raw(7);
        let absent_encoding =
            diagnostics_source_encoding(&encoding(source_id, SourceEncodingId::from_raw(1), None));
        let base_layer_encoding = diagnostics_source_encoding(&encoding(
            source_id,
            SourceEncodingId::from_raw(2),
            Some(SourceTemporalLayerId::base().as_u8()),
        ));

        assert_eq!(
            absent_encoding.temporal_layer_metadata,
            DiagnosticsTemporalLayerMetadata::Absent
        );
        assert_eq!(absent_encoding.max_temporal_layer_id, None);
        assert_eq!(
            base_layer_encoding.temporal_layer_metadata,
            DiagnosticsTemporalLayerMetadata::Advertised
        );
        assert_eq!(base_layer_encoding.max_temporal_layer_id, Some(0));
    }

    #[test]
    fn source_selection_diagnostics_distinguish_unselected_temporal_layer_from_base_layer() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encoding(encoding(
            source_id,
            encoding_id,
            Some(SourceTemporalLayerId::base().as_u8()),
        ));

        let mut rid_selection = ConsumerSourceSelection::open(true);
        rid_selection.set_selector(SourceSelector::Encoding(encoding_id));
        let rid_diagnostics = diagnostics_source_selection(&source, rid_selection);
        assert_eq!(
            rid_diagnostics.temporal_layer_selection,
            DiagnosticsTemporalLayerSelection::NotSelected
        );
        assert_eq!(rid_diagnostics.selected_temporal_layer_id, None);

        let mut base_layer_selection = ConsumerSourceSelection::open(true);
        base_layer_selection.set_selector(SourceSelector::OperatingPoint(
            SourceOperatingPoint::new(encoding_id, SourceTemporalLayerId::base()),
        ));
        let base_layer_diagnostics = diagnostics_source_selection(&source, base_layer_selection);
        assert_eq!(
            base_layer_diagnostics.temporal_layer_selection,
            DiagnosticsTemporalLayerSelection::Selected
        );
        assert_eq!(
            base_layer_diagnostics.selected_temporal_layer_id,
            Some(SourceTemporalLayerId::base().as_u8())
        );
    }
}
