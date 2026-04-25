//! Pure room policy for source-layer selection.
//!
//! This module maps chanel state plus transport observations into
//! `SourceSelector` decisions. It does not call the tranpsort adapter and it
//! does not mutate packet-loop state. The async effect layer projects accepted
//! decisions into transport gates, then commits only the selectors that still
//! match the current room state.
//!
//! Producer-wide gates are kept only as a compatibility cleanup surface.
//! Receiver-driven simulcast policy lives on consumer selections so different
//! receivers may choose different encodings from the same published source.

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_protocol::shared::{SessionId, StreamType};

use super::{
    super::{ChannelEventMessage, outbound::MessageFanout},
    ids::ProducerRuntimeId,
    shared::{ChannelState, SourceKey},
};
#[cfg(test)]
use crate::runtime::source_model::SourceEncodingId;
use crate::runtime::{
    ConnectionId,
    source_model::{
        ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
        SourceEncodingDescriptor, SourceSelector,
    },
    transport_adapter::{
        ActiveSpeakerSource, ReceiverBandwidthSnapshot, SourcePacketGate, TransportMediaId,
    },
};

const MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD: usize = 3;
const ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT: usize = 5;
const DOWNSWITCH_PRESSURE_OBSERVATIONS: u8 = 2;
const UPSWITCH_STABLE_OBSERVATIONS: u8 = 3;
const THUMBNAIL_BUDGET_DIVISOR: u64 = 2;

// Pressure and recovery are counted in policy refreshes, not wall time. This
// keeps the policy deterministic under coalesced transport wakeups.
/// Producer-level selector update used to clear stale source-wide gates.
///
/// Adaptive simulcast should not use this path for receiver quality. Keep it
/// narrow so producer gates remain a transport cleanup surface while consumer
/// selections own per-receiver policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct SourcePacketSelectionUpdate {
    producer_id: ProducerRuntimeId,
    owner_session_id: SessionId,
    owner_connection_id: ConnectionId,
    transport_media_id: TransportMediaId,
    selector: SourceSelector,
    packet_gate: SourcePacketGate,
}

/// One receiver-side source selection that is ready for the effect boundary.
///
/// The update carries the transport handles and connection ids observed while
/// planning. Commit revalidate them after async transport work so stale
/// replacement or cleanup events cannot write selector state onto a newer route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ConsumerPacketSelectionUpdate {
    consumer_session_id: SessionId,
    consumer_connection_id: ConnectionId,
    source_session_id: SessionId,
    source_connection_id: ConnectionId,
    source_transport_media_id: TransportMediaId,
    consumer_transport_media_id: TransportMediaId,
    source_id: PublishedSourceId,
    selector: SourceSelector,
    pressure_observations: u8,
    upgrade_observations: u8,
    packet_gate: Option<SourcePacketGate>,
    request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub(in crate::runtime::channel) fn consumer_session_id(&self) -> &SessionId {
        &self.consumer_session_id
    }

    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub(in crate::runtime::channel) fn source_session_id(&self) -> &SessionId {
        &self.source_session_id
    }

    pub(in crate::runtime::channel) const fn source_connection_id(&self) -> ConnectionId {
        self.source_connection_id
    }

    pub(in crate::runtime::channel) const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    pub(in crate::runtime::channel) const fn consumer_transport_media_id(
        &self,
    ) -> TransportMediaId {
        self.consumer_transport_media_id
    }

    pub(in crate::runtime::channel) const fn selector(&self) -> SourceSelector {
        self.selector
    }

    pub(in crate::runtime::channel) const fn pressure_observations(&self) -> u8 {
        self.pressure_observations
    }

    pub(in crate::runtime::channel) const fn upgrade_observations(&self) -> u8 {
        self.upgrade_observations
    }

    pub(in crate::runtime::channel) fn packet_gate(&self) -> Option<&SourcePacketGate> {
        self.packet_gate.as_ref()
    }

    pub(in crate::runtime::channel) const fn request_keyframe(&self) -> bool {
        self.request_keyframe
    }
}

impl SourcePacketSelectionUpdate {
    pub(in crate::runtime::channel) const fn producer_id(&self) -> ProducerRuntimeId {
        self.producer_id
    }

    pub(in crate::runtime::channel) fn owner_session_id(&self) -> &SessionId {
        &self.owner_session_id
    }

    pub(in crate::runtime::channel) const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    pub(in crate::runtime::channel) const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    pub(in crate::runtime::channel) const fn selector(&self) -> SourceSelector {
        self.selector
    }

    pub(in crate::runtime::channel) fn packet_gate(&self) -> &SourcePacketGate {
        &self.packet_gate
    }
}

/// Server-owned featured state derived from active-speaker observations.
///
/// This is kept next to packet policy becuase the same observation decides both
/// visible layout state and the quality floor used for receiver adaptation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct FeaturedSessionUpdate {
    session_id: SessionId,
    featured: Option<bool>,
}

impl FeaturedSessionUpdate {
    pub(in crate::runtime::channel) fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub(in crate::runtime::channel) const fn featured(&self) -> Option<bool> {
        self.featured
    }
}

impl ChannelState {
    /// Plans producer-wide packet gates for stale source-level state only.
    ///
    /// Receiver quality is no longer expressed by constraining the producer.
    /// Per-receiver choices are emitted by `consumer_packet_selection_updates`
    /// so two consumers can select diferent encodings from one source.
    pub(in crate::runtime::channel) fn source_packet_selection_updates(
        &self,
        _active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Vec<SourcePacketSelectionUpdate> {
        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                let transport_media_id = producer.transport_media_id?;
                let source = self.sources.get(&producer.source_id)?;
                let desired_selector = desired_source_packet_selector(producer.stream_type, source);
                if desired_selector == producer.source_packet_selector {
                    return None;
                }
                let packet_gate = source_packet_gate_for_selector(source, desired_selector)?;
                Some(SourcePacketSelectionUpdate {
                    producer_id: *producer_id,
                    owner_session_id: producer.owner_session_id.clone(),
                    owner_connection_id: producer.owner_connection_id,
                    transport_media_id,
                    selector: desired_selector,
                    packet_gate,
                })
            })
            .collect()
    }

    /// Plans deterministic per-consumer source selectors for live video routes.
    ///
    /// `packet_gate` is present only when the selected encoding changes at the
    /// transport boundary. Counter-only updates are still emitted so sustained
    /// pressure and stable recovery survive across policy refreshes.
    pub(in crate::runtime::channel) fn consumer_packet_selection_updates(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        let featured_camera_session_ids =
            self.cleared_camera_session_ids_for_active_speakers(active_speaker_sources);
        let receiver_bandwidth_by_session =
            receiver_bandwidth_by_session(receiver_bandwidth_snapshot);
        let active_video_route_counts = self.active_video_route_counts_by_consumer();
        self.consumer_index
            .iter()
            .filter_map(|(consumer_key, consumer_state)| {
                let source = self.sources.get(&consumer_key.source_id)?;
                let producer_id = self.producer_id_by_source_id.get(&consumer_key.source_id)?;
                let producer = self.producers.get(producer_id)?;
                if !producer.active {
                    return None;
                }
                let selection = self
                    .consumer_source_selections
                    .get(consumer_key)
                    .copied()
                    .unwrap_or_else(|| ConsumerSourceSelection::open(true));
                if !selection.active() {
                    return None;
                }
                let adaptation = consumer_adaptation_plan(
                    self.session_count(),
                    source,
                    selection,
                    featured_camera_session_ids.contains(source.owner().session_id()),
                    active_video_route_counts
                        .get(&consumer_key.consumer_session_id)
                        .copied()
                        .unwrap_or(1),
                    receiver_bandwidth_by_session
                        .get(&consumer_key.consumer_session_id)
                        .copied(),
                )?;
                let packet_gate = if adaptation.selector == selection.selector() {
                    None
                } else {
                    Some(source_packet_gate_for_selector(
                        source,
                        adaptation.selector,
                    )?)
                };
                if packet_gate.is_none()
                    && adaptation.pressure_observations == selection.pressure_observations()
                    && adaptation.upgrade_observations == selection.upgrade_observations()
                {
                    return None;
                }
                Some(ConsumerPacketSelectionUpdate {
                    consumer_session_id: consumer_key.consumer_session_id.clone(),
                    consumer_connection_id: consumer_state.consumer_connection_id,
                    source_session_id: source.owner().session_id().clone(),
                    source_connection_id: consumer_state.source_connection_id,
                    source_transport_media_id: consumer_state.source_media,
                    consumer_transport_media_id: consumer_state.consumer_media,
                    source_id: source.source_id(),
                    selector: adaptation.selector,
                    pressure_observations: adaptation.pressure_observations,
                    upgrade_observations: adaptation.upgrade_observations,
                    packet_gate,
                    request_keyframe: adaptation.request_keyframe,
                })
            })
            .collect()
    }

    fn active_video_route_counts_by_consumer(&self) -> BTreeMap<SessionId, usize> {
        let mut counts = BTreeMap::new();
        for (consumer_key, consumer_state) in &self.consumer_index {
            let Some(source) = self.sources.get(&consumer_key.source_id) else {
                continue;
            };
            if !matches!(
                source.stream_type(),
                StreamType::Camera | StreamType::Screen
            ) {
                continue;
            }
            let Some(producer_id) = self.producer_id_by_source_id.get(&consumer_key.source_id)
            else {
                continue;
            };
            let Some(producer) = self.producers.get(producer_id) else {
                continue;
            };
            let Some(consumer_connection_id) =
                self.session_connection_id(&consumer_key.consumer_session_id)
            else {
                continue;
            };
            if !producer.active
                || consumer_state.consumer_connection_id != consumer_connection_id
                || !self
                    .consumer_source_selections
                    .get(consumer_key)
                    .is_none_or(|selection| selection.active())
            {
                continue;
            }
            *counts
                .entry(consumer_key.consumer_session_id.clone())
                .or_default() += 1;
        }
        counts
    }

    fn cleared_camera_session_ids_for_active_speakers(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> BTreeSet<SessionId> {
        active_speaker_sources
            .iter()
            .filter_map(|source| {
                let owner_session_id = self.producer_owner_session_id_for_transport_media_id(
                    source.transport_media_id(),
                    StreamType::Audio,
                )?;
                self.source_ids_by_owner_stream
                    .contains_key(&SourceKey::new(&owner_session_id, StreamType::Camera))
                    .then_some(owner_session_id)
            })
            .take(ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT)
            .collect()
    }

    pub(in crate::runtime::channel) fn featured_session_updates(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Vec<FeaturedSessionUpdate> {
        let desired_featured_session_id =
            self.featured_session_id_for_active_speakers(active_speaker_sources);
        let should_clear_featured_state = desired_featured_session_id.is_none()
            && self
                .sessions
                .values()
                .any(|session| session.layout.featured().is_some());
        if desired_featured_session_id.is_none() && !should_clear_featured_state {
            return Vec::new();
        }
        self.sessions
            .iter()
            .filter_map(|(session_id, session)| {
                let desired_featured = desired_featured_session_id.as_ref().map_or_else(
                    || session.layout.featured().is_some().then_some(false),
                    |featured_session_id| Some(featured_session_id == session_id),
                );
                (desired_featured != session.layout.featured()).then(|| FeaturedSessionUpdate {
                    session_id: session_id.clone(),
                    featured: desired_featured,
                })
            })
            .collect()
    }

    fn featured_session_id_for_active_speakers(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Option<SessionId> {
        active_speaker_sources.iter().find_map(|source| {
            let owner_session_id = self.producer_owner_session_id_for_transport_media_id(
                source.transport_media_id(),
                StreamType::Audio,
            )?;
            self.source_ids_by_owner_stream
                .contains_key(&SourceKey::new(&owner_session_id, StreamType::Camera))
                .then_some(owner_session_id)
        })
    }

    fn producer_owner_session_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
        stream_type: StreamType,
    ) -> Option<SessionId> {
        self.source_transport_media_entry(transport_media_id)
            .filter(|entry| entry.stream_type() == stream_type)
            .map(|entry| entry.owner_session_id().clone())
    }

    pub(in crate::runtime::channel) fn commit_source_packet_selection_updates(
        &mut self,
        updates: &[SourcePacketSelectionUpdate],
    ) {
        for update in updates {
            let Some(producer) = self.producers.get_mut(&update.producer_id()) else {
                continue;
            };
            if producer.owner_session_id != *update.owner_session_id()
                || producer.owner_connection_id != update.owner_connection_id()
                || producer.transport_media_id != Some(update.transport_media_id())
            {
                continue;
            }
            producer.source_packet_selector = update.selector();
        }
    }

    /// Commits selector updates that still match the routed consumer media.
    ///
    /// The effect executor may await tranpsort work before calling this method.
    /// Every connection and transport handle is checked again so stale effects
    /// from a replaced socket or removed route become no-ops
    pub(in crate::runtime::channel) fn commit_consumer_packet_selection_updates(
        &mut self,
        updates: &[ConsumerPacketSelectionUpdate],
    ) {
        for update in updates {
            let key =
                super::shared::ConsumerKey::new(update.consumer_session_id(), update.source_id);
            let Some(consumer_state) = self.consumer_index.get(&key) else {
                continue;
            };
            if consumer_state.consumer_connection_id != update.consumer_connection_id()
                || consumer_state.source_connection_id != update.source_connection_id()
                || consumer_state.source_media != update.source_transport_media_id()
                || consumer_state.consumer_media != update.consumer_transport_media_id()
            {
                continue;
            }
            let selection = self
                .consumer_source_selections
                .entry(key)
                .or_insert_with(|| ConsumerSourceSelection::open(true));
            selection.set_selector(update.selector());
            selection.set_adaptation_observations(
                update.pressure_observations(),
                update.upgrade_observations(),
            );
        }
    }

    pub(in crate::runtime::channel) fn commit_featured_session_updates(
        &mut self,
        updates: &[FeaturedSessionUpdate],
    ) -> Option<MessageFanout> {
        let changed_session_ids = updates
            .iter()
            .filter_map(|update| {
                let session = self.sessions.get_mut(update.session_id())?;
                if session.layout.featured() == update.featured() {
                    return None;
                }
                session.layout.set_featured(update.featured());
                Some(update.session_id().clone())
            })
            .collect::<Vec<_>>();
        if changed_session_ids.is_empty() {
            return None;
        }
        let snapshot = changed_session_ids
            .into_iter()
            .filter_map(|session_id| self.session_info_snapshot(&session_id))
            .collect();
        Some(self.fanout_all(&ChannelEventMessage::SessionInfoChanged(snapshot)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConsumerAdaptationPlan {
    selector: SourceSelector,
    pressure_observations: u8,
    upgrade_observations: u8,
    request_keyframe: bool,
}

fn receiver_bandwidth_by_session(snapshot: &ReceiverBandwidthSnapshot) -> BTreeMap<SessionId, u64> {
    snapshot
        .per_session
        .iter()
        .map(|(session_key, estimate_bps)| (session_key.session_id().clone(), *estimate_bps))
        .collect()
}

fn desired_source_packet_selector(
    _stream_type: StreamType,
    _source: &PublishedSourceDescriptor,
) -> SourceSelector {
    SourceSelector::Open
}

/// Chooses the next receiver selector using deterministic hysteresis.
///
/// Downswitches wait for repeated pressure so one low estimate does not drop a
/// layer immediately. Upswitches wait longer and request a keyframe when the
/// chosen layer changes, because the receiver needs a fresh decodable frame
fn consumer_adaptation_plan(
    session_count: usize,
    source: &PublishedSourceDescriptor,
    current: ConsumerSourceSelection,
    featured: bool,
    active_video_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
) -> Option<ConsumerAdaptationPlan> {
    if source.stream_type() != StreamType::Camera {
        return None;
    }
    let encodings = selectable_encodings(source);
    if encodings.len() < 2 {
        return None;
    }
    let current_index = selector_index(current.selector(), &encodings);
    let target_index = desired_encoding_index(
        session_count,
        featured,
        active_video_route_count,
        receiver_bandwidth_bps,
        &encodings,
    );
    let target_selector = SourceSelector::Encoding(encodings.get(target_index)?.encoding_id());
    if target_index == current_index {
        return Some(ConsumerAdaptationPlan {
            selector: target_selector,
            pressure_observations: 0,
            upgrade_observations: 0,
            request_keyframe: false,
        });
    }
    if receiver_bandwidth_bps.is_none() {
        return Some(ConsumerAdaptationPlan {
            selector: target_selector,
            pressure_observations: 0,
            upgrade_observations: 0,
            request_keyframe: target_index > current_index,
        });
    }
    if target_index < current_index {
        let pressure_observations = current
            .pressure_observations()
            .saturating_add(1)
            .min(DOWNSWITCH_PRESSURE_OBSERVATIONS);
        if pressure_observations >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
            return Some(ConsumerAdaptationPlan {
                selector: target_selector,
                pressure_observations: 0,
                upgrade_observations: 0,
                request_keyframe: false,
            });
        }
        return Some(ConsumerAdaptationPlan {
            selector: current.selector(),
            pressure_observations,
            upgrade_observations: 0,
            request_keyframe: false,
        });
    }
    if target_index > current_index {
        let upgrade_observations = current
            .upgrade_observations()
            .saturating_add(1)
            .min(UPSWITCH_STABLE_OBSERVATIONS);
        if upgrade_observations >= UPSWITCH_STABLE_OBSERVATIONS {
            return Some(ConsumerAdaptationPlan {
                selector: target_selector,
                pressure_observations: 0,
                upgrade_observations: 0,
                request_keyframe: true,
            });
        }
        return Some(ConsumerAdaptationPlan {
            selector: current.selector(),
            pressure_observations: 0,
            upgrade_observations,
            request_keyframe: false,
        });
    }
    Some(ConsumerAdaptationPlan {
        selector: current.selector(),
        pressure_observations: 0,
        upgrade_observations: 0,
        request_keyframe: false,
    })
}

/// Converts room role and receiver bandwidth into an encoding index.
///
/// In small rooms every camera keeps the highest encoding. In larger rooms a
/// featured camera gets the receiver budget, while thumbnails share that budget
/// across the active video routes for that receiver
fn desired_encoding_index(
    session_count: usize,
    featured: bool,
    active_video_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
    encodings: &[&SourceEncodingDescriptor],
) -> usize {
    let highest_index = encodings.len().saturating_sub(1);
    if session_count < MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD {
        return highest_index;
    }
    let Some(receiver_bandwidth_bps) = receiver_bandwidth_bps else {
        return if featured { highest_index } else { 0 };
    };
    let budget_bps = if featured {
        receiver_bandwidth_bps
    } else {
        let divisor = u64::try_from(active_video_route_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(THUMBNAIL_BUDGET_DIVISOR)
            .max(1);
        receiver_bandwidth_bps / divisor
    };
    highest_affordable_encoding_index(encodings, budget_bps, featured)
}

/// Picks the best encoding that has an explicit budget fit.
///
/// Missing `max-br` means the sender did not give us enough bitrate guidance.
/// If all encodings miss it, keep the existing featured versus thumbnail floor
/// instead of pretending the unknown layers are affordable.
fn highest_affordable_encoding_index(
    encodings: &[&SourceEncodingDescriptor],
    budget_bps: u64,
    featured: bool,
) -> usize {
    if encodings
        .iter()
        .all(|encoding| encoding.max_bitrate().is_none())
    {
        return if featured {
            encodings.len().saturating_sub(1)
        } else {
            0
        };
    }
    encodings
        .iter()
        .enumerate()
        .rev()
        .find(|(_index, encoding)| {
            encoding
                .max_bitrate()
                .is_some_and(|bitrate| bitrate <= budget_bps)
        })
        .map_or(0, |(index, _encoding)| index)
}

fn selector_index(selector: SourceSelector, encodings: &[&SourceEncodingDescriptor]) -> usize {
    selector
        .selected_encoding()
        .and_then(|encoding_id| {
            encodings
                .iter()
                .position(|encoding| encoding.encoding_id() == encoding_id)
        })
        .unwrap_or_else(|| encodings.len().saturating_sub(1))
}

/// Projects room-domain selector intent into the transport gate vocabulary.
///
/// `RoomPolicy` selectors must be resolved before this bridge. The transport
/// worker only understands an open route or one concrete RID.
fn source_packet_gate_for_selector(
    source: &PublishedSourceDescriptor,
    selector: SourceSelector,
) -> Option<SourcePacketGate> {
    match selector {
        SourceSelector::Open => Some(SourcePacketGate::Open),
        SourceSelector::Encoding(encoding_id) => source
            .encoding(encoding_id)
            .and_then(SourceEncodingDescriptor::rid)
            .map(|rid| SourcePacketGate::Rid(rid.as_str().to_owned())),
        SourceSelector::RoomPolicy(_) => None,
    }
}

#[cfg(test)]
fn lowest_declared_encoding(source: &PublishedSourceDescriptor) -> Option<SourceEncodingId> {
    let encodings = selectable_encodings(source);
    if encodings.len() < 2 || encodings.iter().any(|encoding| encoding.rid().is_none()) {
        return None;
    }
    let use_declared_order = encodings
        .iter()
        .all(|encoding| encoding.max_bitrate().is_none());
    encodings
        .into_iter()
        .enumerate()
        .min_by_key(|(index, encoding)| {
            if use_declared_order {
                (0_u64, *index)
            } else {
                (encoding.max_bitrate().unwrap_or(u64::MAX), *index)
            }
        })
        .map(|(_index, encoding)| encoding.encoding_id())
}

fn selectable_encodings(source: &PublishedSourceDescriptor) -> Vec<&SourceEncodingDescriptor> {
    let mut encodings = source.encodings().collect::<Vec<_>>();
    if encodings.iter().any(|encoding| encoding.rid().is_none()) {
        return Vec::new();
    }
    let use_declared_order = encodings
        .iter()
        .all(|encoding| encoding.max_bitrate().is_none());
    if !use_declared_order {
        encodings.sort_by_key(|encoding| encoding.max_bitrate().unwrap_or(u64::MAX));
    }
    encodings
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test fixtures should fail loudly when they build invalid source graphs"
    )]

    use o_sfu_router::{MediaKind, Rid};

    use super::*;
    use crate::runtime::source_model::{
        PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
        SourceEncodingDescriptorParts, SourceEncodingId,
    };

    fn source_with_encodings(
        encodings: Vec<SourceEncodingDescriptor>,
    ) -> PublishedSourceDescriptor {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id: PublishedSourceId::from_raw(7),
            owner: PublishedSourceOwner::new(SessionId::Integer(42), ConnectionId::from_raw(9)),
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: None,
            encodings,
        })
        .expect("test source graph should be valid")
    }

    fn encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: Option<&str>,
        max_bitrate: Option<u64>,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: rid.map(Rid::new),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate,
            negotiated_format: None,
            transport_binding: None,
        })
    }

    #[test]
    fn source_selector_bridge_projects_selected_encoding_to_transport_rid() {
        let source_id = PublishedSourceId::from_raw(7);
        let high_encoding_id = SourceEncodingId::from_raw(1);
        let low_encoding_id = SourceEncodingId::from_raw(2);
        let source = source_with_encodings(vec![
            encoding(source_id, high_encoding_id, Some("hi"), Some(750_000)),
            encoding(source_id, low_encoding_id, Some("lo"), Some(150_000)),
        ]);

        let selector = lowest_declared_encoding(&source)
            .map_or(SourceSelector::Open, SourceSelector::Encoding);

        assert_eq!(selector, SourceSelector::Encoding(low_encoding_id));
        assert_eq!(
            source_packet_gate_for_selector(&source, selector),
            Some(SourcePacketGate::Rid(String::from("lo")))
        );
    }

    #[test]
    fn source_selector_bridge_keeps_open_as_an_explicit_transport_gate() {
        let source_id = PublishedSourceId::from_raw(7);
        let source = source_with_encodings(vec![encoding(
            source_id,
            SourceEncodingId::from_raw(1),
            None,
            None,
        )]);

        assert_eq!(
            source_packet_gate_for_selector(&source, SourceSelector::Open),
            Some(SourcePacketGate::Open)
        );
    }

    #[test]
    fn source_selector_bridge_requires_rid_for_rid_packet_selection() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![encoding(source_id, encoding_id, None, None)]);

        assert_eq!(
            source_packet_gate_for_selector(&source, SourceSelector::Encoding(encoding_id)),
            None
        );
    }
}
