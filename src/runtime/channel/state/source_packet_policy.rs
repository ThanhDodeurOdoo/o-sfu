//! Room-owned source-layer policy and post-effect commit checks.
//!
//! # Boundary role
//!
//! This module is the channel side of simulcast and layered source selection.
//! It maps authoritative room state plus best-effort transport observations
//! into per-consumer [`SourceSelector`] values, then describes the transport
//! calls needed to apply those values as packet gates.
//!
//! No function in this file awaits transport work or mutates packet-loop state.
//! The async effect layer consumes the planned updates, calls the transport
//! adapter without holding the channel lock, then calls the commit methods here
//! with only the updates that the transport accepted.
//!
//! Receiver-driven simulcast policy lives on consumer selections so different
//! receivers may choose different encodings from the same published source.

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_protocol::shared::{SessionId, StreamType};

use super::{
    super::{ChannelEventMessage, outbound::MessageFanout},
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
        ActiveSpeakerSource, ReceiverBandwidthSnapshot, SourcePacketGate,
        SourcePacketOperatingPoint, TransportMediaId,
    },
};

/// Minimum room size where camera simulcast adaptation starts constraining receivers.
///
/// With the current value, one-on-one calls and two-session rooms keep every
/// camera on the highest encoding. The third joined session turns on multiparty
/// policy, so non-featured camera routes may be constrained to a lower RID when
/// the receiver budget does not fit the high layer.
const MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD: usize = 3;

/// Maximum number of active-speaker cameras that keep featured quality treatment.
///
/// Active speaker observations arrive as an ordered transport snapshot. If the
/// snapshot resolves to seven camera owners, only the first five keep featured
/// camera treatment. The remaining speakers are budgeted like thumbnails until
/// a later refresh moves them into the first five.
const ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT: usize = 5;

/// Number of policy refreshes that must agree before a lower encoding is committed.
///
/// Downswitch pressure is counted only when receiver bandwidth is known. With
/// the current value, the first refresh that targets a lower encoding stores
/// pressure but keeps the current gate. If the next refresh still targets the
/// lower encoding, the selector is committed and the pressure counter resets.
const DOWNSWITCH_PRESSURE_OBSERVATIONS: u8 = 2;

/// Number of policy refreshes that must agree before a higher encoding is committed.
///
/// Upswitches are intentionally slower than downswitches. With the current
/// value, a low route needs three refreshes that still target a higher encoding
/// before the gate changes and a keyframe is requested. One optimistic estimate
/// is not enough to move the receiver back to a larger layer.
const UPSWITCH_STABLE_OBSERVATIONS: u8 = 3;

/// Extra conservatism applied after thumbnail budget is split across visible videos.
///
/// Thumbnail budget is `receiver_bandwidth / (active_video_route_count * this
/// value)`. With the current value, a receiver estimated at `1_800_000` bps and
/// watching three active videos gives each thumbnail a `300_000` bps budget,
/// enough for the `150_000` bps low layer but not the `900_000` bps high layer.
const THUMBNAIL_BUDGET_DIVISOR: u64 = 2;

/// One receiver-side source selection that is ready for the effect boundary.
///
/// The update carries the transport handles and connection ids observed while
/// planning. Commit revalidates them after async transport work so stale
/// replacement or cleanup events cannot write selector state onto a newer route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ConsumerPacketSelectionUpdate {
    /// Receiver whose route should be constrained or whose hysteresis counters should move.
    consumer_session_id: SessionId,
    /// Receiver connection observed before the effect boundary awaited transport work.
    consumer_connection_id: ConnectionId,
    /// Publisher session used to address the source-side transport route.
    source_session_id: SessionId,
    /// Publisher connection observed before the effect boundary awaited transport work.
    source_connection_id: ConnectionId,
    /// Source-side media handle that identifies the published packets at transport level.
    source_transport_media_id: TransportMediaId,
    /// Consumer-side media handle that identifies this receiver's subscribed route.
    consumer_transport_media_id: TransportMediaId,
    /// Room-domain source identity used to find the same consumer selection during commit.
    source_id: PublishedSourceId,
    /// Authoritative selector to store if the route still matches this staged update.
    selector: SourceSelector,
    /// Pending downswitch pressure counted in policy refreshes, not wall time.
    pressure_observations: u8,
    /// Pending upswitch stability counted in policy refreshes, not wall time.
    upgrade_observations: u8,
    /// Transport gate to apply when the packet-facing selector changed.
    ///
    /// `None` means the update only advances hysteresis counters. The effect
    /// executor should then skip packet-gate I/O but still commit the counters
    /// if the route is still current.
    packet_gate: Option<SourcePacketGate>,
    /// Whether a successful upswitch should ask the receiver for a fresh keyframe.
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

/// Server-owned featured state derived from active-speaker observations.
///
/// This is kept next to packet policy because the same observation decides both
/// visible layout state and the quality floor used for receiver adaptation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct FeaturedSessionUpdate {
    /// Session whose public layout projection should change.
    session_id: SessionId,
    /// New featured value, where `None` means no explicit server-derived state.
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
    /// Plans deterministic per-consumer source selectors for live video routes.
    ///
    /// The snapshot inputs are best-effort transport observations. They do not
    /// change room authority on their own. This method combines them with
    /// committed source descriptors, subscription state and active-speaker
    /// layout state to build staged updates for the effect executor.
    ///
    /// A `packet_gate` is present only when the selected encoding changes at
    /// the transport boundary. Counter-only updates are still emitted so
    /// sustained pressure and stable recovery survive across policy refreshes.
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
        let selectable_encoding_ladders = selectable_encoding_ladders_by_source(&self.sources);
        self.consumer_index
            .iter()
            .filter_map(|(consumer_key, consumer_state)| {
                let source = self.sources.get(&consumer_key.source_id)?;
                let encodings = selectable_encoding_ladders.get(&consumer_key.source_id)?;
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
                    source.stream_type(),
                    encodings,
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
                    Some(source_packet_gate_for_selector(source, adaptation.selector).ok()?)
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

    /// Counts active visible routes per receiver for thumbnail budget sharing.
    ///
    /// The count is built from current channel indexes, not from transport
    /// snapshots. Removed routes, inactive producers and stale consumer
    /// connections are ignored so the budget reflects the room state that will
    /// also be used during commit.
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

    /// Maps active audio speakers to camera owners that should keep featured quality.
    ///
    /// The transport reports active sources by media handle. Channel state owns
    /// the source graph, so this helper resolves audio activity back to owner
    /// sessions that also publish a camera. Only the first few active speakers
    /// are treated as featured to keep large rooms from opening too many high
    /// quality camera routes at once.
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

    /// Plans public featured-state changes from the same active-speaker snapshot.
    ///
    /// The returned updates are committed together with source-policy effects
    /// so the user-visible `isFeatured` projection and the quality floor come
    /// from the same observation. If there is no current active speaker, the
    /// method only emits clears when some session still has server-derived
    /// featured state.
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

    /// Commits selector updates that still match the routed consumer media.
    ///
    /// The effect executor may await transport work before calling this method.
    /// Every connection and transport handle is checked again so stale effects
    /// from a replaced socket or removed route become no-ops. Accepted updates
    /// store both the selected source-domain intent and the hysteresis counters
    /// that make later refreshes deterministic.
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

    /// Commits featured-state updates and builds the compatibility fanout.
    ///
    /// The returned fanout is emitted after the caller releases the channel
    /// write lock. That keeps layout projection consistent with room state
    /// while preserving the no-I/O-under-lock rule used by the effect layer.
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
    /// Selector that should become authoritative after any required transport work.
    selector: SourceSelector,
    /// Downswitch pressure to persist when the lower target is not committed yet.
    pressure_observations: u8,
    /// Upswitch stability to persist when the higher target is not committed yet.
    upgrade_observations: u8,
    /// Whether the effect executor should request a decodable frame after applying the gate.
    request_keyframe: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourcePacketGateProjectionError {
    MissingEncoding,
    MissingRid,
    MissingTemporalMetadata,
    TemporalLayerExceedsAdvertised,
    UnsupportedRoomPolicy,
}

/// Normalizes transport-session bandwidth observations to room session ids.
///
/// The selector policy is keyed by receiver session, while the transport
/// snapshot is keyed by transport session. This conversion keeps the transport
/// identity at the boundary and lets the rest of the planner use room-domain
/// keys only.
fn receiver_bandwidth_by_session(snapshot: &ReceiverBandwidthSnapshot) -> BTreeMap<SessionId, u64> {
    snapshot
        .per_session
        .iter()
        .map(|(session_key, estimate_bps)| (session_key.session_id().clone(), *estimate_bps))
        .collect()
}

/// Chooses the next receiver selector using deterministic refresh-based hysteresis.
///
/// This is the policy core for camera simulcast. It returns `None` when a
/// source is not eligible for receiver-driven adaptation, which keeps audio,
/// screen share and single-encoding sources out of the RID gate path.
///
/// Pressure and recovery are counted in policy refreshes rather than wall time.
/// That makes behavior deterministic even when transport observation wakeups
/// are coalesced. Downswitches wait for repeated pressure so one low estimate
/// does not drop a layer immediately. Upswitches wait longer and request a
/// keyframe when the chosen layer changes, because the receiver needs a fresh
/// decodable frame.
fn consumer_adaptation_plan(
    session_count: usize,
    stream_type: StreamType,
    encodings: &[&SourceEncodingDescriptor],
    current: ConsumerSourceSelection,
    featured: bool,
    active_video_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
) -> Option<ConsumerAdaptationPlan> {
    if stream_type != StreamType::Camera {
        return None;
    }
    if encodings.len() < 2 {
        return None;
    }
    let current_index = selector_index(current.selector(), encodings);
    let target_index = desired_encoding_index(
        session_count,
        featured,
        active_video_route_count,
        receiver_bandwidth_bps,
        encodings,
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
/// across the active video routes for that receiver.
///
/// Missing bandwidth is treated as startup state. Featured cameras keep the
/// highest declared encoding and thumbnails start at the lowest encoding until
/// the transport has enough receiver-side evidence to refine the choice.
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

/// Interprets an existing selector as an index in the sorted encoding ladder.
///
/// An unconstrained or unrecognized selector is treated as the highest layer.
/// That matches what `Open` means at the transport boundary and avoids
/// inventing pressure for a route that is already receiving the best available
/// encoding.
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
/// `RoomPolicy` selectors must be resolved before this bridge. `Encoding`
/// selectors require a negotiated RID because the current packet gate for
/// simulcast is RID-based. `OperatingPoint` selectors validate their temporal
/// ceiling against the source descriptor before crossing into transport state.
///
/// Errors mean the selector cannot be enforced safely at the transport
/// boundary. Callers should skip the update rather than store a selector that
/// the packet loop cannot apply.
fn source_packet_gate_for_selector(
    source: &PublishedSourceDescriptor,
    selector: SourceSelector,
) -> Result<SourcePacketGate, SourcePacketGateProjectionError> {
    match selector {
        SourceSelector::Open => Ok(SourcePacketGate::Open),
        SourceSelector::Encoding(encoding_id) => {
            let encoding = source
                .encoding(encoding_id)
                .ok_or(SourcePacketGateProjectionError::MissingEncoding)?;
            let rid = encoding
                .rid()
                .ok_or(SourcePacketGateProjectionError::MissingRid)?;
            Ok(SourcePacketGate::Rid(rid.as_str().to_owned()))
        }
        SourceSelector::OperatingPoint(operating_point) => {
            let encoding = source
                .encoding(operating_point.encoding_id())
                .ok_or(SourcePacketGateProjectionError::MissingEncoding)?;
            let max_temporal_layer_id = encoding
                .max_temporal_layer_id()
                .ok_or(SourcePacketGateProjectionError::MissingTemporalMetadata)?;
            if operating_point.max_temporal_layer_id() > max_temporal_layer_id {
                return Err(SourcePacketGateProjectionError::TemporalLayerExceedsAdvertised);
            }
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(
                    encoding.rid().map(|rid| rid.as_str().to_owned()),
                    operating_point.max_temporal_layer_id().as_u8(),
                ),
            ))
        }
        SourceSelector::RoomPolicy(_) => {
            Err(SourcePacketGateProjectionError::UnsupportedRoomPolicy)
        }
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

/// Returns the RID-addressable encoding ladder used by camera adaptation.
///
/// The current transport gate can only constrain camera simulcast through RID
/// or through an operating point derived from a concrete encoding. If any
/// encoding lacks a RID, this helper returns an empty ladder so the planner
/// leaves that source open instead of applying a partial policy.
///
/// When bitrate ceilings are available they define the ladder order. If no
/// ceiling is available, declaration order is preserved so negotiation remains
/// the source of truth.
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

fn selectable_encoding_ladders_by_source(
    sources: &BTreeMap<PublishedSourceId, PublishedSourceDescriptor>,
) -> BTreeMap<PublishedSourceId, Vec<&SourceEncodingDescriptor>> {
    sources
        .iter()
        .filter_map(|(source_id, source)| {
            let encodings = selectable_encodings(source);
            (!encodings.is_empty()).then_some((*source_id, encodings))
        })
        .collect()
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
        SourceEncodingDescriptorParts, SourceEncodingId, SourceOperatingPoint,
        SourceTemporalLayerId,
    };

    fn source_with_encodings(
        encodings: Vec<SourceEncodingDescriptor>,
    ) -> PublishedSourceDescriptor {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id: PublishedSourceId::from_raw(7),
            owner: PublishedSourceOwner::new(SessionId::Integer(42)),
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
            max_temporal_layer_id: None,
            negotiated_format: None,
        })
    }

    fn layered_encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: Option<&str>,
        max_temporal_layer_id: u8,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: rid.map(Rid::new),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: None,
            max_temporal_layer_id: SourceTemporalLayerId::new(max_temporal_layer_id),
            negotiated_format: None,
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
            Ok(SourcePacketGate::Rid(String::from("lo")))
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
            Ok(SourcePacketGate::Open)
        );
    }

    #[test]
    fn source_selector_bridge_requires_rid_for_rid_packet_selection() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![encoding(source_id, encoding_id, None, None)]);

        assert_eq!(
            source_packet_gate_for_selector(&source, SourceSelector::Encoding(encoding_id)),
            Err(SourcePacketGateProjectionError::MissingRid)
        );
    }

    #[test]
    fn source_selector_bridge_projects_operating_point_to_transport_layer_gate() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![layered_encoding(
            source_id,
            encoding_id,
            Some("hi"),
            2,
        )]);
        let temporal_layer = SourceTemporalLayerId::new(1)
            .expect("test temporal layer should fit the RFC 9626 TID range");

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(Some(String::from("hi")), 1)
            ))
        );
    }

    #[test]
    fn source_selector_bridge_rejects_operating_points_without_advertised_temporal_metadata() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source =
            source_with_encodings(vec![encoding(source_id, encoding_id, Some("hi"), None)]);
        let temporal_layer = SourceTemporalLayerId::base();

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Err(SourcePacketGateProjectionError::MissingTemporalMetadata)
        );
    }

    #[test]
    fn source_selector_bridge_projects_advertised_base_layer_operating_point() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![layered_encoding(
            source_id,
            encoding_id,
            Some("hi"),
            0,
        )]);
        let temporal_layer = SourceTemporalLayerId::base();

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(Some(String::from("hi")), 0)
            ))
        );
    }

    #[test]
    fn source_selector_bridge_rejects_operating_points_above_advertised_layer() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![layered_encoding(source_id, encoding_id, None, 1)]);
        let temporal_layer = SourceTemporalLayerId::new(2)
            .expect("test temporal layer should fit the RFC 9626 TID range");

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Err(SourcePacketGateProjectionError::TemporalLayerExceedsAdvertised)
        );
    }
}
