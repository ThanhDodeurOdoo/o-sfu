use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    time::Instant,
};

use o_sfu_rfc::rtp;
use str0m::{
    media::{Mid, Rid},
    rtp::Ssrc,
};
use tracing::{debug, warn};

use super::{
    commands::RemoteSourceControl,
    demux::MediaRouteDestination,
    forwarded_packet::ForwardedPacketSource,
    route_control::PacketLayerGate,
    slots::{MediaStore, SessionHandle},
    state::PacketLoopState,
};
use crate::engine::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey, TransportSourceKey},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RegisteredMediaHandle {
    Producer {
        session_key: TransportSessionKey,
        mid: Mid,
    },
    Consumer {
        session_key: TransportSessionKey,
        mid: Mid,
        source_transport_media_id: TransportMediaId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConsumerKeyframeTarget {
    pub(super) source_transport_media_id: TransportMediaId,
    pub(super) rid: Option<Rid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConsumerMidBinding {
    consumer_transport_media_id: TransportMediaId,
    source_transport_media_id: TransportMediaId,
    destination_index: Option<usize>,
}

impl RegisteredMediaHandle {
    pub(super) fn session_key(&self) -> &TransportSessionKey {
        match self {
            Self::Producer { session_key, .. } | Self::Consumer { session_key, .. } => session_key,
        }
    }

    pub(super) fn mid(&self) -> Mid {
        match self {
            Self::Producer { mid, .. } | Self::Consumer { mid, .. } => *mid,
        }
    }
}

pub(in crate::engine::media_transport::rtc) enum DestinationKeyframeTarget {
    Current(Option<Rid>),
    Stale,
}

pub(in crate::engine::media_transport::rtc) fn destination_keyframe_target_rid(
    destination: &MediaRouteDestination,
    open_rid: Option<Rid>,
) -> DestinationKeyframeTarget {
    let target_rid = match destination.packet_gate {
        PacketLayerGate::Rid(rid) => Some(rid),
        PacketLayerGate::OperatingPoint(operating_point) => operating_point.rid(),
        PacketLayerGate::Block => match destination
            .pending_packet_gate
            .as_ref()
            .and_then(PacketLayerGate::selected_rid)
        {
            Some(rid) => Some(rid),
            None => return DestinationKeyframeTarget::Stale,
        },
        PacketLayerGate::Open => open_rid,
    };
    DestinationKeyframeTarget::Current(target_rid)
}

#[derive(Debug, Clone)]
pub(super) struct RemoteSourceRegistration {
    source: TransportSourceKey,
    source_control: RemoteSourceControl,
    pending_packet_gate: Option<PacketLayerGate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DecoderRefreshCodec {
    H264(rtp::h264::PacketizationMode),
    Vp8,
    Unsupported,
}

impl DecoderRefreshCodec {
    fn from_parameters(parameters: &o_sfu_router::MediaStream) -> Option<Self> {
        let mut has_vp8 = false;
        let mut has_unsupported_primary = false;
        for format in parameters.formats() {
            match format.codec() {
                &rtp::CodecName::H264 => return Some(Self::from_h264_format(format)),
                &rtp::CodecName::Vp8 => has_vp8 = true,
                codec if !codec.is_rtx() => has_unsupported_primary = true,
                _ => {}
            }
        }
        if has_vp8 {
            Some(Self::Vp8)
        } else {
            has_unsupported_primary.then_some(Self::Unsupported)
        }
    }

    fn from_h264_format(format: &o_sfu_router::MediaFormat) -> Self {
        let packetization_mode = format
            .settings()
            .find_map(|setting| match setting {
                o_sfu_router::CodecSetting::H264PacketizationMode(mode) => Some(*mode),
                _ => None,
            })
            .unwrap_or(rtp::fmtp::H264_DEFAULT_PACKETIZATION_MODE);
        rtp::h264::PacketizationMode::from_fmtp_value(packetization_mode)
            .map_or(Self::Unsupported, Self::H264)
    }
}

impl RemoteSourceRegistration {
    pub(in crate::engine::media_transport::rtc) fn new(
        source: TransportSourceKey,
        source_control: RemoteSourceControl,
    ) -> Self {
        Self {
            source,
            source_control,
            pending_packet_gate: None,
        }
    }

    pub(super) fn source_session_key(&self) -> &TransportSessionKey {
        self.source.session_key()
    }

    pub(super) fn source(&self) -> &TransportSourceKey {
        &self.source
    }

    pub(super) fn source_control(&self) -> &RemoteSourceControl {
        &self.source_control
    }

    #[cfg(test)]
    pub(super) const fn pending_packet_gate(&self) -> Option<PacketLayerGate> {
        self.pending_packet_gate
    }

    pub(in crate::engine::media_transport::rtc) const fn has_pending_packet_gate(&self) -> bool {
        self.pending_packet_gate.is_some()
    }

    pub(in crate::engine::media_transport::rtc) fn publish_packet_gate(
        &mut self,
        packet_gate: PacketLayerGate,
    ) -> bool {
        if self
            .source_control
            .set_packet_gate(&self.source, packet_gate)
        {
            self.pending_packet_gate = None;
            false
        } else {
            self.pending_packet_gate = Some(packet_gate);
            true
        }
    }

    pub(in crate::engine::media_transport::rtc) fn flush_pending_packet_gate(&mut self) -> bool {
        let Some(packet_gate) = self.pending_packet_gate else {
            return false;
        };
        self.source_control.record_packet_gate_retry();
        if self
            .source_control
            .set_packet_gate(&self.source, packet_gate)
        {
            self.pending_packet_gate = None;
            self.source_control.record_packet_gate_flushed();
            false
        } else {
            true
        }
    }
}

pub(super) type SessionMediaRegistry = BTreeMap<TransportSessionKey, SessionMediaLookup>;

#[derive(Debug, Default, Clone)]
pub(super) struct SessionMediaLookup {
    owned_media: Vec<TransportMediaId>,
    producer_mids: TinyLookup<Mid, TransportMediaId>,
    producer_ssrcs: TinyLookup<Ssrc, TransportMediaId>,
    producer_ssrc_rids: TinyLookup<Ssrc, Rid>,
    consumer_mids: TinyLookup<Mid, ConsumerMidBinding>,
}

impl SessionMediaLookup {
    fn insert_owned_media(&mut self, transport_media_id: TransportMediaId) {
        if !self.owned_media.contains(&transport_media_id) {
            self.owned_media.push(transport_media_id);
        }
    }

    fn remove_owned_media(&mut self, transport_media_id: TransportMediaId) {
        if let Some(position) = self
            .owned_media
            .iter()
            .position(|id| *id == transport_media_id)
        {
            self.owned_media.swap_remove(position);
        }
    }

    fn is_empty(&self) -> bool {
        self.owned_media.is_empty()
            && self.producer_mids.is_empty()
            && self.producer_ssrcs.is_empty()
            && self.producer_ssrc_rids.is_empty()
            && self.consumer_mids.is_empty()
    }
}

fn bind_producer_ssrc(
    mid_registry: &MediaStore,
    session_media: &mut SessionMediaRegistry,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    ssrc: Ssrc,
    rid: Option<Rid>,
) -> bool {
    let Some(RegisteredMediaHandle::Producer {
        session_key: registered_session_key,
        mid,
    }) = mid_registry.get(&transport_media_id)
    else {
        return false;
    };
    if registered_session_key != session_key {
        return false;
    }
    let mid = *mid;

    let Some(session_lookup) = session_media.get_mut(session_key) else {
        debug_assert!(
            session_media.contains_key(session_key),
            "producer SSRC binding missing session media index for registered producer"
        );
        return false;
    };
    if let Some(existing_transport_media_id) = session_lookup.producer_ssrcs.get(&ssrc)
        && existing_transport_media_id != transport_media_id
    {
        warn!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            ?transport_media_id,
            ?existing_transport_media_id,
            ?mid,
            ?ssrc,
            ?rid,
            "ignored producer SSRC binding because SSRC already belongs to another media"
        );
        return false;
    }
    let inserted = session_lookup
        .producer_ssrcs
        .insert(ssrc, transport_media_id)
        .is_none();
    let previous_rid = rid.and_then(|rid| session_lookup.producer_ssrc_rids.insert(ssrc, rid));
    if inserted || previous_rid != rid {
        debug!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            ?transport_media_id,
            ?mid,
            ?ssrc,
            ?rid,
            "learned dynamic producer SSRC binding from RTP header extensions"
        );
    }
    true
}

#[derive(Debug, Clone)]
struct TinyLookup<K, V> {
    entries: Vec<(K, V)>,
}

impl<K, V> Default for TinyLookup<K, V> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<K: Eq, V: Copy> TinyLookup<K, V> {
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some((_entry_key, entry_value)) = self
            .entries
            .iter_mut()
            .find(|(entry_key, _value)| entry_key == &key)
        {
            return Some(mem::replace(entry_value, value));
        }
        self.entries.push((key, value));
        None
    }

    fn remove(&mut self, key: &K) {
        if let Some(position) = self
            .entries
            .iter()
            .position(|(entry_key, _value)| entry_key == key)
        {
            self.entries.swap_remove(position);
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.entries
            .iter()
            .find_map(|(entry_key, value)| (entry_key == key).then_some(*value))
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.entries
            .iter_mut()
            .find_map(|(entry_key, value)| (entry_key == key).then_some(value))
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl PacketLoopState {
    pub(super) fn register_media_handle(
        &mut self,
        handle: RegisteredMediaHandle,
    ) -> TransportMediaId {
        let id = self.next_media_id;
        self.next_media_id = self.next_media_id.saturating_add(1);
        let transport_media_id = TransportMediaId::new(id);
        if let RegisteredMediaHandle::Producer { session_key, mid } = &handle {
            let session_lookup = self.session_media.entry(session_key.clone()).or_default();
            session_lookup.insert_owned_media(transport_media_id);
            session_lookup
                .producer_mids
                .insert(*mid, transport_media_id);
            self.routes.register_local_source(transport_media_id);
        } else if let RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            source_transport_media_id,
        } = &handle
        {
            let session_lookup = self.session_media.entry(session_key.clone()).or_default();
            session_lookup.insert_owned_media(transport_media_id);
            session_lookup.consumer_mids.insert(
                *mid,
                ConsumerMidBinding {
                    consumer_transport_media_id: transport_media_id,
                    source_transport_media_id: *source_transport_media_id,
                    destination_index: None,
                },
            );
        }
        self.mid_registry.insert(transport_media_id, handle);
        transport_media_id
    }

    pub(super) fn resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.mid_registry
            .get(&transport_media_id)
            .map(RegisteredMediaHandle::mid)
    }

    pub(super) fn media_handle(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&RegisteredMediaHandle> {
        self.mid_registry.get(&transport_media_id)
    }

    pub(super) fn producer_media_snapshot_for_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Vec<(TransportMediaId, Mid)> {
        self.session_media
            .get(session_key)
            .map_or_else(Vec::new, |session_lookup| {
                session_lookup
                    .producer_mids
                    .entries
                    .iter()
                    .map(|(mid, transport_media_id)| (*transport_media_id, *mid))
                    .collect()
            })
    }

    pub(super) fn session_has_other_media_mid(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
        excluded_transport_media_id: TransportMediaId,
    ) -> bool {
        self.session_media
            .get(session_key)
            .is_some_and(|session_lookup| {
                session_lookup
                    .owned_media
                    .iter()
                    .copied()
                    .any(|transport_media_id| {
                        transport_media_id != excluded_transport_media_id
                            && self
                                .mid_registry
                                .get(&transport_media_id)
                                .is_some_and(|handle| handle.mid() == mid)
                    })
            })
    }

    /// remove one media handle and every dependent reverse index owned by it
    ///
    /// producer removal clears source packet policy, incoming bitrate counters,
    /// decoder-refresh metadata, live RID state and SSRC lookups
    /// consumer removal clears only the consumer MID lookup because route and
    /// local rewrite teardown are owned by media lifecycle code
    pub(super) fn remove_media_handle(
        &mut self,
        transport_media_id: TransportMediaId,
    ) -> Option<RegisteredMediaHandle> {
        let handle = self.mid_registry.remove(&transport_media_id)?;
        let owner_session_key = handle.session_key().clone();
        match &handle {
            RegisteredMediaHandle::Producer { session_key, mid } => {
                if let Some(session_lookup) = self.session_media.get_mut(session_key) {
                    session_lookup.remove_owned_media(transport_media_id);
                    session_lookup.producer_mids.remove(mid);
                }
                self.clear_producer_ssrcs(session_key, transport_media_id);
                self.routes.unregister_local_source(transport_media_id);
                self.remove_incoming_bitrate_counter(transport_media_id);
            }
            RegisteredMediaHandle::Consumer {
                session_key, mid, ..
            } => {
                if let Some(session_lookup) = self.session_media.get_mut(session_key) {
                    session_lookup.remove_owned_media(transport_media_id);
                    session_lookup.consumer_mids.remove(mid);
                }
            }
        }
        self.prune_empty_session_media(&owner_session_key);
        Some(handle)
    }

    pub(super) fn session_has_registered_media(&self, session_key: &TransportSessionKey) -> bool {
        self.session_media
            .get(session_key)
            .is_some_and(|session_lookup| !session_lookup.owned_media.is_empty())
    }

    fn prune_empty_session_media(&mut self, session_key: &TransportSessionKey) {
        if self
            .session_media
            .get(session_key)
            .is_some_and(SessionMediaLookup::is_empty)
        {
            self.session_media.remove(session_key);
        }
    }

    /// refresh the decoder-refresh classifier from negotiated RTP parameters
    ///
    /// returns the previous classifier so route registration rollback can put
    /// the source metadata back exactly as it was before a failed mutation
    pub(super) fn refresh_source_decoder_refresh_codec(
        &mut self,
        source_transport_media_id: TransportMediaId,
        parameters: &o_sfu_router::MediaStream,
    ) -> Option<DecoderRefreshCodec> {
        let previous = self.routes.decoder_refresh_codec(source_transport_media_id);
        self.routes.set_decoder_refresh_codec(
            source_transport_media_id,
            DecoderRefreshCodec::from_parameters(parameters),
        );
        previous
    }

    pub(super) fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.routes
            .expired_active_speaker_source_ids(now)
            .into_iter()
            .filter_map(|source_transport_media_id| {
                self.source_room_instance_id(source_transport_media_id)
            })
            .collect()
    }

    pub(super) fn source_transport_media_id_for_mid(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.session_media
            .get(source_session_key)
            .and_then(|source_lookup| source_lookup.producer_mids.get(&source_mid))
    }

    pub(super) fn source_transport_media_id_for_ssrc(
        &self,
        source_session_key: &TransportSessionKey,
        source_ssrc: Ssrc,
    ) -> Option<TransportMediaId> {
        self.session_media
            .get(source_session_key)
            .and_then(|source_lookup| source_lookup.producer_ssrcs.get(&source_ssrc))
    }

    pub(super) fn source_rid_for_ssrc(
        &self,
        source_session_key: &TransportSessionKey,
        source_ssrc: Ssrc,
    ) -> Option<Rid> {
        self.session_media
            .get(source_session_key)
            .and_then(|source_lookup| source_lookup.producer_ssrc_rids.get(&source_ssrc))
    }

    /// learn the SSRC chosen by a RID-only publisher from RTP header metadata
    ///
    /// chrome can answer simulcast offers with RIDs but no SSRC attributes
    /// str0m can still demux the first packets through MID/RID header
    /// extensions
    /// the adapter must persist that discovery here because later packets may
    /// only carry the SSRC
    /// without this late binding, packet routing, RID gate metadata and bitrate
    /// accounting lose the producer as soon as the browser stops repeating
    /// mid/rid extensions
    ///
    /// the binding is accepted only for the already resolved producer media id
    /// a same-session SSRC collision with a different media id is treated as a
    /// suspicious transport fact and ignored rather than stealing ownership
    pub(super) fn learn_producer_ssrc_binding(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        ssrc: Ssrc,
        rid: Option<Rid>,
    ) {
        if bind_producer_ssrc(
            &self.mid_registry,
            &mut self.session_media,
            session_key,
            transport_media_id,
            ssrc,
            rid,
        ) {
            self.routes.remember_producer_ssrc(transport_media_id, ssrc);
        }
    }

    pub(super) fn learn_producer_ssrc_binding_from_forwarded_source(
        &mut self,
        source: &ForwardedPacketSource,
        transport_media_id: TransportMediaId,
        ssrc: Ssrc,
        rid: Option<Rid>,
    ) {
        match source {
            ForwardedPacketSource::Relayed(session_key) => {
                self.learn_producer_ssrc_binding(session_key, transport_media_id, ssrc, rid);
            }
            ForwardedPacketSource::Local(session_handle) => {
                self.learn_producer_ssrc_binding_from_session_handle(
                    *session_handle,
                    transport_media_id,
                    ssrc,
                    rid,
                );
            }
        }
    }

    fn learn_producer_ssrc_binding_from_session_handle(
        &mut self,
        session_handle: SessionHandle,
        transport_media_id: TransportMediaId,
        ssrc: Ssrc,
        rid: Option<Rid>,
    ) {
        let Some(session_key) = self.users.key_for_handle(session_handle) else {
            return;
        };
        if bind_producer_ssrc(
            &self.mid_registry,
            &mut self.session_media,
            session_key,
            transport_media_id,
            ssrc,
            rid,
        ) {
            self.routes.remember_producer_ssrc(transport_media_id, ssrc);
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn consumer_source_transport_media_id_for_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.session_media
            .get(consumer_session_key)
            .and_then(|consumer_lookup| consumer_lookup.consumer_mids.get(&consumer_mid))
            .map(|binding| binding.source_transport_media_id)
    }

    /// resolve consumer RTCP feedback to the currently active producer target
    ///
    /// this is the packet-loop feedback path
    /// missing indexes, removed routes and inactive routes are treated as stale
    /// feedback because those can race with teardown after `str0m` emits a
    /// request
    ///
    /// selected destination gates override the feedback RID so RID-less browser
    /// PLI stays scoped to the routed simulcast layer
    #[inline]
    pub(super) fn active_consumer_keyframe_target_for_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
        feedback_rid: Option<Rid>,
    ) -> Option<ConsumerKeyframeTarget> {
        let binding = self
            .session_media
            .get(consumer_session_key)?
            .consumer_mids
            .get(&consumer_mid)?;
        let route_entry = self.routes.local_route(binding.source_transport_media_id)?;
        // a miss means feedback raced with route teardown or index repair
        let destination = route_entry.destinations.get(binding.destination_index?)?;
        debug_assert_eq!(&destination.dest_session, consumer_session_key);
        debug_assert_eq!(
            destination.dest_transport_media_id,
            binding.consumer_transport_media_id
        );
        if !route_entry.source_active || !destination.active {
            return None;
        }
        let DestinationKeyframeTarget::Current(rid) =
            destination_keyframe_target_rid(destination, feedback_rid)
        else {
            return None;
        };
        Some(ConsumerKeyframeTarget {
            source_transport_media_id: binding.source_transport_media_id,
            rid,
        })
    }

    /// updates the cached destination slot for one consumer MID binding
    ///
    /// callers pass both sides of the route identity so a late repair from an
    /// old route cannot relink a MID to a different source
    pub(super) fn set_consumer_destination_index(
        &mut self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
        consumer_transport_media_id: TransportMediaId,
        source_transport_media_id: TransportMediaId,
        destination_index: Option<usize>,
    ) {
        let Some(binding) = self
            .session_media
            .get_mut(consumer_session_key)
            .and_then(|consumer_lookup| consumer_lookup.consumer_mids.get_mut(&consumer_mid))
        else {
            return;
        };
        if binding.consumer_transport_media_id == consumer_transport_media_id
            && binding.source_transport_media_id == source_transport_media_id
        {
            binding.destination_index = destination_index;
        }
    }

    pub(super) fn consumer_destination_index(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
        consumer_transport_media_id: TransportMediaId,
        source_transport_media_id: TransportMediaId,
    ) -> Option<usize> {
        let binding = self
            .session_media
            .get(consumer_session_key)?
            .consumer_mids
            .get(&consumer_mid)?;
        (binding.consumer_transport_media_id == consumer_transport_media_id
            && binding.source_transport_media_id == source_transport_media_id)
            .then_some(binding.destination_index?)
    }

    fn source_room_instance_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<RoomInstanceId> {
        self.media_handle(source_transport_media_id)
            .map(|handle| handle.session_key().room_instance_id())
            .or_else(|| {
                self.routes
                    .remote_source(source_transport_media_id)
                    .map(|registration| registration.source_session_key().room_instance_id())
            })
    }

    pub(super) fn remove_session_media_handles(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Vec<(TransportMediaId, RegisteredMediaHandle)> {
        let removed_ids = self.session_media.get(session_key).map_or_else(
            || {
                debug_assert!(
                    self.mid_registry
                        .iter()
                        .all(|(_transport_media_id, handle)| handle.session_key() != session_key),
                    "session media index missing handles for session"
                );
                Vec::new()
            },
            |session_lookup| session_lookup.owned_media.clone(),
        );
        let mut removed_handles = Vec::with_capacity(removed_ids.len());
        for transport_media_id in &removed_ids {
            if let Some(handle) = self.remove_media_handle(*transport_media_id) {
                removed_handles.push((*transport_media_id, handle));
            }
        }
        removed_handles
    }

    pub(super) fn refresh_producer_ssrc_bindings(
        &mut self,
        session_key: &TransportSessionKey,
        mid: Mid,
        parameters: &o_sfu_router::MediaStream,
    ) {
        let Some(transport_media_id) = self
            .session_media
            .get(session_key)
            .and_then(|producer_lookup| producer_lookup.producer_mids.get(&mid))
        else {
            return;
        };
        self.clear_producer_ssrcs(session_key, transport_media_id);
        self.refresh_source_decoder_refresh_codec(transport_media_id, parameters);
        let accepted_ssrcs = parameters
            .bindings()
            .filter_map(|binding| {
                let ssrc = binding.ssrc().map(Ssrc::from)?;
                Some((ssrc, binding.rid().map(Rid::from)))
            })
            .filter_map(|(ssrc, rid)| {
                bind_producer_ssrc(
                    &self.mid_registry,
                    &mut self.session_media,
                    session_key,
                    transport_media_id,
                    ssrc,
                    rid,
                )
                .then_some(ssrc)
            })
            .collect::<Vec<_>>();
        self.routes
            .replace_producer_ssrcs(transport_media_id, accepted_ssrcs);
    }

    pub(super) fn clear_producer_ssrc_bindings_for_mid(
        &mut self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) {
        let Some(transport_media_id) = self
            .session_media
            .get(session_key)
            .and_then(|producer_lookup| producer_lookup.producer_mids.get(&mid))
        else {
            return;
        };
        self.clear_producer_ssrcs(session_key, transport_media_id);
        self.routes
            .set_decoder_refresh_codec(transport_media_id, None);
        self.routes
            .replace_producer_ssrcs(transport_media_id, Vec::new());
    }

    fn clear_producer_ssrcs(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) {
        let Some(ssrcs) = self.routes.clear_producer_ssrcs(transport_media_id) else {
            return;
        };
        let Some(session_lookup) = self.session_media.get_mut(session_key) else {
            return;
        };
        for ssrc in ssrcs {
            if session_lookup.producer_ssrcs.get(&ssrc) == Some(transport_media_id) {
                session_lookup.producer_ssrcs.remove(&ssrc);
                session_lookup.producer_ssrc_rids.remove(&ssrc);
            }
        }
    }
}

#[cfg(test)]
mod tests;
