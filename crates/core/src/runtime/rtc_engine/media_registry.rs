//! transport-media ownership indexes for the rtc packet loop
//!
//! this module keeps every worker-local lookup that turns negotiated browser
//! media identity into `TransportMediaId` values used by packet routing
//! producers, consumers and remote sources all share that id space, while their
//! reverse indexes stay separate so teardown can clean only the ownership facts
//! that belong to one registration
//!
//! public media ids remain sparse and stable because room state, diagnostics,
//! recording and relay state all expose them
//! packet lookup stays session-scoped: one live session owns small MID and SSRC
//! vectors that are cheaper to scan than nested composite-key maps on the hot
//! path
//!
//! the registry is stateful packet-loop metadata
//! it does not decide room policy or route selection, but incorrect entries here
//! make ingress routing, local forwarding, relay fanout, active-speaker expiry
//! and bitrate accounting point at the wrong source

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
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
    forwarded_packet::ForwardedPacketSource,
    route_control::PacketLayerGate,
    slots::{MediaStore, SessionHandle},
    state::PacketLoopState,
};
use crate::runtime::{
    RoomInstanceId,
    media_transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportAdapterError,
        TransportMediaId, TransportSessionKey, TransportSourceKey,
    },
};

/// worker-owned browser media handle stored under one transport media id
///
/// producers represent browser uploads that can become packet sources
/// consumers represent browser downloads that consume packets from one source
/// media id
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RegisteredMediaHandle {
    /// browser upload owned by a session and negotiated under one MID
    Producer {
        /// session that owns the uploading `Rtc`
        session_key: TransportSessionKey,
        /// browser-facing MID for this producer media section
        mid: Mid,
    },
    /// browser download owned by a session and fed by one source media id
    Consumer {
        /// session that owns the receiving `Rtc`
        session_key: TransportSessionKey,
        /// browser-facing MID for this consumer media section
        mid: Mid,
        /// producer or remote-source id that feeds this consumer route
        source_transport_media_id: TransportMediaId,
    },
}

impl RegisteredMediaHandle {
    /// returns the session that owns the browser media section
    pub(super) fn session_key(&self) -> &TransportSessionKey {
        match self {
            Self::Producer { session_key, .. } | Self::Consumer { session_key, .. } => session_key,
        }
    }

    /// returns the negotiated MID that identifies the browser media section
    pub(super) fn mid(&self) -> Mid {
        match self {
            Self::Producer { mid, .. } | Self::Consumer { mid, .. } => *mid,
        }
    }
}

/// remote producer placeholder used by consumer routes on this worker
///
/// a remote source has no local `RegisteredMediaHandle`, but the packet loop
/// still needs source ownership and a command path back to the producer worker
/// for keyframe requests and relay-layer gates
#[derive(Debug, Clone)]
pub(super) struct RemoteSourceRegistration {
    /// source identity owned by the producer worker
    source: TransportSourceKey,
    /// best-effort command path back to the producer worker
    source_control: RemoteSourceControl,
    /// latest source gate still waiting for source-worker command acceptance
    pending_packet_gate: Option<PacketLayerGate>,
}

/// codec class used for decoder-refresh detection on forwarded keyframes
///
/// this is intentionally smaller than the negotiated codec list
/// the packet path only needs to know whether it can recognize a refresh frame
/// from packet payload bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DecoderRefreshCodec {
    /// h264 source where IDR detection is available
    H264,
    /// vp8 source where keyframe detection is available
    Vp8,
    /// primary codec exists but packet-level refresh detection is not supported
    Unsupported,
}

impl DecoderRefreshCodec {
    /// derive the packet-level refresh classifier from negotiated RTP parameters
    ///
    /// h264 wins over vp8 because an IDR is the strongest explicit decoder
    /// refresh signal this packet path can inspect
    fn from_parameters(parameters: &o_sfu_router::MediaStream) -> Option<Self> {
        let mut has_vp8 = false;
        let mut has_unsupported_primary = false;
        for format in parameters.formats() {
            match format.codec() {
                &rtp::CodecName::H264 => return Some(Self::H264),
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
}

impl RemoteSourceRegistration {
    /// create a remote-source entry for one source owner and control handle
    fn new(source: TransportSourceKey, source_control: RemoteSourceControl) -> Self {
        Self {
            source,
            source_control,
            pending_packet_gate: None,
        }
    }

    /// returns the session that owns the remote producer
    pub(super) fn source_session_key(&self) -> &TransportSessionKey {
        self.source.session_key()
    }

    pub(super) fn source(&self) -> &TransportSourceKey {
        &self.source
    }

    /// returns the command handle used to reach the remote producer worker
    pub(super) fn source_control(&self) -> &RemoteSourceControl {
        &self.source_control
    }

    #[cfg(test)]
    pub(super) const fn pending_packet_gate(&self) -> Option<PacketLayerGate> {
        self.pending_packet_gate
    }

    fn publish_packet_gate(&mut self, packet_gate: PacketLayerGate) {
        if self
            .source_control
            .set_packet_gate(&self.source, packet_gate)
        {
            self.pending_packet_gate = None;
        } else {
            self.pending_packet_gate = Some(packet_gate);
        }
    }

    fn flush_pending_packet_gate(&mut self) {
        let Some(packet_gate) = self.pending_packet_gate else {
            return;
        };
        self.source_control.record_packet_gate_retry();
        if self
            .source_control
            .set_packet_gate(&self.source, packet_gate)
        {
            self.pending_packet_gate = None;
            self.source_control.record_packet_gate_flushed();
        }
    }
}

/// per-session packet lookup state keyed by public session identity
pub(super) type SessionMediaRegistry = BTreeMap<TransportSessionKey, SessionMediaLookup>;

/// media lookup tables owned by one live browser session
///
/// this keeps packet lookup scoped to one session instead of using composite-key
/// maps for every MID or SSRC probe
/// each vector is expected to stay small
/// because one browser session owns only its negotiated media lines
#[derive(Debug, Default, Clone)]
pub(super) struct SessionMediaLookup {
    owned_media: Vec<TransportMediaId>,
    producer_mids: TinyLookup<Mid, TransportMediaId>,
    producer_ssrcs: TinyLookup<Ssrc, TransportMediaId>,
    producer_ssrc_rids: TinyLookup<Ssrc, Rid>,
    consumer_mids: TinyLookup<Mid, TransportMediaId>,
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

fn learn_producer_ssrc_binding_for_session(
    mid_registry: &MediaStore,
    session_media: &mut SessionMediaRegistry,
    producer_ssrcs_by_media: &mut BTreeMap<TransportMediaId, Vec<Ssrc>>,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    ssrc: Ssrc,
    rid: Option<Rid>,
) {
    let Some(RegisteredMediaHandle::Producer {
        session_key: registered_session_key,
        mid,
    }) = mid_registry.get(&transport_media_id)
    else {
        return;
    };
    if registered_session_key != session_key {
        return;
    }
    let mid = *mid;

    let (inserted, previous_rid) = if let Some(session_lookup) = session_media.get_mut(session_key)
    {
        if let Some(existing_transport_media_id) = session_lookup.producer_ssrcs.get(&ssrc)
            && existing_transport_media_id != transport_media_id
        {
            warn!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                ?transport_media_id,
                ?existing_transport_media_id,
                ?mid,
                ?ssrc,
                ?rid,
                "ignored dynamic producer SSRC binding because SSRC already belongs to another media"
            );
            return;
        }
        let inserted = session_lookup
            .producer_ssrcs
            .insert(ssrc, transport_media_id)
            .is_none();
        let previous_rid = rid.and_then(|rid| session_lookup.producer_ssrc_rids.insert(ssrc, rid));
        (inserted, previous_rid)
    } else {
        let session_lookup = session_media.entry(session_key.clone()).or_default();
        let inserted = session_lookup
            .producer_ssrcs
            .insert(ssrc, transport_media_id)
            .is_none();
        let previous_rid = rid.and_then(|rid| session_lookup.producer_ssrc_rids.insert(ssrc, rid));
        (inserted, previous_rid)
    };

    let ssrcs = producer_ssrcs_by_media
        .entry(transport_media_id)
        .or_default();
    if !ssrcs.contains(&ssrc) {
        ssrcs.push(ssrc);
    }
    if inserted || previous_rid != rid {
        debug!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id(),
            ?transport_media_id,
            ?mid,
            ?ssrc,
            ?rid,
            "learned dynamic producer SSRC binding from RTP header extensions"
        );
    }
}

/// small linear lookup used only behind one session
///
/// the packet path prefers this over a map because per-session MID and SSRC sets
/// are tiny while avoiding node allocation or hashing on each probe
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

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl PacketLoopState {
    /// allocate a transport media id and register its primary reverse indexes
    ///
    /// producers gain a `(session, MID)` lookup and an empty SSRC set
    /// ssrc bindings are filled later by negotiation or dynamic RTP header discovery
    /// consumers gain a `(session, MID)` lookup to their source media id so RTCP
    /// feedback can be mapped back to the producer route
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
            self.producer_ssrcs_by_media
                .insert(transport_media_id, Vec::new());
        } else if let RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            source_transport_media_id,
        } = &handle
        {
            let session_lookup = self.session_media.entry(session_key.clone()).or_default();
            session_lookup.insert_owned_media(transport_media_id);
            session_lookup
                .consumer_mids
                .insert(*mid, *source_transport_media_id);
        }
        self.mid_registry.insert(transport_media_id, handle);
        transport_media_id
    }

    /// resolve a transport media id back to its browser-facing MID
    pub(super) fn resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.mid_registry
            .get(&transport_media_id)
            .map(RegisteredMediaHandle::mid)
    }

    /// return the primary media handle for a transport media id
    pub(super) fn media_handle(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&RegisteredMediaHandle> {
        self.mid_registry.get(&transport_media_id)
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
                self.clear_producer_ssrc_bindings(transport_media_id, session_key);
                self.forget_source_side_tables(transport_media_id);
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

    /// report whether a session owns any registered producer or consumer media
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

    /// register or replace the command path for a remote source media id
    ///
    /// a remote source id can be reused only when it still names the same source
    /// session
    /// a different owner is rejected so stale room effects cannot steal an
    /// existing source id
    ///
    /// # errors
    ///
    /// returns `InvalidInput` when the media id is already registered for a
    /// different remote source session
    pub(super) fn register_remote_source(
        &mut self,
        source: &TransportSourceKey,
        source_control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        let source_transport_media_id = source.transport_media_id();
        let registration = RemoteSourceRegistration::new(source.clone(), source_control);
        match self.remote_source_registry.entry(source_transport_media_id) {
            Entry::Occupied(mut entry) if entry.get().source() == source => {
                Ok(Some(entry.insert(registration)))
            }
            Entry::Occupied(_entry) => Err(TransportAdapterError::InvalidInput),
            Entry::Vacant(entry) => {
                entry.insert(registration);
                Ok(None)
            }
        }
    }

    /// restore the previous remote-source registration after failed route setup
    ///
    /// this is the rollback half of consumer registration
    /// when no previous registration existed, source-local packet policy and
    /// decoder-refresh metadata must also be removed because no route owns the
    /// placeholder anymore
    pub(super) fn restore_remote_source_registration(
        &mut self,
        source_transport_media_id: TransportMediaId,
        previous_registration: Option<RemoteSourceRegistration>,
    ) {
        if let Some(previous_registration) = previous_registration {
            self.remote_source_registry
                .insert(source_transport_media_id, previous_registration);
        } else {
            self.remove_remote_source_side_tables(source_transport_media_id);
        }
    }

    /// return the remote-source registration for a source media id
    ///
    /// only remote sources live in this registry
    /// local producers must be resolved through `media_handle`
    pub(super) fn remote_source_registration(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<&RemoteSourceRegistration> {
        self.remote_source_registry.get(&source_transport_media_id)
    }

    pub(super) fn publish_remote_source_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        if let Some(registration) = self
            .remote_source_registry
            .get_mut(&source_transport_media_id)
        {
            registration.publish_packet_gate(packet_gate);
        }
    }

    pub(super) fn flush_pending_remote_source_packet_gates(&mut self) {
        for registration in self.remote_source_registry.values_mut() {
            registration.flush_pending_packet_gate();
        }
    }

    /// remove a remote-source placeholder when no local route still references it
    ///
    /// this is called after consumer-route removal
    /// if the last consumer route disappeared, source-level route-control state,
    /// live RID memory and decoder-refresh metadata must disappear with the
    /// remote-source entry
    pub(super) fn prune_remote_source_if_unrouted(
        &mut self,
        source_transport_media_id: TransportMediaId,
    ) {
        if !self
            .media_route_index
            .contains_key(&source_transport_media_id)
        {
            self.remove_remote_source_side_tables(source_transport_media_id);
        }
    }

    /// remove every remote-source side table entry that no longer has a route
    ///
    /// session teardown can remove several media handles and route entries at
    /// once
    /// this sweep keeps relay-facing source state aligned with the final
    /// route graph after that bulk removal
    pub(super) fn prune_unrouted_remote_sources(&mut self) {
        self.remote_source_registry
            .retain(|source_transport_media_id, _registration| {
                self.media_route_index
                    .contains_key(source_transport_media_id)
            });
        self.retain_registered_source_side_tables();
    }

    /// remove every packet-loop side table owned by one remote-source entry
    ///
    /// remote sources are placeholders for producers that live on another
    /// worker
    /// when the last local route disappears, the placeholder and every cached
    /// source-side fact must disappear together so late packet-gate or
    /// decoder-refresh work cannot keep addressing a stale producer id
    fn remove_remote_source_side_tables(&mut self, source_transport_media_id: TransportMediaId) {
        self.remote_source_registry
            .remove(&source_transport_media_id);
        self.forget_source_side_tables(source_transport_media_id);
    }

    /// remove source-local side tables shared by local and remote producer ids
    fn forget_source_side_tables(&mut self, source_transport_media_id: TransportMediaId) {
        self.forget_live_producer_rids(source_transport_media_id);
        self.route_control.forget_source(source_transport_media_id);
        self.set_source_decoder_refresh_codec(source_transport_media_id, None);
    }

    /// keep source side tables only for local producers or routed remote sources
    fn retain_registered_source_side_tables(&mut self) {
        let mid_registry = &self.mid_registry;
        let remote_source_registry = &self.remote_source_registry;
        let source_is_registered = |source_transport_media_id: &TransportMediaId| {
            mid_registry.contains_key(source_transport_media_id)
                || remote_source_registry.contains_key(source_transport_media_id)
        };
        self.route_control.retain_sources(&source_is_registered);
        self.live_producer_rids
            .retain(|source_transport_media_id, _rids| {
                source_is_registered(source_transport_media_id)
            });
        self.source_decoder_refresh_codecs
            .retain(|source_transport_media_id, _codec| {
                source_is_registered(source_transport_media_id)
            });
    }

    /// return the packet-level decoder-refresh classifier for one source
    pub(super) fn source_decoder_refresh_codec(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DecoderRefreshCodec> {
        self.source_decoder_refresh_codecs
            .get(&source_transport_media_id)
            .copied()
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
        let previous = self.source_decoder_refresh_codec(source_transport_media_id);
        self.set_source_decoder_refresh_codec(
            source_transport_media_id,
            DecoderRefreshCodec::from_parameters(parameters),
        );
        previous
    }

    /// restore decoder-refresh metadata captured before a tentative mutation
    pub(super) fn restore_source_decoder_refresh_codec(
        &mut self,
        source_transport_media_id: TransportMediaId,
        previous_codec: Option<DecoderRefreshCodec>,
    ) {
        self.set_source_decoder_refresh_codec(source_transport_media_id, previous_codec);
    }

    /// set or clear packet-level decoder-refresh metadata for one source
    fn set_source_decoder_refresh_codec(
        &mut self,
        source_transport_media_id: TransportMediaId,
        codec: Option<DecoderRefreshCodec>,
    ) {
        if let Some(codec) = codec {
            self.source_decoder_refresh_codecs
                .insert(source_transport_media_id, codec);
        } else {
            self.source_decoder_refresh_codecs
                .remove(&source_transport_media_id);
        }
    }

    /// snapshot active-speaker sources from route-control state
    ///
    /// registry ownership is used indirectly by expiry helpers, while this
    /// snapshot preserves route-control's source-centric view
    pub(super) fn active_speaker_source_snapshot(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        self.route_control.active_speaker_sources(now)
    }

    /// snapshot active-speaker diagnostics from route-control state
    pub(super) fn active_speaker_diagnostic_snapshot(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.route_control.active_speaker_diagnostics(now)
    }

    /// return room ids whose active-speaker sources expired by `now`
    ///
    /// route-control only knows source media ids
    /// the registry maps each local or remote source back to the owning room
    /// before the worker reports expiry to room orchestration
    pub(super) fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.route_control
            .expired_active_speaker_source_ids(now)
            .into_iter()
            .filter_map(|source_transport_media_id| {
                self.source_room_instance_id(source_transport_media_id)
            })
            .collect()
    }

    /// resolve producer media by the session and MID observed on a packet
    pub(super) fn source_transport_media_id_for_mid(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.session_media
            .get(source_session_key)
            .and_then(|source_lookup| source_lookup.producer_mids.get(&source_mid))
    }

    /// resolve producer media by the session and SSRC observed on a packet
    ///
    /// this lookup is populated from negotiated bindings when available and can
    /// later be extended by RTP header discovery for RID-only publishers
    pub(super) fn source_transport_media_id_for_ssrc(
        &self,
        source_session_key: &TransportSessionKey,
        source_ssrc: Ssrc,
    ) -> Option<TransportMediaId> {
        self.session_media
            .get(source_session_key)
            .and_then(|source_lookup| source_lookup.producer_ssrcs.get(&source_ssrc))
    }

    /// resolve the producer RID learned for a session-scoped SSRC
    ///
    /// a missing value means either the source is not simulcast or the worker
    /// has not learned the RID for this SSRC yet
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
        learn_producer_ssrc_binding_for_session(
            &self.mid_registry,
            &mut self.session_media,
            &mut self.producer_ssrcs_by_media,
            session_key,
            transport_media_id,
            ssrc,
            rid,
        );
    }

    /// learn an SSRC binding from a forwarded packet source without requiring
    /// local packets to materialize an owned session key
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
        learn_producer_ssrc_binding_for_session(
            &self.mid_registry,
            &mut self.session_media,
            &mut self.producer_ssrcs_by_media,
            session_key,
            transport_media_id,
            ssrc,
            rid,
        );
    }

    /// resolve the source media id that feeds a consumer MID
    ///
    /// rtcp feedback arrives from the consumer session, so the packet loop uses
    /// this lookup to route keyframe requests back to the correct source
    pub(super) fn consumer_source_transport_media_id_for_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.session_media
            .get(consumer_session_key)
            .and_then(|consumer_lookup| consumer_lookup.consumer_mids.get(&consumer_mid))
    }

    /// resolve the room that owns a local or remote source media id
    fn source_room_instance_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<RoomInstanceId> {
        self.media_handle(source_transport_media_id)
            .map(|handle| handle.session_key().room_instance_id())
            .or_else(|| {
                self.remote_source_registration(source_transport_media_id)
                    .map(|registration| registration.source_session_key().room_instance_id())
            })
    }

    /// remove every media handle owned by a session
    ///
    /// each handle goes through `remove_media_handle` so producer and consumer
    /// reverse indexes are cleaned consistently
    /// callers still own route cleanup that spans other sessions
    pub(super) fn remove_session_media_handles(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Vec<(TransportMediaId, RegisteredMediaHandle)> {
        let removed_ids = self.session_media.get(session_key).map_or_else(
            || {
                self.mid_registry
                    .iter()
                    .filter_map(|(transport_media_id, handle)| {
                        (handle.session_key() == session_key).then_some(*transport_media_id)
                    })
                    .collect::<Vec<_>>()
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

    /// replace producer SSRC and RID bindings from negotiated parameters
    ///
    /// this is the authoritative refresh after answer application
    /// old SSRC mappings for the media id are cleared before inserting the new
    /// set so packet routing cannot keep stale producer identities alive
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
        self.clear_producer_ssrc_bindings(transport_media_id, session_key);
        self.refresh_source_decoder_refresh_codec(transport_media_id, parameters);
        let bindings = parameters
            .bindings()
            .filter_map(|binding| {
                let ssrc = binding.ssrc().map(Ssrc::from)?;
                Some((ssrc, binding.rid().map(Rid::from)))
            })
            .collect::<Vec<_>>();
        let ssrcs = bindings
            .iter()
            .map(|(ssrc, _rid)| *ssrc)
            .collect::<Vec<_>>();
        if ssrcs.is_empty() {
            self.producer_ssrcs_by_media
                .entry(transport_media_id)
                .or_default();
            return;
        }
        let session_lookup = self.session_media.entry(session_key.clone()).or_default();
        for (ssrc, rid) in &bindings {
            session_lookup
                .producer_ssrcs
                .insert(*ssrc, transport_media_id);
            if let Some(rid) = rid {
                session_lookup.producer_ssrc_rids.insert(*ssrc, *rid);
            }
        }
        self.producer_ssrcs_by_media
            .insert(transport_media_id, ssrcs);
    }

    /// clear negotiated producer SSRC bindings for a MID while keeping media alive
    ///
    /// this is used when a staged removal or negotiation refresh invalidates the
    /// packet identities before the media handle itself is removed
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
        self.clear_producer_ssrc_bindings(transport_media_id, session_key);
        self.set_source_decoder_refresh_codec(transport_media_id, None);
        self.producer_ssrcs_by_media
            .entry(transport_media_id)
            .or_default();
    }

    /// remove every SSRC and RID lookup currently attached to a producer id
    fn clear_producer_ssrc_bindings(
        &mut self,
        transport_media_id: TransportMediaId,
        session_key: &TransportSessionKey,
    ) {
        if let Some(ssrcs) = self.producer_ssrcs_by_media.remove(&transport_media_id) {
            for ssrc in ssrcs {
                if let Some(session_lookup) = self.session_media.get_mut(session_key) {
                    session_lookup.producer_ssrcs.remove(&ssrc);
                    session_lookup.producer_ssrc_rids.remove(&ssrc);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use o_sfu_router::{MediaStream as RouterRtpParameters, StreamBinding};

    use super::*;
    use crate::runtime::{UserId, rtc_engine::test_support::test_transport_session_key};

    #[test]
    fn consumer_media_lookup_uses_the_reverse_index() {
        let mut state = PacketLoopState::default();
        let source_transport_media_id = TransportMediaId::new(8);
        let consumer_session = test_transport_session_key(12, 0, 13, UserId::Integer(14));
        let consumer_mid = Mid::from("aud-down");

        let _consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: consumer_mid,
                source_transport_media_id,
            });

        assert_eq!(
            state.consumer_source_transport_media_id_for_mid(&consumer_session, consumer_mid),
            Some(source_transport_media_id)
        );
    }

    #[test]
    fn consumer_media_lookup_clears_when_the_handle_is_removed() {
        let mut state = PacketLoopState::default();
        let source_transport_media_id = TransportMediaId::new(9);
        let consumer_session = test_transport_session_key(15, 0, 16, UserId::Integer(17));
        let consumer_mid = Mid::from("cam-down");

        let consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: consumer_mid,
                source_transport_media_id,
            });
        assert_eq!(
            state.consumer_source_transport_media_id_for_mid(&consumer_session, consumer_mid),
            Some(source_transport_media_id)
        );

        let removed_handle = state.remove_media_handle(consumer_transport_media_id);

        assert!(matches!(
            removed_handle,
            Some(RegisteredMediaHandle::Consumer {
                session_key,
                mid,
                source_transport_media_id: removed_source_transport_media_id,
            }) if session_key == consumer_session
                && mid == consumer_mid
                && removed_source_transport_media_id == source_transport_media_id
        ));
        assert_eq!(
            state.consumer_source_transport_media_id_for_mid(&consumer_session, consumer_mid),
            None
        );
    }

    #[test]
    fn producer_media_lookup_is_session_scoped_by_mid() {
        let mut state = PacketLoopState::default();
        let first_session = test_transport_session_key(16, 0, 18, UserId::Integer(19));
        let second_session = test_transport_session_key(17, 0, 18, UserId::Integer(19));
        let shared_mid = Mid::from("cam-up");
        let first_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: first_session.clone(),
            mid: shared_mid,
        });
        let second_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: second_session.clone(),
            mid: shared_mid,
        });

        assert_eq!(
            state.source_transport_media_id_for_mid(&first_session, shared_mid),
            Some(first_media_id)
        );
        assert_eq!(
            state.source_transport_media_id_for_mid(&second_session, shared_mid),
            Some(second_media_id)
        );

        let _removed_handle = state.remove_media_handle(first_media_id);

        assert_eq!(
            state.source_transport_media_id_for_mid(&first_session, shared_mid),
            None
        );
        assert_eq!(
            state.source_transport_media_id_for_mid(&second_session, shared_mid),
            Some(second_media_id)
        );
    }

    #[test]
    fn producer_media_lookup_falls_back_to_negotiated_ssrc() {
        let producer_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 55_555_u32;
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: producer_session.clone(),
            mid: producer_mid,
        });
        state.refresh_producer_ssrc_bindings(
            &producer_session,
            producer_mid,
            &RouterRtpParameters::new(
                vec![],
                vec![],
                vec![StreamBinding::new().with_ssrc(producer_ssrc)],
            )
            .with_mid(producer_mid.to_string()),
        );

        assert_eq!(
            state.source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(producer_ssrc)),
            Some(transport_media_id)
        );
    }

    #[test]
    fn dynamic_producer_ssrc_rid_lookup_clears_with_media_handle() {
        let producer_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = Ssrc::from(99_999_u32);
        let learned_rid = Rid::from("hi");
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: producer_session.clone(),
            mid: producer_mid,
        });

        state.learn_producer_ssrc_binding(
            &producer_session,
            transport_media_id,
            producer_ssrc,
            Some(learned_rid),
        );

        assert_eq!(
            state.source_transport_media_id_for_ssrc(&producer_session, producer_ssrc),
            Some(transport_media_id)
        );
        assert_eq!(
            state.source_rid_for_ssrc(&producer_session, producer_ssrc),
            Some(learned_rid)
        );

        let _removed_handle = state.remove_media_handle(transport_media_id);

        assert_eq!(
            state.source_transport_media_id_for_ssrc(&producer_session, producer_ssrc),
            None
        );
        assert_eq!(
            state.source_rid_for_ssrc(&producer_session, producer_ssrc),
            None
        );
    }

    #[test]
    fn producer_ssrc_lookup_refresh_replaces_stale_bindings() {
        let producer_session = test_transport_session_key(21, 0, 22, UserId::Integer(23));
        let producer_mid = Mid::from("cam-up");
        let first_ssrc = 77_777_u32;
        let second_ssrc = 88_888_u32;
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: producer_session.clone(),
            mid: producer_mid,
        });

        state.refresh_producer_ssrc_bindings(
            &producer_session,
            producer_mid,
            &RouterRtpParameters::new(
                vec![],
                vec![],
                vec![StreamBinding::new().with_ssrc(first_ssrc)],
            )
            .with_mid(producer_mid.to_string()),
        );
        assert_eq!(
            state.source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(first_ssrc)),
            Some(transport_media_id)
        );

        state.refresh_producer_ssrc_bindings(
            &producer_session,
            producer_mid,
            &RouterRtpParameters::new(
                vec![],
                vec![],
                vec![StreamBinding::new().with_ssrc(second_ssrc)],
            )
            .with_mid(producer_mid.to_string()),
        );

        assert_eq!(
            state.source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(first_ssrc)),
            None
        );
        assert_eq!(
            state.source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(second_ssrc)),
            Some(transport_media_id)
        );
    }

    #[test]
    fn producer_mid_lookup_survives_ssrc_binding_refresh() {
        let producer_session = test_transport_session_key(28, 0, 29, UserId::Integer(30));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 44_444_u32;
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: producer_session.clone(),
            mid: producer_mid,
        });

        assert_eq!(
            state.source_transport_media_id_for_mid(&producer_session, producer_mid),
            Some(transport_media_id)
        );

        state.refresh_producer_ssrc_bindings(
            &producer_session,
            producer_mid,
            &RouterRtpParameters::new(
                vec![],
                vec![],
                vec![StreamBinding::new().with_ssrc(producer_ssrc)],
            )
            .with_mid(producer_mid.to_string()),
        );

        assert_eq!(
            state.source_transport_media_id_for_mid(&producer_session, producer_mid),
            Some(transport_media_id)
        );
        assert_eq!(
            state.source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(producer_ssrc)),
            Some(transport_media_id)
        );
    }

    #[test]
    fn dynamic_producer_ssrc_binding_cannot_steal_another_media_id() {
        let producer_session = test_transport_session_key(28, 0, 31, UserId::Integer(32));
        let first_mid = Mid::from("cam-up-a");
        let second_mid = Mid::from("cam-up-b");
        let shared_ssrc = Ssrc::from(66_666_u32);
        let mut state = PacketLoopState::default();
        let first_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: producer_session.clone(),
            mid: first_mid,
        });
        let second_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: producer_session.clone(),
            mid: second_mid,
        });

        state.learn_producer_ssrc_binding(&producer_session, first_media_id, shared_ssrc, None);
        state.learn_producer_ssrc_binding(&producer_session, second_media_id, shared_ssrc, None);

        assert_eq!(
            state.source_transport_media_id_for_ssrc(&producer_session, shared_ssrc),
            Some(first_media_id)
        );
        assert_eq!(
            state.source_transport_media_id_for_mid(&producer_session, second_mid),
            Some(second_media_id)
        );
    }

    #[test]
    fn expired_active_speaker_channels_are_resolved_from_source_owners() {
        let mut state = PacketLoopState::default();
        let first_session = test_transport_session_key(31, 0, 32, UserId::Integer(33));
        let second_session = test_transport_session_key(34, 0, 35, UserId::Integer(36));
        let first_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: first_session.clone(),
            mid: Mid::from("cam-up-a"),
        });
        let second_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: second_session,
            mid: Mid::from("cam-up-b"),
        });
        let start = Instant::now();

        state
            .route_control
            .observe_audio_activity(first_media_id, Some(true), None, start);
        state.route_control.observe_audio_activity(
            second_media_id,
            Some(true),
            None,
            start + Duration::from_millis(100),
        );

        assert_eq!(
            state.expired_active_speaker_room_instance_ids(start + Duration::from_millis(251)),
            BTreeSet::from([first_session.room_instance_id()])
        );
    }
}
