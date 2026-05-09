//! Media handle tracking for the RTC transport shard.
//!
//! Owns the transport-media registry and the negotiation-facing producer
//! `(session_key, mid)` reverse lookup within `RtcBootstrapState`, plus the
//! worker-local remote-source placeholders used by cross-worker relay routes.

use std::{
    collections::{BTreeSet, btree_map::Entry},
    time::Instant,
};

use str0m::{
    media::{Media, Mid, Rid},
    rtp::Ssrc,
};
use tracing::{debug, warn};

use super::{
    commands::RemoteSourceControl, packet_loop::machine::state::PacketLoopState,
    state::RtcBootstrapState,
};
use crate::runtime::{
    RoomInstanceId,
    media_transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportAdapterError,
        TransportMediaId, TransportSessionKey,
    },
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

#[derive(Debug, Clone)]
pub(super) struct RemoteSourceRegistration {
    source_session_key: TransportSessionKey,
    source_control: RemoteSourceControl,
}

impl RemoteSourceRegistration {
    fn new(source_session_key: TransportSessionKey, source_control: RemoteSourceControl) -> Self {
        Self {
            source_session_key,
            source_control,
        }
    }

    pub(super) fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    pub(super) fn source_control(&self) -> &RemoteSourceControl {
        &self.source_control
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ProducerMidLookupKey {
    session_key: TransportSessionKey,
    mid: Mid,
}

impl ProducerMidLookupKey {
    fn new(session_key: TransportSessionKey, mid: Mid) -> Self {
        Self { session_key, mid }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ProducerSsrcLookupKey {
    session_key: TransportSessionKey,
    ssrc: Ssrc,
}

impl ProducerSsrcLookupKey {
    fn new(session_key: TransportSessionKey, ssrc: Ssrc) -> Self {
        Self { session_key, ssrc }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ConsumerMidLookupKey {
    session_key: TransportSessionKey,
    mid: Mid,
}

impl ConsumerMidLookupKey {
    fn new(session_key: TransportSessionKey, mid: Mid) -> Self {
        Self { session_key, mid }
    }
}

impl PacketLoopState {
    pub(super) fn media_handle(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&RegisteredMediaHandle> {
        self.mid_registry.get(&transport_media_id.as_u64())
    }

    pub(super) fn source_transport_media_id_for_mid(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.producer_mid_registry
            .get(&ProducerMidLookupKey::new(
                source_session_key.clone(),
                source_mid,
            ))
            .copied()
    }

    pub(super) fn source_transport_media_id_for_ssrc(
        &self,
        source_session_key: &TransportSessionKey,
        source_ssrc: Ssrc,
    ) -> Option<TransportMediaId> {
        self.producer_ssrc_registry
            .get(&ProducerSsrcLookupKey::new(
                source_session_key.clone(),
                source_ssrc,
            ))
            .copied()
    }

    pub(super) fn source_rid_for_ssrc(
        &self,
        source_session_key: &TransportSessionKey,
        source_ssrc: Ssrc,
    ) -> Option<Rid> {
        self.producer_ssrc_rid_registry
            .get(&ProducerSsrcLookupKey::new(
                source_session_key.clone(),
                source_ssrc,
            ))
            .copied()
    }

    pub(super) fn consumer_source_transport_media_id_for_mid(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.consumer_mid_registry
            .get(&ConsumerMidLookupKey::new(
                consumer_session_key.clone(),
                consumer_mid,
            ))
            .copied()
    }

    pub(super) fn learn_producer_ssrc_binding(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        ssrc: Ssrc,
        rid: Option<Rid>,
    ) {
        let Some(RegisteredMediaHandle::Producer {
            session_key: registered_session_key,
            mid,
        }) = self.media_handle(transport_media_id)
        else {
            return;
        };
        if registered_session_key != session_key {
            return;
        }
        let mid = *mid;
        let key = ProducerSsrcLookupKey::new(session_key.clone(), ssrc);
        if let Some(existing_transport_media_id) = self.producer_ssrc_registry.get(&key).copied()
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

        let inserted = self
            .producer_ssrc_registry
            .insert(key.clone(), transport_media_id)
            .is_none();
        let previous_rid = rid.and_then(|rid| self.producer_ssrc_rid_registry.insert(key, rid));
        let ssrcs = self
            .producer_ssrcs_by_media
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

    pub(super) fn remote_source_registration(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<&RemoteSourceRegistration> {
        self.remote_source_registry.get(&source_transport_media_id)
    }
}

impl RtcBootstrapState {
    pub(super) fn register_media_handle(
        &mut self,
        handle: RegisteredMediaHandle,
    ) -> TransportMediaId {
        let id = self.packet_loop.next_media_id;
        let transport_media_id = TransportMediaId::new(id);
        self.packet_loop.next_media_id = self.packet_loop.next_media_id.saturating_add(1);
        if let RegisteredMediaHandle::Producer { session_key, mid } = &handle {
            self.packet_loop.producer_mid_registry.insert(
                ProducerMidLookupKey::new(session_key.clone(), *mid),
                transport_media_id,
            );
            self.packet_loop
                .producer_ssrcs_by_media
                .insert(transport_media_id, Vec::new());
            if let Some(media_kind) = self
                .users
                .get(session_key)
                .and_then(|session_state| session_state.host_session.media(*mid))
                .map(Media::kind)
            {
                self.packet_loop
                    .set_source_kind(transport_media_id, media_kind);
            }
        } else if let RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            source_transport_media_id,
        } = &handle
        {
            self.packet_loop.consumer_mid_registry.insert(
                ConsumerMidLookupKey::new(session_key.clone(), *mid),
                *source_transport_media_id,
            );
        }
        self.packet_loop.mid_registry.insert(id, handle);
        transport_media_id
    }

    pub(super) fn resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.packet_loop
            .mid_registry
            .get(&transport_media_id.as_u64())
            .map(RegisteredMediaHandle::mid)
    }

    pub(super) fn remove_media_handle(
        &mut self,
        transport_media_id: TransportMediaId,
    ) -> Option<RegisteredMediaHandle> {
        let handle = self
            .packet_loop
            .mid_registry
            .remove(&transport_media_id.as_u64())?;
        if let RegisteredMediaHandle::Producer { session_key, mid } = &handle {
            self.packet_loop
                .producer_mid_registry
                .remove(&ProducerMidLookupKey::new(session_key.clone(), *mid));
            self.clear_producer_ssrc_bindings(transport_media_id, session_key);
            self.packet_loop
                .forget_live_producer_rids(transport_media_id);
            self.packet_loop.forget_source_facts(transport_media_id);
            self.packet_loop
                .route_control
                .forget_source(transport_media_id);
            self.remove_incoming_bitrate_counter(transport_media_id);
        } else if let RegisteredMediaHandle::Consumer {
            session_key, mid, ..
        } = &handle
        {
            self.packet_loop
                .consumer_mid_registry
                .remove(&ConsumerMidLookupKey::new(session_key.clone(), *mid));
        }
        Some(handle)
    }

    pub(super) fn session_has_registered_media(&self, session_key: &TransportSessionKey) -> bool {
        self.packet_loop
            .mid_registry
            .values()
            .any(|handle| handle.session_key() == session_key)
    }

    pub(super) fn register_remote_source(
        &mut self,
        source_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        match self
            .packet_loop
            .remote_source_registry
            .entry(source_transport_media_id)
        {
            Entry::Occupied(mut entry)
                if entry.get().source_session_key() == source_session_key =>
            {
                Ok(Some(entry.insert(RemoteSourceRegistration::new(
                    source_session_key.clone(),
                    source_control,
                ))))
            }
            Entry::Occupied(_entry) => Err(TransportAdapterError::InvalidInput),
            Entry::Vacant(entry) => {
                entry.insert(RemoteSourceRegistration::new(
                    source_session_key.clone(),
                    source_control,
                ));
                Ok(None)
            }
        }
    }

    pub(super) fn restore_remote_source_registration(
        &mut self,
        source_transport_media_id: TransportMediaId,
        previous_registration: Option<RemoteSourceRegistration>,
    ) {
        if let Some(previous_registration) = previous_registration {
            self.packet_loop
                .remote_source_registry
                .insert(source_transport_media_id, previous_registration);
        } else {
            self.packet_loop
                .remote_source_registry
                .remove(&source_transport_media_id);
            self.forget_remote_source_runtime_state(source_transport_media_id);
        }
    }

    pub(super) fn remote_source_registration(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<&RemoteSourceRegistration> {
        self.packet_loop
            .remote_source_registry
            .get(&source_transport_media_id)
    }

    pub(super) fn prune_remote_source_if_unrouted(
        &mut self,
        source_transport_media_id: TransportMediaId,
    ) {
        if self
            .packet_loop
            .media_route_index
            .contains_key(&source_transport_media_id)
        {
            return;
        }
        self.packet_loop
            .remote_source_registry
            .remove(&source_transport_media_id);
        self.forget_remote_source_runtime_state(source_transport_media_id);
    }

    pub(super) fn prune_unrouted_remote_sources(&mut self) {
        let routed_sources = self
            .packet_loop
            .media_route_index
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        self.packet_loop.remote_source_registry.retain(
            |source_transport_media_id, _registration| {
                routed_sources.contains(source_transport_media_id)
            },
        );
        let local_sources = self
            .packet_loop
            .mid_registry
            .keys()
            .copied()
            .map(TransportMediaId::new)
            .collect::<BTreeSet<_>>();
        let remote_sources = self
            .packet_loop
            .remote_source_registry
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        self.packet_loop
            .route_control
            .retain_sources(|source_transport_media_id| {
                local_sources.contains(source_transport_media_id)
                    || remote_sources.contains(source_transport_media_id)
            });
        self.packet_loop
            .live_producer_rids
            .retain(|source_transport_media_id, _rids| {
                local_sources.contains(source_transport_media_id)
                    || remote_sources.contains(source_transport_media_id)
            });
    }

    pub(super) fn active_speaker_source_snapshot(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        self.packet_loop.route_control.active_speaker_sources(now)
    }

    pub(super) fn active_speaker_diagnostic_snapshot(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.packet_loop
            .route_control
            .active_speaker_diagnostics(now)
    }

    pub(super) fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.packet_loop
            .route_control
            .expired_active_speaker_source_ids(now)
            .into_iter()
            .filter_map(|source_transport_media_id| {
                self.source_room_instance_id(source_transport_media_id)
            })
            .collect()
    }

    fn source_room_instance_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<RoomInstanceId> {
        self.packet_loop
            .media_handle(source_transport_media_id)
            .map(|handle| handle.session_key().room_instance_id())
            .or_else(|| {
                self.remote_source_registration(source_transport_media_id)
                    .map(|registration| registration.source_session_key().room_instance_id())
            })
    }

    pub(super) fn remove_session_media_handles(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Vec<(TransportMediaId, RegisteredMediaHandle)> {
        let removed_ids = self
            .packet_loop
            .mid_registry
            .iter()
            .filter_map(|(raw_id, handle)| {
                (handle.session_key() == session_key).then_some(TransportMediaId::new(*raw_id))
            })
            .collect::<Vec<_>>();
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
            .packet_loop
            .producer_mid_registry
            .get(&ProducerMidLookupKey::new(session_key.clone(), mid))
            .copied()
        else {
            return;
        };
        self.packet_loop
            .set_source_facts_from_parameters(transport_media_id, parameters);
        self.clear_producer_ssrc_bindings(transport_media_id, session_key);
        let mut ssrcs = Vec::new();
        for binding in parameters.bindings() {
            let Some(ssrc) = binding.ssrc().map(Ssrc::from) else {
                continue;
            };
            let key = ProducerSsrcLookupKey::new(session_key.clone(), ssrc);
            self.packet_loop
                .producer_ssrc_registry
                .insert(key.clone(), transport_media_id);
            if let Some(rid) = binding.rid().map(Rid::from) {
                self.packet_loop.producer_ssrc_rid_registry.insert(key, rid);
            }
            ssrcs.push(ssrc);
        }
        if ssrcs.is_empty() {
            self.packet_loop
                .producer_ssrcs_by_media
                .entry(transport_media_id)
                .or_default();
            return;
        }
        self.packet_loop
            .producer_ssrcs_by_media
            .insert(transport_media_id, ssrcs);
    }

    pub(super) fn clear_producer_ssrc_bindings_for_mid(
        &mut self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) {
        let Some(transport_media_id) = self
            .packet_loop
            .producer_mid_registry
            .get(&ProducerMidLookupKey::new(session_key.clone(), mid))
            .copied()
        else {
            return;
        };
        self.clear_producer_ssrc_bindings(transport_media_id, session_key);
        self.packet_loop
            .producer_ssrcs_by_media
            .entry(transport_media_id)
            .or_default();
    }

    fn clear_producer_ssrc_bindings(
        &mut self,
        transport_media_id: TransportMediaId,
        session_key: &TransportSessionKey,
    ) {
        if let Some(ssrcs) = self
            .packet_loop
            .producer_ssrcs_by_media
            .remove(&transport_media_id)
        {
            for ssrc in ssrcs {
                let key = ProducerSsrcLookupKey::new(session_key.clone(), ssrc);
                self.packet_loop.producer_ssrc_registry.remove(&key);
                self.packet_loop.producer_ssrc_rid_registry.remove(&key);
            }
        }
    }

    fn forget_remote_source_runtime_state(&mut self, source_transport_media_id: TransportMediaId) {
        self.packet_loop
            .forget_live_producer_rids(source_transport_media_id);
        self.packet_loop
            .route_control
            .forget_source(source_transport_media_id);
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
        let mut state = RtcBootstrapState::default();
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
            state
                .packet_loop
                .consumer_source_transport_media_id_for_mid(&consumer_session, consumer_mid),
            Some(source_transport_media_id)
        );
    }

    #[test]
    fn consumer_media_lookup_clears_when_the_handle_is_removed() {
        let mut state = RtcBootstrapState::default();
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
            state
                .packet_loop
                .consumer_source_transport_media_id_for_mid(&consumer_session, consumer_mid),
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
            state
                .packet_loop
                .consumer_source_transport_media_id_for_mid(&consumer_session, consumer_mid),
            None
        );
    }

    #[test]
    fn producer_media_lookup_falls_back_to_negotiated_ssrc() {
        let producer_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 55_555_u32;
        let mut state = RtcBootstrapState::default();
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
            state
                .packet_loop
                .source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(producer_ssrc)),
            Some(transport_media_id)
        );
    }

    #[test]
    fn producer_ssrc_lookup_refresh_replaces_stale_bindings() {
        let producer_session = test_transport_session_key(21, 0, 22, UserId::Integer(23));
        let producer_mid = Mid::from("cam-up");
        let first_ssrc = 77_777_u32;
        let second_ssrc = 88_888_u32;
        let mut state = RtcBootstrapState::default();
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
            state
                .packet_loop
                .source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(first_ssrc)),
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
            state
                .packet_loop
                .source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(first_ssrc)),
            None
        );
        assert_eq!(
            state
                .packet_loop
                .source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(second_ssrc)),
            Some(transport_media_id)
        );
    }

    #[test]
    fn expired_active_speaker_channels_are_resolved_from_source_owners() {
        let mut state = RtcBootstrapState::default();
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

        state.packet_loop.route_control.observe_audio_activity(
            first_media_id,
            Some(true),
            None,
            start,
        );
        state.packet_loop.route_control.observe_audio_activity(
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
