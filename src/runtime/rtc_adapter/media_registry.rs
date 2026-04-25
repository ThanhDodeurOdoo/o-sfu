//! Media handle tracking for the RTC transport adapter.
//!
//! Owns the transport-media registry and the negotiation-facing producer
//! `(session_key, mid)` reverse lookup within `RtcBootstrapState`, plus the
//! worker-local remote-source placeholders used by cross-worker relay routes.

use std::{collections::BTreeSet, time::Instant};

use str0m::{
    media::{Mid, Rid},
    rtp::Ssrc,
};

use super::{commands::RemoteSourceControl, state::RtcBootstrapState};
use crate::runtime::{
    RoomInstanceId,
    transport_adapter::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportAdapterError,
        TransportMediaId, TransportSessionKey,
    },
};

// ---------------------------------------------------------------------------
// Registered media handle
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Media registry methods on RtcBootstrapState
// ---------------------------------------------------------------------------

impl RtcBootstrapState {
    pub(super) fn register_media_handle(
        &mut self,
        handle: RegisteredMediaHandle,
    ) -> TransportMediaId {
        let id = self.next_media_id;
        self.next_media_id = self.next_media_id.saturating_add(1);
        if let RegisteredMediaHandle::Producer { session_key, mid } = &handle {
            self.producer_mid_registry.insert(
                ProducerMidLookupKey::new(session_key.clone(), *mid),
                TransportMediaId::new(id),
            );
            self.producer_ssrcs_by_media
                .insert(TransportMediaId::new(id), Vec::new());
        } else if let RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            source_transport_media_id,
        } = &handle
        {
            self.consumer_mid_registry.insert(
                ConsumerMidLookupKey::new(session_key.clone(), *mid),
                *source_transport_media_id,
            );
        }
        self.mid_registry.insert(id, handle);
        TransportMediaId::new(id)
    }

    pub(super) fn resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.mid_registry
            .get(&transport_media_id.as_u64())
            .map(RegisteredMediaHandle::mid)
    }

    pub(super) fn media_handle(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&RegisteredMediaHandle> {
        self.mid_registry.get(&transport_media_id.as_u64())
    }

    pub(super) fn remove_media_handle(
        &mut self,
        transport_media_id: TransportMediaId,
    ) -> Option<RegisteredMediaHandle> {
        let handle = self.mid_registry.remove(&transport_media_id.as_u64())?;
        if let RegisteredMediaHandle::Producer { session_key, mid } = &handle {
            self.producer_mid_registry
                .remove(&ProducerMidLookupKey::new(session_key.clone(), *mid));
            self.clear_producer_ssrc_bindings(transport_media_id, session_key);
            self.route_control.forget_source(transport_media_id);
            self.remove_incoming_bitrate_counter(transport_media_id);
        } else if let RegisteredMediaHandle::Consumer {
            session_key, mid, ..
        } = &handle
        {
            self.consumer_mid_registry
                .remove(&ConsumerMidLookupKey::new(session_key.clone(), *mid));
        }
        Some(handle)
    }

    pub(super) fn session_has_registered_media(&self, session_key: &TransportSessionKey) -> bool {
        self.mid_registry
            .values()
            .any(|handle| handle.session_key() == session_key)
    }

    pub(super) fn register_remote_source(
        &mut self,
        source_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        match self.remote_source_registry.get(&source_transport_media_id) {
            Some(existing) if existing.source_session_key() == source_session_key => {
                let previous = self
                    .remote_source_registry
                    .get(&source_transport_media_id)
                    .cloned();
                self.remote_source_registry.insert(
                    source_transport_media_id,
                    RemoteSourceRegistration::new(source_session_key.clone(), source_control),
                );
                Ok(previous)
            }
            Some(_existing) => Err(TransportAdapterError::InvalidInput),
            None => {
                self.remote_source_registry.insert(
                    source_transport_media_id,
                    RemoteSourceRegistration::new(source_session_key.clone(), source_control),
                );
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
            self.remote_source_registry
                .insert(source_transport_media_id, previous_registration);
        } else {
            self.remote_source_registry
                .remove(&source_transport_media_id);
            self.route_control.forget_source(source_transport_media_id);
        }
    }

    pub(super) fn remote_source_registration(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<&RemoteSourceRegistration> {
        self.remote_source_registry.get(&source_transport_media_id)
    }

    pub(super) fn prune_remote_source_if_unrouted(
        &mut self,
        source_transport_media_id: TransportMediaId,
    ) {
        if self
            .media_route_index
            .contains_key(&source_transport_media_id)
        {
            return;
        }
        self.remote_source_registry
            .remove(&source_transport_media_id);
        self.route_control.forget_source(source_transport_media_id);
    }

    pub(super) fn prune_unrouted_remote_sources(&mut self) {
        self.remote_source_registry
            .retain(|source_transport_media_id, _registration| {
                self.media_route_index
                    .contains_key(source_transport_media_id)
            });
        self.route_control
            .retain_sources(|source_transport_media_id| {
                self.mid_registry
                    .contains_key(&source_transport_media_id.as_u64())
                    || self
                        .remote_source_registry
                        .contains_key(source_transport_media_id)
            });
    }

    pub(super) fn active_speaker_source_snapshot(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        self.route_control.active_speaker_sources(now)
    }

    pub(super) fn active_speaker_diagnostic_snapshot(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.route_control.active_speaker_diagnostics(now)
    }

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

    pub(super) fn remove_session_media_handles(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Vec<(TransportMediaId, RegisteredMediaHandle)> {
        let removed_ids = self
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
            .producer_mid_registry
            .get(&ProducerMidLookupKey::new(session_key.clone(), mid))
            .copied()
        else {
            return;
        };
        self.clear_producer_ssrc_bindings(transport_media_id, session_key);
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
        for (ssrc, rid) in &bindings {
            let key = ProducerSsrcLookupKey::new(session_key.clone(), *ssrc);
            self.producer_ssrc_registry
                .insert(key.clone(), transport_media_id);
            if let Some(rid) = rid {
                self.producer_ssrc_rid_registry.insert(key, *rid);
            }
        }
        self.producer_ssrcs_by_media
            .insert(transport_media_id, ssrcs);
    }

    pub(super) fn clear_producer_ssrc_bindings_for_mid(
        &mut self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) {
        let Some(transport_media_id) = self
            .producer_mid_registry
            .get(&ProducerMidLookupKey::new(session_key.clone(), mid))
            .copied()
        else {
            return;
        };
        self.clear_producer_ssrc_bindings(transport_media_id, session_key);
        self.producer_ssrcs_by_media
            .entry(transport_media_id)
            .or_default();
    }

    fn clear_producer_ssrc_bindings(
        &mut self,
        transport_media_id: TransportMediaId,
        session_key: &TransportSessionKey,
    ) {
        if let Some(ssrcs) = self.producer_ssrcs_by_media.remove(&transport_media_id) {
            for ssrc in ssrcs {
                let key = ProducerSsrcLookupKey::new(session_key.clone(), ssrc);
                self.producer_ssrc_registry.remove(&key);
                self.producer_ssrc_rid_registry.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use o_sfu_protocol::shared::UserId;
    use o_sfu_router::{MediaStream as RouterRtpParameters, StreamBinding};

    use super::*;
    use crate::runtime::rtc_adapter::test_support::test_transport_session_key;

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
            state.consumer_source_transport_media_id_for_mid(&consumer_session, consumer_mid),
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
            state.source_transport_media_id_for_ssrc(&producer_session, Ssrc::from(producer_ssrc)),
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
