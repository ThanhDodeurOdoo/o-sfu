//! Media handle tracking for the RTC transport adapter.
//!
//! Owns the transport-media registry and the negotiation-facing producer
//! `(session_key, mid)` reverse lookup within `RtcBootstrapState`, plus the
//! worker-local remote-source placeholders used by cross-worker relay routes.

use str0m::media::Mid;

use crate::runtime::transport_adapter::{
    TransportAdapterError, TransportMediaId, TransportSessionKey,
};

use super::state::RtcBootstrapState;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RemoteSourceRegistration {
    source_session_key: TransportSessionKey,
}

impl RemoteSourceRegistration {
    fn new(source_session_key: TransportSessionKey) -> Self {
        Self { source_session_key }
    }

    pub(super) fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
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
        }
        self.mid_registry.insert(id, handle);
        TransportMediaId::new(id)
    }

    #[cfg(test)]
    pub(super) fn resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.mid_registry
            .get(&transport_media_id.as_u64())
            .map(RegisteredMediaHandle::mid)
    }

    pub(super) fn remove_media_handle(
        &mut self,
        transport_media_id: TransportMediaId,
    ) -> Option<RegisteredMediaHandle> {
        let handle = self.mid_registry.remove(&transport_media_id.as_u64())?;
        if let RegisteredMediaHandle::Producer { session_key, mid } = &handle {
            self.producer_mid_registry
                .remove(&ProducerMidLookupKey::new(session_key.clone(), *mid));
        }
        Some(handle)
    }

    pub(super) fn session_has_mid(&self, session_key: &TransportSessionKey, mid: Mid) -> bool {
        self.mid_registry
            .values()
            .any(|handle| handle.session_key() == session_key && handle.mid() == mid)
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
    ) -> Result<(), TransportAdapterError> {
        match self.remote_source_registry.get(&source_transport_media_id) {
            Some(existing) if existing.source_session_key() == source_session_key => Ok(()),
            Some(_existing) => Err(TransportAdapterError::InvalidInput),
            None => {
                self.remote_source_registry.insert(
                    source_transport_media_id,
                    RemoteSourceRegistration::new(source_session_key.clone()),
                );
                Ok(())
            }
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
    }

    pub(super) fn prune_unrouted_remote_sources(&mut self) {
        self.remote_source_registry
            .retain(|source_transport_media_id, _registration| {
                self.media_route_index
                    .contains_key(source_transport_media_id)
            });
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
}
