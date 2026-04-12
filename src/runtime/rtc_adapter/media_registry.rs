//! Media handle tracking for the RTC transport adapter.
//!
//! Owns the `mid_registry` and `(session_key, mid)` reverse lookups
//! within `RtcBootstrapState`.

use str0m::media::Mid;

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

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
        source_session_key: TransportSessionKey,
        source_mid: Mid,
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

    pub(super) fn is_producer_for(&self, session_key: &TransportSessionKey, mid: Mid) -> bool {
        matches!(
            self,
            Self::Producer {
                session_key: owner_session_key,
                mid: owner_mid,
            } if owner_session_key == session_key && *owner_mid == mid
        )
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
        self.mid_registry.insert(id, handle);
        TransportMediaId::new(id)
    }

    pub(super) fn resolve_mid(&self, transport_media_id: TransportMediaId) -> Option<Mid> {
        self.mid_registry
            .get(&transport_media_id.as_u64())
            .map(RegisteredMediaHandle::mid)
    }

    pub(super) fn remove_media_handle(
        &mut self,
        transport_media_id: TransportMediaId,
    ) -> Option<RegisteredMediaHandle> {
        self.mid_registry.remove(&transport_media_id.as_u64())
    }

    pub(super) fn session_has_mid(&self, session_key: &TransportSessionKey, mid: Mid) -> bool {
        self.mid_registry
            .values()
            .any(|handle| handle.session_key() == session_key && handle.mid() == mid)
    }

    pub(super) fn session_has_producer_mid(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) -> bool {
        self.mid_registry
            .values()
            .any(|handle| handle.is_producer_for(session_key, mid))
    }

    pub(super) fn session_has_registered_media(&self, session_key: &TransportSessionKey) -> bool {
        self.mid_registry
            .values()
            .any(|handle| handle.session_key() == session_key)
    }

    pub(super) fn transport_media_id_for_source(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.recv_media_ids
            .get(&(source_session_key.clone(), source_mid))
            .copied()
    }
}
