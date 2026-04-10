//! IP hash-indexed demux and media route entries for the RTC transport adapter.

use std::net::SocketAddr;

use str0m::media::Mid;

use crate::runtime::transport_adapter::TransportSessionKey;

use super::state::{RtcBootstrapState, RtcSnapshotState};

// ---------------------------------------------------------------------------
// Media route types
// ---------------------------------------------------------------------------

/// A single forwarding destination within the media route index.
#[derive(Debug, Clone)]
pub(super) struct MediaRouteDestination {
    pub(super) dest_session: TransportSessionKey,
    pub(super) dest_mid: Mid,
    pub(super) active: bool,
}

#[derive(Debug, Clone)]
pub(super) struct MediaRouteEntry {
    pub(super) source_active: bool,
    pub(super) destinations: Vec<MediaRouteDestination>,
}

/// Media route source key: `(producer session, producer mid)`.
pub(super) type MediaRouteKey = (TransportSessionKey, Mid);

// ---------------------------------------------------------------------------
// Remote address demux on RtcBootstrapState
// ---------------------------------------------------------------------------

impl RtcBootstrapState {
    pub(super) fn session_key_for_remote_addr(
        &self,
        source_addr: SocketAddr,
    ) -> Option<&TransportSessionKey> {
        self.remote_addr_index.get(&source_addr)
    }

    pub(super) fn remember_remote_addr(
        &mut self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) {
        let previous_session = self
            .remote_addr_index
            .insert(source_addr, session_key.clone());
        if let Some(previous_session) = previous_session {
            self.remove_remote_addr_from_session(&previous_session, source_addr);
        }
        let session_addrs = self
            .remote_addrs_by_session
            .entry(session_key.clone())
            .or_default();
        if !session_addrs.contains(&source_addr) {
            session_addrs.push(source_addr);
        }
    }

    pub(super) fn forget_remote_addr(&mut self, source_addr: SocketAddr) {
        let Some(session_key) = self.remote_addr_index.remove(&source_addr) else {
            return;
        };
        self.remove_remote_addr_from_session(&session_key, source_addr);
    }

    pub(super) fn forget_session_remote_addrs(&mut self, session_key: &TransportSessionKey) {
        let Some(session_addrs) = self.remote_addrs_by_session.remove(session_key) else {
            return;
        };
        for source_addr in session_addrs {
            self.remote_addr_index.remove(&source_addr);
        }
    }

    fn remove_remote_addr_from_session(
        &mut self,
        session_key: &TransportSessionKey,
        source_addr: SocketAddr,
    ) {
        let should_remove_session_entry = self
            .remote_addrs_by_session
            .get_mut(session_key)
            .is_some_and(|session_addrs| {
                if let Some(position) = session_addrs.iter().position(|addr| *addr == source_addr) {
                    session_addrs.swap_remove(position);
                }
                session_addrs.is_empty()
            });
        if should_remove_session_entry {
            self.remote_addrs_by_session.remove(session_key);
        }
    }
}

// ---------------------------------------------------------------------------
// Remote address demux on RtcSnapshotState
// ---------------------------------------------------------------------------

impl RtcSnapshotState {
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub(super) fn session_key_for_remote_addr(
        &self,
        source_addr: SocketAddr,
    ) -> Option<&TransportSessionKey> {
        self.remote_addr_index.get(&source_addr)
    }

    pub(super) fn remember_remote_addr(
        &mut self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) {
        let previous_session = self
            .remote_addr_index
            .insert(source_addr, session_key.clone());
        if let Some(previous_session) = previous_session {
            self.remove_remote_addr_from_session(&previous_session, source_addr);
        }
        let session_addrs = self
            .remote_addrs_by_session
            .entry(session_key.clone())
            .or_default();
        if !session_addrs.contains(&source_addr) {
            session_addrs.push(source_addr);
        }
    }

    pub(super) fn forget_remote_addr(&mut self, source_addr: SocketAddr) {
        let Some(session_key) = self.remote_addr_index.remove(&source_addr) else {
            return;
        };
        self.remove_remote_addr_from_session(&session_key, source_addr);
    }

    pub(super) fn forget_session_remote_addrs(&mut self, session_key: &TransportSessionKey) {
        let Some(session_addrs) = self.remote_addrs_by_session.remove(session_key) else {
            return;
        };
        for source_addr in session_addrs {
            self.remote_addr_index.remove(&source_addr);
        }
    }

    fn remove_remote_addr_from_session(
        &mut self,
        session_key: &TransportSessionKey,
        source_addr: SocketAddr,
    ) {
        let should_remove_session_entry = self
            .remote_addrs_by_session
            .get_mut(session_key)
            .is_some_and(|session_addrs| {
                if let Some(position) = session_addrs.iter().position(|addr| *addr == source_addr) {
                    session_addrs.swap_remove(position);
                }
                session_addrs.is_empty()
            });
        if should_remove_session_entry {
            self.remote_addrs_by_session.remove(session_key);
        }
    }
}
