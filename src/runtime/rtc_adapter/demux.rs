//! IP hash-indexed demux and media route entries for the RTC transport adapter.

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
};

use str0m::media::Mid;

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

/// A single forwarding destination within the media route index.
#[derive(Debug, Clone)]
pub(super) struct MediaRouteDestination {
    pub(super) dest_session: TransportSessionKey,
    pub(super) dest_transport_media_id: TransportMediaId,
    pub(super) dest_mid: Mid,
    pub(super) active: bool,
}

#[derive(Debug, Clone)]
pub(super) struct MediaRouteEntry {
    pub(super) source_active: bool,
    pub(super) destinations: Vec<MediaRouteDestination>,
}

/// Media route source key: transport-native producer media identity.
pub(super) type MediaRouteKey = TransportMediaId;

#[derive(Debug, Default)]
pub(super) struct RemoteAddrDemux {
    remote_addr_index: HashMap<SocketAddr, TransportSessionKey>,
    remote_addrs_by_session: BTreeMap<TransportSessionKey, Vec<SocketAddr>>,
}

impl RemoteAddrDemux {
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
    ) -> bool {
        if self
            .remote_addr_index
            .get(&source_addr)
            .is_some_and(|current_session| current_session == session_key)
        {
            return false;
        }
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
        true
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

    #[cfg(feature = "internal-benchmarks")]
    pub(super) fn session_entries(
        &self,
    ) -> impl Iterator<Item = (&TransportSessionKey, &[SocketAddr])> {
        self.remote_addrs_by_session
            .iter()
            .map(|(session_key, addrs)| (session_key, addrs.as_slice()))
    }

    #[cfg(test)]
    pub(super) fn session_addrs_for(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<&[SocketAddr]> {
        self.remote_addrs_by_session
            .get(session_key)
            .map(Vec::as_slice)
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.remote_addrs_by_session.is_empty()
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::RemoteAddrDemux;
    use crate::runtime::transport_adapter::TransportSessionKey;
    use crate::signaling::shared::SessionId;

    fn session_key(channel_runtime_id: u64, session_numeric_id: i64) -> TransportSessionKey {
        TransportSessionKey::new(
            0,
            0,
            channel_runtime_id,
            SessionId::Integer(session_numeric_id),
        )
    }

    #[test]
    fn remember_remote_addr_reports_stable_mapping_without_churn() {
        let mut demux = RemoteAddrDemux::default();
        let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46_001);
        let session_key = session_key(9, 3);

        assert!(demux.remember_remote_addr(source_addr, &session_key));
        assert!(!demux.remember_remote_addr(source_addr, &session_key));
        assert_eq!(
            demux.session_key_for_remote_addr(source_addr),
            Some(&session_key)
        );
        assert_eq!(
            demux.session_addrs_for(&session_key),
            Some([source_addr].as_slice())
        );
    }
}
