//! IP hash-indexed demux and media route entries for the RTC transport shard.

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
};

use str0m::media::{Mid, Pt};

use super::route_control::PacketLayerGate;
use crate::runtime::media_transport::{TransportMediaId, TransportSessionKey};

/// A single forwarding destination within the media route index.
///
/// One source can have manny destinations, each with its own selected packet
/// gate. The destination also carries the consumer-negotiated RTP identity so
/// local forwarding can present one browser stream even when the publisher
/// source is simulcast.
#[derive(Debug, Clone)]
pub(super) struct MediaRouteDestination {
    pub(super) dest_session: TransportSessionKey,
    pub(super) dest_transport_media_id: TransportMediaId,
    pub(super) dest_mid: Mid,
    /// Payload type negotiated for this consumer stream.
    ///
    /// Source payload types can differ from consumer payload types after router
    /// negotiation, so forwarding must not reuse the publisher value blindly.
    pub(super) dest_payload_type: Option<Pt>,
    pub(super) active: bool,
    /// Effective transport gate used by the packet loop right now.
    pub(super) packet_gate: PacketLayerGate,
    /// Selected strict gate that is waiting for a decodable live RID.
    ///
    /// Pending gates keepp a route from opening to multiple publisher RIDs wile
    /// some browsers may is still bringing up or refreshing the selected layer.
    pub(super) pending_packet_gate: Option<PacketLayerGate>,
}

#[derive(Debug, Clone)]
pub(super) struct MediaRouteEntry {
    pub(super) source_active: bool,
    pub(super) destinations: Vec<MediaRouteDestination>,
}

/// Media route source key: transport-native producer media identity.
pub(super) type MediaRouteKey = TransportMediaId;

#[derive(Debug, Default)]
pub struct RemoteAddrDemux {
    remote_addr_index: HashMap<SocketAddr, TransportSessionKey>,
    remote_addrs_by_session: BTreeMap<TransportSessionKey, Vec<SocketAddr>>,
    local_ice_ufrag_index: HashMap<String, TransportSessionKey>,
    local_ice_ufrag_by_session: BTreeMap<TransportSessionKey, String>,
    remote_candidate_addr_index: HashMap<SocketAddr, Vec<TransportSessionKey>>,
    remote_candidate_addrs_by_session: BTreeMap<TransportSessionKey, Vec<SocketAddr>>,
}

impl RemoteAddrDemux {
    #[must_use]
    pub fn session_key_for_remote_addr(
        &self,
        source_addr: SocketAddr,
    ) -> Option<&TransportSessionKey> {
        self.remote_addr_index.get(&source_addr)
    }

    #[must_use]
    pub fn remember_remote_addr(
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

    pub(super) fn session_key_for_local_ice_ufrag(
        &self,
        local_ice_ufrag: &str,
    ) -> Option<&TransportSessionKey> {
        self.local_ice_ufrag_index.get(local_ice_ufrag)
    }

    pub(super) fn remember_local_ice_ufrag(
        &mut self,
        local_ice_ufrag: &str,
        session_key: &TransportSessionKey,
    ) -> bool {
        if self
            .local_ice_ufrag_index
            .get(local_ice_ufrag)
            .is_some_and(|current_session| current_session == session_key)
        {
            return false;
        }
        let previous_ufrag = self
            .local_ice_ufrag_by_session
            .insert(session_key.clone(), local_ice_ufrag.to_owned());
        if let Some(previous_ufrag) = previous_ufrag {
            self.local_ice_ufrag_index.remove(&previous_ufrag);
        }
        let previous_session = self
            .local_ice_ufrag_index
            .insert(local_ice_ufrag.to_owned(), session_key.clone());
        if let Some(previous_session) = previous_session {
            self.local_ice_ufrag_by_session.remove(&previous_session);
        }
        true
    }

    #[must_use]
    pub fn candidate_sessions_for_source_addr(
        &self,
        source_addr: SocketAddr,
    ) -> Option<&[TransportSessionKey]> {
        self.remote_candidate_addr_index
            .get(&source_addr)
            .map(Vec::as_slice)
    }

    pub fn replace_session_remote_candidate_addrs<I>(
        &mut self,
        session_key: &TransportSessionKey,
        candidate_addrs: I,
    ) where
        I: IntoIterator<Item = SocketAddr>,
    {
        self.forget_user_remote_candidate_addrs(session_key);
        let session_candidate_addrs = self
            .remote_candidate_addrs_by_session
            .entry(session_key.clone())
            .or_default();
        for candidate_addr in candidate_addrs {
            if session_candidate_addrs.contains(&candidate_addr) {
                continue;
            }
            session_candidate_addrs.push(candidate_addr);
            self.remote_candidate_addr_index
                .entry(candidate_addr)
                .or_default()
                .push(session_key.clone());
        }
        if session_candidate_addrs.is_empty() {
            self.remote_candidate_addrs_by_session.remove(session_key);
        }
    }

    pub(super) fn forget_remote_addr(&mut self, source_addr: SocketAddr) {
        let Some(session_key) = self.remote_addr_index.remove(&source_addr) else {
            return;
        };
        self.remove_remote_addr_from_session(&session_key, source_addr);
    }

    pub(super) fn forget_user_remote_addrs(&mut self, session_key: &TransportSessionKey) {
        let Some(session_addrs) = self.remote_addrs_by_session.remove(session_key) else {
            return;
        };
        for source_addr in session_addrs {
            self.remote_addr_index.remove(&source_addr);
        }
    }

    pub(super) fn forget_user_local_ice_ufrag(&mut self, session_key: &TransportSessionKey) {
        let Some(local_ice_ufrag) = self.local_ice_ufrag_by_session.remove(session_key) else {
            return;
        };
        self.local_ice_ufrag_index.remove(&local_ice_ufrag);
    }

    pub(super) fn forget_user_remote_candidate_addrs(&mut self, session_key: &TransportSessionKey) {
        let Some(candidate_addrs) = self.remote_candidate_addrs_by_session.remove(session_key)
        else {
            return;
        };
        for candidate_addr in candidate_addrs {
            let should_remove_index_entry = self
                .remote_candidate_addr_index
                .get_mut(&candidate_addr)
                .is_some_and(|session_keys| {
                    session_keys.retain(|candidate_session| candidate_session != session_key);
                    session_keys.is_empty()
                });
            if should_remove_index_entry {
                self.remote_candidate_addr_index.remove(&candidate_addr);
            }
        }
    }

    pub fn session_entries(&self) -> impl Iterator<Item = (&TransportSessionKey, &[SocketAddr])> {
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
    pub(super) fn local_ice_ufrag_for(&self, session_key: &TransportSessionKey) -> Option<&str> {
        self.local_ice_ufrag_by_session
            .get(session_key)
            .map(String::as_str)
    }

    #[cfg(test)]
    pub(super) fn remote_candidate_addrs_for(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<&[SocketAddr]> {
        self.remote_candidate_addrs_by_session
            .get(session_key)
            .map(Vec::as_slice)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn is_empty(&self) -> bool {
        self.remote_addrs_by_session.is_empty()
            && self.local_ice_ufrag_by_session.is_empty()
            && self.remote_candidate_addrs_by_session.is_empty()
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
    use crate::runtime::{
        UserId, media_transport::TransportSessionKey,
        rtc_engine::test_support::test_transport_session_key,
    };

    fn session_key(room_instance_id: u64, session_numeric_id: i64) -> TransportSessionKey {
        test_transport_session_key(0, 0, room_instance_id, UserId::Integer(session_numeric_id))
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

    #[test]
    fn remember_local_ice_ufrag_tracks_the_latest_session_mapping() {
        let mut demux = RemoteAddrDemux::default();
        let first_session = session_key(9, 3);
        let second_session = session_key(9, 4);

        assert!(demux.remember_local_ice_ufrag("ufrag-a", &first_session));
        assert!(!demux.remember_local_ice_ufrag("ufrag-a", &first_session));
        assert!(demux.remember_local_ice_ufrag("ufrag-a", &second_session));

        assert_eq!(
            demux.session_key_for_local_ice_ufrag("ufrag-a"),
            Some(&second_session)
        );
        assert_eq!(demux.local_ice_ufrag_for(&first_session), None);
        assert_eq!(demux.local_ice_ufrag_for(&second_session), Some("ufrag-a"));
    }

    #[test]
    fn replace_session_remote_candidate_addrs_deduplicates_and_cleans_previous_entries() {
        let mut demux = RemoteAddrDemux::default();
        let first_session = session_key(9, 3);
        let second_session = session_key(9, 4);
        let first_candidate = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46_001);
        let second_candidate = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46_002);

        demux.replace_session_remote_candidate_addrs(
            &first_session,
            [first_candidate, first_candidate, second_candidate],
        );
        demux.replace_session_remote_candidate_addrs(&second_session, [second_candidate]);

        assert_eq!(
            demux.remote_candidate_addrs_for(&first_session),
            Some([first_candidate, second_candidate].as_slice())
        );
        assert_eq!(
            demux.candidate_sessions_for_source_addr(second_candidate),
            Some([first_session.clone(), second_session.clone()].as_slice())
        );

        demux.replace_session_remote_candidate_addrs(&first_session, [first_candidate]);

        assert_eq!(
            demux.remote_candidate_addrs_for(&first_session),
            Some([first_candidate].as_slice())
        );
        assert_eq!(
            demux.candidate_sessions_for_source_addr(second_candidate),
            Some([second_session].as_slice())
        );
    }
}
