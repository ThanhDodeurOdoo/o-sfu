//! packet-loop demux indexes for rtc ingress
//!
//! this module keeps the compact lookup state that the rtc packet loop needs
//! while it routes UDP datagrams
//! address demux entries map learned or signaled network hints back to sessions so
//! ingress recovery can probe a bounded candidate set before `Rtc::accepts()`
//! makes the final ownership decision
//!
//! the indexes are worker-local state
//! snapshot state may mirror selected entries for observation
//! callers must update each demux value through the methods here so forward and
//! reverse indexes cannot drift apart

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
};

use crate::engine::media_transport::TransportSessionKey;

/// bidirectional demux indexes for worker-local UDP ingress recovery
///
/// learned remote addresses are the fast path for packets that already passed
/// `Rtc::accepts()`
/// local ICE ufrags and remote candidate addresses are recovery hints used for
/// unknown source tuples
/// all mappings are non-authoritative because ICE state can change after an
/// index was written
///
/// mutating methods keep reverse indexes in sync so session teardown can remove
/// every tuple, ufrag and candidate hint owned by one session without scanning
/// unrelated sessions
#[derive(Debug, Default)]
pub struct RemoteAddrDemux {
    /// learned UDP source tuple to session pin
    remote_addr_index: HashMap<SocketAddr, TransportSessionKey>,
    /// reverse lookup for learned UDP source tuple cleanup
    remote_addrs_by_session: BTreeMap<TransportSessionKey, Vec<SocketAddr>>,
    /// local ICE ufrag to session recovery hint
    local_ice_ufrag_index: HashMap<String, TransportSessionKey>,
    /// reverse lookup for replacing or removing a session local ICE ufrag
    local_ice_ufrag_by_session: BTreeMap<TransportSessionKey, String>,
    /// signaled remote candidate address to possible sessions
    remote_candidate_addr_index: HashMap<SocketAddr, Vec<TransportSessionKey>>,
    /// reverse lookup for candidate hint cleanup after renegotiation or teardown
    remote_candidate_addrs_by_session: BTreeMap<TransportSessionKey, Vec<SocketAddr>>,
}

impl RemoteAddrDemux {
    /// returns the session currently pinned to a UDP source tuple
    ///
    /// this is a hot-path hint for cached ingress routing
    /// callers must still re-check the packet with `Rtc::accepts()` before
    /// feeding it into a session because ICE state can move after the pin was
    /// learned
    #[must_use]
    pub fn session_key_for_remote_addr(
        &self,
        source_addr: SocketAddr,
    ) -> Option<&TransportSessionKey> {
        self.remote_addr_index.get(&source_addr)
    }

    /// pins a UDP source tuple to the session that just accepted traffic
    ///
    /// returns `true` when the visible mapping changed
    /// remapping a tuple also removes it from the previous session reverse
    /// index so session teardown can later clean every learned tuple with one
    /// key
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
            self.remove_remote_addr(&previous_session, source_addr);
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

    /// returns the session advertised by a local ICE ufrag
    ///
    /// this index is used to narrow STUN recovery when the USERNAME attribute
    /// names the local fragment
    /// the returned session is still only a candidate for `Rtc::accepts()`
    pub(super) fn session_for_local_ufrag(
        &self,
        local_ice_ufrag: &str,
    ) -> Option<&TransportSessionKey> {
        self.local_ice_ufrag_index.get(local_ice_ufrag)
    }

    /// replaces the local ICE ufrag registered for a session
    ///
    /// each session owns at most one local ufrag
    /// each local ufrag maps to at most one session
    /// returning `false` means the existing mapping already expressed that
    /// contract
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

    /// returns sessions whose signaled candidates match the observed source
    ///
    /// candidate addresses are weaker than learned source pins because many
    /// sessions can advertise the same address
    /// callers must treat the slice as a bounded probe set and let
    /// `Rtc::accepts()` decide ownership
    #[must_use]
    pub fn candidates_for_src_addr(
        &self,
        source_addr: SocketAddr,
    ) -> Option<&[TransportSessionKey]> {
        self.remote_candidate_addr_index
            .get(&source_addr)
            .map(Vec::as_slice)
    }

    /// replaces all signaled remote candidate addresses for one session
    ///
    /// this is called after answer application when str0m has updated the
    /// candidate set
    /// old hints are removed first so recovery cannot keep probing a session
    /// through candidates that no longer belong to it
    pub fn replace_remote_candidates<I>(
        &mut self,
        session_key: &TransportSessionKey,
        candidate_addrs: I,
    ) where
        I: IntoIterator<Item = SocketAddr>,
    {
        self.forget_user_remote_candidates(session_key);
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

    /// removes one learned UDP source tuple pin
    ///
    /// this is used when the cached path no longer passes `Rtc::accepts()` or
    /// when snapshot state mirrors a worker cleanup
    pub(super) fn forget_remote_addr(&mut self, source_addr: SocketAddr) {
        let Some(session_key) = self.remote_addr_index.remove(&source_addr) else {
            return;
        };
        self.remove_remote_addr(&session_key, source_addr);
    }

    /// removes every learned UDP source tuple owned by a session
    ///
    /// session teardown calls this on both worker and snapshot demux state so
    /// stale source pins cannot route packets to a removed user
    pub(super) fn forget_user_remote_addrs(&mut self, session_key: &TransportSessionKey) {
        let Some(session_addrs) = self.remote_addrs_by_session.remove(session_key) else {
            return;
        };
        for source_addr in session_addrs {
            self.remote_addr_index.remove(&source_addr);
        }
    }

    /// removes the local ICE ufrag recovery hint for a session
    pub(super) fn forget_user_local_ice_ufrag(&mut self, session_key: &TransportSessionKey) {
        let Some(local_ice_ufrag) = self.local_ice_ufrag_by_session.remove(session_key) else {
            return;
        };
        self.local_ice_ufrag_index.remove(&local_ice_ufrag);
    }

    /// removes all remote candidate recovery hints owned by a session
    ///
    /// candidate address indexes can contain several sessions for one address
    /// cleanup removes only the target session from each fanout list and drops
    /// empty address entries afterward
    pub(super) fn forget_user_remote_candidates(&mut self, session_key: &TransportSessionKey) {
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

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.remote_addrs_by_session.is_empty()
            && self.local_ice_ufrag_by_session.is_empty()
            && self.remote_candidate_addrs_by_session.is_empty()
    }

    fn remove_remote_addr(&mut self, session_key: &TransportSessionKey, source_addr: SocketAddr) {
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
    use crate::engine::{
        UserId,
        media_transport::{TransportSessionKey, rtc::test_support::test_transport_session_key},
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
            demux.session_for_local_ufrag("ufrag-a"),
            Some(&second_session)
        );
        assert_eq!(demux.local_ice_ufrag_for(&first_session), None);
        assert_eq!(demux.local_ice_ufrag_for(&second_session), Some("ufrag-a"));
    }

    #[test]
    fn replace_remote_candidates_deduplicates_and_cleans_previous_entries() {
        let mut demux = RemoteAddrDemux::default();
        let first_session = session_key(9, 3);
        let second_session = session_key(9, 4);
        let first_candidate = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46_001);
        let second_candidate = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46_002);

        demux.replace_remote_candidates(
            &first_session,
            [first_candidate, first_candidate, second_candidate],
        );
        demux.replace_remote_candidates(&second_session, [second_candidate]);

        assert_eq!(
            demux.remote_candidate_addrs_for(&first_session),
            Some([first_candidate, second_candidate].as_slice())
        );
        assert_eq!(
            demux.candidates_for_src_addr(second_candidate),
            Some([first_session.clone(), second_session.clone()].as_slice())
        );

        demux.replace_remote_candidates(&first_session, [first_candidate]);

        assert_eq!(
            demux.remote_candidate_addrs_for(&first_session),
            Some([first_candidate].as_slice())
        );
        assert_eq!(
            demux.candidates_for_src_addr(second_candidate),
            Some([second_session].as_slice())
        );
    }
}
