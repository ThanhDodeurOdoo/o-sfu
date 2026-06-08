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
