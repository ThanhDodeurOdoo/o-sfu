use super::fixtures::*;
use crate::runtime::rtc_engine::state::RtcBootstrapState;

#[test]
fn rtc_bootstrap_state_reassigns_remote_addr_between_sessions() {
    let mut bootstrap_state = RtcBootstrapState::default();
    let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_001);
    let first_session_key = transport_key_on_worker(1, 0, 30, UserId::Integer(30));
    let second_session_key = transport_key_on_worker(2, 1, 30, UserId::Integer(30));

    let _ = bootstrap_state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &first_session_key);
    assert_eq!(
        bootstrap_state
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr),
        Some(&first_session_key)
    );

    let _ = bootstrap_state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &second_session_key);

    assert_eq!(
        bootstrap_state
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr),
        Some(&second_session_key)
    );
    assert!(
        bootstrap_state
            .remote_addr_demux
            .session_addrs_for(&first_session_key)
            .is_none()
    );
    assert_eq!(
        bootstrap_state
            .remote_addr_demux
            .session_addrs_for(&second_session_key),
        Some([source_addr].as_slice())
    );
}

#[test]
fn rtc_bootstrap_state_tracks_dirty_and_timed_out_sessions_separately() {
    let mut state = RtcBootstrapState::default();
    let first_session_key = transport_key_on_worker(1, 0, 31, UserId::Integer(31));
    let second_session_key = transport_key_on_worker(1, 0, 32, UserId::Integer(32));
    let now = Instant::now();
    let first_timeout = now + Duration::from_millis(20);
    let second_timeout = now + Duration::from_millis(40);

    state.update_session_timeout(&first_session_key, Some(first_timeout));
    state.update_session_timeout(&second_session_key, Some(second_timeout));
    state.mark_session_dirty(&second_session_key);

    assert_eq!(state.next_timeout_deadline(), Some(first_timeout));

    let ready_sessions = state.take_ready_sessions(now + Duration::from_millis(25));
    assert!(ready_sessions.contains(&first_session_key));
    assert!(ready_sessions.contains(&second_session_key));
    assert_eq!(ready_sessions.len(), 2);
    assert_eq!(state.next_timeout_deadline(), Some(second_timeout));
}

#[test]
fn rtc_bootstrap_state_prefers_latest_session_timeout_deadline() {
    let mut state = RtcBootstrapState::default();
    let session_key = transport_key_on_worker(1, 0, 33, UserId::Integer(33));
    let now = Instant::now();
    let first_timeout = now + Duration::from_millis(50);
    let updated_timeout = now + Duration::from_millis(10);

    state.update_session_timeout(&session_key, Some(first_timeout));
    state.update_session_timeout(&session_key, Some(updated_timeout));

    assert_eq!(state.next_timeout_deadline(), Some(updated_timeout));

    let ready_sessions = state.take_ready_sessions(now + Duration::from_millis(15));
    assert_eq!(ready_sessions.len(), 1);
    assert!(ready_sessions.contains(&session_key));
    assert_eq!(state.next_timeout_deadline(), None);
}
