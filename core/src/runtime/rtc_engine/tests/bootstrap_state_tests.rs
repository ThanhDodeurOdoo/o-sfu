use super::fixtures::*;
use crate::runtime::rtc_engine::state::RtcBootstrapState;

#[test]
fn rtc_bootstrap_state_reassigns_remote_addr_between_sessions() {
    let mut bootstrap_state = RtcBootstrapState::default();
    let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_001);
    let first_session_key = transport_key_on_worker(1, 0, 30, UserId::Integer(30));
    let second_session_key = transport_key_on_worker(2, 1, 30, UserId::Integer(30));

    let _ = bootstrap_state
        .packet_loop
        .remote_addr_demux
        .remember_remote_addr(source_addr, &first_session_key);
    assert_eq!(
        bootstrap_state
            .packet_loop
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr),
        Some(&first_session_key)
    );

    let _ = bootstrap_state
        .packet_loop
        .remote_addr_demux
        .remember_remote_addr(source_addr, &second_session_key);

    assert_eq!(
        bootstrap_state
            .packet_loop
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr),
        Some(&second_session_key)
    );
    assert!(
        bootstrap_state
            .packet_loop
            .remote_addr_demux
            .session_addrs_for(&first_session_key)
            .is_none()
    );
    assert_eq!(
        bootstrap_state
            .packet_loop
            .remote_addr_demux
            .session_addrs_for(&second_session_key),
        Some([source_addr].as_slice())
    );
}
