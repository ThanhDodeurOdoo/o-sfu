use super::fixtures::*;
use str0m::{Event, IceConnectionState};

fn first_candidate_port(offer_sdp: &str) -> Option<u16> {
    offer_sdp
        .lines()
        .find_map(|line| line.trim().strip_prefix("a=candidate:"))
        .and_then(|candidate| candidate.split_whitespace().nth(5))
        .and_then(|port| port.parse::<u16>().ok())
}

#[tokio::test]
async fn rtc_initial_session_offer_starts_packet_loop() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 15, SessionId::Integer(15));

    assert!(!adapter.packet_loop_started.load(Ordering::Acquire));
    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started.load(Ordering::Acquire));
}

#[tokio::test]
async fn rtc_initial_session_offer_contains_real_ice_and_dtls_parameters() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 13, SessionId::Integer(13));

    let offer_sdp = prepare_transport_session(&adapter, &session_key)
        .await
        .expect("initial offer should succeed")
        .into_sdp();

    assert!(offer_sdp.contains("a=ice-ufrag:"));
    assert!(offer_sdp.contains("a=ice-pwd:"));
    assert!(offer_sdp.contains("a=setup:actpass"));
    assert!(offer_sdp.contains("a=fingerprint:sha-256 "));

    let Some(candidate_port) = first_candidate_port(&offer_sdp) else {
        panic!("offer should expose at least one candidate line");
    };
    assert!((40_000..=49_999).contains(&candidate_port));
}

#[test]
fn rtc_transport_health_maps_connected_and_disconnected_events() {
    assert_eq!(
        super::super::packet_loop::transport_health_from_event(&Event::Connected),
        Some(super::super::state::TransportSessionHealth::Connected)
    );
    assert_eq!(
        super::super::packet_loop::transport_health_from_event(&Event::IceConnectionStateChange(
            IceConnectionState::Connected
        )),
        Some(super::super::state::TransportSessionHealth::Connected)
    );
    assert_eq!(
        super::super::packet_loop::transport_health_from_event(&Event::IceConnectionStateChange(
            IceConnectionState::Disconnected
        )),
        Some(super::super::state::TransportSessionHealth::Disconnected)
    );
    assert_eq!(
        super::super::packet_loop::transport_health_from_event(&Event::IceConnectionStateChange(
            IceConnectionState::New
        )),
        None
    );
}

#[test]
fn rtc_transport_ice_state_metric_maps_all_supported_states() {
    use crate::runtime::metrics::TransportIceState;

    assert_eq!(
        super::super::packet_loop::transport_ice_state(IceConnectionState::New),
        TransportIceState::New
    );
    assert_eq!(
        super::super::packet_loop::transport_ice_state(IceConnectionState::Checking),
        TransportIceState::Checking
    );
    assert_eq!(
        super::super::packet_loop::transport_ice_state(IceConnectionState::Connected),
        TransportIceState::Connected
    );
    assert_eq!(
        super::super::packet_loop::transport_ice_state(IceConnectionState::Completed),
        TransportIceState::Completed
    );
    assert_eq!(
        super::super::packet_loop::transport_ice_state(IceConnectionState::Disconnected),
        TransportIceState::Disconnected
    );
}

#[tokio::test]
async fn rtc_transport_close_session_allows_recreating_the_initial_offer() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 14, SessionId::Integer(14));

    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    assert_eq!(adapter.close_session(&session_key).await, Ok(()));
    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn rtc_transport_close_session_cleans_transport_health_snapshot() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 143, SessionId::Integer(143));
    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );

    set_transport_health(
        &adapter,
        &session_key,
        super::super::state::TransportSessionHealth::Disconnected,
    );
    let metrics_snapshot = adapter.metrics.snapshot();
    assert_eq!(metrics_snapshot.connected_transport_sessions, 0);
    assert_eq!(metrics_snapshot.disconnected_transport_sessions, 1);
    assert_eq!(
        adapter.session_transport_health(&session_key),
        Some(super::super::state::TransportSessionHealth::Disconnected)
    );

    assert_eq!(adapter.close_session(&session_key).await, Ok(()));
    assert_eq!(adapter.session_transport_health(&session_key), None);
    let metrics_snapshot = adapter.metrics.snapshot();
    assert_eq!(metrics_snapshot.connected_transport_sessions, 0);
    assert_eq!(metrics_snapshot.disconnected_transport_sessions, 0);
}

#[tokio::test]
async fn rtc_transport_close_session_cleans_remote_addr_demux_state() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 140, SessionId::Integer(140));
    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );

    let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_000);
    remember_remote_addr(&adapter, source_addr, &session_key).await;
    assert_eq!(
        remote_addr_owner(&adapter, source_addr).await,
        Some(session_key.clone())
    );

    assert_eq!(adapter.close_session(&session_key).await, Ok(()));

    assert_eq!(remote_addr_owner(&adapter, source_addr).await, None);
    assert!(!has_any_remote_addr_session(&adapter).await);
}

#[tokio::test]
async fn rtc_transport_close_last_session_resets_packet_loop_worker() {
    let adapter = RtcTransportAdapter::default();
    let first_session_key = transport_key(1, 141, SessionId::Integer(141));
    assert!(
        prepare_transport_session(&adapter, &first_session_key)
            .await
            .is_ok()
    );
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started.load(Ordering::Acquire));

    assert_eq!(adapter.close_session(&first_session_key).await, Ok(()));
    assert!(!adapter.packet_loop_started.load(Ordering::Acquire));
    assert!(matches!(adapter.worker_handle(), Ok(None)));

    let second_session_key = transport_key(1, 142, SessionId::Integer(142));
    assert!(
        prepare_transport_session(&adapter, &second_session_key)
            .await
            .is_ok()
    );
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started.load(Ordering::Acquire));
}

#[tokio::test]
async fn rtc_transport_distinguishes_same_session_id_across_channels() {
    let adapter = RtcTransportAdapter::default();
    let first_session_key = transport_key_on_worker(1, 0, 30, SessionId::Integer(30));
    let second_session_key = transport_key_on_worker(2, 1, 30, SessionId::Integer(30));

    assert!(
        prepare_transport_session(&adapter, &first_session_key)
            .await
            .is_ok()
    );
    assert!(
        prepare_transport_session(&adapter, &second_session_key)
            .await
            .is_ok()
    );

    set_transport_health(
        &adapter,
        &second_session_key,
        super::super::state::TransportSessionHealth::Disconnected,
    );
    assert_eq!(adapter.close_session(&first_session_key).await, Ok(()));
    assert_eq!(
        adapter.session_transport_health(&second_session_key),
        Some(super::super::state::TransportSessionHealth::Disconnected)
    );
    assert!(adapter.packet_loop_started.load(Ordering::Acquire));
}
