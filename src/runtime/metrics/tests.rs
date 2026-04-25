use std::time::Duration;

use o_sfu_protocol::signaling::WebSocketCloseCode;

use super::{
    HttpRoute, RtcDatagramDropReason, RtcDatagramRoutePath, RtcRouteControlOutcome,
    RtpForwardDestinationKind, RtpRelayDropKind, RuntimeMetrics, RuntimeMetricsSnapshot,
    TransportIceState, WsSessionLoopExitReason,
};
use crate::runtime::{
    rtc_adapter::TransportSessionHealth,
    source_model::{SourceEncodingId, SourceSelector},
};

fn assert_live_gauges(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.active_channels, 1);
    assert_eq!(snapshot.active_sessions, 2);
    assert_eq!(snapshot.active_publications, 3);
    assert_eq!(snapshot.active_subscriptions, 4);
    assert_eq!(snapshot.active_recording_channels, 1);
    assert_eq!(snapshot.active_transport_sessions, 1);
    assert_eq!(snapshot.connected_transport_sessions, 1);
    assert_eq!(snapshot.disconnected_transport_sessions, 0);
}

fn assert_recording_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.recording_start_accepted, 1);
    assert_eq!(snapshot.recording_captured_packets, 1);
    assert_eq!(snapshot.recording_captured_streams, 1);
}

fn assert_transport_lifecycle_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.transport_health_transitions_unset_to_connected, 1);
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_disconnected,
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_disconnected,
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_connected,
        0
    );
    assert_eq!(snapshot.transport_health_transitions_connected_to_unset, 0);
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_unset,
        0
    );
    assert_eq!(snapshot.transport_ice_state_changes_new, 0);
    assert_eq!(snapshot.transport_ice_state_changes_checking, 1);
    assert_eq!(snapshot.transport_ice_state_changes_connected, 1);
    assert_eq!(snapshot.transport_ice_state_changes_completed, 0);
    assert_eq!(snapshot.transport_ice_state_changes_disconnected, 0);
    assert_eq!(snapshot.transport_dtls_connected, 1);
    assert_eq!(snapshot.transport_session_lifetime_le_1_second, 0);
    assert_eq!(snapshot.transport_session_lifetime_le_10_seconds, 1);
    assert_eq!(snapshot.transport_session_lifetime_le_60_seconds, 1);
    assert_eq!(snapshot.transport_session_lifetime_le_300_seconds, 1);
    assert_eq!(snapshot.transport_session_lifetime_count, 1);
    assert_eq!(snapshot.transport_session_lifetime_sum_micros, 1_500_000);
}

fn assert_rtp_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtp_packets_ingress, 1);
    assert_eq!(snapshot.rtp_packets_egress, 1);
    assert_eq!(snapshot.rtp_payload_bytes_ingress, 1200);
    assert_eq!(snapshot.rtp_payload_bytes_egress, 900);
}

fn assert_forwarding_volume_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc, 1);
    assert_eq!(snapshot.rtp_forwarded_packets_recording, 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay, 1);
    assert_eq!(snapshot.rtp_forwarded_packets_inter_node_relay, 1);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc, 900);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording, 700);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay, 500);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_inter_node_relay, 300);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay, 1);
    assert_eq!(snapshot.rtp_relay_overload_drops_inter_node_relay, 1);
}

fn assert_rtc_datagram_and_route_control_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtc_datagram_routes_indexed, 1);
    assert_eq!(snapshot.rtc_datagram_routes_scan, 1);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 1);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited, 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_session, 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed, 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans, 1);
    assert_eq!(snapshot.rtc_datagram_scan_sessions, 3);
    assert_eq!(snapshot.rtc_route_control_absorbed, 1);
    assert_eq!(snapshot.rtc_route_control_forwarded, 1);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops, 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed, 1);
    assert_eq!(snapshot.rtc_route_control_layer_dropped, 1);
}

fn assert_source_selection_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.source_selection_updates_open, 0);
    assert_eq!(snapshot.source_selection_updates_encoding, 1);
    assert_eq!(snapshot.source_selection_updates_room_policy_featured, 0);
    assert_eq!(snapshot.source_selection_updates_room_policy_thumbnail, 0);
}

fn assert_control_plane_latency_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.http_inflight.noop, 1);
    assert_eq!(snapshot.http_inflight.metrics, 0);
    assert_eq!(snapshot.http_request_duration.noop.le_50_millis, 1);
    assert_eq!(snapshot.http_request_duration.noop.le_10_millis, 0);
    assert_eq!(snapshot.http_request_duration.noop.count, 1);
    assert_eq!(snapshot.http_request_duration.noop.sum_micros, 25_000);
    assert_eq!(snapshot.ws_handshake_duration.le_100_millis, 1);
    assert_eq!(snapshot.ws_handshake_duration.count, 1);
    assert_eq!(snapshot.ws_handshake_duration.sum_micros, 80_000);
    assert_eq!(snapshot.ws_auth_duration.le_10_millis, 1);
    assert_eq!(snapshot.ws_auth_duration.count, 1);
    assert_eq!(snapshot.ws_auth_duration.sum_micros, 8_000);
    assert_eq!(snapshot.ws_session_initialize_duration.le_250_millis, 1);
    assert_eq!(snapshot.ws_session_initialize_duration.le_100_millis, 0);
    assert_eq!(snapshot.ws_session_initialize_duration.count, 1);
    assert_eq!(snapshot.ws_session_initialize_duration.sum_micros, 120_000);
}

#[test]
fn metrics_snapshot_tracks_http_and_websocket_counters() {
    let metrics = RuntimeMetrics::default();
    metrics.add_http_inflight_requests(HttpRoute::Noop, 1);
    metrics.record_http_channel_request();
    metrics.record_http_channel_unauthorized();
    metrics.record_http_disconnect_request();
    metrics.record_http_disconnect_unprocessable_entity();
    metrics.record_http_metrics_request();
    metrics.record_http_request_duration(HttpRoute::Noop, Duration::from_millis(25));
    metrics.record_ws_connection_accepted();
    metrics.record_ws_handshake_credentials_received();
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::AuthTimeout));
    metrics.record_ws_session_joined();
    metrics.record_ws_session_loop_started();
    metrics.record_ws_session_loop_exit(WsSessionLoopExitReason::PeerClosed);
    metrics.record_ws_bus_batch_received(3);
    metrics.record_ws_bus_invalid_input_failure();
    metrics.record_ws_bus_unsupported_feature_failure();
    metrics.record_ws_bus_client_request();
    metrics.record_ws_bus_client_message();
    metrics.record_ws_bus_batch_sent(2);
    metrics.record_ws_bus_send_failure();
    metrics.record_ws_handshake_duration(Duration::from_millis(80));
    metrics.record_ws_auth_duration(Duration::from_millis(8));
    metrics.record_ws_session_initialize_duration(Duration::from_millis(120));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.http_channel_requests, 1);
    assert_eq!(snapshot.http_channel_unauthorized, 1);
    assert_eq!(snapshot.http_disconnect_requests, 1);
    assert_eq!(snapshot.http_disconnect_unprocessable_entity, 1);
    assert_eq!(snapshot.http_metrics_requests, 1);
    assert_eq!(snapshot.ws_connections_accepted, 1);
    assert_eq!(snapshot.ws_handshake_credentials_received, 1);
    assert_eq!(snapshot.ws_handshake_rejected_timeout, 1);
    assert_eq!(snapshot.ws_handshake_rejected_protocol_error, 0);
    assert_eq!(snapshot.ws_sessions_joined, 1);
    assert_eq!(snapshot.ws_session_loops_started, 1);
    assert_eq!(snapshot.ws_session_loop_exits_peer_closed, 1);
    assert_eq!(snapshot.ws_session_loop_exits_ping_timeout, 0);
    assert_eq!(snapshot.ws_session_loop_exits_transport_disconnected, 0);
    assert_eq!(snapshot.ws_bus_parse_failures, 2);
    assert_eq!(snapshot.ws_bus_invalid_input_failures, 1);
    assert_eq!(snapshot.ws_bus_unsupported_feature_failures, 1);
    assert_eq!(snapshot.ws_bus_batches_received, 1);
    assert_eq!(snapshot.ws_bus_envelopes_received, 3);
    assert_eq!(snapshot.ws_bus_client_requests, 1);
    assert_eq!(snapshot.ws_bus_client_messages, 1);
    assert_eq!(snapshot.ws_bus_batches_sent, 1);
    assert_eq!(snapshot.ws_bus_envelopes_sent, 2);
    assert_eq!(snapshot.ws_bus_send_failures, 1);
    assert_control_plane_latency_metrics(&snapshot);
}

#[test]
fn metrics_snapshot_tracks_live_gauges_and_rtp_counters() {
    let metrics = RuntimeMetrics::default();
    metrics.add_active_channels(1);
    metrics.add_active_sessions(2);
    metrics.add_active_publications(3);
    metrics.add_active_subscriptions(4);
    metrics.add_active_recording_channels(1);
    metrics.add_active_transport_sessions(1);
    metrics.record_transport_health_transition(None, Some(TransportSessionHealth::Connected));
    metrics.record_recording_start_accepted();
    metrics.record_recording_captured_packet();
    metrics.record_recording_captured_stream();
    metrics.record_rtp_ingress(1200);
    metrics.record_rtp_egress(900);
    metrics.record_rtp_forwarded(RtpForwardDestinationKind::LocalRtc, 900);
    metrics.record_rtp_forwarded(RtpForwardDestinationKind::Recording, 700);
    metrics.record_rtp_forwarded(RtpForwardDestinationKind::IntraNodeRelay, 500);
    metrics.record_rtp_forwarded(RtpForwardDestinationKind::InterNodeRelay, 300);
    metrics.record_rtp_relay_overload_drop(RtpRelayDropKind::IntraNodeRelay);
    metrics.record_rtp_relay_overload_drop(RtpRelayDropKind::InterNodeRelay);
    metrics.record_transport_ice_state_change(TransportIceState::Checking);
    metrics.record_transport_ice_state_change(TransportIceState::Connected);
    metrics.record_transport_dtls_connected();
    metrics.record_transport_session_lifetime(Duration::from_millis(1500));
    metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
    metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    metrics.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
    metrics.record_rtc_datagram_drop(RtcDatagramDropReason::SourceRateLimited);
    metrics.record_rtc_datagram_drop(RtcDatagramDropReason::NoSession);
    metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
    metrics.record_rtc_datagram_fallback_scan(3);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
    metrics.record_source_selection_update(SourceSelector::Encoding(SourceEncodingId::from_raw(1)));

    let snapshot = metrics.snapshot();

    assert_live_gauges(&snapshot);
    assert_recording_metrics(&snapshot);
    assert_transport_lifecycle_metrics(&snapshot);
    assert_rtp_metrics(&snapshot);
    assert_forwarding_volume_metrics(&snapshot);
    assert_rtc_datagram_and_route_control_metrics(&snapshot);
    assert_source_selection_metrics(&snapshot);
}

#[test]
fn transport_health_transition_updates_connected_and_disconnected_gauges() {
    let metrics = RuntimeMetrics::default();

    metrics.record_transport_health_transition(None, Some(TransportSessionHealth::Connected));
    metrics.record_transport_health_transition(
        Some(TransportSessionHealth::Connected),
        Some(TransportSessionHealth::Disconnected),
    );
    metrics.record_transport_health_transition(Some(TransportSessionHealth::Disconnected), None);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.connected_transport_sessions, 0);
    assert_eq!(snapshot.disconnected_transport_sessions, 0);
    assert_eq!(snapshot.transport_health_transitions_unset_to_connected, 1);
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_disconnected,
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_unset,
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_disconnected,
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_connected,
        0
    );
    assert_eq!(snapshot.transport_health_transitions_connected_to_unset, 0);
}

#[test]
fn transport_lifecycle_metrics_track_ice_and_dtls_events() {
    let metrics = RuntimeMetrics::default();

    metrics.record_transport_ice_state_change(TransportIceState::New);
    metrics.record_transport_ice_state_change(TransportIceState::Checking);
    metrics.record_transport_ice_state_change(TransportIceState::Connected);
    metrics.record_transport_ice_state_change(TransportIceState::Completed);
    metrics.record_transport_ice_state_change(TransportIceState::Disconnected);
    metrics.record_transport_dtls_connected();
    metrics.record_transport_session_lifetime(Duration::from_secs(301));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.transport_ice_state_changes_new, 1);
    assert_eq!(snapshot.transport_ice_state_changes_checking, 1);
    assert_eq!(snapshot.transport_ice_state_changes_connected, 1);
    assert_eq!(snapshot.transport_ice_state_changes_completed, 1);
    assert_eq!(snapshot.transport_ice_state_changes_disconnected, 1);
    assert_eq!(snapshot.transport_dtls_connected, 1);
    assert_eq!(snapshot.transport_session_lifetime_le_1_second, 0);
    assert_eq!(snapshot.transport_session_lifetime_le_10_seconds, 0);
    assert_eq!(snapshot.transport_session_lifetime_le_60_seconds, 0);
    assert_eq!(snapshot.transport_session_lifetime_le_300_seconds, 0);
    assert_eq!(snapshot.transport_session_lifetime_count, 1);
    assert_eq!(snapshot.transport_session_lifetime_sum_micros, 301_000_000);
}

#[test]
fn handshake_rejection_buckets_are_distinct() {
    let metrics = RuntimeMetrics::default();
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::AuthFailed));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ProtocolError));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ChannelFull));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::Error));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.ws_handshake_rejected_authentication_failed, 1);
    assert_eq!(snapshot.ws_handshake_rejected_protocol_error, 1);
    assert_eq!(snapshot.ws_handshake_rejected_channel_full, 1);
    assert_eq!(snapshot.ws_handshake_rejected_error, 1);
}
