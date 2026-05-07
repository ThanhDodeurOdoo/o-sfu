use std::time::Duration;

use o_sfu_model::WebSocketCloseCode;

use super::{
    BudgetSolverOutcome, HttpRoute, MetricName, RtcDatagramDropReason, RtcDatagramRoutePath,
    RtcRouteControlOutcome, RtpForwardDestinationKind, RtpRelayDropKind, RuntimeMetrics,
    RuntimeMetricsSnapshot, SourceSelectionKind, TransportHealthState, TransportIceState,
    WsSessionLoopExitReason,
    test_support::{DurationHistogramSnapshot, RuntimeMetricsSnapshotLookup},
};

struct HttpInflightSnapshot {
    noop: i64,
    metrics: i64,
}

struct HttpRequestDurationSnapshot {
    noop: DurationHistogramSnapshot,
}

macro_rules! counter_accessors {
    ($($method:ident => $metric:ident $labels:expr),+ $(,)?) => {
        $(fn $method(&self) -> u64 {
            self.counter_value(MetricName::$metric, $labels)
        })+
    };
}

macro_rules! gauge_accessors {
    ($($method:ident => $metric:ident $labels:expr),+ $(,)?) => {
        $(fn $method(&self) -> i64 {
            self.gauge_value(MetricName::$metric, $labels)
        })+
    };
}

trait RuntimeMetricsSnapshotTestExt: RuntimeMetricsSnapshotLookup {
    fn http_inflight(&self) -> HttpInflightSnapshot {
        HttpInflightSnapshot {
            noop: self.gauge_value(MetricName::HttpInflightRequests, &[("route", "noop")]),
            metrics: self.gauge_value(MetricName::HttpInflightRequests, &[("route", "metrics")]),
        }
    }

    fn http_request_duration(&self) -> HttpRequestDurationSnapshot {
        HttpRequestDurationSnapshot {
            noop: self
                .duration_snapshot(MetricName::HttpRequestDurationSeconds, &[("route", "noop")]),
        }
    }

    fn ws_handshake_duration(&self) -> DurationHistogramSnapshot {
        self.duration_snapshot(MetricName::WsHandshakeDurationSeconds, &[])
    }

    fn ws_auth_duration(&self) -> DurationHistogramSnapshot {
        self.duration_snapshot(MetricName::WsAuthDurationSeconds, &[])
    }

    fn ws_user_initialize_duration(&self) -> DurationHistogramSnapshot {
        self.duration_snapshot(MetricName::WsUserInitializeDurationSeconds, &[])
    }

    counter_accessors! {
        http_room_requests => HttpRoomRequestsTotal &[],
        http_room_unauthorized => HttpRoomResponsesTotal &[("status", "unauthorized")],
        http_disconnect_requests => HttpDisconnectRequestsTotal &[],
        http_disconnect_unprocessable_entity => HttpDisconnectResponsesTotal &[("status", "unprocessable_entity")],
        http_metrics_requests => HttpMetricsRequestsTotal &[],
        ws_connections_accepted => WsConnectionsTotal &[("stage", "accepted")],
        ws_handshake_credentials_received => WsConnectionsTotal &[("stage", "credentials_received")],
        ws_users_joined => WsConnectionsTotal &[("stage", "joined")],
        ws_handshake_rejected_timeout => WsHandshakeRejectionsTotal &[("close_code", "auth_timeout")],
        ws_handshake_rejected_authentication_failed => WsHandshakeRejectionsTotal &[("close_code", "auth_failed")],
        ws_handshake_rejected_protocol_error => WsHandshakeRejectionsTotal &[("close_code", "protocol_error")],
        ws_handshake_rejected_room_full => WsHandshakeRejectionsTotal &[("close_code", "room_full")],
        ws_handshake_rejected_error => WsHandshakeRejectionsTotal &[("close_code", "error")],
        ws_user_loops_started => WsUserLoopsStartedTotal &[],
        ws_user_loop_exits_user_closed => WsUserLoopExitsTotal &[("reason", "user_closed")],
        ws_user_loop_exits_ping_timeout => WsUserLoopExitsTotal &[("reason", "ping_timeout")],
        ws_user_loop_exits_transport_disconnected => WsUserLoopExitsTotal &[("reason", "transport_disconnected")],
        ws_bus_parse_failures => WsBusParseFailuresTotal &[],
        ws_bus_invalid_input_failures => WsBusFailuresTotal &[("kind", "invalid_input")],
        ws_bus_unsupported_feature_failures => WsBusFailuresTotal &[("kind", "unsupported_feature")],
        ws_bus_batches_received => WsBusBatchesTotal &[("direction", "received")],
        ws_bus_envelopes_received => WsBusEnvelopesTotal &[("direction", "received")],
        ws_bus_client_requests => WsBusClientFramesTotal &[("kind", "request")],
        ws_bus_client_messages => WsBusClientFramesTotal &[("kind", "message")],
        ws_bus_batches_sent => WsBusBatchesTotal &[("direction", "sent")],
        ws_bus_envelopes_sent => WsBusEnvelopesTotal &[("direction", "sent")],
        ws_bus_send_failures => WsBusFailuresTotal &[("kind", "send")],
        recording_start_accepted => RecordingActionsTotal &[("action", "start"), ("outcome", "accepted")],
        recording_captured_packets => RecordingCapturedPacketsTotal &[],
        recording_captured_streams => RecordingCapturedStreamsTotal &[],
        rtp_packets_ingress => RtpPacketsTotal &[("direction", "ingress")],
        rtp_packets_egress => RtpPacketsTotal &[("direction", "egress")],
        rtp_payload_bytes_ingress => RtpPayloadBytesTotal &[("direction", "ingress")],
        rtp_payload_bytes_egress => RtpPayloadBytesTotal &[("direction", "egress")],
        rtp_forwarded_packets_local_rtc => RtpForwardedPacketsTotal &[("destination", "local_rtc")],
        rtp_forwarded_packets_recording => RtpForwardedPacketsTotal &[("destination", "recording")],
        rtp_forwarded_packets_intra_node_relay => RtpForwardedPacketsTotal &[("destination", "intra_node_relay")],
        rtp_forwarded_packets_inter_node_relay => RtpForwardedPacketsTotal &[("destination", "inter_node_relay")],
        rtp_forwarded_payload_bytes_local_rtc => RtpForwardedPayloadBytesTotal &[("destination", "local_rtc")],
        rtp_forwarded_payload_bytes_recording => RtpForwardedPayloadBytesTotal &[("destination", "recording")],
        rtp_forwarded_payload_bytes_intra_node_relay => RtpForwardedPayloadBytesTotal &[("destination", "intra_node_relay")],
        rtp_forwarded_payload_bytes_inter_node_relay => RtpForwardedPayloadBytesTotal &[("destination", "inter_node_relay")],
        rtp_relay_overload_drops_intra_node_relay => RtpRelayOverloadDropsTotal &[("destination", "intra_node_relay")],
        rtp_relay_overload_drops_inter_node_relay => RtpRelayOverloadDropsTotal &[("destination", "inter_node_relay")],
        transport_health_transitions_unset_to_connected => TransportHealthTransitionsTotal &[("from", "unset"), ("to", "connected")],
        transport_health_transitions_unset_to_disconnected => TransportHealthTransitionsTotal &[("from", "unset"), ("to", "disconnected")],
        transport_health_transitions_connected_to_disconnected => TransportHealthTransitionsTotal &[("from", "connected"), ("to", "disconnected")],
        transport_health_transitions_disconnected_to_connected => TransportHealthTransitionsTotal &[("from", "disconnected"), ("to", "connected")],
        transport_health_transitions_connected_to_unset => TransportHealthTransitionsTotal &[("from", "connected"), ("to", "unset")],
        transport_health_transitions_disconnected_to_unset => TransportHealthTransitionsTotal &[("from", "disconnected"), ("to", "unset")],
        transport_ice_state_changes_new => TransportIceStateChangesTotal &[("state", "new")],
        transport_ice_state_changes_checking => TransportIceStateChangesTotal &[("state", "checking")],
        transport_ice_state_changes_connected => TransportIceStateChangesTotal &[("state", "connected")],
        transport_ice_state_changes_completed => TransportIceStateChangesTotal &[("state", "completed")],
        transport_ice_state_changes_disconnected => TransportIceStateChangesTotal &[("state", "disconnected")],
        transport_dtls_connected => TransportDtlsConnectedTotal &[],
        transport_cleanup_retries => TransportCleanupRetriesTotal &[],
        transport_cleanup_retry_successes => TransportCleanupRetrySuccessesTotal &[],
        transport_cleanup_failures_terminal => TransportCleanupFailuresTotal &[("kind", "terminal")],
        transport_cleanup_failures_retry_exhausted => TransportCleanupFailuresTotal &[("kind", "retry_exhausted")],
        transport_cleanup_failures_queue_full => TransportCleanupFailuresTotal &[("kind", "queue_full")],
        transport_cleanup_failures_shutdown => TransportCleanupFailuresTotal &[("kind", "shutdown")],
        rtc_datagram_routes_indexed => RtcDatagramRoutesTotal &[("path", "indexed")],
        rtc_datagram_routes_scan => RtcDatagramRoutesTotal &[("path", "scan")],
        rtc_datagram_drops_recent_miss_cache => RtcDatagramDropsTotal &[("reason", "recent_miss_cache")],
        rtc_datagram_drops_source_rate_limited => RtcDatagramDropsTotal &[("reason", "source_rate_limited")],
        rtc_datagram_drops_no_user => RtcDatagramDropsTotal &[("reason", "no_user")],
        rtc_datagram_drops_malformed => RtcDatagramDropsTotal &[("reason", "malformed")],
        rtc_datagram_fallback_scans => RtcDatagramFallbackScansTotal &[],
        rtc_datagram_scan_users => RtcDatagramScanUsersTotal &[],
        rtc_route_control_absorbed => RtcRouteControlTotal &[("outcome", "absorbed")],
        rtc_route_control_forwarded => RtcRouteControlTotal &[("outcome", "forwarded")],
        rtc_route_control_route_gated_relay_drops => RtcRouteControlTotal &[("outcome", "route_gated_relay_drop")],
        rtc_route_control_layer_allowed => RtcRouteControlTotal &[("outcome", "layer_allowed")],
        rtc_route_control_layer_dropped => RtcRouteControlTotal &[("outcome", "layer_dropped")],
        source_selection_updates_open => SourceSelectionUpdatesTotal &[("selector", "open")],
        source_selection_updates_encoding => SourceSelectionUpdatesTotal &[("selector", "encoding")],
        source_selection_updates_operating_point => SourceSelectionUpdatesTotal &[("selector", "operating_point")],
        source_selection_updates_room_policy_featured => SourceSelectionUpdatesTotal &[("selector", "room_policy_featured")],
        source_selection_updates_room_policy_thumbnail => SourceSelectionUpdatesTotal &[("selector", "room_policy_thumbnail")],
        budget_solver_outcomes_degraded => BudgetSolverOutcomesTotal &[("outcome", "degraded")],
        budget_solver_outcomes_paused => BudgetSolverOutcomesTotal &[("outcome", "paused")],
        budget_solver_outcomes_resumed => BudgetSolverOutcomesTotal &[("outcome", "resumed")],
        budget_solver_outcomes_protected_over_budget => BudgetSolverOutcomesTotal &[("outcome", "protected_over_budget")],
    }

    gauge_accessors! {
        active_rooms => RoomsActive &[],
        active_users => UsersActive &[],
        active_publications => PublicationsActive &[],
        active_subscriptions => SubscriptionsActive &[],
        active_recording_rooms => RecordingRoomsActive &[],
        active_transport_users => TransportUsersActive &[],
        connected_transport_users => TransportHealthUsers &[("state", "connected")],
        disconnected_transport_users => TransportHealthUsers &[("state", "disconnected")],
    }

    fn transport_user_lifetime_le_1_second(&self) -> u64 {
        self.transport_user_lifetime_bucket("1")
    }

    fn transport_user_lifetime_le_10_seconds(&self) -> u64 {
        self.transport_user_lifetime_bucket("10")
    }

    fn transport_user_lifetime_le_60_seconds(&self) -> u64 {
        self.transport_user_lifetime_bucket("60")
    }

    fn transport_user_lifetime_le_300_seconds(&self) -> u64 {
        self.transport_user_lifetime_bucket("300")
    }

    fn transport_user_lifetime_count(&self) -> u64 {
        self.histogram_count_value(MetricName::TransportUserLifetimeSeconds, &[])
    }

    fn transport_user_lifetime_sum_micros(&self) -> u64 {
        self.histogram_sum_micros_value(MetricName::TransportUserLifetimeSeconds, &[])
    }

    fn transport_user_lifetime_bucket(&self, upper_bound: &str) -> u64 {
        self.histogram_bucket_value(MetricName::TransportUserLifetimeSeconds, &[], upper_bound)
    }
}

impl RuntimeMetricsSnapshotTestExt for RuntimeMetricsSnapshot {}

fn assert_live_gauges(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.active_rooms(), 1);
    assert_eq!(snapshot.active_users(), 2);
    assert_eq!(snapshot.active_publications(), 3);
    assert_eq!(snapshot.active_subscriptions(), 4);
    assert_eq!(snapshot.active_recording_rooms(), 1);
    assert_eq!(snapshot.active_transport_users(), 1);
    assert_eq!(snapshot.connected_transport_users(), 1);
    assert_eq!(snapshot.disconnected_transport_users(), 0);
}

fn assert_recording_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.recording_start_accepted(), 1);
    assert_eq!(snapshot.recording_captured_packets(), 1);
    assert_eq!(snapshot.recording_captured_streams(), 1);
}

fn assert_transport_lifecycle_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_connected(),
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_disconnected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_disconnected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_connected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_unset(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_unset(),
        0
    );
    assert_eq!(snapshot.transport_ice_state_changes_new(), 0);
    assert_eq!(snapshot.transport_ice_state_changes_checking(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_connected(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_completed(), 0);
    assert_eq!(snapshot.transport_ice_state_changes_disconnected(), 0);
    assert_eq!(snapshot.transport_dtls_connected(), 1);
    assert_eq!(snapshot.transport_user_lifetime_le_1_second(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_10_seconds(), 1);
    assert_eq!(snapshot.transport_user_lifetime_le_60_seconds(), 1);
    assert_eq!(snapshot.transport_user_lifetime_le_300_seconds(), 1);
    assert_eq!(snapshot.transport_user_lifetime_count(), 1);
    assert_eq!(snapshot.transport_user_lifetime_sum_micros(), 1_500_000);
    assert_eq!(snapshot.transport_cleanup_retries(), 1);
    assert_eq!(snapshot.transport_cleanup_retry_successes(), 1);
    assert_eq!(snapshot.transport_cleanup_failures_retry_exhausted(), 1);
    assert_eq!(snapshot.transport_cleanup_failures_terminal(), 0);
    assert_eq!(snapshot.transport_cleanup_failures_queue_full(), 0);
    assert_eq!(snapshot.transport_cleanup_failures_shutdown(), 0);
}

fn assert_rtp_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtp_packets_ingress(), 1);
    assert_eq!(snapshot.rtp_packets_egress(), 1);
    assert_eq!(snapshot.rtp_payload_bytes_ingress(), 1200);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 900);
}

fn assert_forwarding_volume_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_inter_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc(), 900);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 700);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay(), 500);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_inter_node_relay(), 300);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay(), 1);
    assert_eq!(snapshot.rtp_relay_overload_drops_inter_node_relay(), 1);
}

fn assert_rtc_datagram_and_route_control_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtc_datagram_routes_indexed(), 1);
    assert_eq!(snapshot.rtc_datagram_routes_scan(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 3);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 1);
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
}

fn assert_source_selection_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.source_selection_updates_open(), 0);
    assert_eq!(snapshot.source_selection_updates_encoding(), 1);
    assert_eq!(snapshot.source_selection_updates_operating_point(), 0);
    assert_eq!(snapshot.source_selection_updates_room_policy_featured(), 0);
    assert_eq!(snapshot.source_selection_updates_room_policy_thumbnail(), 0);
    assert_eq!(snapshot.budget_solver_outcomes_degraded(), 1);
    assert_eq!(snapshot.budget_solver_outcomes_paused(), 1);
    assert_eq!(snapshot.budget_solver_outcomes_resumed(), 1);
    assert_eq!(snapshot.budget_solver_outcomes_protected_over_budget(), 1);
}

fn assert_control_plane_latency_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.http_inflight().noop, 1);
    assert_eq!(snapshot.http_inflight().metrics, 0);
    assert_eq!(snapshot.http_request_duration().noop.le_50_millis, 1);
    assert_eq!(snapshot.http_request_duration().noop.le_10_millis, 0);
    assert_eq!(snapshot.http_request_duration().noop.count, 1);
    assert_eq!(snapshot.http_request_duration().noop.sum_micros, 25_000);
    assert_eq!(snapshot.ws_handshake_duration().le_100_millis, 1);
    assert_eq!(snapshot.ws_handshake_duration().count, 1);
    assert_eq!(snapshot.ws_handshake_duration().sum_micros, 80_000);
    assert_eq!(snapshot.ws_auth_duration().le_10_millis, 1);
    assert_eq!(snapshot.ws_auth_duration().count, 1);
    assert_eq!(snapshot.ws_auth_duration().sum_micros, 8_000);
    assert_eq!(snapshot.ws_user_initialize_duration().le_250_millis, 1);
    assert_eq!(snapshot.ws_user_initialize_duration().le_100_millis, 0);
    assert_eq!(snapshot.ws_user_initialize_duration().count, 1);
    assert_eq!(snapshot.ws_user_initialize_duration().sum_micros, 120_000);
}

#[test]
fn metrics_snapshot_tracks_http_and_websocket_counters() {
    let metrics = RuntimeMetrics::default();
    metrics.add_http_inflight_requests(HttpRoute::Noop, 1);
    metrics.record_http_room_request();
    metrics.record_http_room_unauthorized();
    metrics.record_http_disconnect_request();
    metrics.record_http_disconnect_unprocessable_entity();
    metrics.record_http_metrics_request();
    metrics.record_http_request_duration(HttpRoute::Noop, Duration::from_millis(25));
    metrics.record_ws_connection_accepted();
    metrics.record_ws_handshake_credentials_received();
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::AuthTimeout));
    metrics.record_ws_user_joined();
    metrics.record_ws_user_loop_started();
    metrics.record_ws_user_loop_exit(WsSessionLoopExitReason::UserClosed);
    metrics.record_ws_bus_batch_received(3);
    metrics.record_ws_bus_invalid_input_failure();
    metrics.record_ws_bus_unsupported_feature_failure();
    metrics.record_ws_bus_client_request();
    metrics.record_ws_bus_client_message();
    metrics.record_ws_bus_batch_sent(2);
    metrics.record_ws_bus_send_failure();
    metrics.record_ws_handshake_duration(Duration::from_millis(80));
    metrics.record_ws_auth_duration(Duration::from_millis(8));
    metrics.record_ws_user_initialize_duration(Duration::from_millis(120));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.http_room_requests(), 1);
    assert_eq!(snapshot.http_room_unauthorized(), 1);
    assert_eq!(snapshot.http_disconnect_requests(), 1);
    assert_eq!(snapshot.http_disconnect_unprocessable_entity(), 1);
    assert_eq!(snapshot.http_metrics_requests(), 1);
    assert_eq!(snapshot.ws_connections_accepted(), 1);
    assert_eq!(snapshot.ws_handshake_credentials_received(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_timeout(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_protocol_error(), 0);
    assert_eq!(snapshot.ws_users_joined(), 1);
    assert_eq!(snapshot.ws_user_loops_started(), 1);
    assert_eq!(snapshot.ws_user_loop_exits_user_closed(), 1);
    assert_eq!(snapshot.ws_user_loop_exits_ping_timeout(), 0);
    assert_eq!(snapshot.ws_user_loop_exits_transport_disconnected(), 0);
    assert_eq!(snapshot.ws_bus_parse_failures(), 2);
    assert_eq!(snapshot.ws_bus_invalid_input_failures(), 1);
    assert_eq!(snapshot.ws_bus_unsupported_feature_failures(), 1);
    assert_eq!(snapshot.ws_bus_batches_received(), 1);
    assert_eq!(snapshot.ws_bus_envelopes_received(), 3);
    assert_eq!(snapshot.ws_bus_client_requests(), 1);
    assert_eq!(snapshot.ws_bus_client_messages(), 1);
    assert_eq!(snapshot.ws_bus_batches_sent(), 1);
    assert_eq!(snapshot.ws_bus_envelopes_sent(), 2);
    assert_eq!(snapshot.ws_bus_send_failures(), 1);
    assert_control_plane_latency_metrics(&snapshot);
}

#[test]
fn metrics_snapshot_tracks_live_gauges_and_rtp_counters() {
    let metrics = RuntimeMetrics::default();
    metrics.add_active_rooms(1);
    metrics.add_active_users(2);
    metrics.add_active_publications(3);
    metrics.add_active_subscriptions(4);
    metrics.add_active_recording_rooms(1);
    metrics.add_active_transport_users(1);
    metrics.record_transport_health_transition(None, Some(TransportHealthState::Connected));
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
    metrics.record_transport_user_lifetime(Duration::from_millis(1500));
    metrics.record_transport_cleanup_retry_scheduled();
    metrics.record_transport_cleanup_retry_succeeded();
    metrics.record_transport_cleanup_failure(super::TransportCleanupFailureKind::RetryExhausted);
    metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
    metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    metrics.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
    metrics.record_rtc_datagram_drop(RtcDatagramDropReason::SourceRateLimited);
    metrics.record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);
    metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
    metrics.record_rtc_datagram_fallback_scan(3);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
    metrics.record_source_selection_update(SourceSelectionKind::Encoding);
    metrics.record_budget_solver_outcome(BudgetSolverOutcome::Degraded);
    metrics.record_budget_solver_outcome(BudgetSolverOutcome::Paused);
    metrics.record_budget_solver_outcome(BudgetSolverOutcome::Resumed);
    metrics.record_budget_solver_outcome(BudgetSolverOutcome::ProtectedOverBudget);

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

    metrics.record_transport_health_transition(None, Some(TransportHealthState::Connected));
    metrics.record_transport_health_transition(
        Some(TransportHealthState::Connected),
        Some(TransportHealthState::Disconnected),
    );
    metrics.record_transport_health_transition(Some(TransportHealthState::Disconnected), None);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.connected_transport_users(), 0);
    assert_eq!(snapshot.disconnected_transport_users(), 0);
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_connected(),
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_disconnected(),
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_unset(),
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_disconnected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_connected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_unset(),
        0
    );
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
    metrics.record_transport_user_lifetime(Duration::from_secs(301));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.transport_ice_state_changes_new(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_checking(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_connected(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_completed(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_disconnected(), 1);
    assert_eq!(snapshot.transport_dtls_connected(), 1);
    assert_eq!(snapshot.transport_user_lifetime_le_1_second(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_10_seconds(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_60_seconds(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_300_seconds(), 0);
    assert_eq!(snapshot.transport_user_lifetime_count(), 1);
    assert_eq!(snapshot.transport_user_lifetime_sum_micros(), 301_000_000);
}

#[test]
fn handshake_rejection_buckets_are_distinct() {
    let metrics = RuntimeMetrics::default();
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::AuthFailed));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ProtocolError));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::RoomFull));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::Error));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.ws_handshake_rejected_authentication_failed(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_protocol_error(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_room_full(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_error(), 1);
}
