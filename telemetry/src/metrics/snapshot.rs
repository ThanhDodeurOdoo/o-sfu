use super::{catalog::RuntimeMetrics, descriptor::build_snapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricLabel {
    pub name: &'static str,
    pub value: &'static str,
}

impl MetricLabel {
    #[must_use]
    pub const fn new(name: &'static str, value: &'static str) -> Self {
        Self { name, value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricHistogramBucketSnapshot {
    pub upper_bound: &'static str,
    pub value: u64,
}

impl MetricHistogramBucketSnapshot {
    #[must_use]
    pub const fn new(upper_bound: &'static str, value: u64) -> Self {
        Self { upper_bound, value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricHistogramSnapshot {
    pub buckets: Box<[MetricHistogramBucketSnapshot]>,
    pub count: u64,
    pub sum_micros: u64,
}

impl MetricHistogramSnapshot {
    #[must_use]
    pub fn new(buckets: Box<[MetricHistogramBucketSnapshot]>, count: u64, sum_micros: u64) -> Self {
        Self {
            buckets,
            count,
            sum_micros,
        }
    }

    #[must_use]
    pub fn bucket(&self, upper_bound: &str) -> u64 {
        self.buckets
            .iter()
            .find(|bucket| bucket.upper_bound == upper_bound)
            .map_or(0, |bucket| bucket.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
    Histogram(MetricHistogramSnapshot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricSample {
    pub labels: Box<[MetricLabel]>,
    pub value: MetricValue,
}

impl MetricSample {
    #[must_use]
    pub fn counter(labels: Box<[MetricLabel]>, value: u64) -> Self {
        Self {
            labels,
            value: MetricValue::Counter(value),
        }
    }

    #[must_use]
    pub fn gauge(labels: Box<[MetricLabel]>, value: i64) -> Self {
        Self {
            labels,
            value: MetricValue::Gauge(value),
        }
    }

    #[must_use]
    pub fn histogram(labels: Box<[MetricLabel]>, value: MetricHistogramSnapshot) -> Self {
        Self {
            labels,
            value: MetricValue::Histogram(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricFamilySnapshot {
    pub id: super::descriptor::MetricName,
    pub name: &'static str,
    pub help: &'static str,
    pub kind: MetricKind,
    samples: Box<[MetricSample]>,
}

impl MetricFamilySnapshot {
    pub(super) fn new(
        id: super::descriptor::MetricName,
        name: &'static str,
        help: &'static str,
        kind: MetricKind,
        samples: Box<[MetricSample]>,
    ) -> Self {
        Self {
            id,
            name,
            help,
            kind,
            samples,
        }
    }

    #[must_use]
    pub fn samples(&self) -> &[MetricSample] {
        &self.samples
    }

    fn sample(&self, labels: &[(&str, &str)]) -> Option<&MetricSample> {
        self.samples
            .iter()
            .find(|sample| labels_match(&sample.labels, labels))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    families: Box<[MetricFamilySnapshot]>,
}

impl RuntimeMetricsSnapshot {
    pub(super) fn new(families: Box<[MetricFamilySnapshot]>) -> Self {
        Self { families }
    }

    #[must_use]
    pub fn families(&self) -> &[MetricFamilySnapshot] {
        &self.families
    }

    #[must_use]
    pub fn family(&self, name: super::descriptor::MetricName) -> Option<&MetricFamilySnapshot> {
        self.families.iter().find(|family| family.id == name)
    }

    #[must_use]
    pub fn counter(
        &self,
        name: super::descriptor::MetricName,
        labels: &[(&str, &str)],
    ) -> Option<u64> {
        let MetricValue::Counter(value) = &self.family(name)?.sample(labels)?.value else {
            return None;
        };
        Some(*value)
    }

    #[must_use]
    pub fn gauge(
        &self,
        name: super::descriptor::MetricName,
        labels: &[(&str, &str)],
    ) -> Option<i64> {
        let MetricValue::Gauge(value) = &self.family(name)?.sample(labels)?.value else {
            return None;
        };
        Some(*value)
    }

    #[must_use]
    pub fn histogram(
        &self,
        name: super::descriptor::MetricName,
        labels: &[(&str, &str)],
    ) -> Option<&MetricHistogramSnapshot> {
        let MetricValue::Histogram(value) = &self.family(name)?.sample(labels)?.value else {
            return None;
        };
        Some(value)
    }

    #[must_use]
    pub fn http_inflight(&self) -> HttpInflightSnapshot {
        HttpInflightSnapshot {
            noop: self
                .gauge(
                    super::descriptor::MetricName::HttpInflightRequests,
                    &[("route", "noop")],
                )
                .unwrap_or(0),
            stats: self
                .gauge(
                    super::descriptor::MetricName::HttpInflightRequests,
                    &[("route", "stats")],
                )
                .unwrap_or(0),
            room: self
                .gauge(
                    super::descriptor::MetricName::HttpInflightRequests,
                    &[("route", "room")],
                )
                .unwrap_or(0),
            disconnect: self
                .gauge(
                    super::descriptor::MetricName::HttpInflightRequests,
                    &[("route", "disconnect")],
                )
                .unwrap_or(0),
            metrics: self
                .gauge(
                    super::descriptor::MetricName::HttpInflightRequests,
                    &[("route", "metrics")],
                )
                .unwrap_or(0),
        }
    }

    #[must_use]
    pub fn http_request_duration(&self) -> HttpRequestDurationSnapshot {
        HttpRequestDurationSnapshot {
            noop: self.duration_snapshot(
                super::descriptor::MetricName::HttpRequestDurationSeconds,
                &[("route", "noop")],
            ),
            stats: self.duration_snapshot(
                super::descriptor::MetricName::HttpRequestDurationSeconds,
                &[("route", "stats")],
            ),
            room: self.duration_snapshot(
                super::descriptor::MetricName::HttpRequestDurationSeconds,
                &[("route", "room")],
            ),
            disconnect: self.duration_snapshot(
                super::descriptor::MetricName::HttpRequestDurationSeconds,
                &[("route", "disconnect")],
            ),
            metrics: self.duration_snapshot(
                super::descriptor::MetricName::HttpRequestDurationSeconds,
                &[("route", "metrics")],
            ),
        }
    }

    #[must_use]
    pub fn ws_handshake_duration(&self) -> DurationHistogramSnapshot {
        self.duration_snapshot(
            super::descriptor::MetricName::WsHandshakeDurationSeconds,
            &[],
        )
    }

    #[must_use]
    pub fn ws_auth_duration(&self) -> DurationHistogramSnapshot {
        self.duration_snapshot(super::descriptor::MetricName::WsAuthDurationSeconds, &[])
    }

    #[must_use]
    pub fn ws_user_initialize_duration(&self) -> DurationHistogramSnapshot {
        self.duration_snapshot(
            super::descriptor::MetricName::WsUserInitializeDurationSeconds,
            &[],
        )
    }

    fn duration_snapshot(
        &self,
        name: super::descriptor::MetricName,
        labels: &[(&str, &str)],
    ) -> DurationHistogramSnapshot {
        let Some(histogram) = self.histogram(name, labels) else {
            return DurationHistogramSnapshot::default();
        };
        DurationHistogramSnapshot {
            le_10_millis: histogram.bucket("0.01"),
            le_50_millis: histogram.bucket("0.05"),
            le_100_millis: histogram.bucket("0.1"),
            le_250_millis: histogram.bucket("0.25"),
            le_500_millis: histogram.bucket("0.5"),
            le_1_second: histogram.bucket("1"),
            le_5_seconds: histogram.bucket("5"),
            count: histogram.count,
            sum_micros: histogram.sum_micros,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DurationHistogramSnapshot {
    pub le_10_millis: u64,
    pub le_50_millis: u64,
    pub le_100_millis: u64,
    pub le_250_millis: u64,
    pub le_500_millis: u64,
    pub le_1_second: u64,
    pub le_5_seconds: u64,
    pub count: u64,
    pub sum_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpInflightSnapshot {
    pub noop: i64,
    pub stats: i64,
    pub room: i64,
    pub disconnect: i64,
    pub metrics: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequestDurationSnapshot {
    pub noop: DurationHistogramSnapshot,
    pub stats: DurationHistogramSnapshot,
    pub room: DurationHistogramSnapshot,
    pub disconnect: DurationHistogramSnapshot,
    pub metrics: DurationHistogramSnapshot,
}

macro_rules! counter_accessors {
    ($($method:ident => $metric:ident $labels:expr),+ $(,)?) => {
        impl RuntimeMetricsSnapshot {
            $(
                pub fn $method(&self) -> u64 {
                    self.counter(super::descriptor::MetricName::$metric, $labels)
                        .unwrap_or(0)
                }
            )+
        }
    };
}

macro_rules! gauge_accessors {
    ($($method:ident => $metric:ident $labels:expr),+ $(,)?) => {
        impl RuntimeMetricsSnapshot {
            $(
                pub fn $method(&self) -> i64 {
                    self.gauge(super::descriptor::MetricName::$metric, $labels)
                        .unwrap_or(0)
                }
            )+
        }
    };
}

counter_accessors! {
    http_noop_requests => HttpNoopRequestsTotal &[],
    http_stats_requests => HttpStatsRequestsTotal &[],
    http_metrics_requests => HttpMetricsRequestsTotal &[],
    http_room_requests => HttpRoomRequestsTotal &[],
    http_room_success => HttpRoomResponsesTotal &[("status", "success")],
    http_room_unauthorized => HttpRoomResponsesTotal &[("status", "unauthorized")],
    http_room_forbidden => HttpRoomResponsesTotal &[("status", "forbidden")],
    http_room_bad_request => HttpRoomResponsesTotal &[("status", "bad_request")],
    http_disconnect_requests => HttpDisconnectRequestsTotal &[],
    http_disconnect_success => HttpDisconnectResponsesTotal &[("status", "success")],
    http_disconnect_bad_request => HttpDisconnectResponsesTotal &[("status", "bad_request")],
    http_disconnect_unprocessable_entity => HttpDisconnectResponsesTotal &[("status", "unprocessable_entity")],
    ws_connections_accepted => WsConnectionsTotal &[("stage", "accepted")],
    ws_handshake_credentials_received => WsConnectionsTotal &[("stage", "credentials_received")],
    ws_users_joined => WsConnectionsTotal &[("stage", "joined")],
    ws_handshake_rejected_timeout => WsHandshakeRejectionsTotal &[("close_code", "auth_timeout")],
    ws_handshake_rejected_authentication_failed => WsHandshakeRejectionsTotal &[("close_code", "auth_failed")],
    ws_handshake_rejected_protocol_error => WsHandshakeRejectionsTotal &[("close_code", "protocol_error")],
    ws_handshake_rejected_room_full => WsHandshakeRejectionsTotal &[("close_code", "room_full")],
    ws_handshake_rejected_error => WsHandshakeRejectionsTotal &[("close_code", "error")],
    ws_startup_send_failures => WsStartupFailuresTotal &[("kind", "startup_send")],
    ws_user_initialize_failures => WsStartupFailuresTotal &[("kind", "user_initialize")],
    ws_user_loops_started => WsUserLoopsStartedTotal &[],
    ws_user_loop_exits_user_closed => WsUserLoopExitsTotal &[("reason", "user_closed")],
    ws_user_loop_exits_reader_error => WsUserLoopExitsTotal &[("reason", "reader_error")],
    ws_user_loop_exits_bus_break => WsUserLoopExitsTotal &[("reason", "bus_break")],
    ws_user_loop_exits_ping_timeout => WsUserLoopExitsTotal &[("reason", "ping_timeout")],
    ws_user_loop_exits_transport_disconnected => WsUserLoopExitsTotal &[("reason", "transport_disconnected")],
    ws_user_loop_exits_outbound_room_closed => WsUserLoopExitsTotal &[("reason", "outbound_room_closed")],
    ws_user_loop_exits_outbound_close_signal => WsUserLoopExitsTotal &[("reason", "outbound_close_signal")],
    ws_user_loop_exits_outbound_message_send_failure => WsUserLoopExitsTotal &[("reason", "outbound_message_send_failure")],
    ws_bus_batches_received => WsBusBatchesTotal &[("direction", "received")],
    ws_bus_batches_sent => WsBusBatchesTotal &[("direction", "sent")],
    ws_bus_envelopes_received => WsBusEnvelopesTotal &[("direction", "received")],
    ws_bus_envelopes_sent => WsBusEnvelopesTotal &[("direction", "sent")],
    ws_bus_parse_failures => WsBusParseFailuresTotal &[],
    ws_bus_invalid_input_failures => WsBusFailuresTotal &[("kind", "invalid_input")],
    ws_bus_unsupported_feature_failures => WsBusFailuresTotal &[("kind", "unsupported_feature")],
    ws_bus_send_failures => WsBusFailuresTotal &[("kind", "send")],
    ws_bus_client_requests => WsBusClientFramesTotal &[("kind", "request")],
    ws_bus_client_messages => WsBusClientFramesTotal &[("kind", "message")],
    recording_start_accepted => RecordingActionsTotal &[("action", "start"), ("outcome", "accepted")],
    recording_start_rejected => RecordingActionsTotal &[("action", "start"), ("outcome", "rejected")],
    recording_stop_accepted => RecordingActionsTotal &[("action", "stop"), ("outcome", "accepted")],
    recording_stop_rejected => RecordingActionsTotal &[("action", "stop"), ("outcome", "rejected")],
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

impl RuntimeMetricsSnapshot {
    #[must_use]
    pub fn transport_user_lifetime_le_1_second(&self) -> u64 {
        self.histogram_bucket(
            super::descriptor::MetricName::TransportUserLifetimeSeconds,
            "1",
        )
    }

    #[must_use]
    pub fn transport_user_lifetime_le_10_seconds(&self) -> u64 {
        self.histogram_bucket(
            super::descriptor::MetricName::TransportUserLifetimeSeconds,
            "10",
        )
    }

    #[must_use]
    pub fn transport_user_lifetime_le_60_seconds(&self) -> u64 {
        self.histogram_bucket(
            super::descriptor::MetricName::TransportUserLifetimeSeconds,
            "60",
        )
    }

    #[must_use]
    pub fn transport_user_lifetime_le_300_seconds(&self) -> u64 {
        self.histogram_bucket(
            super::descriptor::MetricName::TransportUserLifetimeSeconds,
            "300",
        )
    }

    #[must_use]
    pub fn transport_user_lifetime_count(&self) -> u64 {
        self.histogram(
            super::descriptor::MetricName::TransportUserLifetimeSeconds,
            &[],
        )
        .map_or(0, |histogram| histogram.count)
    }

    #[must_use]
    pub fn transport_user_lifetime_sum_micros(&self) -> u64 {
        self.histogram(
            super::descriptor::MetricName::TransportUserLifetimeSeconds,
            &[],
        )
        .map_or(0, |histogram| histogram.sum_micros)
    }

    fn histogram_bucket(&self, name: super::descriptor::MetricName, upper_bound: &str) -> u64 {
        self.histogram(name, &[])
            .map_or(0, |histogram| histogram.bucket(upper_bound))
    }
}

impl RuntimeMetrics {
    #[must_use]
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        build_snapshot(self)
    }
}

fn labels_match(sample_labels: &[MetricLabel], labels: &[(&str, &str)]) -> bool {
    sample_labels.len() == labels.len()
        && labels.iter().all(|(name, value)| {
            sample_labels
                .iter()
                .any(|label| label.name == *name && label.value == *value)
        })
}
