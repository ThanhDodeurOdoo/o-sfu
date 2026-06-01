use super::{
    BudgetSolverOutcome, HttpRoute, MetricName, RtcDatagramDropReason, RtcDatagramRoutePath,
    RtcRelayEnqueueResult, RtcRemoteControlDropKind, RtcRemotePacketGateConvergence,
    RtcRouteControlOutcome, RtpDecoderRefreshScope, RtpForwardDestinationKind, RtpRelayDropKind,
    RuntimeMetricsSnapshot, SourceSelectionKind, TransportCleanupFailureKind, TransportHealthState,
    TransportIceState, counter::ExportedMetricLabel, labels::ExportedMetricLabelPair,
};

macro_rules! snapshot_counter_accessors {
    ($($method:ident => $metric:ident $labels:expr),+ $(,)?) => {
        $(fn $method(&self) -> u64 {
            self.counter_value(MetricName::$metric, $labels)
        })+
    };
}

macro_rules! snapshot_gauge_accessors {
    ($($method:ident => $metric:ident $labels:expr),+ $(,)?) => {
        $(fn $method(&self) -> i64 {
            self.gauge_value(MetricName::$metric, $labels)
        })+
    };
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DurationHistogramSnapshot {
    pub le_10_millis: u64,
    pub le_50_millis: u64,
    pub le_100_millis: u64,
    pub le_250_millis: u64,
    pub count: u64,
    pub sum_micros: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HttpInflightSnapshot {
    pub noop: i64,
    pub metrics: i64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HttpRequestDurationSnapshot {
    pub noop: DurationHistogramSnapshot,
    pub metrics: DurationHistogramSnapshot,
}

pub trait RuntimeMetricsSnapshotLookup {
    fn counter_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64;
    fn gauge_value(&self, name: MetricName, labels: &[(&str, &str)]) -> i64;
    fn duration_snapshot(
        &self,
        name: MetricName,
        labels: &[(&str, &str)],
    ) -> DurationHistogramSnapshot;
    fn histogram_bucket_value(
        &self,
        name: MetricName,
        labels: &[(&str, &str)],
        upper_bound: &str,
    ) -> u64;
    fn histogram_count_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64;
    fn histogram_sum_micros_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64;
}

impl RuntimeMetricsSnapshotLookup for RuntimeMetricsSnapshot {
    fn counter_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64 {
        let value = self.counter(name, labels);
        assert!(
            value.is_some(),
            "missing counter sample {name:?} with labels {labels:?}"
        );
        value.unwrap_or(0)
    }

    fn gauge_value(&self, name: MetricName, labels: &[(&str, &str)]) -> i64 {
        self.gauge(name, labels).unwrap_or(0)
    }

    fn duration_snapshot(
        &self,
        name: MetricName,
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
            count: histogram.count,
            sum_micros: histogram.sum_micros,
        }
    }

    fn histogram_bucket_value(
        &self,
        name: MetricName,
        labels: &[(&str, &str)],
        upper_bound: &str,
    ) -> u64 {
        self.histogram(name, labels)
            .map_or(0, |histogram| histogram.bucket(upper_bound))
    }

    fn histogram_count_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64 {
        self.histogram(name, labels)
            .map_or(0, |histogram| histogram.count)
    }

    fn histogram_sum_micros_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64 {
        self.histogram(name, labels)
            .map_or(0, |histogram| histogram.sum_micros)
    }
}

pub trait RuntimeMetricsSnapshotTestExt: RuntimeMetricsSnapshotLookup {
    fn http_inflight(&self) -> HttpInflightSnapshot {
        HttpInflightSnapshot {
            noop: self.gauge_value(
                MetricName::HttpInflightRequests,
                &[("route", metric_label(HttpRoute::Noop))],
            ),
            metrics: self.gauge_value(
                MetricName::HttpInflightRequests,
                &[("route", metric_label(HttpRoute::Metrics))],
            ),
        }
    }

    fn http_request_duration(&self) -> HttpRequestDurationSnapshot {
        HttpRequestDurationSnapshot {
            noop: self.duration_snapshot(
                MetricName::HttpRequestDurationSeconds,
                &[("route", metric_label(HttpRoute::Noop))],
            ),
            metrics: self.duration_snapshot(
                MetricName::HttpRequestDurationSeconds,
                &[("route", metric_label(HttpRoute::Metrics))],
            ),
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

    snapshot_counter_accessors! {
        http_noop_requests => HttpNoopRequestsTotal &[],
        http_stats_requests => HttpStatsRequestsTotal &[],
        http_room_requests => HttpRoomRequestsTotal &[],
        http_room_success => HttpRoomResponsesTotal &[("status", "success")],
        http_room_unauthorized => HttpRoomResponsesTotal &[("status", "unauthorized")],
        http_room_forbidden => HttpRoomResponsesTotal &[("status", "forbidden")],
        http_room_bad_request => HttpRoomResponsesTotal &[("status", "bad_request")],
        http_disconnect_requests => HttpDisconnectRequestsTotal &[],
        http_disconnect_success => HttpDisconnectResponsesTotal &[("status", "success")],
        http_disconnect_bad_request => HttpDisconnectResponsesTotal &[("status", "bad_request")],
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
        ws_outbound_queue_overflows => WsOutboundQueueOverflowsTotal &[],
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
        transport_health_transitions_unset_to_connected => TransportHealthTransitionsTotal &[("from", "unset"), ("to", "connected")],
        transport_health_transitions_unset_to_disconnected => TransportHealthTransitionsTotal &[("from", "unset"), ("to", "disconnected")],
        transport_health_transitions_connected_to_disconnected => TransportHealthTransitionsTotal &[("from", "connected"), ("to", "disconnected")],
        transport_health_transitions_disconnected_to_connected => TransportHealthTransitionsTotal &[("from", "disconnected"), ("to", "connected")],
        transport_health_transitions_connected_to_unset => TransportHealthTransitionsTotal &[("from", "connected"), ("to", "unset")],
        transport_health_transitions_disconnected_to_unset => TransportHealthTransitionsTotal &[("from", "disconnected"), ("to", "unset")],
        transport_dtls_connected => TransportDtlsConnectedTotal &[],
        transport_cleanup_retries => TransportCleanupRetriesTotal &[],
        transport_cleanup_retry_successes => TransportCleanupRetrySuccessesTotal &[],
        rtc_datagram_fallback_scans => RtcDatagramFallbackScansTotal &[],
        rtc_datagram_scan_users => RtcDatagramScanUsersTotal &[],
        rtc_relay_mailbox_depth_samples => RtcRelayMailboxDepthSamplesTotal &[],
        rtc_relay_mailbox_depth_observed => RtcRelayMailboxDepthObservedTotal &[],
        rtc_relay_drain_batches => RtcRelayDrainBatchesTotal &[],
        rtc_relay_drained_packets => RtcRelayDrainedPacketsTotal &[],
        rtc_relay_drain_cap_hits => RtcRelayDrainCapHitsTotal &[],
    }

    snapshot_gauge_accessors! {
        active_rooms => RoomsActive &[],
        active_users => UsersActive &[],
        active_publications => PublicationsActive &[],
        active_subscriptions => SubscriptionsActive &[],
        active_recording_rooms => RecordingRoomsActive &[],
        active_transport_users => TransportUsersActive &[],
        ws_outbound_queued_messages => WsOutboundQueuedMessages &[],
    }

    fn transport_health_users(&self, state: TransportHealthState) -> i64 {
        self.gauge_value(
            MetricName::TransportHealthUsers,
            &[("state", metric_label(state))],
        )
    }

    fn connected_transport_users(&self) -> i64 {
        self.transport_health_users(TransportHealthState::Connected)
    }

    fn disconnected_transport_users(&self) -> i64 {
        self.transport_health_users(TransportHealthState::Disconnected)
    }

    fn transport_ice_state_changes(&self, state: TransportIceState) -> u64 {
        self.counter_value(
            MetricName::TransportIceStateChangesTotal,
            &[("state", metric_label(state))],
        )
    }

    fn transport_ice_state_changes_new(&self) -> u64 {
        self.transport_ice_state_changes(TransportIceState::New)
    }

    fn transport_ice_state_changes_checking(&self) -> u64 {
        self.transport_ice_state_changes(TransportIceState::Checking)
    }

    fn transport_ice_state_changes_connected(&self) -> u64 {
        self.transport_ice_state_changes(TransportIceState::Connected)
    }

    fn transport_ice_state_changes_completed(&self) -> u64 {
        self.transport_ice_state_changes(TransportIceState::Completed)
    }

    fn transport_ice_state_changes_disconnected(&self) -> u64 {
        self.transport_ice_state_changes(TransportIceState::Disconnected)
    }

    fn transport_cleanup_failures(&self, kind: TransportCleanupFailureKind) -> u64 {
        self.counter_value(
            MetricName::TransportCleanupFailuresTotal,
            &[("kind", metric_label(kind))],
        )
    }

    fn transport_cleanup_failures_terminal(&self) -> u64 {
        self.transport_cleanup_failures(TransportCleanupFailureKind::Terminal)
    }

    fn transport_cleanup_failures_retry_exhausted(&self) -> u64 {
        self.transport_cleanup_failures(TransportCleanupFailureKind::RetryExhausted)
    }

    fn transport_cleanup_failures_queue_full(&self) -> u64 {
        self.transport_cleanup_failures(TransportCleanupFailureKind::QueueFull)
    }

    fn transport_cleanup_failures_shutdown(&self) -> u64 {
        self.transport_cleanup_failures(TransportCleanupFailureKind::Shutdown)
    }

    fn rtp_forwarded_packets(&self, destination: RtpForwardDestinationKind) -> u64 {
        self.counter_value(
            MetricName::RtpForwardedPacketsTotal,
            &[("destination", metric_label(destination))],
        )
    }

    fn rtp_forwarded_packets_local_rtc(&self) -> u64 {
        self.rtp_forwarded_packets(RtpForwardDestinationKind::LocalRtc)
    }

    fn rtp_forwarded_packets_recording(&self) -> u64 {
        self.rtp_forwarded_packets(RtpForwardDestinationKind::Recording)
    }

    fn rtp_forwarded_packets_intra_node_relay(&self) -> u64 {
        self.rtp_forwarded_packets(RtpForwardDestinationKind::IntraNodeRelay)
    }

    fn rtp_forwarded_payload_bytes(&self, destination: RtpForwardDestinationKind) -> u64 {
        self.counter_value(
            MetricName::RtpForwardedPayloadBytesTotal,
            &[("destination", metric_label(destination))],
        )
    }

    fn rtp_forwarded_payload_bytes_local_rtc(&self) -> u64 {
        self.rtp_forwarded_payload_bytes(RtpForwardDestinationKind::LocalRtc)
    }

    fn rtp_forwarded_payload_bytes_recording(&self) -> u64 {
        self.rtp_forwarded_payload_bytes(RtpForwardDestinationKind::Recording)
    }

    fn rtp_forwarded_payload_bytes_intra_node_relay(&self) -> u64 {
        self.rtp_forwarded_payload_bytes(RtpForwardDestinationKind::IntraNodeRelay)
    }

    fn rtp_relay_overload_drops(&self, destination: RtpRelayDropKind) -> u64 {
        self.counter_value(
            MetricName::RtpRelayOverloadDropsTotal,
            &[("destination", metric_label(destination))],
        )
    }

    fn rtp_relay_overload_drops_intra_node_relay(&self) -> u64 {
        self.rtp_relay_overload_drops(RtpRelayDropKind::IntraNodeRelay)
    }

    fn rtp_decoder_refreshes(&self, scope: RtpDecoderRefreshScope) -> u64 {
        self.counter_value(
            MetricName::RtpDecoderRefreshesTotal,
            &[("scope", metric_label(scope))],
        )
    }

    fn rtp_decoder_refreshes_rid(&self) -> u64 {
        self.rtp_decoder_refreshes(RtpDecoderRefreshScope::Rid)
    }

    fn rtp_decoder_refreshes_source(&self) -> u64 {
        self.rtp_decoder_refreshes(RtpDecoderRefreshScope::Source)
    }

    fn rtc_datagram_routes(&self, path: RtcDatagramRoutePath) -> u64 {
        self.counter_value(
            MetricName::RtcDatagramRoutesTotal,
            &[("path", metric_label(path))],
        )
    }

    fn rtc_datagram_routes_indexed(&self) -> u64 {
        self.rtc_datagram_routes(RtcDatagramRoutePath::Indexed)
    }

    fn rtc_datagram_routes_scan(&self) -> u64 {
        self.rtc_datagram_routes(RtcDatagramRoutePath::Scan)
    }

    fn rtc_datagram_drops(&self, reason: RtcDatagramDropReason) -> u64 {
        self.counter_value(
            MetricName::RtcDatagramDropsTotal,
            &[("reason", metric_label(reason))],
        )
    }

    fn rtc_datagram_drops_recent_miss_cache(&self) -> u64 {
        self.rtc_datagram_drops(RtcDatagramDropReason::RecentMissCache)
    }

    fn rtc_datagram_drops_source_rate_limited(&self) -> u64 {
        self.rtc_datagram_drops(RtcDatagramDropReason::SourceRateLimited)
    }

    fn rtc_datagram_drops_no_user(&self) -> u64 {
        self.rtc_datagram_drops(RtcDatagramDropReason::NoUser)
    }

    fn rtc_datagram_drops_malformed(&self) -> u64 {
        self.rtc_datagram_drops(RtcDatagramDropReason::Malformed)
    }

    fn rtc_route_control(&self, outcome: RtcRouteControlOutcome) -> u64 {
        self.counter_value(
            MetricName::RtcRouteControlTotal,
            &[("outcome", metric_label(outcome))],
        )
    }

    fn rtc_route_control_absorbed(&self) -> u64 {
        self.rtc_route_control(RtcRouteControlOutcome::Absorbed)
    }

    fn rtc_route_control_forwarded(&self) -> u64 {
        self.rtc_route_control(RtcRouteControlOutcome::Forwarded)
    }

    fn rtc_route_control_route_gated_relay_drops(&self) -> u64 {
        self.rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop)
    }

    fn rtc_route_control_layer_allowed(&self) -> u64 {
        self.rtc_route_control(RtcRouteControlOutcome::LayerAllowed)
    }

    fn rtc_route_control_layer_dropped(&self) -> u64 {
        self.rtc_route_control(RtcRouteControlOutcome::LayerDropped)
    }

    fn rtc_relay_enqueue(&self, result: RtcRelayEnqueueResult) -> u64 {
        self.counter_value(MetricName::RtcRelayEnqueuesTotal, &result.label_pair())
    }

    fn rtc_relay_enqueue_intra_node_enqueued(&self) -> u64 {
        self.rtc_relay_enqueue(RtcRelayEnqueueResult::IntraNodeEnqueued)
    }

    fn rtc_relay_enqueue_intra_node_overloaded(&self) -> u64 {
        self.rtc_relay_enqueue(RtcRelayEnqueueResult::IntraNodeOverloaded)
    }

    fn rtc_relay_enqueue_intra_node_closed(&self) -> u64 {
        self.rtc_relay_enqueue(RtcRelayEnqueueResult::IntraNodeClosed)
    }

    fn rtc_remote_control_drops(&self, kind: RtcRemoteControlDropKind) -> u64 {
        self.counter_value(
            MetricName::RtcRemoteControlDropsTotal,
            &[("kind", metric_label(kind))],
        )
    }

    fn rtc_remote_control_keyframe_drops(&self) -> u64 {
        self.rtc_remote_control_drops(RtcRemoteControlDropKind::Keyframe)
    }

    fn rtc_remote_control_packet_gate_drops(&self) -> u64 {
        self.rtc_remote_control_drops(RtcRemoteControlDropKind::PacketGate)
    }

    fn rtc_remote_packet_gate_convergence(&self, outcome: RtcRemotePacketGateConvergence) -> u64 {
        self.counter_value(
            MetricName::RtcRemotePacketGateConvergenceTotal,
            &[("outcome", metric_label(outcome))],
        )
    }

    fn rtc_remote_packet_gate_retries(&self) -> u64 {
        self.rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Retry)
    }

    fn rtc_remote_packet_gate_flushes(&self) -> u64 {
        self.rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Flushed)
    }

    fn source_selection_updates(&self, selector: SourceSelectionKind) -> u64 {
        self.counter_value(
            MetricName::SourceSelectionUpdatesTotal,
            &[("selector", metric_label(selector))],
        )
    }

    fn source_selection_updates_open(&self) -> u64 {
        self.source_selection_updates(SourceSelectionKind::Open)
    }

    fn source_selection_updates_encoding(&self) -> u64 {
        self.source_selection_updates(SourceSelectionKind::Encoding)
    }

    fn source_selection_updates_operating_point(&self) -> u64 {
        self.source_selection_updates(SourceSelectionKind::OperatingPoint)
    }

    fn source_selection_updates_room_policy_featured(&self) -> u64 {
        self.source_selection_updates(SourceSelectionKind::RoomPolicyFeatured)
    }

    fn source_selection_updates_room_policy_thumbnail(&self) -> u64 {
        self.source_selection_updates(SourceSelectionKind::RoomPolicyThumbnail)
    }

    fn budget_solver_outcomes(&self, outcome: BudgetSolverOutcome) -> u64 {
        self.counter_value(
            MetricName::BudgetSolverOutcomesTotal,
            &[("outcome", metric_label(outcome))],
        )
    }

    fn budget_solver_outcomes_degraded(&self) -> u64 {
        self.budget_solver_outcomes(BudgetSolverOutcome::Degraded)
    }

    fn budget_solver_outcomes_paused(&self) -> u64 {
        self.budget_solver_outcomes(BudgetSolverOutcome::Paused)
    }

    fn budget_solver_outcomes_resumed(&self) -> u64 {
        self.budget_solver_outcomes(BudgetSolverOutcome::Resumed)
    }

    fn budget_solver_outcomes_protected_over_budget(&self) -> u64 {
        self.budget_solver_outcomes(BudgetSolverOutcome::ProtectedOverBudget)
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

fn metric_label<L: ExportedMetricLabel>(label: L) -> &'static str {
    label.label_value()
}
