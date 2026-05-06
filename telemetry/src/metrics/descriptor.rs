use o_sfu_model::WebSocketCloseCode;

use super::{
    catalog::RuntimeMetrics,
    counter::{Histogram, HistogramFamily},
    labels::{
        BudgetSolverOutcome, ControlPlaneDurationBucket, HttpDisconnectResponseStatus,
        HttpRoomResponseStatus, HttpRoute, RecordingActionOutcome, RtcDatagramDropReason,
        RtcDatagramRoutePath, RtcRouteControlOutcome, RtpFlowDirection, RtpForwardDestinationKind,
        RtpRelayDropKind, SourceSelectionKind, TransportCleanupFailureKind,
        TransportHealthTransition, TransportIceState, TransportUserLifetimeBucket,
        WsBusClientFrameKind, WsBusDirection, WsBusFailureKind, WsConnectionStage,
        WsSessionLoopExitReason, WsStartupFailureKind,
    },
    snapshot::{
        MetricFamilySnapshot, MetricHistogramBucketSnapshot, MetricHistogramSnapshot, MetricKind,
        MetricLabel, MetricSample, RuntimeMetricsSnapshot,
    },
};

macro_rules! metric_catalog {
    ($($id:ident {
        name: $name:literal,
        help: $help:literal,
        kind: $kind:ident,
        samples: |$metrics:ident| $samples:expr
    }),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum MetricName {
            $($id),+
        }

        const DESCRIPTORS: &[MetricDescriptor] = &[
            $(
                MetricDescriptor {
                    id: MetricName::$id,
                    name: $name,
                    help: $help,
                    kind: MetricKind::$kind,
                },
            )+
        ];

        pub(super) fn build_snapshot(metrics: &RuntimeMetrics) -> RuntimeMetricsSnapshot {
            RuntimeMetricsSnapshot::new(
                DESCRIPTORS
                    .iter()
                    .map(|descriptor| {
                        let samples = match descriptor.id {
                            $(
                                MetricName::$id => {
                                    let $metrics = metrics;
                                    $samples
                                }
                            ),+
                        };
                        descriptor.snapshot(samples)
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        }
    };
}

#[derive(Clone, Copy)]
struct MetricDescriptor {
    id: MetricName,
    name: &'static str,
    help: &'static str,
    kind: MetricKind,
}

impl MetricDescriptor {
    fn snapshot(self, samples: Vec<MetricSample>) -> MetricFamilySnapshot {
        MetricFamilySnapshot::new(
            self.id,
            self.name,
            self.help,
            self.kind,
            samples.into_boxed_slice(),
        )
    }
}

metric_catalog! {
    HttpNoopRequestsTotal {
        name: "osfu_http_noop_requests_total",
        help: "Total HTTP requests served by /v1/noop.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.http_requests.load(HttpRoute::Noop))]
    },
    HttpStatsRequestsTotal {
        name: "osfu_http_stats_requests_total",
        help: "Total HTTP requests served by /v1/stats.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.http_requests.load(HttpRoute::Stats))]
    },
    HttpRoomRequestsTotal {
        name: "osfu_http_room_requests_total",
        help: "Total HTTP requests received by /v1/room.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.http_requests.load(HttpRoute::Room))]
    },
    HttpRoomResponsesTotal {
        name: "osfu_http_room_responses_total",
        help: "Total HTTP /v1/room responses by status.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("status", "success")], metrics.http_room_responses.load(HttpRoomResponseStatus::Success)),
            counter([("status", "unauthorized")], metrics.http_room_responses.load(HttpRoomResponseStatus::Unauthorized)),
            counter([("status", "forbidden")], metrics.http_room_responses.load(HttpRoomResponseStatus::Forbidden)),
            counter([("status", "bad_request")], metrics.http_room_responses.load(HttpRoomResponseStatus::BadRequest)),
        ]
    },
    HttpDisconnectRequestsTotal {
        name: "osfu_http_disconnect_requests_total",
        help: "Total HTTP requests received by /v1/disconnect.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.http_requests.load(HttpRoute::Disconnect))]
    },
    HttpDisconnectResponsesTotal {
        name: "osfu_http_disconnect_responses_total",
        help: "Total HTTP /v1/disconnect responses by status.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("status", "success")], metrics.http_disconnect_responses.load(HttpDisconnectResponseStatus::Success)),
            counter([("status", "bad_request")], metrics.http_disconnect_responses.load(HttpDisconnectResponseStatus::BadRequest)),
            counter(
                [("status", "unprocessable_entity")],
                metrics.http_disconnect_responses.load(HttpDisconnectResponseStatus::UnprocessableEntity),
            ),
        ]
    },
    HttpMetricsRequestsTotal {
        name: "osfu_http_metrics_requests_total",
        help: "Total HTTP requests served by /metrics.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.http_requests.load(HttpRoute::Metrics))]
    },
    HttpInflightRequests {
        name: "osfu_http_inflight_requests",
        help: "Current in-flight HTTP requests by route.",
        kind: Gauge,
        samples: |metrics| vec![
            gauge([("route", "noop")], metrics.http_inflight_requests.load(HttpRoute::Noop)),
            gauge([("route", "stats")], metrics.http_inflight_requests.load(HttpRoute::Stats)),
            gauge([("route", "room")], metrics.http_inflight_requests.load(HttpRoute::Room)),
            gauge(
                [("route", "disconnect")],
                metrics.http_inflight_requests.load(HttpRoute::Disconnect),
            ),
            gauge([("route", "metrics")], metrics.http_inflight_requests.load(HttpRoute::Metrics)),
        ]
    },
    HttpRequestDurationSeconds {
        name: "osfu_http_request_duration_seconds",
        help: "HTTP request duration by route.",
        kind: Histogram,
        samples: |metrics| vec![
            histogram(
                [("route", "noop")],
                control_plane_histogram_for_route(&metrics.http_request_duration, HttpRoute::Noop),
            ),
            histogram(
                [("route", "stats")],
                control_plane_histogram_for_route(&metrics.http_request_duration, HttpRoute::Stats),
            ),
            histogram(
                [("route", "room")],
                control_plane_histogram_for_route(&metrics.http_request_duration, HttpRoute::Room),
            ),
            histogram(
                [("route", "disconnect")],
                control_plane_histogram_for_route(&metrics.http_request_duration, HttpRoute::Disconnect),
            ),
            histogram(
                [("route", "metrics")],
                control_plane_histogram_for_route(&metrics.http_request_duration, HttpRoute::Metrics),
            ),
        ]
    },
    WsConnectionsTotal {
        name: "osfu_ws_connections_total",
        help: "Total websocket connections observed at each handshake stage.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("stage", "accepted")], metrics.ws_connections.load(WsConnectionStage::Accepted)),
            counter(
                [("stage", "credentials_received")],
                metrics.ws_connections.load(WsConnectionStage::CredentialsReceived),
            ),
            counter([("stage", "joined")], metrics.ws_connections.load(WsConnectionStage::Joined)),
        ]
    },
    WsHandshakeRejectionsTotal {
        name: "osfu_ws_handshake_rejections_total",
        help: "Total websocket handshake rejections by close code bucket.",
        kind: Counter,
        samples: |metrics| vec![
            counter(
                [("close_code", close_code_label(WebSocketCloseCode::AuthTimeout))],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::AuthTimeout),
            ),
            counter(
                [("close_code", close_code_label(WebSocketCloseCode::AuthFailed))],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::AuthFailed),
            ),
            counter(
                [("close_code", close_code_label(WebSocketCloseCode::ProtocolError))],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::ProtocolError),
            ),
            counter(
                [("close_code", close_code_label(WebSocketCloseCode::RoomFull))],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::RoomFull),
            ),
            counter([("close_code", "error")], metrics.ws_handshake_rejections_other.load()),
        ]
    },
    WsStartupFailuresTotal {
        name: "osfu_ws_startup_failures_total",
        help: "Total websocket startup failures before the steady-state user loop.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("kind", "startup_send")], metrics.ws_startup_failures.load(WsStartupFailureKind::StartupSend)),
            counter(
                [("kind", "user_initialize")],
                metrics.ws_startup_failures.load(WsStartupFailureKind::SessionInitialize),
            ),
        ]
    },
    WsHandshakeDurationSeconds {
        name: "osfu_ws_handshake_duration_seconds",
        help: "Websocket handshake duration from upgrade to user readiness or rejection.",
        kind: Histogram,
        samples: |metrics| vec![unlabeled_histogram(control_plane_histogram(&metrics.ws_handshake_duration))]
    },
    WsAuthDurationSeconds {
        name: "osfu_ws_auth_duration_seconds",
        help: "Websocket authentication duration from first auth wait through token validation.",
        kind: Histogram,
        samples: |metrics| vec![unlabeled_histogram(control_plane_histogram(&metrics.ws_auth_duration))]
    },
    WsUserInitializeDurationSeconds {
        name: "osfu_ws_user_initialize_duration_seconds",
        help: "Websocket user initialization duration after room admission.",
        kind: Histogram,
        samples: |metrics| vec![unlabeled_histogram(control_plane_histogram(&metrics.ws_user_initialize_duration))]
    },
    WsUserLoopsStartedTotal {
        name: "osfu_ws_user_loops_started_total",
        help: "Total websocket user loops started after a successful join.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.ws_user_loops_started.load())]
    },
    WsUserLoopExitsTotal {
        name: "osfu_ws_user_loop_exits_total",
        help: "Total websocket user loop exits by reason.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("reason", "user_closed")], metrics.ws_user_loop_exits.load(WsSessionLoopExitReason::UserClosed)),
            counter([("reason", "reader_error")], metrics.ws_user_loop_exits.load(WsSessionLoopExitReason::ReaderError)),
            counter([("reason", "bus_break")], metrics.ws_user_loop_exits.load(WsSessionLoopExitReason::BusBreak)),
            counter([("reason", "ping_timeout")], metrics.ws_user_loop_exits.load(WsSessionLoopExitReason::PingTimeout)),
            counter(
                [("reason", "transport_disconnected")],
                metrics.ws_user_loop_exits.load(WsSessionLoopExitReason::TransportDisconnected),
            ),
            counter(
                [("reason", "outbound_room_closed")],
                metrics.ws_user_loop_exits.load(WsSessionLoopExitReason::OutboundChannelClosed),
            ),
            counter(
                [("reason", "outbound_close_signal")],
                metrics.ws_user_loop_exits.load(WsSessionLoopExitReason::OutboundCloseSignal),
            ),
            counter(
                [("reason", "outbound_message_send_failure")],
                metrics.ws_user_loop_exits.load(WsSessionLoopExitReason::OutboundMessageSendFailure),
            ),
        ]
    },
    WsBusBatchesTotal {
        name: "osfu_ws_bus_batches_total",
        help: "Total websocket signaling batches processed by direction.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("direction", "received")], metrics.ws_bus_batches.load(WsBusDirection::Received)),
            counter([("direction", "sent")], metrics.ws_bus_batches.load(WsBusDirection::Sent)),
        ]
    },
    WsBusEnvelopesTotal {
        name: "osfu_ws_bus_envelopes_total",
        help: "Total websocket signaling envelopes processed by direction.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("direction", "received")], metrics.ws_bus_envelopes.load(WsBusDirection::Received)),
            counter([("direction", "sent")], metrics.ws_bus_envelopes.load(WsBusDirection::Sent)),
        ]
    },
    WsBusParseFailuresTotal {
        name: "osfu_ws_bus_parse_failures_total",
        help: "Total websocket signaling parse failures.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.ws_bus_parse_failures.load())]
    },
    WsBusFailuresTotal {
        name: "osfu_ws_bus_failures_total",
        help: "Total websocket signaling failures by kind.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("kind", "invalid_input")], metrics.ws_bus_failures.load(WsBusFailureKind::InvalidInput)),
            counter(
                [("kind", "unsupported_feature")],
                metrics.ws_bus_failures.load(WsBusFailureKind::UnsupportedFeature),
            ),
            counter([("kind", "send")], metrics.ws_bus_failures.load(WsBusFailureKind::Send)),
        ]
    },
    WsBusClientFramesTotal {
        name: "osfu_ws_bus_client_frames_total",
        help: "Total client websocket signaling frames by kind.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("kind", "request")], metrics.ws_bus_client_frames.load(WsBusClientFrameKind::Request)),
            counter([("kind", "message")], metrics.ws_bus_client_frames.load(WsBusClientFrameKind::Message)),
        ]
    },
    RoomsActive {
        name: "osfu_rooms_active",
        help: "Current number of live rooms owned by this runtime.",
        kind: Gauge,
        samples: |metrics| vec![unlabeled_gauge(metrics.active_rooms.load())]
    },
    UsersActive {
        name: "osfu_users_active",
        help: "Current number of live room users owned by this runtime.",
        kind: Gauge,
        samples: |metrics| vec![unlabeled_gauge(metrics.active_users.load())]
    },
    PublicationsActive {
        name: "osfu_publications_active",
        help: "Current number of committed or pending published media entries owned by this runtime.",
        kind: Gauge,
        samples: |metrics| vec![unlabeled_gauge(metrics.active_publications.load())]
    },
    SubscriptionsActive {
        name: "osfu_subscriptions_active",
        help: "Current number of committed or pending consumer subscriptions owned by this runtime.",
        kind: Gauge,
        samples: |metrics| vec![unlabeled_gauge(metrics.active_subscriptions.load())]
    },
    TransportUsersActive {
        name: "osfu_transport_users_active",
        help: "Current number of live RTC transport users on this runtime.",
        kind: Gauge,
        samples: |metrics| vec![unlabeled_gauge(metrics.active_transport_users.load())]
    },
    RecordingActionsTotal {
        name: "osfu_recording_actions_total",
        help: "Total recording control actions by action and outcome.",
        kind: Counter,
        samples: |metrics| vec![
            counter(
                [("action", "start"), ("outcome", "accepted")],
                metrics.recording_actions.load(RecordingActionOutcome::StartAccepted),
            ),
            counter(
                [("action", "start"), ("outcome", "rejected")],
                metrics.recording_actions.load(RecordingActionOutcome::StartRejected),
            ),
            counter(
                [("action", "stop"), ("outcome", "accepted")],
                metrics.recording_actions.load(RecordingActionOutcome::StopAccepted),
            ),
            counter(
                [("action", "stop"), ("outcome", "rejected")],
                metrics.recording_actions.load(RecordingActionOutcome::StopRejected),
            ),
        ]
    },
    RecordingRoomsActive {
        name: "osfu_recording_rooms_active",
        help: "Current number of rooms with an active recording user.",
        kind: Gauge,
        samples: |metrics| vec![unlabeled_gauge(metrics.active_recording_rooms.load())]
    },
    RecordingCapturedPacketsTotal {
        name: "osfu_recording_captured_packets_total",
        help: "Total packets accepted by the recording capture path.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.recording_captured_packets.load())]
    },
    RecordingCapturedStreamsTotal {
        name: "osfu_recording_captured_streams_total",
        help: "Total unique media streams first seen by the recording capture path.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.recording_captured_streams.load())]
    },
    TransportHealthUsers {
        name: "osfu_transport_health_users",
        help: "Current number of transport users by observed health state.",
        kind: Gauge,
        samples: |metrics| vec![
            gauge([("state", "connected")], metrics.connected_transport_users.load()),
            gauge([("state", "disconnected")], metrics.disconnected_transport_users.load()),
        ]
    },
    TransportHealthTransitionsTotal {
        name: "osfu_transport_health_transitions_total",
        help: "Total transport health-state transitions observed from the transport adapter.",
        kind: Counter,
        samples: |metrics| vec![
            counter(
                [("from", "unset"), ("to", "connected")],
                metrics.transport_health_transitions.load(TransportHealthTransition::UnsetToConnected),
            ),
            counter(
                [("from", "unset"), ("to", "disconnected")],
                metrics.transport_health_transitions.load(TransportHealthTransition::UnsetToDisconnected),
            ),
            counter(
                [("from", "connected"), ("to", "disconnected")],
                metrics.transport_health_transitions.load(TransportHealthTransition::ConnectedToDisconnected),
            ),
            counter(
                [("from", "disconnected"), ("to", "connected")],
                metrics.transport_health_transitions.load(TransportHealthTransition::DisconnectedToConnected),
            ),
            counter(
                [("from", "connected"), ("to", "unset")],
                metrics.transport_health_transitions.load(TransportHealthTransition::ConnectedToUnset),
            ),
            counter(
                [("from", "disconnected"), ("to", "unset")],
                metrics.transport_health_transitions.load(TransportHealthTransition::DisconnectedToUnset),
            ),
        ]
    },
    RtpPacketsTotal {
        name: "osfu_rtp_packets_total",
        help: "Total RTP packets processed by flow direction.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("direction", "ingress")], metrics.rtp_packets.load(RtpFlowDirection::Ingress)),
            counter([("direction", "egress")], metrics.rtp_packets.load(RtpFlowDirection::Egress)),
        ]
    },
    RtpPayloadBytesTotal {
        name: "osfu_rtp_payload_bytes_total",
        help: "Total RTP payload bytes processed by flow direction.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("direction", "ingress")], metrics.rtp_payload_bytes.load(RtpFlowDirection::Ingress)),
            counter([("direction", "egress")], metrics.rtp_payload_bytes.load(RtpFlowDirection::Egress)),
        ]
    },
    RtpForwardedPacketsTotal {
        name: "osfu_rtp_forwarded_packets_total",
        help: "Total RTP packet fan-out operations by forwarding destination.",
        kind: Counter,
        samples: |metrics| forwarding_samples(&metrics.rtp_forwarded_packets)
    },
    RtpForwardedPayloadBytesTotal {
        name: "osfu_rtp_forwarded_payload_bytes_total",
        help: "Total RTP payload bytes fanned out by forwarding destination.",
        kind: Counter,
        samples: |metrics| forwarding_samples(&metrics.rtp_forwarded_payload_bytes)
    },
    RtpRelayOverloadDropsTotal {
        name: "osfu_rtp_relay_overload_drops_total",
        help: "Total RTP relay packets dropped because the bounded relay mailbox was full.",
        kind: Counter,
        samples: |metrics| vec![
            counter(
                [("destination", "intra_node_relay")],
                metrics.rtp_relay_overload_drops.load(RtpRelayDropKind::IntraNodeRelay),
            ),
            counter(
                [("destination", "inter_node_relay")],
                metrics.rtp_relay_overload_drops.load(RtpRelayDropKind::InterNodeRelay),
            ),
        ]
    },
    TransportIceStateChangesTotal {
        name: "osfu_transport_ice_state_changes_total",
        help: "Total RTC ICE state-change events observed from the transport adapter.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("state", "new")], metrics.transport_ice_state_changes.load(TransportIceState::New)),
            counter([("state", "checking")], metrics.transport_ice_state_changes.load(TransportIceState::Checking)),
            counter([("state", "connected")], metrics.transport_ice_state_changes.load(TransportIceState::Connected)),
            counter([("state", "completed")], metrics.transport_ice_state_changes.load(TransportIceState::Completed)),
            counter(
                [("state", "disconnected")],
                metrics.transport_ice_state_changes.load(TransportIceState::Disconnected),
            ),
        ]
    },
    TransportDtlsConnectedTotal {
        name: "osfu_transport_dtls_connected_total",
        help: "Total RTC DTLS-connected events observed from the transport adapter.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.transport_dtls_connected.load())]
    },
    TransportUserLifetimeSeconds {
        name: "osfu_transport_user_lifetime_seconds",
        help: "Lifetime of closed RTC transport users observed at cold-path teardown.",
        kind: Histogram,
        samples: |metrics| vec![unlabeled_histogram(transport_lifetime_histogram(metrics))]
    },
    TransportCleanupRetriesTotal {
        name: "osfu_transport_cleanup_retries_total",
        help: "Total room-owned transport cleanup retry attempts scheduled after cleanup failures.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.transport_cleanup_retries.load())]
    },
    TransportCleanupRetrySuccessesTotal {
        name: "osfu_transport_cleanup_retry_successes_total",
        help: "Total room-owned transport cleanup retry attempts that eventually succeeded.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.transport_cleanup_retry_successes.load())]
    },
    TransportCleanupFailuresTotal {
        name: "osfu_transport_cleanup_failures_total",
        help: "Total room-owned transport cleanup failures by final handling kind.",
        kind: Counter,
        samples: |metrics| vec![
            counter(
                [("kind", "terminal")],
                metrics.transport_cleanup_failures.load(TransportCleanupFailureKind::Terminal),
            ),
            counter(
                [("kind", "retry_exhausted")],
                metrics.transport_cleanup_failures.load(TransportCleanupFailureKind::RetryExhausted),
            ),
            counter(
                [("kind", "queue_full")],
                metrics.transport_cleanup_failures.load(TransportCleanupFailureKind::QueueFull),
            ),
            counter(
                [("kind", "shutdown")],
                metrics.transport_cleanup_failures.load(TransportCleanupFailureKind::Shutdown),
            ),
        ]
    },
    RtcDatagramRoutesTotal {
        name: "osfu_rtc_datagram_routes_total",
        help: "Total RTC UDP datagrams accepted by routing path.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("path", "indexed")], metrics.rtc_datagram_routes.load(RtcDatagramRoutePath::Indexed)),
            counter([("path", "scan")], metrics.rtc_datagram_routes.load(RtcDatagramRoutePath::Scan)),
        ]
    },
    RtcDatagramDropsTotal {
        name: "osfu_rtc_datagram_drops_total",
        help: "Total RTC UDP datagrams dropped before reaching a live user.",
        kind: Counter,
        samples: |metrics| vec![
            counter(
                [("reason", "recent_miss_cache")],
                metrics.rtc_datagram_drops.load(RtcDatagramDropReason::RecentMissCache),
            ),
            counter(
                [("reason", "source_rate_limited")],
                metrics.rtc_datagram_drops.load(RtcDatagramDropReason::SourceRateLimited),
            ),
            counter([("reason", "no_user")], metrics.rtc_datagram_drops.load(RtcDatagramDropReason::NoUser)),
            counter([("reason", "malformed")], metrics.rtc_datagram_drops.load(RtcDatagramDropReason::Malformed)),
        ]
    },
    RtcDatagramFallbackScansTotal {
        name: "osfu_rtc_datagram_fallback_scans_total",
        help: "Total fallback scans across RTC users for UDP datagram routing.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.rtc_datagram_fallback_scans.load())]
    },
    RtcDatagramScanUsersTotal {
        name: "osfu_rtc_datagram_scan_users_total",
        help: "Total RTC users examined by UDP fallback scans.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.rtc_datagram_scan_users.load())]
    },
    RtcRouteControlTotal {
        name: "osfu_rtc_route_control_total",
        help: "Total RTC route-control decisions observed at the transport boundary.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("outcome", "absorbed")], metrics.rtc_route_control.load(RtcRouteControlOutcome::Absorbed)),
            counter([("outcome", "forwarded")], metrics.rtc_route_control.load(RtcRouteControlOutcome::Forwarded)),
            counter(
                [("outcome", "route_gated_relay_drop")],
                metrics.rtc_route_control.load(RtcRouteControlOutcome::RouteGatedRelayDrop),
            ),
            counter(
                [("outcome", "layer_allowed")],
                metrics.rtc_route_control.load(RtcRouteControlOutcome::LayerAllowed),
            ),
            counter(
                [("outcome", "layer_dropped")],
                metrics.rtc_route_control.load(RtcRouteControlOutcome::LayerDropped),
            ),
        ]
    },
    SourceSelectionUpdatesTotal {
        name: "osfu_source_selection_updates_total",
        help: "Total room-owned source selector updates accepted by source policy.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("selector", "open")], metrics.source_selection_updates.load(SourceSelectionKind::Open)),
            counter([("selector", "encoding")], metrics.source_selection_updates.load(SourceSelectionKind::Encoding)),
            counter(
                [("selector", "operating_point")],
                metrics.source_selection_updates.load(SourceSelectionKind::OperatingPoint),
            ),
            counter(
                [("selector", "room_policy_featured")],
                metrics.source_selection_updates.load(SourceSelectionKind::RoomPolicyFeatured),
            ),
            counter(
                [("selector", "room_policy_thumbnail")],
                metrics.source_selection_updates.load(SourceSelectionKind::RoomPolicyThumbnail),
            ),
        ]
    },
    BudgetSolverOutcomesTotal {
        name: "osfu_budget_solver_outcomes_total",
        help: "Total receiver video budget solver outcomes accepted by room policy.",
        kind: Counter,
        samples: |metrics| vec![
            counter([("outcome", "degraded")], metrics.budget_solver_outcomes.load(BudgetSolverOutcome::Degraded)),
            counter([("outcome", "paused")], metrics.budget_solver_outcomes.load(BudgetSolverOutcome::Paused)),
            counter([("outcome", "resumed")], metrics.budget_solver_outcomes.load(BudgetSolverOutcome::Resumed)),
            counter(
                [("outcome", "protected_over_budget")],
                metrics.budget_solver_outcomes.load(BudgetSolverOutcome::ProtectedOverBudget),
            ),
        ]
    },
}

fn forwarding_samples(
    family: &super::counter::CounterFamily<RtpForwardDestinationKind>,
) -> Vec<MetricSample> {
    let value = |destination| family.load(destination);
    vec![
        counter(
            [("destination", "local_rtc")],
            value(RtpForwardDestinationKind::LocalRtc),
        ),
        counter(
            [("destination", "recording")],
            value(RtpForwardDestinationKind::Recording),
        ),
        counter(
            [("destination", "intra_node_relay")],
            value(RtpForwardDestinationKind::IntraNodeRelay),
        ),
        counter(
            [("destination", "inter_node_relay")],
            value(RtpForwardDestinationKind::InterNodeRelay),
        ),
    ]
}

fn unlabeled_counter(value: u64) -> MetricSample {
    MetricSample::counter(Box::default(), value)
}

fn counter<const N: usize>(labels: [(&'static str, &'static str); N], value: u64) -> MetricSample {
    MetricSample::counter(label_set(labels), value)
}

fn unlabeled_gauge(value: i64) -> MetricSample {
    MetricSample::gauge(Box::default(), value)
}

fn gauge<const N: usize>(labels: [(&'static str, &'static str); N], value: i64) -> MetricSample {
    MetricSample::gauge(label_set(labels), value)
}

fn unlabeled_histogram(value: MetricHistogramSnapshot) -> MetricSample {
    MetricSample::histogram(Box::default(), value)
}

fn histogram<const N: usize>(
    labels: [(&'static str, &'static str); N],
    value: MetricHistogramSnapshot,
) -> MetricSample {
    MetricSample::histogram(label_set(labels), value)
}

fn label_set<const N: usize>(labels: [(&'static str, &'static str); N]) -> Box<[MetricLabel]> {
    labels
        .map(|(name, value)| MetricLabel::new(name, value))
        .into()
}

fn control_plane_histogram(
    histogram: &Histogram<ControlPlaneDurationBucket>,
) -> MetricHistogramSnapshot {
    MetricHistogramSnapshot::new(
        control_plane_buckets(|bucket| histogram.load_bucket(bucket)),
        histogram.load_count(),
        histogram.load_sum_micros(),
    )
}

fn control_plane_histogram_for_route(
    histogram: &HistogramFamily<HttpRoute, ControlPlaneDurationBucket>,
    route: HttpRoute,
) -> MetricHistogramSnapshot {
    MetricHistogramSnapshot::new(
        control_plane_buckets(|bucket| histogram.load_bucket(route, bucket)),
        histogram.load_count(route),
        histogram.load_sum_micros(route),
    )
}

fn control_plane_buckets(
    load: impl Fn(ControlPlaneDurationBucket) -> u64,
) -> Box<[MetricHistogramBucketSnapshot]> {
    [
        MetricHistogramBucketSnapshot::new("0.01", load(ControlPlaneDurationBucket::Le10Millis)),
        MetricHistogramBucketSnapshot::new("0.05", load(ControlPlaneDurationBucket::Le50Millis)),
        MetricHistogramBucketSnapshot::new("0.1", load(ControlPlaneDurationBucket::Le100Millis)),
        MetricHistogramBucketSnapshot::new("0.25", load(ControlPlaneDurationBucket::Le250Millis)),
        MetricHistogramBucketSnapshot::new("0.5", load(ControlPlaneDurationBucket::Le500Millis)),
        MetricHistogramBucketSnapshot::new("1", load(ControlPlaneDurationBucket::Le1Second)),
        MetricHistogramBucketSnapshot::new("5", load(ControlPlaneDurationBucket::Le5Seconds)),
    ]
    .into()
}

fn transport_lifetime_histogram(metrics: &RuntimeMetrics) -> MetricHistogramSnapshot {
    MetricHistogramSnapshot::new(
        [
            MetricHistogramBucketSnapshot::new(
                "1",
                metrics
                    .transport_user_lifetime_buckets
                    .load(TransportUserLifetimeBucket::Le1Second),
            ),
            MetricHistogramBucketSnapshot::new(
                "10",
                metrics
                    .transport_user_lifetime_buckets
                    .load(TransportUserLifetimeBucket::Le10Seconds),
            ),
            MetricHistogramBucketSnapshot::new(
                "60",
                metrics
                    .transport_user_lifetime_buckets
                    .load(TransportUserLifetimeBucket::Le60Seconds),
            ),
            MetricHistogramBucketSnapshot::new(
                "300",
                metrics
                    .transport_user_lifetime_buckets
                    .load(TransportUserLifetimeBucket::Le300Seconds),
            ),
        ]
        .into(),
        metrics.transport_user_lifetime_count.load(),
        metrics.transport_user_lifetime_sum_micros.load(),
    )
}

const fn close_code_label(close_code: WebSocketCloseCode) -> &'static str {
    match close_code {
        WebSocketCloseCode::AuthTimeout => "auth_timeout",
        WebSocketCloseCode::AuthFailed => "auth_failed",
        WebSocketCloseCode::ProtocolError => "protocol_error",
        WebSocketCloseCode::RoomFull => "room_full",
        WebSocketCloseCode::Error => "error",
        WebSocketCloseCode::Clean => "clean",
        WebSocketCloseCode::Leaving => "leaving",
        WebSocketCloseCode::Kicked => "kicked",
    }
}
