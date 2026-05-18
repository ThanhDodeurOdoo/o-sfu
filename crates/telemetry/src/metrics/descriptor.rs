use o_sfu_model::WebSocketCloseCode;

use super::{
    catalog::RuntimeMetrics,
    counter::{
        CounterFamily, ExportedMetricLabel, Histogram, HistogramBucketLabel, HistogramFamily,
        MetricBucketLabel, MetricLabel as MetricStorageLabel, UpDownCounterFamily,
    },
    labels::{
        ControlPlaneDurationBucket, HttpRoute, RecordingActionOutcome, RtcDatagramDropReason,
        RtcDatagramRoutePath, RtcRelayEnqueueResult, RtcRemoteControlDropKind,
        RtcRemotePacketGateConvergence, RtcRouteControlOutcome, RtpFlowDirection,
        RtpForwardDestinationKind, TransportHealthTransition,
    },
    rtc::RtcMetricsSnapshot,
    rtp::{RtpMetricsSnapshot, RtpWorkerMetricsSnapshot},
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
        samples: |metrics| counter_family_samples(&metrics.http_room_responses, "status")
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
        samples: |metrics| counter_family_samples(&metrics.http_disconnect_responses, "status")
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
        samples: |metrics| up_down_counter_family_samples(&metrics.http_inflight_requests, "route")
    },
    HttpRequestDurationSeconds {
        name: "osfu_http_request_duration_seconds",
        help: "HTTP request duration by route.",
        kind: Histogram,
        samples: |metrics| histogram_family_samples(&metrics.http_request_duration, "route")
    },
    WsConnectionsTotal {
        name: "osfu_ws_connections_total",
        help: "Total websocket connections observed at each handshake stage.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.ws_connections, "stage")
    },
    WsHandshakeRejectionsTotal {
        name: "osfu_ws_handshake_rejections_total",
        help: "Total websocket handshake rejections by close code bucket.",
        kind: Counter,
        samples: |metrics| vec![
            counter(
                [("close_code", WebSocketCloseCode::AuthTimeout.label_value())],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::AuthTimeout),
            ),
            counter(
                [("close_code", WebSocketCloseCode::AuthFailed.label_value())],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::AuthFailed),
            ),
            counter(
                [("close_code", WebSocketCloseCode::ProtocolError.label_value())],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::ProtocolError),
            ),
            counter(
                [("close_code", WebSocketCloseCode::RoomFull.label_value())],
                metrics.ws_handshake_rejections.load(WebSocketCloseCode::RoomFull),
            ),
            counter([("close_code", "error")], metrics.ws_handshake_rejections_other.load()),
        ]
    },
    WsStartupFailuresTotal {
        name: "osfu_ws_startup_failures_total",
        help: "Total websocket startup failures before the steady-state user loop.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.ws_startup_failures, "kind")
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
        samples: |metrics| counter_family_samples(&metrics.ws_user_loop_exits, "reason")
    },
    WsBusBatchesTotal {
        name: "osfu_ws_bus_batches_total",
        help: "Total websocket signaling batches processed by direction.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.ws_bus_batches, "direction")
    },
    WsBusEnvelopesTotal {
        name: "osfu_ws_bus_envelopes_total",
        help: "Total websocket signaling envelopes processed by direction.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.ws_bus_envelopes, "direction")
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
        samples: |metrics| counter_family_samples(&metrics.ws_bus_failures, "kind")
    },
    WsBusClientFramesTotal {
        name: "osfu_ws_bus_client_frames_total",
        help: "Total client websocket signaling frames by kind.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.ws_bus_client_frames, "kind")
    },
    WsOutboundQueuedMessages {
        name: "osfu_ws_outbound_queued_messages",
        help: "Current websocket outbound room messages waiting in per-user queues.",
        kind: Gauge,
        samples: |metrics| vec![unlabeled_gauge(metrics.ws_outbound_queued_messages.load())]
    },
    WsOutboundQueueOverflowsTotal {
        name: "osfu_ws_outbound_queue_overflows_total",
        help: "Total websocket users marked for slow-consumer shutdown after outbound queue overflow.",
        kind: Counter,
        samples: |metrics| vec![unlabeled_counter(metrics.ws_outbound_queue_overflows.load())]
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
        samples: |metrics| up_down_counter_family_samples(&metrics.transport_health_users, "state")
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
        samples: |metrics| rtp_flow_samples(
            &metrics.rtp_metrics.snapshot(),
            "direction",
            RtpMetricsSnapshot::packets
        )
    },
    RtpPayloadBytesTotal {
        name: "osfu_rtp_payload_bytes_total",
        help: "Total RTP payload bytes processed by flow direction.",
        kind: Counter,
        samples: |metrics| rtp_flow_samples(
            &metrics.rtp_metrics.snapshot(),
            "direction",
            RtpMetricsSnapshot::payload_bytes
        )
    },
    RtpForwardedPacketsTotal {
        name: "osfu_rtp_forwarded_packets_total",
        help: "Total RTP packet fan-out operations by forwarding destination.",
        kind: Counter,
        samples: |metrics| rtp_forward_destination_samples(
            &metrics.rtp_metrics.snapshot(),
            "destination",
            RtpMetricsSnapshot::forwarded_packets
        )
    },
    RtpForwardedPayloadBytesTotal {
        name: "osfu_rtp_forwarded_payload_bytes_total",
        help: "Total RTP payload bytes fanned out by forwarding destination.",
        kind: Counter,
        samples: |metrics| rtp_forward_destination_samples(
            &metrics.rtp_metrics.snapshot(),
            "destination",
            RtpMetricsSnapshot::forwarded_payload_bytes
        )
    },
    WorkerRtpPacketsTotal {
        name: "osfu_worker_rtp_packets_total",
        help: "Total RTP packets processed by media worker and flow direction.",
        kind: Counter,
        samples: |metrics| rtp_worker_flow_samples(
            &metrics.rtp_metrics.snapshot(),
            "direction",
            RtpWorkerMetricsSnapshot::packets
        )
    },
    WorkerRtpPayloadBytesTotal {
        name: "osfu_worker_rtp_payload_bytes_total",
        help: "Total RTP payload bytes processed by media worker and flow direction.",
        kind: Counter,
        samples: |metrics| rtp_worker_flow_samples(
            &metrics.rtp_metrics.snapshot(),
            "direction",
            RtpWorkerMetricsSnapshot::payload_bytes
        )
    },
    WorkerRtpForwardedPacketsTotal {
        name: "osfu_worker_rtp_forwarded_packets_total",
        help: "Total RTP packet fan-out operations by media worker and forwarding destination.",
        kind: Counter,
        samples: |metrics| rtp_worker_forward_destination_samples(
            &metrics.rtp_metrics.snapshot(),
            "destination",
            RtpWorkerMetricsSnapshot::forwarded_packets
        )
    },
    WorkerRtpForwardedPayloadBytesTotal {
        name: "osfu_worker_rtp_forwarded_payload_bytes_total",
        help: "Total RTP payload bytes fanned out by media worker and forwarding destination.",
        kind: Counter,
        samples: |metrics| rtp_worker_forward_destination_samples(
            &metrics.rtp_metrics.snapshot(),
            "destination",
            RtpWorkerMetricsSnapshot::forwarded_payload_bytes
        )
    },
    RtpRelayOverloadDropsTotal {
        name: "osfu_rtp_relay_overload_drops_total",
        help: "Total RTP relay packets dropped because the bounded relay mailbox was full.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.rtp_relay_overload_drops, "destination")
    },
    TransportIceStateChangesTotal {
        name: "osfu_transport_ice_state_changes_total",
        help: "Total RTC ICE state-change events observed from the transport adapter.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.transport_ice_state_changes, "state")
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
        samples: |metrics| counter_family_samples(&metrics.transport_cleanup_failures, "kind")
    },
    RtcDatagramRoutesTotal {
        name: "osfu_rtc_datagram_routes_total",
        help: "Total RTC UDP datagrams accepted by routing path.",
        kind: Counter,
        samples: |metrics| rtc_datagram_route_samples(
            &metrics.rtc_metrics.snapshot(),
            "path",
            RtcMetricsSnapshot::datagram_routes
        )
    },
    RtcDatagramDropsTotal {
        name: "osfu_rtc_datagram_drops_total",
        help: "Total RTC UDP datagrams dropped before reaching a live user.",
        kind: Counter,
        samples: |metrics| rtc_datagram_drop_samples(
            &metrics.rtc_metrics.snapshot(),
            "reason",
            RtcMetricsSnapshot::datagram_drops
        )
    },
    RtcDatagramFallbackScansTotal {
        name: "osfu_rtc_datagram_fallback_scans_total",
        help: "Total fallback scans across RTC users for UDP datagram routing.",
        kind: Counter,
        samples: |metrics| vec![
            unlabeled_counter(metrics.rtc_metrics.snapshot().datagram_fallback_scans())
        ]
    },
    RtcDatagramScanUsersTotal {
        name: "osfu_rtc_datagram_scan_users_total",
        help: "Total RTC users examined by UDP fallback scans.",
        kind: Counter,
        samples: |metrics| vec![
            unlabeled_counter(metrics.rtc_metrics.snapshot().datagram_scan_users())
        ]
    },
    RtcRouteControlTotal {
        name: "osfu_rtc_route_control_total",
        help: "Total RTC route-control decisions observed at the transport boundary.",
        kind: Counter,
        samples: |metrics| rtc_route_control_samples(
            &metrics.rtc_metrics.snapshot(),
            "outcome",
            RtcMetricsSnapshot::route_control
        )
    },
    RtcRelayEnqueuesTotal {
        name: "osfu_rtc_relay_enqueues_total",
        help: "Total relay enqueue attempts by target kind and outcome.",
        kind: Counter,
        samples: |metrics| rtc_relay_enqueue_samples(&metrics.rtc_metrics.snapshot())
    },
    RtcRelayMailboxDepthSamplesTotal {
        name: "osfu_rtc_relay_mailbox_depth_samples_total",
        help: "Total sampled intra-node relay mailbox depth observations.",
        kind: Counter,
        samples: |metrics| vec![
            unlabeled_counter(metrics.rtc_metrics.snapshot().relay_mailbox_depth_samples())
        ]
    },
    RtcRelayMailboxDepthObservedTotal {
        name: "osfu_rtc_relay_mailbox_depth_observed_total",
        help: "Sum of sampled intra-node relay mailbox depths.",
        kind: Counter,
        samples: |metrics| vec![
            unlabeled_counter(metrics.rtc_metrics.snapshot().relay_mailbox_depth_total())
        ]
    },
    RtcRelayDrainBatchesTotal {
        name: "osfu_rtc_relay_drain_batches_total",
        help: "Total non-empty packet-loop relay drain batches.",
        kind: Counter,
        samples: |metrics| vec![
            unlabeled_counter(metrics.rtc_metrics.snapshot().relay_drain_batches())
        ]
    },
    RtcRelayDrainedPacketsTotal {
        name: "osfu_rtc_relay_drained_packets_total",
        help: "Total relay packets drained into packet-loop batches.",
        kind: Counter,
        samples: |metrics| vec![
            unlabeled_counter(metrics.rtc_metrics.snapshot().relay_drained_packets())
        ]
    },
    RtcRelayDrainCapHitsTotal {
        name: "osfu_rtc_relay_drain_cap_hits_total",
        help: "Total relay drain batches that left queued relay packets behind after hitting the per-turn cap.",
        kind: Counter,
        samples: |metrics| vec![
            unlabeled_counter(metrics.rtc_metrics.snapshot().relay_drain_cap_hits())
        ]
    },
    RtcRemoteControlDropsTotal {
        name: "osfu_rtc_remote_control_drops_total",
        help: "Total remote-source control commands dropped before enqueue by command kind.",
        kind: Counter,
        samples: |metrics| rtc_remote_control_drop_samples(
            &metrics.rtc_metrics.snapshot(),
            "kind",
            RtcMetricsSnapshot::remote_control_drops
        )
    },
    RtcRemotePacketGateConvergenceTotal {
        name: "osfu_rtc_remote_packet_gate_convergence_total",
        help: "Total remote packet-gate convergence retry attempts and successful pending flushes.",
        kind: Counter,
        samples: |metrics| rtc_remote_packet_gate_convergence_samples(
            &metrics.rtc_metrics.snapshot(),
            "outcome",
            RtcMetricsSnapshot::remote_packet_gate_convergence
        )
    },
    SourceSelectionUpdatesTotal {
        name: "osfu_source_selection_updates_total",
        help: "Total room-owned source selector updates accepted by source policy.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.source_selection_updates, "selector")
    },
    BudgetSolverOutcomesTotal {
        name: "osfu_budget_solver_outcomes_total",
        help: "Total receiver video budget solver outcomes accepted by room policy.",
        kind: Counter,
        samples: |metrics| counter_family_samples(&metrics.budget_solver_outcomes, "outcome")
    },
}

fn counter_family_samples<L>(
    family: &CounterFamily<L>,
    label_name: &'static str,
) -> Vec<MetricSample>
where
    L: ExportedMetricLabel,
{
    L::VARIANTS
        .iter()
        .map(|label| counter([(label_name, label.label_value())], family.load(*label)))
        .collect()
}

fn rtp_flow_samples(
    snapshot: &RtpMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtpMetricsSnapshot, RtpFlowDirection) -> u64,
) -> Vec<MetricSample> {
    <RtpFlowDirection as MetricStorageLabel>::VARIANTS
        .iter()
        .map(|label| counter([(label_name, label.label_value())], read(snapshot, *label)))
        .collect()
}

fn rtp_forward_destination_samples(
    snapshot: &RtpMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtpMetricsSnapshot, RtpForwardDestinationKind) -> u64,
) -> Vec<MetricSample> {
    <RtpForwardDestinationKind as MetricStorageLabel>::VARIANTS
        .iter()
        .map(|label| counter([(label_name, label.label_value())], read(snapshot, *label)))
        .collect()
}

fn rtp_worker_flow_samples(
    snapshot: &RtpMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtpWorkerMetricsSnapshot, RtpFlowDirection) -> u64,
) -> Vec<MetricSample> {
    snapshot
        .worker_snapshots()
        .iter()
        .flat_map(|worker| {
            <RtpFlowDirection as MetricStorageLabel>::VARIANTS
                .iter()
                .map(move |label| {
                    worker_counter(
                        worker.media_worker_id(),
                        label_name,
                        label.label_value(),
                        read(worker, *label),
                    )
                })
        })
        .collect()
}

fn rtp_worker_forward_destination_samples(
    snapshot: &RtpMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtpWorkerMetricsSnapshot, RtpForwardDestinationKind) -> u64,
) -> Vec<MetricSample> {
    snapshot
        .worker_snapshots()
        .iter()
        .flat_map(|worker| {
            <RtpForwardDestinationKind as MetricStorageLabel>::VARIANTS
                .iter()
                .map(move |label| {
                    worker_counter(
                        worker.media_worker_id(),
                        label_name,
                        label.label_value(),
                        read(worker, *label),
                    )
                })
        })
        .collect()
}

fn rtc_datagram_route_samples(
    snapshot: &RtcMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtcMetricsSnapshot, RtcDatagramRoutePath) -> u64,
) -> Vec<MetricSample> {
    <RtcDatagramRoutePath as MetricStorageLabel>::VARIANTS
        .iter()
        .map(|label| counter([(label_name, label.label_value())], read(snapshot, *label)))
        .collect()
}

fn rtc_datagram_drop_samples(
    snapshot: &RtcMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtcMetricsSnapshot, RtcDatagramDropReason) -> u64,
) -> Vec<MetricSample> {
    <RtcDatagramDropReason as MetricStorageLabel>::VARIANTS
        .iter()
        .map(|label| counter([(label_name, label.label_value())], read(snapshot, *label)))
        .collect()
}

fn rtc_route_control_samples(
    snapshot: &RtcMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtcMetricsSnapshot, RtcRouteControlOutcome) -> u64,
) -> Vec<MetricSample> {
    <RtcRouteControlOutcome as MetricStorageLabel>::VARIANTS
        .iter()
        .map(|label| counter([(label_name, label.label_value())], read(snapshot, *label)))
        .collect()
}

fn rtc_relay_enqueue_samples(snapshot: &RtcMetricsSnapshot) -> Vec<MetricSample> {
    <RtcRelayEnqueueResult as MetricStorageLabel>::VARIANTS
        .iter()
        .map(|result| {
            counter(
                [
                    ("target", result.target_label()),
                    ("outcome", result.outcome_label()),
                ],
                snapshot.relay_enqueues(*result),
            )
        })
        .collect()
}

fn rtc_remote_control_drop_samples(
    snapshot: &RtcMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtcMetricsSnapshot, RtcRemoteControlDropKind) -> u64,
) -> Vec<MetricSample> {
    <RtcRemoteControlDropKind as MetricStorageLabel>::VARIANTS
        .iter()
        .map(|label| counter([(label_name, label.label_value())], read(snapshot, *label)))
        .collect()
}

fn rtc_remote_packet_gate_convergence_samples(
    snapshot: &RtcMetricsSnapshot,
    label_name: &'static str,
    read: fn(&RtcMetricsSnapshot, RtcRemotePacketGateConvergence) -> u64,
) -> Vec<MetricSample> {
    <RtcRemotePacketGateConvergence as MetricStorageLabel>::VARIANTS
        .iter()
        .map(|label| counter([(label_name, label.label_value())], read(snapshot, *label)))
        .collect()
}

fn up_down_counter_family_samples<L>(
    family: &UpDownCounterFamily<L>,
    label_name: &'static str,
) -> Vec<MetricSample>
where
    L: ExportedMetricLabel,
{
    L::VARIANTS
        .iter()
        .map(|label| gauge([(label_name, label.label_value())], family.load(*label)))
        .collect()
}

fn histogram_family_samples<L, B>(
    family: &HistogramFamily<L, B>,
    label_name: &'static str,
) -> Vec<MetricSample>
where
    L: ExportedMetricLabel,
    B: HistogramBucketLabel,
{
    L::VARIANTS
        .iter()
        .map(|label| {
            histogram(
                [(label_name, label.label_value())],
                histogram_family_snapshot(family, *label),
            )
        })
        .collect()
}

fn unlabeled_counter(value: u64) -> MetricSample {
    MetricSample::counter(Box::default(), value)
}

fn counter<const N: usize>(labels: [(&'static str, &'static str); N], value: u64) -> MetricSample {
    MetricSample::counter(label_set(labels), value)
}

fn worker_counter(
    media_worker_id: usize,
    label_name: &'static str,
    label_value: &'static str,
    value: u64,
) -> MetricSample {
    MetricSample::counter(
        [
            MetricLabel::new("media_worker_id", media_worker_id.to_string()),
            MetricLabel::new(label_name, label_value),
        ]
        .into(),
        value,
    )
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
    histogram_snapshot(histogram)
}

fn histogram_snapshot<B>(histogram: &Histogram<B>) -> MetricHistogramSnapshot
where
    B: HistogramBucketLabel,
{
    MetricHistogramSnapshot::new(
        bucket_snapshots(|bucket| histogram.load_bucket(bucket)),
        histogram.load_count(),
        histogram.load_sum_micros(),
    )
}

fn histogram_family_snapshot<L, B>(
    histogram: &HistogramFamily<L, B>,
    label: L,
) -> MetricHistogramSnapshot
where
    L: MetricStorageLabel,
    B: HistogramBucketLabel,
{
    MetricHistogramSnapshot::new(
        bucket_snapshots(|bucket| histogram.load_bucket(label, bucket)),
        histogram.load_count(label),
        histogram.load_sum_micros(label),
    )
}

fn bucket_snapshots<B>(load: impl Fn(B) -> u64) -> Box<[MetricHistogramBucketSnapshot]>
where
    B: MetricBucketLabel,
{
    B::VARIANTS
        .iter()
        .map(|bucket| MetricHistogramBucketSnapshot::new(bucket.upper_bound(), load(*bucket)))
        .collect()
}

fn transport_lifetime_histogram(metrics: &RuntimeMetrics) -> MetricHistogramSnapshot {
    MetricHistogramSnapshot::new(
        bucket_snapshots(|bucket| metrics.transport_user_lifetime_buckets.load(bucket)),
        metrics.transport_user_lifetime_count.load(),
        metrics.transport_user_lifetime_sum_micros.load(),
    )
}
