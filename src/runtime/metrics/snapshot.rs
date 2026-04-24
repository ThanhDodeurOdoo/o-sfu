use o_sfu_protocol::signaling::WebSocketCloseCode;

use super::{
    catalog::RuntimeMetrics,
    labels::{
        ControlPlaneDurationBucket, HttpChannelResponseStatus, HttpDisconnectResponseStatus,
        HttpRoute, RecordingActionOutcome, RtcDatagramDropReason, RtcDatagramRoutePath,
        RtcRouteControlOutcome, RtpFlowDirection, RtpForwardDestinationKind, RtpRelayDropKind,
        TransportHealthTransition, TransportIceState, TransportSessionLifetimeBucket,
        WsBusClientFrameKind, WsBusDirection, WsBusFailureKind, WsConnectionStage,
        WsSessionLoopExitReason, WsStartupFailureKind,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurationHistogramSnapshot {
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
pub(crate) struct HttpInflightSnapshot {
    pub noop: i64,
    pub stats: i64,
    pub channel: i64,
    pub disconnect: i64,
    pub metrics: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpRequestDurationSnapshot {
    pub noop: DurationHistogramSnapshot,
    pub stats: DurationHistogramSnapshot,
    pub channel: DurationHistogramSnapshot,
    pub disconnect: DurationHistogramSnapshot,
    pub metrics: DurationHistogramSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Snapshot reads are part of the runtime observability boundary and are consumed incrementally as exporter integration lands."
)]
pub(crate) struct RuntimeMetricsSnapshot {
    pub http_noop_requests: u64,
    pub http_stats_requests: u64,
    pub http_metrics_requests: u64,
    pub http_channel_requests: u64,
    pub http_channel_success: u64,
    pub http_channel_unauthorized: u64,
    pub http_channel_forbidden: u64,
    pub http_channel_bad_request: u64,
    pub http_disconnect_requests: u64,
    pub http_disconnect_success: u64,
    pub http_disconnect_bad_request: u64,
    pub http_disconnect_unprocessable_entity: u64,
    pub http_inflight: HttpInflightSnapshot,
    pub http_request_duration: HttpRequestDurationSnapshot,
    pub ws_connections_accepted: u64,
    pub ws_handshake_credentials_received: u64,
    pub ws_handshake_rejected_timeout: u64,
    pub ws_handshake_rejected_authentication_failed: u64,
    pub ws_handshake_rejected_protocol_error: u64,
    pub ws_handshake_rejected_channel_full: u64,
    pub ws_handshake_rejected_error: u64,
    pub ws_sessions_joined: u64,
    pub ws_startup_send_failures: u64,
    pub ws_session_initialize_failures: u64,
    pub ws_session_loops_started: u64,
    pub ws_session_loop_exits_peer_closed: u64,
    pub ws_session_loop_exits_reader_error: u64,
    pub ws_session_loop_exits_bus_break: u64,
    pub ws_session_loop_exits_ping_timeout: u64,
    pub ws_session_loop_exits_transport_disconnected: u64,
    pub ws_session_loop_exits_outbound_channel_closed: u64,
    pub ws_session_loop_exits_outbound_close_signal: u64,
    pub ws_session_loop_exits_outbound_message_send_failure: u64,
    pub ws_bus_batches_received: u64,
    pub ws_bus_envelopes_received: u64,
    pub ws_bus_parse_failures: u64,
    pub ws_bus_invalid_input_failures: u64,
    pub ws_bus_unsupported_feature_failures: u64,
    pub ws_bus_client_requests: u64,
    pub ws_bus_client_messages: u64,
    pub ws_bus_batches_sent: u64,
    pub ws_bus_envelopes_sent: u64,
    pub ws_bus_send_failures: u64,
    pub ws_handshake_duration: DurationHistogramSnapshot,
    pub ws_auth_duration: DurationHistogramSnapshot,
    pub ws_session_initialize_duration: DurationHistogramSnapshot,
    pub active_channels: i64,
    pub active_sessions: i64,
    pub active_publications: i64,
    pub active_subscriptions: i64,
    pub active_recording_channels: i64,
    pub active_transport_sessions: i64,
    pub connected_transport_sessions: i64,
    pub disconnected_transport_sessions: i64,
    pub recording_start_accepted: u64,
    pub recording_start_rejected: u64,
    pub recording_stop_accepted: u64,
    pub recording_stop_rejected: u64,
    pub recording_captured_packets: u64,
    pub recording_captured_streams: u64,
    pub rtp_packets_ingress: u64,
    pub rtp_packets_egress: u64,
    pub rtp_payload_bytes_ingress: u64,
    pub rtp_payload_bytes_egress: u64,
    pub rtp_forwarded_packets_local_rtc: u64,
    pub rtp_forwarded_packets_recording: u64,
    pub rtp_forwarded_packets_intra_node_relay: u64,
    pub rtp_forwarded_packets_inter_node_relay: u64,
    pub rtp_forwarded_payload_bytes_local_rtc: u64,
    pub rtp_forwarded_payload_bytes_recording: u64,
    pub rtp_forwarded_payload_bytes_intra_node_relay: u64,
    pub rtp_forwarded_payload_bytes_inter_node_relay: u64,
    pub rtp_relay_overload_drops_intra_node_relay: u64,
    pub rtp_relay_overload_drops_inter_node_relay: u64,
    pub transport_health_transitions_unset_to_connected: u64,
    pub transport_health_transitions_unset_to_disconnected: u64,
    pub transport_health_transitions_connected_to_disconnected: u64,
    pub transport_health_transitions_disconnected_to_connected: u64,
    pub transport_health_transitions_connected_to_unset: u64,
    pub transport_health_transitions_disconnected_to_unset: u64,
    pub transport_ice_state_changes_new: u64,
    pub transport_ice_state_changes_checking: u64,
    pub transport_ice_state_changes_connected: u64,
    pub transport_ice_state_changes_completed: u64,
    pub transport_ice_state_changes_disconnected: u64,
    pub transport_dtls_connected: u64,
    pub transport_session_lifetime_le_1_second: u64,
    pub transport_session_lifetime_le_10_seconds: u64,
    pub transport_session_lifetime_le_60_seconds: u64,
    pub transport_session_lifetime_le_300_seconds: u64,
    pub transport_session_lifetime_count: u64,
    pub transport_session_lifetime_sum_micros: u64,
    pub rtc_datagram_routes_indexed: u64,
    pub rtc_datagram_routes_scan: u64,
    pub rtc_datagram_drops_recent_miss_cache: u64,
    pub rtc_datagram_drops_source_rate_limited: u64,
    pub rtc_datagram_drops_no_session: u64,
    pub rtc_datagram_drops_malformed: u64,
    pub rtc_datagram_fallback_scans: u64,
    pub rtc_datagram_scan_sessions: u64,
    pub rtc_route_control_absorbed: u64,
    pub rtc_route_control_forwarded: u64,
    pub rtc_route_control_route_gated_relay_drops: u64,
    pub rtc_route_control_layer_allowed: u64,
    pub rtc_route_control_layer_dropped: u64,
}

struct HttpSnapshot {
    noop_requests: u64,
    stats_requests: u64,
    metrics_requests: u64,
    channel_requests: u64,
    channel_success: u64,
    channel_unauthorized: u64,
    channel_forbidden: u64,
    channel_bad_request: u64,
    disconnect_requests: u64,
    disconnect_success: u64,
    disconnect_bad_request: u64,
    disconnect_unprocessable_entity: u64,
}

struct WebSocketSnapshot {
    connections_accepted: u64,
    handshake_credentials_received: u64,
    handshake_rejected_timeout: u64,
    handshake_rejected_authentication_failed: u64,
    handshake_rejected_protocol_error: u64,
    handshake_rejected_channel_full: u64,
    handshake_rejected_error: u64,
    sessions_joined: u64,
    startup_send_failures: u64,
    session_initialize_failures: u64,
    session_loops_started: u64,
    session_loop_exits_peer_closed: u64,
    session_loop_exits_reader_error: u64,
    session_loop_exits_bus_break: u64,
    session_loop_exits_ping_timeout: u64,
    session_loop_exits_transport_disconnected: u64,
    session_loop_exits_outbound_channel_closed: u64,
    session_loop_exits_outbound_close_signal: u64,
    session_loop_exits_outbound_message_send_failure: u64,
    bus_batches_received: u64,
    bus_envelopes_received: u64,
    bus_parse_failures: u64,
    bus_invalid_input_failures: u64,
    bus_unsupported_feature_failures: u64,
    bus_client_requests: u64,
    bus_client_messages: u64,
    bus_batches_sent: u64,
    bus_envelopes_sent: u64,
    bus_send_failures: u64,
}

struct LiveSnapshot {
    channels: i64,
    sessions: i64,
    publications: i64,
    subscriptions: i64,
    recording_channels: i64,
    transport_sessions: i64,
    connected_transport_sessions: i64,
    disconnected_transport_sessions: i64,
}

struct RecordingSnapshot {
    start_accepted: u64,
    start_rejected: u64,
    stop_accepted: u64,
    stop_rejected: u64,
    captured_packets: u64,
    captured_streams: u64,
}

struct RtpSnapshot {
    packets_ingress: u64,
    packets_egress: u64,
    payload_bytes_ingress: u64,
    payload_bytes_egress: u64,
    forwarded_packets_local_rtc: u64,
    forwarded_packets_recording: u64,
    forwarded_packets_intra_node_relay: u64,
    forwarded_packets_inter_node_relay: u64,
    forwarded_payload_bytes_local_rtc: u64,
    forwarded_payload_bytes_recording: u64,
    forwarded_payload_bytes_intra_node_relay: u64,
    forwarded_payload_bytes_inter_node_relay: u64,
    relay_overload_drops_intra_node_relay: u64,
    relay_overload_drops_inter_node_relay: u64,
}

struct TransportLifecycleSnapshot {
    health_transitions_unset_to_connected: u64,
    health_transitions_unset_to_disconnected: u64,
    health_transitions_connected_to_disconnected: u64,
    health_transitions_disconnected_to_connected: u64,
    health_transitions_connected_to_unset: u64,
    health_transitions_disconnected_to_unset: u64,
    ice_state_changes_new: u64,
    ice_state_changes_checking: u64,
    ice_state_changes_connected: u64,
    ice_state_changes_completed: u64,
    ice_state_changes_disconnected: u64,
    dtls_connected: u64,
    session_lifetime_le_1_second: u64,
    session_lifetime_le_10_seconds: u64,
    session_lifetime_le_60_seconds: u64,
    session_lifetime_le_300_seconds: u64,
    session_lifetime_count: u64,
    session_lifetime_sum_micros: u64,
}

struct RtcDatagramSnapshot {
    routes_indexed: u64,
    routes_scan: u64,
    drops_recent_miss_cache: u64,
    drops_source_rate_limited: u64,
    drops_no_session: u64,
    drops_malformed: u64,
    fallback_scans: u64,
    scan_sessions: u64,
}

struct RtcRouteControlSnapshot {
    absorbed: u64,
    forwarded: u64,
    route_gated_relay_drops: u64,
    layer_allowed: u64,
    layer_dropped: u64,
}

impl RuntimeMetrics {
    #[allow(
        dead_code,
        reason = "Snapshot reads are intentionally available before external exporters are wired."
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "the snapshot builder is a flat counter-to-field table, and keeping the mapping literal makes the exported metrics surface easier to audit"
    )]
    pub(crate) fn snapshot(&self) -> RuntimeMetricsSnapshot {
        let http = self.snapshot_http();
        let http_inflight = self.snapshot_http_inflight();
        let http_request_duration = self.snapshot_http_request_duration();
        let websocket = self.snapshot_websocket();
        let ws_handshake_duration = snapshot_duration_histogram(&self.ws_handshake_duration);
        let ws_auth_duration = snapshot_duration_histogram(&self.ws_auth_duration);
        let ws_session_initialize_duration =
            snapshot_duration_histogram(&self.ws_session_initialize_duration);
        let live = self.snapshot_live();
        let recording = self.snapshot_recording();
        let rtp = self.snapshot_rtp();
        let transport_lifecycle = self.snapshot_transport_lifecycle();
        let rtc_datagram = self.snapshot_rtc_datagram();
        let rtc_route_control = self.snapshot_rtc_route_control();
        RuntimeMetricsSnapshot {
            http_noop_requests: http.noop_requests,
            http_stats_requests: http.stats_requests,
            http_metrics_requests: http.metrics_requests,
            http_channel_requests: http.channel_requests,
            http_channel_success: http.channel_success,
            http_channel_unauthorized: http.channel_unauthorized,
            http_channel_forbidden: http.channel_forbidden,
            http_channel_bad_request: http.channel_bad_request,
            http_disconnect_requests: http.disconnect_requests,
            http_disconnect_success: http.disconnect_success,
            http_disconnect_bad_request: http.disconnect_bad_request,
            http_disconnect_unprocessable_entity: http.disconnect_unprocessable_entity,
            http_inflight,
            http_request_duration,
            ws_connections_accepted: websocket.connections_accepted,
            ws_handshake_credentials_received: websocket.handshake_credentials_received,
            ws_handshake_rejected_timeout: websocket.handshake_rejected_timeout,
            ws_handshake_rejected_authentication_failed: websocket
                .handshake_rejected_authentication_failed,
            ws_handshake_rejected_protocol_error: websocket.handshake_rejected_protocol_error,
            ws_handshake_rejected_channel_full: websocket.handshake_rejected_channel_full,
            ws_handshake_rejected_error: websocket.handshake_rejected_error,
            ws_sessions_joined: websocket.sessions_joined,
            ws_startup_send_failures: websocket.startup_send_failures,
            ws_session_initialize_failures: websocket.session_initialize_failures,
            ws_session_loops_started: websocket.session_loops_started,
            ws_session_loop_exits_peer_closed: websocket.session_loop_exits_peer_closed,
            ws_session_loop_exits_reader_error: websocket.session_loop_exits_reader_error,
            ws_session_loop_exits_bus_break: websocket.session_loop_exits_bus_break,
            ws_session_loop_exits_ping_timeout: websocket.session_loop_exits_ping_timeout,
            ws_session_loop_exits_transport_disconnected: websocket
                .session_loop_exits_transport_disconnected,
            ws_session_loop_exits_outbound_channel_closed: websocket
                .session_loop_exits_outbound_channel_closed,
            ws_session_loop_exits_outbound_close_signal: websocket
                .session_loop_exits_outbound_close_signal,
            ws_session_loop_exits_outbound_message_send_failure: websocket
                .session_loop_exits_outbound_message_send_failure,
            ws_bus_batches_received: websocket.bus_batches_received,
            ws_bus_envelopes_received: websocket.bus_envelopes_received,
            ws_bus_parse_failures: websocket.bus_parse_failures,
            ws_bus_invalid_input_failures: websocket.bus_invalid_input_failures,
            ws_bus_unsupported_feature_failures: websocket.bus_unsupported_feature_failures,
            ws_bus_client_requests: websocket.bus_client_requests,
            ws_bus_client_messages: websocket.bus_client_messages,
            ws_bus_batches_sent: websocket.bus_batches_sent,
            ws_bus_envelopes_sent: websocket.bus_envelopes_sent,
            ws_bus_send_failures: websocket.bus_send_failures,
            ws_handshake_duration,
            ws_auth_duration,
            ws_session_initialize_duration,
            active_channels: live.channels,
            active_sessions: live.sessions,
            active_publications: live.publications,
            active_subscriptions: live.subscriptions,
            active_recording_channels: live.recording_channels,
            active_transport_sessions: live.transport_sessions,
            connected_transport_sessions: live.connected_transport_sessions,
            disconnected_transport_sessions: live.disconnected_transport_sessions,
            recording_start_accepted: recording.start_accepted,
            recording_start_rejected: recording.start_rejected,
            recording_stop_accepted: recording.stop_accepted,
            recording_stop_rejected: recording.stop_rejected,
            recording_captured_packets: recording.captured_packets,
            recording_captured_streams: recording.captured_streams,
            rtp_packets_ingress: rtp.packets_ingress,
            rtp_packets_egress: rtp.packets_egress,
            rtp_payload_bytes_ingress: rtp.payload_bytes_ingress,
            rtp_payload_bytes_egress: rtp.payload_bytes_egress,
            rtp_forwarded_packets_local_rtc: rtp.forwarded_packets_local_rtc,
            rtp_forwarded_packets_recording: rtp.forwarded_packets_recording,
            rtp_forwarded_packets_intra_node_relay: rtp.forwarded_packets_intra_node_relay,
            rtp_forwarded_packets_inter_node_relay: rtp.forwarded_packets_inter_node_relay,
            rtp_forwarded_payload_bytes_local_rtc: rtp.forwarded_payload_bytes_local_rtc,
            rtp_forwarded_payload_bytes_recording: rtp.forwarded_payload_bytes_recording,
            rtp_forwarded_payload_bytes_intra_node_relay: rtp
                .forwarded_payload_bytes_intra_node_relay,
            rtp_forwarded_payload_bytes_inter_node_relay: rtp
                .forwarded_payload_bytes_inter_node_relay,
            rtp_relay_overload_drops_intra_node_relay: rtp.relay_overload_drops_intra_node_relay,
            rtp_relay_overload_drops_inter_node_relay: rtp.relay_overload_drops_inter_node_relay,
            transport_health_transitions_unset_to_connected: transport_lifecycle
                .health_transitions_unset_to_connected,
            transport_health_transitions_unset_to_disconnected: transport_lifecycle
                .health_transitions_unset_to_disconnected,
            transport_health_transitions_connected_to_disconnected: transport_lifecycle
                .health_transitions_connected_to_disconnected,
            transport_health_transitions_disconnected_to_connected: transport_lifecycle
                .health_transitions_disconnected_to_connected,
            transport_health_transitions_connected_to_unset: transport_lifecycle
                .health_transitions_connected_to_unset,
            transport_health_transitions_disconnected_to_unset: transport_lifecycle
                .health_transitions_disconnected_to_unset,
            transport_ice_state_changes_new: transport_lifecycle.ice_state_changes_new,
            transport_ice_state_changes_checking: transport_lifecycle.ice_state_changes_checking,
            transport_ice_state_changes_connected: transport_lifecycle.ice_state_changes_connected,
            transport_ice_state_changes_completed: transport_lifecycle.ice_state_changes_completed,
            transport_ice_state_changes_disconnected: transport_lifecycle
                .ice_state_changes_disconnected,
            transport_dtls_connected: transport_lifecycle.dtls_connected,
            transport_session_lifetime_le_1_second: transport_lifecycle
                .session_lifetime_le_1_second,
            transport_session_lifetime_le_10_seconds: transport_lifecycle
                .session_lifetime_le_10_seconds,
            transport_session_lifetime_le_60_seconds: transport_lifecycle
                .session_lifetime_le_60_seconds,
            transport_session_lifetime_le_300_seconds: transport_lifecycle
                .session_lifetime_le_300_seconds,
            transport_session_lifetime_count: transport_lifecycle.session_lifetime_count,
            transport_session_lifetime_sum_micros: transport_lifecycle.session_lifetime_sum_micros,
            rtc_datagram_routes_indexed: rtc_datagram.routes_indexed,
            rtc_datagram_routes_scan: rtc_datagram.routes_scan,
            rtc_datagram_drops_recent_miss_cache: rtc_datagram.drops_recent_miss_cache,
            rtc_datagram_drops_source_rate_limited: rtc_datagram.drops_source_rate_limited,
            rtc_datagram_drops_no_session: rtc_datagram.drops_no_session,
            rtc_datagram_drops_malformed: rtc_datagram.drops_malformed,
            rtc_datagram_fallback_scans: rtc_datagram.fallback_scans,
            rtc_datagram_scan_sessions: rtc_datagram.scan_sessions,
            rtc_route_control_absorbed: rtc_route_control.absorbed,
            rtc_route_control_forwarded: rtc_route_control.forwarded,
            rtc_route_control_route_gated_relay_drops: rtc_route_control.route_gated_relay_drops,
            rtc_route_control_layer_allowed: rtc_route_control.layer_allowed,
            rtc_route_control_layer_dropped: rtc_route_control.layer_dropped,
        }
    }

    fn snapshot_http(&self) -> HttpSnapshot {
        HttpSnapshot {
            noop_requests: self.http_requests.load(HttpRoute::Noop),
            stats_requests: self.http_requests.load(HttpRoute::Stats),
            metrics_requests: self.http_requests.load(HttpRoute::Metrics),
            channel_requests: self.http_requests.load(HttpRoute::Channel),
            channel_success: self
                .http_channel_responses
                .load(HttpChannelResponseStatus::Success),
            channel_unauthorized: self
                .http_channel_responses
                .load(HttpChannelResponseStatus::Unauthorized),
            channel_forbidden: self
                .http_channel_responses
                .load(HttpChannelResponseStatus::Forbidden),
            channel_bad_request: self
                .http_channel_responses
                .load(HttpChannelResponseStatus::BadRequest),
            disconnect_requests: self.http_requests.load(HttpRoute::Disconnect),
            disconnect_success: self
                .http_disconnect_responses
                .load(HttpDisconnectResponseStatus::Success),
            disconnect_bad_request: self
                .http_disconnect_responses
                .load(HttpDisconnectResponseStatus::BadRequest),
            disconnect_unprocessable_entity: self
                .http_disconnect_responses
                .load(HttpDisconnectResponseStatus::UnprocessableEntity),
        }
    }

    fn snapshot_http_inflight(&self) -> HttpInflightSnapshot {
        HttpInflightSnapshot {
            noop: self.http_inflight_requests.load(HttpRoute::Noop),
            stats: self.http_inflight_requests.load(HttpRoute::Stats),
            channel: self.http_inflight_requests.load(HttpRoute::Channel),
            disconnect: self.http_inflight_requests.load(HttpRoute::Disconnect),
            metrics: self.http_inflight_requests.load(HttpRoute::Metrics),
        }
    }

    fn snapshot_http_request_duration(&self) -> HttpRequestDurationSnapshot {
        HttpRequestDurationSnapshot {
            noop: snapshot_duration_histogram_for_route(
                &self.http_request_duration,
                HttpRoute::Noop,
            ),
            stats: snapshot_duration_histogram_for_route(
                &self.http_request_duration,
                HttpRoute::Stats,
            ),
            channel: snapshot_duration_histogram_for_route(
                &self.http_request_duration,
                HttpRoute::Channel,
            ),
            disconnect: snapshot_duration_histogram_for_route(
                &self.http_request_duration,
                HttpRoute::Disconnect,
            ),
            metrics: snapshot_duration_histogram_for_route(
                &self.http_request_duration,
                HttpRoute::Metrics,
            ),
        }
    }

    fn snapshot_websocket(&self) -> WebSocketSnapshot {
        WebSocketSnapshot {
            connections_accepted: self.ws_connections.load(WsConnectionStage::Accepted),
            handshake_credentials_received: self
                .ws_connections
                .load(WsConnectionStage::CredentialsReceived),
            handshake_rejected_timeout: self
                .ws_handshake_rejections
                .load(WebSocketCloseCode::AuthTimeout),
            handshake_rejected_authentication_failed: self
                .ws_handshake_rejections
                .load(WebSocketCloseCode::AuthFailed),
            handshake_rejected_protocol_error: self
                .ws_handshake_rejections
                .load(WebSocketCloseCode::ProtocolError),
            handshake_rejected_channel_full: self
                .ws_handshake_rejections
                .load(WebSocketCloseCode::ChannelFull),
            handshake_rejected_error: self.ws_handshake_rejections_other.load(),
            sessions_joined: self.ws_connections.load(WsConnectionStage::Joined),
            startup_send_failures: self
                .ws_startup_failures
                .load(WsStartupFailureKind::StartupSend),
            session_initialize_failures: self
                .ws_startup_failures
                .load(WsStartupFailureKind::SessionInitialize),
            session_loops_started: self.ws_session_loops_started.load(),
            session_loop_exits_peer_closed: self
                .ws_session_loop_exits
                .load(WsSessionLoopExitReason::PeerClosed),
            session_loop_exits_reader_error: self
                .ws_session_loop_exits
                .load(WsSessionLoopExitReason::ReaderError),
            session_loop_exits_bus_break: self
                .ws_session_loop_exits
                .load(WsSessionLoopExitReason::BusBreak),
            session_loop_exits_ping_timeout: self
                .ws_session_loop_exits
                .load(WsSessionLoopExitReason::PingTimeout),
            session_loop_exits_transport_disconnected: self
                .ws_session_loop_exits
                .load(WsSessionLoopExitReason::TransportDisconnected),
            session_loop_exits_outbound_channel_closed: self
                .ws_session_loop_exits
                .load(WsSessionLoopExitReason::OutboundChannelClosed),
            session_loop_exits_outbound_close_signal: self
                .ws_session_loop_exits
                .load(WsSessionLoopExitReason::OutboundCloseSignal),
            session_loop_exits_outbound_message_send_failure: self
                .ws_session_loop_exits
                .load(WsSessionLoopExitReason::OutboundMessageSendFailure),
            bus_batches_received: self.ws_bus_batches.load(WsBusDirection::Received),
            bus_envelopes_received: self.ws_bus_envelopes.load(WsBusDirection::Received),
            bus_parse_failures: self.ws_bus_parse_failures.load(),
            bus_invalid_input_failures: self.ws_bus_failures.load(WsBusFailureKind::InvalidInput),
            bus_unsupported_feature_failures: self
                .ws_bus_failures
                .load(WsBusFailureKind::UnsupportedFeature),
            bus_client_requests: self
                .ws_bus_client_frames
                .load(WsBusClientFrameKind::Request),
            bus_client_messages: self
                .ws_bus_client_frames
                .load(WsBusClientFrameKind::Message),
            bus_batches_sent: self.ws_bus_batches.load(WsBusDirection::Sent),
            bus_envelopes_sent: self.ws_bus_envelopes.load(WsBusDirection::Sent),
            bus_send_failures: self.ws_bus_failures.load(WsBusFailureKind::Send),
        }
    }

    fn snapshot_live(&self) -> LiveSnapshot {
        LiveSnapshot {
            channels: self.active_channels.load(),
            sessions: self.active_sessions.load(),
            publications: self.active_publications.load(),
            subscriptions: self.active_subscriptions.load(),
            recording_channels: self.active_recording_channels.load(),
            transport_sessions: self.active_transport_sessions.load(),
            connected_transport_sessions: self.connected_transport_sessions.load(),
            disconnected_transport_sessions: self.disconnected_transport_sessions.load(),
        }
    }

    fn snapshot_recording(&self) -> RecordingSnapshot {
        RecordingSnapshot {
            start_accepted: self
                .recording_actions
                .load(RecordingActionOutcome::StartAccepted),
            start_rejected: self
                .recording_actions
                .load(RecordingActionOutcome::StartRejected),
            stop_accepted: self
                .recording_actions
                .load(RecordingActionOutcome::StopAccepted),
            stop_rejected: self
                .recording_actions
                .load(RecordingActionOutcome::StopRejected),
            captured_packets: self.recording_captured_packets.load(),
            captured_streams: self.recording_captured_streams.load(),
        }
    }

    fn snapshot_rtp(&self) -> RtpSnapshot {
        RtpSnapshot {
            packets_ingress: self.rtp_packets.load(RtpFlowDirection::Ingress),
            packets_egress: self.rtp_packets.load(RtpFlowDirection::Egress),
            payload_bytes_ingress: self.rtp_payload_bytes.load(RtpFlowDirection::Ingress),
            payload_bytes_egress: self.rtp_payload_bytes.load(RtpFlowDirection::Egress),
            forwarded_packets_local_rtc: self
                .rtp_forwarded_packets
                .load(RtpForwardDestinationKind::LocalRtc),
            forwarded_packets_recording: self
                .rtp_forwarded_packets
                .load(RtpForwardDestinationKind::Recording),
            forwarded_packets_intra_node_relay: self
                .rtp_forwarded_packets
                .load(RtpForwardDestinationKind::IntraNodeRelay),
            forwarded_packets_inter_node_relay: self
                .rtp_forwarded_packets
                .load(RtpForwardDestinationKind::InterNodeRelay),
            forwarded_payload_bytes_local_rtc: self
                .rtp_forwarded_payload_bytes
                .load(RtpForwardDestinationKind::LocalRtc),
            forwarded_payload_bytes_recording: self
                .rtp_forwarded_payload_bytes
                .load(RtpForwardDestinationKind::Recording),
            forwarded_payload_bytes_intra_node_relay: self
                .rtp_forwarded_payload_bytes
                .load(RtpForwardDestinationKind::IntraNodeRelay),
            forwarded_payload_bytes_inter_node_relay: self
                .rtp_forwarded_payload_bytes
                .load(RtpForwardDestinationKind::InterNodeRelay),
            relay_overload_drops_intra_node_relay: self
                .rtp_relay_overload_drops
                .load(RtpRelayDropKind::IntraNodeRelay),
            relay_overload_drops_inter_node_relay: self
                .rtp_relay_overload_drops
                .load(RtpRelayDropKind::InterNodeRelay),
        }
    }

    fn snapshot_transport_lifecycle(&self) -> TransportLifecycleSnapshot {
        TransportLifecycleSnapshot {
            health_transitions_unset_to_connected: self
                .transport_health_transitions
                .load(TransportHealthTransition::UnsetToConnected),
            health_transitions_unset_to_disconnected: self
                .transport_health_transitions
                .load(TransportHealthTransition::UnsetToDisconnected),
            health_transitions_connected_to_disconnected: self
                .transport_health_transitions
                .load(TransportHealthTransition::ConnectedToDisconnected),
            health_transitions_disconnected_to_connected: self
                .transport_health_transitions
                .load(TransportHealthTransition::DisconnectedToConnected),
            health_transitions_connected_to_unset: self
                .transport_health_transitions
                .load(TransportHealthTransition::ConnectedToUnset),
            health_transitions_disconnected_to_unset: self
                .transport_health_transitions
                .load(TransportHealthTransition::DisconnectedToUnset),
            ice_state_changes_new: self
                .transport_ice_state_changes
                .load(TransportIceState::New),
            ice_state_changes_checking: self
                .transport_ice_state_changes
                .load(TransportIceState::Checking),
            ice_state_changes_connected: self
                .transport_ice_state_changes
                .load(TransportIceState::Connected),
            ice_state_changes_completed: self
                .transport_ice_state_changes
                .load(TransportIceState::Completed),
            ice_state_changes_disconnected: self
                .transport_ice_state_changes
                .load(TransportIceState::Disconnected),
            dtls_connected: self.transport_dtls_connected.load(),
            session_lifetime_le_1_second: self
                .transport_session_lifetime_buckets
                .load(TransportSessionLifetimeBucket::Le1Second),
            session_lifetime_le_10_seconds: self
                .transport_session_lifetime_buckets
                .load(TransportSessionLifetimeBucket::Le10Seconds),
            session_lifetime_le_60_seconds: self
                .transport_session_lifetime_buckets
                .load(TransportSessionLifetimeBucket::Le60Seconds),
            session_lifetime_le_300_seconds: self
                .transport_session_lifetime_buckets
                .load(TransportSessionLifetimeBucket::Le300Seconds),
            session_lifetime_count: self.transport_session_lifetime_count.load(),
            session_lifetime_sum_micros: self.transport_session_lifetime_sum_micros.load(),
        }
    }

    fn snapshot_rtc_datagram(&self) -> RtcDatagramSnapshot {
        RtcDatagramSnapshot {
            routes_indexed: self.rtc_datagram_routes.load(RtcDatagramRoutePath::Indexed),
            routes_scan: self.rtc_datagram_routes.load(RtcDatagramRoutePath::Scan),
            drops_recent_miss_cache: self
                .rtc_datagram_drops
                .load(RtcDatagramDropReason::RecentMissCache),
            drops_source_rate_limited: self
                .rtc_datagram_drops
                .load(RtcDatagramDropReason::SourceRateLimited),
            drops_no_session: self
                .rtc_datagram_drops
                .load(RtcDatagramDropReason::NoSession),
            drops_malformed: self
                .rtc_datagram_drops
                .load(RtcDatagramDropReason::Malformed),
            fallback_scans: self.rtc_datagram_fallback_scans.load(),
            scan_sessions: self.rtc_datagram_scan_sessions.load(),
        }
    }

    fn snapshot_rtc_route_control(&self) -> RtcRouteControlSnapshot {
        RtcRouteControlSnapshot {
            absorbed: self
                .rtc_route_control
                .load(RtcRouteControlOutcome::Absorbed),
            forwarded: self
                .rtc_route_control
                .load(RtcRouteControlOutcome::Forwarded),
            route_gated_relay_drops: self
                .rtc_route_control
                .load(RtcRouteControlOutcome::RouteGatedRelayDrop),
            layer_allowed: self
                .rtc_route_control
                .load(RtcRouteControlOutcome::LayerAllowed),
            layer_dropped: self
                .rtc_route_control
                .load(RtcRouteControlOutcome::LayerDropped),
        }
    }
}

fn snapshot_duration_histogram(
    histogram: &super::counter::Histogram<ControlPlaneDurationBucket>,
) -> DurationHistogramSnapshot {
    DurationHistogramSnapshot {
        le_10_millis: histogram.load_bucket(ControlPlaneDurationBucket::Le10Millis),
        le_50_millis: histogram.load_bucket(ControlPlaneDurationBucket::Le50Millis),
        le_100_millis: histogram.load_bucket(ControlPlaneDurationBucket::Le100Millis),
        le_250_millis: histogram.load_bucket(ControlPlaneDurationBucket::Le250Millis),
        le_500_millis: histogram.load_bucket(ControlPlaneDurationBucket::Le500Millis),
        le_1_second: histogram.load_bucket(ControlPlaneDurationBucket::Le1Second),
        le_5_seconds: histogram.load_bucket(ControlPlaneDurationBucket::Le5Seconds),
        count: histogram.load_count(),
        sum_micros: histogram.load_sum_micros(),
    }
}

fn snapshot_duration_histogram_for_route(
    histogram: &super::counter::HistogramFamily<HttpRoute, ControlPlaneDurationBucket>,
    route: HttpRoute,
) -> DurationHistogramSnapshot {
    DurationHistogramSnapshot {
        le_10_millis: histogram.load_bucket(route, ControlPlaneDurationBucket::Le10Millis),
        le_50_millis: histogram.load_bucket(route, ControlPlaneDurationBucket::Le50Millis),
        le_100_millis: histogram.load_bucket(route, ControlPlaneDurationBucket::Le100Millis),
        le_250_millis: histogram.load_bucket(route, ControlPlaneDurationBucket::Le250Millis),
        le_500_millis: histogram.load_bucket(route, ControlPlaneDurationBucket::Le500Millis),
        le_1_second: histogram.load_bucket(route, ControlPlaneDurationBucket::Le1Second),
        le_5_seconds: histogram.load_bucket(route, ControlPlaneDurationBucket::Le5Seconds),
        count: histogram.load_count(route),
        sum_micros: histogram.load_sum_micros(route),
    }
}
