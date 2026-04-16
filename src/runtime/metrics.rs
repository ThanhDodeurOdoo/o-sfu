use std::marker::PhantomData;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use crate::runtime::rtc_adapter::TransportSessionHealth;
use crate::signaling::protocol::WebSocketCloseCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsSessionLoopExitReason {
    PeerClosed,
    ReaderError,
    BusBreak,
    PingTimeout,
    TransportDisconnected,
    OutboundChannelClosed,
    OutboundCloseSignal,
    OutboundMessageSendFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpRoute {
    Noop,
    Stats,
    Channel,
    Disconnect,
    Metrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpChannelResponseStatus {
    Success,
    Unauthorized,
    Forbidden,
    BadRequest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpDisconnectResponseStatus {
    Success,
    BadRequest,
    UnprocessableEntity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsConnectionStage {
    Accepted,
    CredentialsReceived,
    Joined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsStartupFailureKind {
    StartupSend,
    SessionInitialize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsBusDirection {
    Received,
    Sent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsBusFailureKind {
    InvalidInput,
    UnsupportedFeature,
    Send,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsBusClientFrameKind {
    Request,
    Message,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RtpFlowDirection {
    Ingress,
    Egress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RtcDatagramRoutePath {
    Indexed,
    Scan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RtcDatagramDropReason {
    RecentMissCache,
    SourceRateLimited,
    NoSession,
    Malformed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RtcRouteControlOutcome {
    Absorbed,
    Forwarded,
    RouteGatedRelayDrop,
    LayerAllowed,
    LayerDropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransportIceState {
    New,
    Checking,
    Connected,
    Completed,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransportSessionLifetimeBucket {
    Le1Second,
    Le10Seconds,
    Le60Seconds,
    Le300Seconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecordingActionOutcome {
    StartAccepted,
    StartRejected,
    StopAccepted,
    StopRejected,
}

trait MetricLabel: Copy {
    const COUNT: usize;

    fn as_index(self) -> usize;
}

#[derive(Debug, Default)]
struct Counter {
    value: AtomicU64,
}

impl Counter {
    fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    fn add(&self, value: usize) {
        if let Ok(value) = u64::try_from(value) {
            self.value.fetch_add(value, Ordering::Relaxed);
        }
    }

    fn add_u64(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }

    fn load(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
struct UpDownCounter {
    value: AtomicI64,
}

impl UpDownCounter {
    fn add(&self, delta: i64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
    }

    fn load(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct CounterFamily<L: MetricLabel> {
    counters: Box<[Counter]>,
    _label: PhantomData<L>,
}

impl<L: MetricLabel> Default for CounterFamily<L> {
    fn default() -> Self {
        let counters = (0..L::COUNT)
            .map(|_| Counter::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            counters,
            _label: PhantomData,
        }
    }
}

impl<L: MetricLabel> CounterFamily<L> {
    fn increment(&self, label: L) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.increment();
        }
    }

    fn add(&self, label: L, value: usize) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.add(value);
        }
    }

    fn load(&self, label: L) -> u64 {
        self.counters.get(label.as_index()).map_or(0, Counter::load)
    }
}

impl MetricLabel for HttpRoute {
    const COUNT: usize = 5;

    fn as_index(self) -> usize {
        match self {
            Self::Noop => 0,
            Self::Stats => 1,
            Self::Channel => 2,
            Self::Disconnect => 3,
            Self::Metrics => 4,
        }
    }
}

impl MetricLabel for HttpChannelResponseStatus {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::Unauthorized => 1,
            Self::Forbidden => 2,
            Self::BadRequest => 3,
        }
    }
}

impl MetricLabel for HttpDisconnectResponseStatus {
    const COUNT: usize = 3;

    fn as_index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::BadRequest => 1,
            Self::UnprocessableEntity => 2,
        }
    }
}

impl MetricLabel for WsConnectionStage {
    const COUNT: usize = 3;

    fn as_index(self) -> usize {
        match self {
            Self::Accepted => 0,
            Self::CredentialsReceived => 1,
            Self::Joined => 2,
        }
    }
}

impl MetricLabel for WebSocketCloseCode {
    const COUNT: usize = 8;

    fn as_index(self) -> usize {
        match self {
            Self::AuthTimeout => 0,
            Self::AuthFailed => 1,
            Self::ProtocolError => 2,
            Self::ChannelFull => 3,
            Self::Error => 4,
            Self::Clean => 5,
            Self::Leaving => 6,
            Self::Kicked => 7,
        }
    }
}

impl MetricLabel for WsStartupFailureKind {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::StartupSend => 0,
            Self::SessionInitialize => 1,
        }
    }
}

impl MetricLabel for WsSessionLoopExitReason {
    const COUNT: usize = 8;

    fn as_index(self) -> usize {
        match self {
            Self::PeerClosed => 0,
            Self::ReaderError => 1,
            Self::BusBreak => 2,
            Self::PingTimeout => 3,
            Self::TransportDisconnected => 4,
            Self::OutboundChannelClosed => 5,
            Self::OutboundCloseSignal => 6,
            Self::OutboundMessageSendFailure => 7,
        }
    }
}

impl MetricLabel for WsBusDirection {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::Received => 0,
            Self::Sent => 1,
        }
    }
}

impl MetricLabel for WsBusFailureKind {
    const COUNT: usize = 3;

    fn as_index(self) -> usize {
        match self {
            Self::InvalidInput => 0,
            Self::UnsupportedFeature => 1,
            Self::Send => 2,
        }
    }
}

impl MetricLabel for WsBusClientFrameKind {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::Request => 0,
            Self::Message => 1,
        }
    }
}

impl MetricLabel for RtpFlowDirection {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::Ingress => 0,
            Self::Egress => 1,
        }
    }
}

impl MetricLabel for RtcDatagramRoutePath {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::Indexed => 0,
            Self::Scan => 1,
        }
    }
}

impl MetricLabel for RtcDatagramDropReason {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::RecentMissCache => 0,
            Self::SourceRateLimited => 1,
            Self::NoSession => 2,
            Self::Malformed => 3,
        }
    }
}

impl MetricLabel for RtcRouteControlOutcome {
    const COUNT: usize = 5;

    fn as_index(self) -> usize {
        match self {
            Self::Absorbed => 0,
            Self::Forwarded => 1,
            Self::RouteGatedRelayDrop => 2,
            Self::LayerAllowed => 3,
            Self::LayerDropped => 4,
        }
    }
}

impl MetricLabel for TransportIceState {
    const COUNT: usize = 5;

    fn as_index(self) -> usize {
        match self {
            Self::New => 0,
            Self::Checking => 1,
            Self::Connected => 2,
            Self::Completed => 3,
            Self::Disconnected => 4,
        }
    }
}

impl MetricLabel for TransportSessionLifetimeBucket {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::Le1Second => 0,
            Self::Le10Seconds => 1,
            Self::Le60Seconds => 2,
            Self::Le300Seconds => 3,
        }
    }
}

impl MetricLabel for RecordingActionOutcome {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::StartAccepted => 0,
            Self::StartRejected => 1,
            Self::StopAccepted => 2,
            Self::StopRejected => 3,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeMetrics {
    http_requests: CounterFamily<HttpRoute>,
    http_channel_responses: CounterFamily<HttpChannelResponseStatus>,
    http_disconnect_responses: CounterFamily<HttpDisconnectResponseStatus>,
    ws_connections: CounterFamily<WsConnectionStage>,
    ws_handshake_rejections: CounterFamily<WebSocketCloseCode>,
    ws_handshake_rejections_other: Counter,
    ws_startup_failures: CounterFamily<WsStartupFailureKind>,
    ws_session_loops_started: Counter,
    ws_session_loop_exits: CounterFamily<WsSessionLoopExitReason>,
    ws_bus_batches: CounterFamily<WsBusDirection>,
    ws_bus_envelopes: CounterFamily<WsBusDirection>,
    ws_bus_parse_failures: Counter,
    ws_bus_failures: CounterFamily<WsBusFailureKind>,
    ws_bus_client_frames: CounterFamily<WsBusClientFrameKind>,
    active_channels: UpDownCounter,
    active_sessions: UpDownCounter,
    active_recording_channels: UpDownCounter,
    active_transport_sessions: UpDownCounter,
    connected_transport_sessions: UpDownCounter,
    disconnected_transport_sessions: UpDownCounter,
    recording_actions: CounterFamily<RecordingActionOutcome>,
    recording_captured_packets: Counter,
    recording_captured_streams: Counter,
    rtp_packets: CounterFamily<RtpFlowDirection>,
    rtp_payload_bytes: CounterFamily<RtpFlowDirection>,
    transport_ice_state_changes: CounterFamily<TransportIceState>,
    transport_dtls_connected: Counter,
    transport_session_lifetime_buckets: CounterFamily<TransportSessionLifetimeBucket>,
    transport_session_lifetime_count: Counter,
    transport_session_lifetime_sum_micros: Counter,
    rtc_datagram_routes: CounterFamily<RtcDatagramRoutePath>,
    rtc_datagram_drops: CounterFamily<RtcDatagramDropReason>,
    rtc_datagram_fallback_scans: Counter,
    rtc_datagram_scan_sessions: Counter,
    rtc_route_control: CounterFamily<RtcRouteControlOutcome>,
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
    pub active_channels: i64,
    pub active_sessions: i64,
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
}

struct TransportLifecycleSnapshot {
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
    pub(super) fn snapshot(&self) -> RuntimeMetricsSnapshot {
        let http = self.snapshot_http();
        let websocket = self.snapshot_websocket();
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
            active_channels: live.channels,
            active_sessions: live.sessions,
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
        }
    }

    fn snapshot_transport_lifecycle(&self) -> TransportLifecycleSnapshot {
        TransportLifecycleSnapshot {
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

    pub(super) fn record_http_noop_request(&self) {
        self.http_requests.increment(HttpRoute::Noop);
    }

    pub(super) fn record_http_stats_request(&self) {
        self.http_requests.increment(HttpRoute::Stats);
    }

    pub(super) fn record_http_metrics_request(&self) {
        self.http_requests.increment(HttpRoute::Metrics);
    }

    pub(super) fn record_http_channel_request(&self) {
        self.http_requests.increment(HttpRoute::Channel);
    }

    pub(super) fn record_http_channel_success(&self) {
        self.http_channel_responses
            .increment(HttpChannelResponseStatus::Success);
    }

    pub(super) fn record_http_channel_unauthorized(&self) {
        self.http_channel_responses
            .increment(HttpChannelResponseStatus::Unauthorized);
    }

    pub(super) fn record_http_channel_forbidden(&self) {
        self.http_channel_responses
            .increment(HttpChannelResponseStatus::Forbidden);
    }

    pub(super) fn record_http_channel_bad_request(&self) {
        self.http_channel_responses
            .increment(HttpChannelResponseStatus::BadRequest);
    }

    pub(super) fn record_http_disconnect_request(&self) {
        self.http_requests.increment(HttpRoute::Disconnect);
    }

    pub(super) fn record_http_disconnect_success(&self) {
        self.http_disconnect_responses
            .increment(HttpDisconnectResponseStatus::Success);
    }

    pub(super) fn record_http_disconnect_bad_request(&self) {
        self.http_disconnect_responses
            .increment(HttpDisconnectResponseStatus::BadRequest);
    }

    pub(super) fn record_http_disconnect_unprocessable_entity(&self) {
        self.http_disconnect_responses
            .increment(HttpDisconnectResponseStatus::UnprocessableEntity);
    }

    pub(super) fn record_ws_connection_accepted(&self) {
        self.ws_connections.increment(WsConnectionStage::Accepted);
    }

    pub(super) fn record_ws_handshake_credentials_received(&self) {
        self.ws_connections
            .increment(WsConnectionStage::CredentialsReceived);
    }

    pub(super) fn record_ws_handshake_rejection(&self, close_code: Option<WebSocketCloseCode>) {
        match close_code {
            Some(
                close_code @ (WebSocketCloseCode::AuthTimeout
                | WebSocketCloseCode::AuthFailed
                | WebSocketCloseCode::ProtocolError
                | WebSocketCloseCode::ChannelFull),
            ) => self.ws_handshake_rejections.increment(close_code),
            Some(
                WebSocketCloseCode::Error
                | WebSocketCloseCode::Clean
                | WebSocketCloseCode::Leaving
                | WebSocketCloseCode::Kicked,
            )
            | None => self.ws_handshake_rejections_other.increment(),
        }
    }

    pub(super) fn record_ws_session_joined(&self) {
        self.ws_connections.increment(WsConnectionStage::Joined);
    }

    pub(super) fn record_ws_startup_send_failure(&self) {
        self.ws_startup_failures
            .increment(WsStartupFailureKind::StartupSend);
    }

    pub(super) fn record_ws_session_initialize_failure(&self) {
        self.ws_startup_failures
            .increment(WsStartupFailureKind::SessionInitialize);
    }

    pub(super) fn record_ws_session_loop_started(&self) {
        self.ws_session_loops_started.increment();
    }

    pub(super) fn record_ws_session_loop_exit(&self, reason: WsSessionLoopExitReason) {
        self.ws_session_loop_exits.increment(reason);
    }

    pub(super) fn record_ws_bus_batch_received(&self, envelope_count: usize) {
        self.ws_bus_batches.increment(WsBusDirection::Received);
        self.ws_bus_envelopes
            .add(WsBusDirection::Received, envelope_count);
    }

    pub(super) fn record_ws_bus_invalid_input_failure(&self) {
        self.ws_bus_parse_failures.increment();
        self.ws_bus_failures
            .increment(WsBusFailureKind::InvalidInput);
    }

    pub(super) fn record_ws_bus_unsupported_feature_failure(&self) {
        self.ws_bus_parse_failures.increment();
        self.ws_bus_failures
            .increment(WsBusFailureKind::UnsupportedFeature);
    }

    pub(super) fn record_ws_bus_client_request(&self) {
        self.ws_bus_client_frames
            .increment(WsBusClientFrameKind::Request);
    }

    pub(super) fn record_ws_bus_client_message(&self) {
        self.ws_bus_client_frames
            .increment(WsBusClientFrameKind::Message);
    }

    pub(super) fn record_ws_bus_batch_sent(&self, envelope_count: usize) {
        self.ws_bus_batches.increment(WsBusDirection::Sent);
        self.ws_bus_envelopes
            .add(WsBusDirection::Sent, envelope_count);
    }

    pub(super) fn record_ws_bus_send_failure(&self) {
        self.ws_bus_failures.increment(WsBusFailureKind::Send);
    }

    pub(super) fn add_active_channels(&self, delta: i64) {
        self.active_channels.add(delta);
    }

    pub(super) fn add_active_sessions(&self, delta: i64) {
        self.active_sessions.add(delta);
    }

    pub(super) fn add_active_recording_channels(&self, delta: i64) {
        self.active_recording_channels.add(delta);
    }

    pub(super) fn add_active_transport_sessions(&self, delta: i64) {
        self.active_transport_sessions.add(delta);
    }

    pub(super) fn record_transport_health_transition(
        &self,
        previous: Option<TransportSessionHealth>,
        next: Option<TransportSessionHealth>,
    ) {
        if previous == next {
            return;
        }
        match previous {
            Some(TransportSessionHealth::Connected) => self.connected_transport_sessions.add(-1),
            Some(TransportSessionHealth::Disconnected) => {
                self.disconnected_transport_sessions.add(-1);
            }
            None => {}
        }
        match next {
            Some(TransportSessionHealth::Connected) => self.connected_transport_sessions.add(1),
            Some(TransportSessionHealth::Disconnected) => {
                self.disconnected_transport_sessions.add(1);
            }
            None => {}
        }
    }

    pub(super) fn record_recording_start_accepted(&self) {
        self.recording_actions
            .increment(RecordingActionOutcome::StartAccepted);
    }

    pub(super) fn record_recording_start_rejected(&self) {
        self.recording_actions
            .increment(RecordingActionOutcome::StartRejected);
    }

    pub(super) fn record_recording_stop_accepted(&self) {
        self.recording_actions
            .increment(RecordingActionOutcome::StopAccepted);
    }

    pub(super) fn record_recording_stop_rejected(&self) {
        self.recording_actions
            .increment(RecordingActionOutcome::StopRejected);
    }

    pub(super) fn record_recording_captured_packet(&self) {
        self.recording_captured_packets.increment();
    }

    pub(super) fn record_recording_captured_stream(&self) {
        self.recording_captured_streams.increment();
    }

    pub(super) fn record_rtp_ingress(&self, payload_bytes: usize) {
        self.rtp_packets.increment(RtpFlowDirection::Ingress);
        self.rtp_payload_bytes
            .add(RtpFlowDirection::Ingress, payload_bytes);
    }

    pub(super) fn record_rtp_egress(&self, payload_bytes: usize) {
        self.rtp_packets.increment(RtpFlowDirection::Egress);
        self.rtp_payload_bytes
            .add(RtpFlowDirection::Egress, payload_bytes);
    }

    pub(super) fn record_transport_ice_state_change(&self, state: TransportIceState) {
        self.transport_ice_state_changes.increment(state);
    }

    pub(super) fn record_transport_dtls_connected(&self) {
        self.transport_dtls_connected.increment();
    }

    pub(super) fn record_transport_session_lifetime(&self, duration: Duration) {
        self.transport_session_lifetime_count.increment();
        self.transport_session_lifetime_sum_micros
            .add_u64(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX));
        if duration <= Duration::from_secs(1) {
            self.transport_session_lifetime_buckets
                .increment(TransportSessionLifetimeBucket::Le1Second);
        }
        if duration <= Duration::from_secs(10) {
            self.transport_session_lifetime_buckets
                .increment(TransportSessionLifetimeBucket::Le10Seconds);
        }
        if duration <= Duration::from_secs(60) {
            self.transport_session_lifetime_buckets
                .increment(TransportSessionLifetimeBucket::Le60Seconds);
        }
        if duration <= Duration::from_secs(300) {
            self.transport_session_lifetime_buckets
                .increment(TransportSessionLifetimeBucket::Le300Seconds);
        }
    }

    pub(super) fn record_rtc_datagram_route(&self, path: RtcDatagramRoutePath) {
        self.rtc_datagram_routes.increment(path);
    }

    pub(super) fn record_rtc_datagram_drop(&self, reason: RtcDatagramDropReason) {
        self.rtc_datagram_drops.increment(reason);
    }

    pub(super) fn record_rtc_datagram_fallback_scan(&self, examined_sessions: usize) {
        self.rtc_datagram_fallback_scans.increment();
        self.rtc_datagram_scan_sessions.add(examined_sessions);
    }

    pub(super) fn record_rtc_route_control(&self, outcome: RtcRouteControlOutcome) {
        self.rtc_route_control.increment(outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RtcDatagramDropReason, RtcDatagramRoutePath, RtcRouteControlOutcome, RuntimeMetrics,
        TransportIceState, WsSessionLoopExitReason,
    };
    use crate::{
        runtime::rtc_adapter::TransportSessionHealth, signaling::protocol::WebSocketCloseCode,
    };
    use std::time::Duration;

    fn assert_live_gauges(snapshot: &super::RuntimeMetricsSnapshot) {
        assert_eq!(snapshot.active_channels, 1);
        assert_eq!(snapshot.active_sessions, 2);
        assert_eq!(snapshot.active_recording_channels, 1);
        assert_eq!(snapshot.active_transport_sessions, 1);
        assert_eq!(snapshot.connected_transport_sessions, 1);
        assert_eq!(snapshot.disconnected_transport_sessions, 0);
    }

    fn assert_recording_metrics(snapshot: &super::RuntimeMetricsSnapshot) {
        assert_eq!(snapshot.recording_start_accepted, 1);
        assert_eq!(snapshot.recording_captured_packets, 1);
        assert_eq!(snapshot.recording_captured_streams, 1);
    }

    fn assert_transport_lifecycle_metrics(snapshot: &super::RuntimeMetricsSnapshot) {
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

    fn assert_rtp_and_datagram_metrics(snapshot: &super::RuntimeMetricsSnapshot) {
        assert_eq!(snapshot.rtp_packets_ingress, 1);
        assert_eq!(snapshot.rtp_packets_egress, 1);
        assert_eq!(snapshot.rtp_payload_bytes_ingress, 1200);
        assert_eq!(snapshot.rtp_payload_bytes_egress, 900);
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

    #[test]
    fn metrics_snapshot_tracks_http_and_websocket_counters() {
        let metrics = RuntimeMetrics::default();
        metrics.record_http_channel_request();
        metrics.record_http_channel_unauthorized();
        metrics.record_http_disconnect_request();
        metrics.record_http_disconnect_unprocessable_entity();
        metrics.record_http_metrics_request();
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
    }

    #[test]
    fn metrics_snapshot_tracks_live_gauges_and_rtp_counters() {
        let metrics = RuntimeMetrics::default();
        metrics.add_active_channels(1);
        metrics.add_active_sessions(2);
        metrics.add_active_recording_channels(1);
        metrics.add_active_transport_sessions(1);
        metrics.record_transport_health_transition(None, Some(TransportSessionHealth::Connected));
        metrics.record_recording_start_accepted();
        metrics.record_recording_captured_packet();
        metrics.record_recording_captured_stream();
        metrics.record_rtp_ingress(1200);
        metrics.record_rtp_egress(900);
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

        let snapshot = metrics.snapshot();

        assert_live_gauges(&snapshot);
        assert_recording_metrics(&snapshot);
        assert_transport_lifecycle_metrics(&snapshot);
        assert_rtp_and_datagram_metrics(&snapshot);
    }

    #[test]
    fn transport_health_transition_updates_connected_and_disconnected_gauges() {
        let metrics = RuntimeMetrics::default();

        metrics.record_transport_health_transition(None, Some(TransportSessionHealth::Connected));
        metrics.record_transport_health_transition(
            Some(TransportSessionHealth::Connected),
            Some(TransportSessionHealth::Disconnected),
        );
        metrics
            .record_transport_health_transition(Some(TransportSessionHealth::Disconnected), None);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.connected_transport_sessions, 0);
        assert_eq!(snapshot.disconnected_transport_sessions, 0);
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
}
