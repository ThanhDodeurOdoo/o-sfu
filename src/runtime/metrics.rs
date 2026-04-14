use std::sync::atomic::{AtomicU64, Ordering};

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

#[derive(Debug, Default)]
pub(super) struct RuntimeMetrics {
    http_noop_requests: AtomicU64,
    http_stats_requests: AtomicU64,
    http_channel_requests: AtomicU64,
    http_channel_success: AtomicU64,
    http_channel_unauthorized: AtomicU64,
    http_channel_forbidden: AtomicU64,
    http_channel_bad_request: AtomicU64,
    http_disconnect_requests: AtomicU64,
    http_disconnect_success: AtomicU64,
    http_disconnect_bad_request: AtomicU64,
    http_disconnect_unprocessable_entity: AtomicU64,
    ws_connections_accepted: AtomicU64,
    ws_handshake_credentials_received: AtomicU64,
    ws_handshake_rejected_timeout: AtomicU64,
    ws_handshake_rejected_authentication_failed: AtomicU64,
    ws_handshake_rejected_channel_full: AtomicU64,
    ws_handshake_rejected_error: AtomicU64,
    ws_sessions_joined: AtomicU64,
    ws_startup_send_failures: AtomicU64,
    ws_session_initialize_failures: AtomicU64,
    ws_session_loops_started: AtomicU64,
    ws_session_loop_exits_peer_closed: AtomicU64,
    ws_session_loop_exits_reader_error: AtomicU64,
    ws_session_loop_exits_bus_break: AtomicU64,
    ws_session_loop_exits_ping_timeout: AtomicU64,
    ws_session_loop_exits_transport_disconnected: AtomicU64,
    ws_session_loop_exits_outbound_channel_closed: AtomicU64,
    ws_session_loop_exits_outbound_close_signal: AtomicU64,
    ws_session_loop_exits_outbound_message_send_failure: AtomicU64,
    ws_bus_batches_received: AtomicU64,
    ws_bus_envelopes_received: AtomicU64,
    ws_bus_parse_failures: AtomicU64,
    ws_bus_client_requests: AtomicU64,
    ws_bus_client_messages: AtomicU64,
    ws_bus_client_responses_ignored: AtomicU64,
    ws_bus_client_request_decode_failures: AtomicU64,
    ws_bus_client_message_decode_failures: AtomicU64,
    ws_bus_stub_publish_requests: AtomicU64,
    ws_bus_batches_sent: AtomicU64,
    ws_bus_envelopes_sent: AtomicU64,
    ws_bus_send_failures: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Snapshot reads are part of the runtime observability boundary and are consumed incrementally as exporter integration lands."
)]
pub(super) struct RuntimeMetricsSnapshot {
    pub http_noop_requests: u64,
    pub http_stats_requests: u64,
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
    pub ws_bus_client_requests: u64,
    pub ws_bus_client_messages: u64,
    pub ws_bus_client_responses_ignored: u64,
    pub ws_bus_client_request_decode_failures: u64,
    pub ws_bus_client_message_decode_failures: u64,
    pub ws_bus_stub_publish_requests: u64,
    pub ws_bus_batches_sent: u64,
    pub ws_bus_envelopes_sent: u64,
    pub ws_bus_send_failures: u64,
}

impl RuntimeMetrics {
    #[allow(
        dead_code,
        reason = "Snapshot reads are intentionally available before external exporters are wired."
    )]
    pub(super) fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            http_noop_requests: load(&self.http_noop_requests),
            http_stats_requests: load(&self.http_stats_requests),
            http_channel_requests: load(&self.http_channel_requests),
            http_channel_success: load(&self.http_channel_success),
            http_channel_unauthorized: load(&self.http_channel_unauthorized),
            http_channel_forbidden: load(&self.http_channel_forbidden),
            http_channel_bad_request: load(&self.http_channel_bad_request),
            http_disconnect_requests: load(&self.http_disconnect_requests),
            http_disconnect_success: load(&self.http_disconnect_success),
            http_disconnect_bad_request: load(&self.http_disconnect_bad_request),
            http_disconnect_unprocessable_entity: load(&self.http_disconnect_unprocessable_entity),
            ws_connections_accepted: load(&self.ws_connections_accepted),
            ws_handshake_credentials_received: load(&self.ws_handshake_credentials_received),
            ws_handshake_rejected_timeout: load(&self.ws_handshake_rejected_timeout),
            ws_handshake_rejected_authentication_failed: load(
                &self.ws_handshake_rejected_authentication_failed,
            ),
            ws_handshake_rejected_channel_full: load(&self.ws_handshake_rejected_channel_full),
            ws_handshake_rejected_error: load(&self.ws_handshake_rejected_error),
            ws_sessions_joined: load(&self.ws_sessions_joined),
            ws_startup_send_failures: load(&self.ws_startup_send_failures),
            ws_session_initialize_failures: load(&self.ws_session_initialize_failures),
            ws_session_loops_started: load(&self.ws_session_loops_started),
            ws_session_loop_exits_peer_closed: load(&self.ws_session_loop_exits_peer_closed),
            ws_session_loop_exits_reader_error: load(&self.ws_session_loop_exits_reader_error),
            ws_session_loop_exits_bus_break: load(&self.ws_session_loop_exits_bus_break),
            ws_session_loop_exits_ping_timeout: load(&self.ws_session_loop_exits_ping_timeout),
            ws_session_loop_exits_transport_disconnected: load(
                &self.ws_session_loop_exits_transport_disconnected,
            ),
            ws_session_loop_exits_outbound_channel_closed: load(
                &self.ws_session_loop_exits_outbound_channel_closed,
            ),
            ws_session_loop_exits_outbound_close_signal: load(
                &self.ws_session_loop_exits_outbound_close_signal,
            ),
            ws_session_loop_exits_outbound_message_send_failure: load(
                &self.ws_session_loop_exits_outbound_message_send_failure,
            ),
            ws_bus_batches_received: load(&self.ws_bus_batches_received),
            ws_bus_envelopes_received: load(&self.ws_bus_envelopes_received),
            ws_bus_parse_failures: load(&self.ws_bus_parse_failures),
            ws_bus_client_requests: load(&self.ws_bus_client_requests),
            ws_bus_client_messages: load(&self.ws_bus_client_messages),
            ws_bus_client_responses_ignored: load(&self.ws_bus_client_responses_ignored),
            ws_bus_client_request_decode_failures: load(
                &self.ws_bus_client_request_decode_failures,
            ),
            ws_bus_client_message_decode_failures: load(
                &self.ws_bus_client_message_decode_failures,
            ),
            ws_bus_stub_publish_requests: load(&self.ws_bus_stub_publish_requests),
            ws_bus_batches_sent: load(&self.ws_bus_batches_sent),
            ws_bus_envelopes_sent: load(&self.ws_bus_envelopes_sent),
            ws_bus_send_failures: load(&self.ws_bus_send_failures),
        }
    }

    pub(super) fn record_http_noop_request(&self) {
        increment(&self.http_noop_requests);
    }

    pub(super) fn record_http_stats_request(&self) {
        increment(&self.http_stats_requests);
    }

    pub(super) fn record_http_channel_request(&self) {
        increment(&self.http_channel_requests);
    }

    pub(super) fn record_http_channel_success(&self) {
        increment(&self.http_channel_success);
    }

    pub(super) fn record_http_channel_unauthorized(&self) {
        increment(&self.http_channel_unauthorized);
    }

    pub(super) fn record_http_channel_forbidden(&self) {
        increment(&self.http_channel_forbidden);
    }

    pub(super) fn record_http_channel_bad_request(&self) {
        increment(&self.http_channel_bad_request);
    }

    pub(super) fn record_http_disconnect_request(&self) {
        increment(&self.http_disconnect_requests);
    }

    pub(super) fn record_http_disconnect_success(&self) {
        increment(&self.http_disconnect_success);
    }

    pub(super) fn record_http_disconnect_bad_request(&self) {
        increment(&self.http_disconnect_bad_request);
    }

    pub(super) fn record_http_disconnect_unprocessable_entity(&self) {
        increment(&self.http_disconnect_unprocessable_entity);
    }

    pub(super) fn record_ws_connection_accepted(&self) {
        increment(&self.ws_connections_accepted);
    }

    pub(super) fn record_ws_handshake_credentials_received(&self) {
        increment(&self.ws_handshake_credentials_received);
    }

    pub(super) fn record_ws_handshake_rejection(&self, close_code: Option<WebSocketCloseCode>) {
        match close_code {
            Some(WebSocketCloseCode::AuthTimeout) => {
                increment(&self.ws_handshake_rejected_timeout);
            }
            Some(WebSocketCloseCode::AuthFailed) => {
                increment(&self.ws_handshake_rejected_authentication_failed);
            }
            Some(WebSocketCloseCode::ChannelFull) => {
                increment(&self.ws_handshake_rejected_channel_full);
            }
            Some(
                WebSocketCloseCode::Error
                | WebSocketCloseCode::Clean
                | WebSocketCloseCode::Leaving
                | WebSocketCloseCode::ProtocolError
                | WebSocketCloseCode::Kicked,
            )
            | None => increment(&self.ws_handshake_rejected_error),
        }
    }

    pub(super) fn record_ws_session_joined(&self) {
        increment(&self.ws_sessions_joined);
    }

    pub(super) fn record_ws_startup_send_failure(&self) {
        increment(&self.ws_startup_send_failures);
    }

    pub(super) fn record_ws_session_initialize_failure(&self) {
        increment(&self.ws_session_initialize_failures);
    }

    pub(super) fn record_ws_session_loop_started(&self) {
        increment(&self.ws_session_loops_started);
    }

    pub(super) fn record_ws_session_loop_exit(&self, reason: WsSessionLoopExitReason) {
        match reason {
            WsSessionLoopExitReason::PeerClosed => {
                increment(&self.ws_session_loop_exits_peer_closed);
            }
            WsSessionLoopExitReason::ReaderError => {
                increment(&self.ws_session_loop_exits_reader_error);
            }
            WsSessionLoopExitReason::BusBreak => {
                increment(&self.ws_session_loop_exits_bus_break);
            }
            WsSessionLoopExitReason::PingTimeout => {
                increment(&self.ws_session_loop_exits_ping_timeout);
            }
            WsSessionLoopExitReason::TransportDisconnected => {
                increment(&self.ws_session_loop_exits_transport_disconnected);
            }
            WsSessionLoopExitReason::OutboundChannelClosed => {
                increment(&self.ws_session_loop_exits_outbound_channel_closed);
            }
            WsSessionLoopExitReason::OutboundCloseSignal => {
                increment(&self.ws_session_loop_exits_outbound_close_signal);
            }
            WsSessionLoopExitReason::OutboundMessageSendFailure => {
                increment(&self.ws_session_loop_exits_outbound_message_send_failure);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn record_ws_bus_batch_received(&self, envelope_count: usize) {
        increment(&self.ws_bus_batches_received);
        add(&self.ws_bus_envelopes_received, envelope_count);
    }

    #[cfg(test)]
    pub(super) fn record_ws_bus_client_request(&self) {
        increment(&self.ws_bus_client_requests);
    }

    #[cfg(test)]
    pub(super) fn record_ws_bus_client_message(&self) {
        increment(&self.ws_bus_client_messages);
    }

    pub(super) fn record_ws_bus_batch_sent(&self, envelope_count: usize) {
        increment(&self.ws_bus_batches_sent);
        add(&self.ws_bus_envelopes_sent, envelope_count);
    }

    pub(super) fn record_ws_bus_send_failure(&self) {
        increment(&self.ws_bus_send_failures);
    }
}

fn increment(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn add(counter: &AtomicU64, value: usize) {
    if let Ok(value) = u64::try_from(value) {
        counter.fetch_add(value, Ordering::Relaxed);
    }
}

#[allow(
    dead_code,
    reason = "Only used by snapshot reads, which are currently consumed in test-time observability checks."
)]
fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::{RuntimeMetrics, WsSessionLoopExitReason};
    use crate::signaling::protocol::WebSocketCloseCode;

    #[test]
    fn metrics_snapshot_tracks_http_and_websocket_counters() {
        let metrics = RuntimeMetrics::default();
        metrics.record_http_channel_request();
        metrics.record_http_channel_unauthorized();
        metrics.record_http_disconnect_request();
        metrics.record_http_disconnect_unprocessable_entity();
        metrics.record_ws_connection_accepted();
        metrics.record_ws_handshake_credentials_received();
        metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::AuthTimeout));
        metrics.record_ws_session_joined();
        metrics.record_ws_session_loop_started();
        metrics.record_ws_session_loop_exit(WsSessionLoopExitReason::PeerClosed);
        metrics.record_ws_bus_batch_received(3);
        metrics.record_ws_bus_client_request();
        metrics.record_ws_bus_client_message();
        metrics.record_ws_bus_batch_sent(2);
        metrics.record_ws_bus_send_failure();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.http_channel_requests, 1);
        assert_eq!(snapshot.http_channel_unauthorized, 1);
        assert_eq!(snapshot.http_disconnect_requests, 1);
        assert_eq!(snapshot.http_disconnect_unprocessable_entity, 1);
        assert_eq!(snapshot.ws_connections_accepted, 1);
        assert_eq!(snapshot.ws_handshake_credentials_received, 1);
        assert_eq!(snapshot.ws_handshake_rejected_timeout, 1);
        assert_eq!(snapshot.ws_sessions_joined, 1);
        assert_eq!(snapshot.ws_session_loops_started, 1);
        assert_eq!(snapshot.ws_session_loop_exits_peer_closed, 1);
        assert_eq!(snapshot.ws_session_loop_exits_ping_timeout, 0);
        assert_eq!(snapshot.ws_session_loop_exits_transport_disconnected, 0);
        assert_eq!(snapshot.ws_bus_batches_received, 1);
        assert_eq!(snapshot.ws_bus_envelopes_received, 3);
        assert_eq!(snapshot.ws_bus_client_requests, 1);
        assert_eq!(snapshot.ws_bus_client_messages, 1);
        assert_eq!(snapshot.ws_bus_batches_sent, 1);
        assert_eq!(snapshot.ws_bus_envelopes_sent, 2);
        assert_eq!(snapshot.ws_bus_send_failures, 1);
    }

    #[test]
    fn handshake_rejection_buckets_are_distinct() {
        let metrics = RuntimeMetrics::default();
        metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::AuthFailed));
        metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ChannelFull));
        metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::Error));
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.ws_handshake_rejected_authentication_failed, 1);
        assert_eq!(snapshot.ws_handshake_rejected_channel_full, 1);
        assert_eq!(snapshot.ws_handshake_rejected_error, 1);
    }
}
