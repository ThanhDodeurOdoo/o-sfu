use o_sfu_protocol::signaling::WebSocketCloseCode;

use super::shared::{
    HistogramBucketValue, LabeledValue, append_counter, append_histogram,
    append_labeled_counter_family, close_code_label,
};
use crate::runtime::metrics::{DurationHistogramSnapshot, RuntimeMetricsSnapshot};

pub(super) fn append_ws_connection_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_counter_family(
        output,
        "osfu_ws_connections_total",
        "Total websocket connections observed at each handshake stage.",
        "stage",
        &[
            LabeledValue::new("accepted", snapshot.ws_connections_accepted),
            LabeledValue::new(
                "credentials_received",
                snapshot.ws_handshake_credentials_received,
            ),
            LabeledValue::new("joined", snapshot.ws_users_joined),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_handshake_rejections_total",
        "Total websocket handshake rejections by close code bucket.",
        "close_code",
        &[
            LabeledValue::new(
                close_code_label(WebSocketCloseCode::AuthTimeout),
                snapshot.ws_handshake_rejected_timeout,
            ),
            LabeledValue::new(
                close_code_label(WebSocketCloseCode::AuthFailed),
                snapshot.ws_handshake_rejected_authentication_failed,
            ),
            LabeledValue::new(
                close_code_label(WebSocketCloseCode::ProtocolError),
                snapshot.ws_handshake_rejected_protocol_error,
            ),
            LabeledValue::new(
                close_code_label(WebSocketCloseCode::RoomFull),
                snapshot.ws_handshake_rejected_room_full,
            ),
            LabeledValue::new("error", snapshot.ws_handshake_rejected_error),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_startup_failures_total",
        "Total websocket startup failures before the steady-state user loop.",
        "kind",
        &[
            LabeledValue::new("startup_send", snapshot.ws_startup_send_failures),
            LabeledValue::new("user_initialize", snapshot.ws_user_initialize_failures),
        ],
    );
    append_histogram(
        output,
        "osfu_ws_handshake_duration_seconds",
        "Websocket handshake duration from upgrade to user readiness or rejection.",
        &duration_histogram_buckets(&snapshot.ws_handshake_duration),
        snapshot.ws_handshake_duration.sum_micros,
        snapshot.ws_handshake_duration.count,
    );
    append_histogram(
        output,
        "osfu_ws_auth_duration_seconds",
        "Websocket authentication duration from first auth wait through token validation.",
        &duration_histogram_buckets(&snapshot.ws_auth_duration),
        snapshot.ws_auth_duration.sum_micros,
        snapshot.ws_auth_duration.count,
    );
    append_histogram(
        output,
        "osfu_ws_user_initialize_duration_seconds",
        "Websocket user initialization duration after room admission.",
        &duration_histogram_buckets(&snapshot.ws_user_initialize_duration),
        snapshot.ws_user_initialize_duration.sum_micros,
        snapshot.ws_user_initialize_duration.count,
    );
}

pub(super) fn append_ws_loop_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_counter(
        output,
        "osfu_ws_user_loops_started_total",
        "Total websocket user loops started after a successful join.",
        snapshot.ws_user_loops_started,
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_user_loop_exits_total",
        "Total websocket user loop exits by reason.",
        "reason",
        &[
            LabeledValue::new("peer_closed", snapshot.ws_user_loop_exits_peer_closed),
            LabeledValue::new("reader_error", snapshot.ws_user_loop_exits_reader_error),
            LabeledValue::new("bus_break", snapshot.ws_user_loop_exits_bus_break),
            LabeledValue::new("ping_timeout", snapshot.ws_user_loop_exits_ping_timeout),
            LabeledValue::new(
                "transport_disconnected",
                snapshot.ws_user_loop_exits_transport_disconnected,
            ),
            LabeledValue::new(
                "outbound_room_closed",
                snapshot.ws_user_loop_exits_outbound_room_closed,
            ),
            LabeledValue::new(
                "outbound_close_signal",
                snapshot.ws_user_loop_exits_outbound_close_signal,
            ),
            LabeledValue::new(
                "outbound_message_send_failure",
                snapshot.ws_user_loop_exits_outbound_message_send_failure,
            ),
        ],
    );
}

pub(super) fn append_ws_bus_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_counter_family(
        output,
        "osfu_ws_bus_batches_total",
        "Total websocket signaling batches processed by direction.",
        "direction",
        &[
            LabeledValue::new("received", snapshot.ws_bus_batches_received),
            LabeledValue::new("sent", snapshot.ws_bus_batches_sent),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_bus_envelopes_total",
        "Total websocket signaling envelopes processed by direction.",
        "direction",
        &[
            LabeledValue::new("received", snapshot.ws_bus_envelopes_received),
            LabeledValue::new("sent", snapshot.ws_bus_envelopes_sent),
        ],
    );
    append_counter(
        output,
        "osfu_ws_bus_parse_failures_total",
        "Total websocket signaling parse failures.",
        snapshot.ws_bus_parse_failures,
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_bus_failures_total",
        "Total websocket signaling failures by kind.",
        "kind",
        &[
            LabeledValue::new("invalid_input", snapshot.ws_bus_invalid_input_failures),
            LabeledValue::new(
                "unsupported_feature",
                snapshot.ws_bus_unsupported_feature_failures,
            ),
            LabeledValue::new("send", snapshot.ws_bus_send_failures),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_bus_client_frames_total",
        "Total client websocket signaling frames by kind.",
        "kind",
        &[
            LabeledValue::new("request", snapshot.ws_bus_client_requests),
            LabeledValue::new("message", snapshot.ws_bus_client_messages),
        ],
    );
}

fn duration_histogram_buckets(snapshot: &DurationHistogramSnapshot) -> [HistogramBucketValue; 7] {
    [
        HistogramBucketValue::new("0.01", snapshot.le_10_millis),
        HistogramBucketValue::new("0.05", snapshot.le_50_millis),
        HistogramBucketValue::new("0.1", snapshot.le_100_millis),
        HistogramBucketValue::new("0.25", snapshot.le_250_millis),
        HistogramBucketValue::new("0.5", snapshot.le_500_millis),
        HistogramBucketValue::new("1", snapshot.le_1_second),
        HistogramBucketValue::new("5", snapshot.le_5_seconds),
    ]
}
