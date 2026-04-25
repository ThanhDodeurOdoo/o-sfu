use super::shared::{LabeledValue2, append_counter, append_gauge, append_labeled_counter_family_2};
use crate::runtime::metrics::RuntimeMetricsSnapshot;

pub(super) fn append_live_gauges(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_gauge(
        output,
        "osfu_rooms_active",
        "Current number of live rooms owned by this runtime.",
        snapshot.active_rooms,
    );
    append_gauge(
        output,
        "osfu_users_active",
        "Current number of live room users owned by this runtime.",
        snapshot.active_users,
    );
    append_gauge(
        output,
        "osfu_publications_active",
        "Current number of committed or pending published media entries owned by this runtime.",
        snapshot.active_publications,
    );
    append_gauge(
        output,
        "osfu_subscriptions_active",
        "Current number of committed or pending consumer subscriptions owned by this runtime.",
        snapshot.active_subscriptions,
    );
    append_gauge(
        output,
        "osfu_transport_users_active",
        "Current number of live RTC transport users on this runtime.",
        snapshot.active_transport_users,
    );
}

pub(super) fn append_recording_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_counter_family_2(
        output,
        "osfu_recording_actions_total",
        "Total recording control actions by action and outcome.",
        ("action", "outcome"),
        &[
            LabeledValue2::new("start", "accepted", snapshot.recording_start_accepted),
            LabeledValue2::new("start", "rejected", snapshot.recording_start_rejected),
            LabeledValue2::new("stop", "accepted", snapshot.recording_stop_accepted),
            LabeledValue2::new("stop", "rejected", snapshot.recording_stop_rejected),
        ],
    );
    append_gauge(
        output,
        "osfu_recording_rooms_active",
        "Current number of rooms with an active recording user.",
        snapshot.active_recording_rooms,
    );
    append_counter(
        output,
        "osfu_recording_captured_packets_total",
        "Total packets accepted by the recording capture path.",
        snapshot.recording_captured_packets,
    );
    append_counter(
        output,
        "osfu_recording_captured_streams_total",
        "Total unique media streams first seen by the recording capture path.",
        snapshot.recording_captured_streams,
    );
}
