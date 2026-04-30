use super::shared::{LabeledValue, append_labeled_counter_family};
use crate::runtime::metrics::RuntimeMetricsSnapshot;

pub(super) fn append_source_selection_metrics(
    output: &mut String,
    snapshot: &RuntimeMetricsSnapshot,
) {
    append_labeled_counter_family(
        output,
        "osfu_source_selection_updates_total",
        "Total room-owned source selector updates accepted by source policy.",
        "selector",
        &[
            LabeledValue::new("open", snapshot.source_selection_updates_open),
            LabeledValue::new("encoding", snapshot.source_selection_updates_encoding),
            LabeledValue::new(
                "operating_point",
                snapshot.source_selection_updates_operating_point,
            ),
            LabeledValue::new(
                "room_policy_featured",
                snapshot.source_selection_updates_room_policy_featured,
            ),
            LabeledValue::new(
                "room_policy_thumbnail",
                snapshot.source_selection_updates_room_policy_thumbnail,
            ),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_budget_solver_outcomes_total",
        "Total receiver video budget solver outcomes accepted by room policy.",
        "outcome",
        &[
            LabeledValue::new("degraded", snapshot.budget_solver_outcomes_degraded),
            LabeledValue::new("paused", snapshot.budget_solver_outcomes_paused),
            LabeledValue::new("resumed", snapshot.budget_solver_outcomes_resumed),
            LabeledValue::new(
                "protected_over_budget",
                snapshot.budget_solver_outcomes_protected_over_budget,
            ),
        ],
    );
}
