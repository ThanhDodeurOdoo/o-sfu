use o_sfu_telemetry::measure_duration;

struct Metrics;

struct State {
    metrics: Metrics,
}

impl Metrics {
    fn record_duration(&self, _elapsed: std::time::Duration) {}
}

#[measure_duration(
    metrics = "state.metrics",
    metrics = "state.metrics",
    record = "record_duration"
)]
fn measured(state: &State) {
    let _ = state;
}

fn main() {}
