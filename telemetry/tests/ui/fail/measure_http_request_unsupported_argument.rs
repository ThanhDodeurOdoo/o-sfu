use o_sfu_telemetry::measure_http_request;

struct Metrics;

#[derive(Clone, Copy)]
enum HttpRoute {
    Noop,
}

struct State {
    metrics: Metrics,
}

impl Metrics {
    fn record_request(&self) {}

    fn add_http_inflight_requests(&self, _route: HttpRoute, _delta: i64) {}

    fn record_http_request_duration(&self, _route: HttpRoute, _elapsed: std::time::Duration) {}
}

#[measure_http_request(
    metrics = "state.metrics",
    request = "record_request",
    route = "HttpRoute::Noop",
    unexpected = "boom"
)]
async fn handled(state: &State) {
    let _ = state;
}

fn main() {}
