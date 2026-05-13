In-repository telemetry crate for `o-sfu`.

This crate contain the runtime telemetry contract: tracing setup, event and field
schema, diagnostics DTOs and store, runtime metrics, Prometheus rendering and
Grafana Node Graph formatting. The sibling `o-sfu-telemetry` repository contains
optional deployment assets that consume these formats.
