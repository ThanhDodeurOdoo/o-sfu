# o-sfu telemetry defaults

This directory is a optional telemetry tools

The actual runtime contract is in `src/runtime/`:

- `/metrics` is the low-cardinality metrics endpoint
- `/v1/stats` stays compatibility-shaped
- log format, service metadata, and trace-export config knobs stay in runtime config

Everything under `telemetry/` is "satellite" tooling, but actual deployment teams may
have their own tools to consume the runtime contract.

So they are more examples then mandatory tools (also useful for dev testing)

## prototype/examples

- `docker-compose.yml`: local reference stack for Prometheus, Grafana, Alertmanager, and an OpenTelemetry Collector placeholder
- `prometheus/`: baseline scrape config for `GET /metrics`
- `grafana/`: configuration for a default Prometheus datasource
- `alertmanager/`: minimal routing stub so the reference stack boots cleanly
- `otel-collector/`: placeholder OTLP receiver config for later trace rollout

intentionally a shell.

 Dashboards, alert rules, and richer collector flows belong will be implemented in later phases of dev

## Local prototype

1. Start `o-sfu` on the host with the normal HTTP listener on `:8080`.
2. From this directory, run `docker compose up`.
3. Open Grafana on `http://localhost:3000` and Prometheus on `http://localhost:9090`.
4. Confirm the `o-sfu` target is up and that Prometheus can scrape `/metrics`.

The compose stack uses `host.docker.internal` with a host-gateway mapping so the containers can scrape a host-run `o-sfu` process.
