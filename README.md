<p align="center">
  <img src="assets/o-sfu.svg" alt="o-sfu logo" width="400">
</p>

[![Tests](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml)
[![Client](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml)
[![Client Browser](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client-browser.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client-browser.yml)
[![Feature Matrix](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/feature-matrix.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/feature-matrix.yml)
[![Fuzzing](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/fuzzing.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/fuzzing.yml)
[![Formal Verification](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml)
[![UB Tests](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/ub-tests.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/ub-tests.yml)
[![Sanitizer](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/sanitizer.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/sanitizer.yml)
[![Cargo Deny](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/cargo-deny.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/cargo-deny.yml)
[![Dependency Review](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/dependency-review.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/dependency-review.yml)
[![CodeQL](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql)

# o-sfu

The goal is to be able to run it as an alternative to odoo/sfu (so the http and ws API and client bundle API are the same), but with:
- higher control on routing
- better recording integration
- better scaling architecture (local and multi server sharding)
- more observability (prometheus, open telemetry,...)
- more?

### API documentation

you can read the one at [odoo/sfu](https://github.com/odoo/sfu), it's roughly the same (Bundle API and http API)

### Env variables (based on odoo/sfu)

| Variable                                     | Default                    | Description                                                                                                                                                       |
| :------------------------------------------- | :------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PUBLIC_IP` (required)                       | -                          | Used to establish WebRTC connections to the server.                                                                                                               |
| `AUTH_KEY` (required)                        | -                          | The base64 encoded encryption key used for JWT authentication.                                                                                                    |
| `BIND_ADDRESS`                               | `0.0.0.0:8070`             | HTTP and WebSocket listening address.                                                                                                                             |
| `PROXY`                                      | `false`                    | Set to true if behind a proxy to trust forwarding headers.                                                                                                        |
| `DIAGNOSTICS_AUTH_TOKEN`                     | unset                      | Optional bearer token for `/internal/diagnostics/...`. If unset, diagnostics are allowed only on loopback listeners (the security is deferd to the reverse proxy) |
| `RTC_MIN_PORT`                               | `40000`                    | Lower bound for the range of ports used by the RTC server (UDP).                                                                                                  |
| `RTC_MAX_PORT`                               | `49999`                    | Upper bound for the range of ports used by the RTC server (UDP).                                                                                                  |
| `RTC_MEDIA_WORKER_COUNT`                     | `1`                        | Number of RTC media workers to spawn.                                                                                                                             |
| `ROOM_MAX_LOCAL_ROUTERS`                     | `1`                        | Maximum local routers/workers one room may reserve. `1` keeps strict single-router behavior; values above one enable load-triggered local spillover by default.   |
| `ROOM_SPILLOVER_MODE`                        | `load` when router cap > 1 | Same-room spillover mode: `strict`, `load`, `load-triggered`, or `bounded`. `bounded` keeps deterministic Phase 2 placement for tests and explicit experiments.   |
| `ROOM_SPILLOVER_MIN_RECEIVERS`               | `16`                       | Minimum live receiver count that can activate load-triggered local spillover.                                                                                     |
| `ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER`    | `64`                       | Active plus pending consumer-route pressure per active local router before load-triggered spillover may attach more capacity.                                     |
| `ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE`       | `48`                       | Maximum active plus pending receiver fan-out per source and receiver worker before load-triggered spillover may attach capacity for the next join.                 |
| `ROOM_SPILLOVER_EGRESS_BITRATE_BPS`          | `750000000`                | Room egress bitrate pressure threshold for load-triggered spillover. `0` disables this signal.                                                                    |
| `ROOM_SPILLOVER_PACKET_LOOP_LAG_MS`          | `20`                       | Packet-loop lag pressure threshold for load-triggered spillover. `0` disables this signal.                                                                        |
| `ROOM_SPILLOVER_COMMAND_BACKLOG`             | `128`                      | Command backlog pressure threshold for load-triggered spillover. `0` disables this signal.                                                                        |
| `ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH`         | `128`                      | Relay mailbox pressure threshold for load-triggered spillover. `0` disables this signal.                                                                          |
| `ROOM_SPILLOVER_WORKER_PRESSURE`             | `80`                       | Worker pressure score threshold from 0 to 100 for load-triggered spillover. `0` disables this signal.                                                             |
| `ROOM_SPILLOVER_ACTIVATION_WINDOW`           | `2`                        | Consecutive join-placement pressure observations required before attaching another local router.                                                                  |
| `ROOM_SPILLOVER_COOLDOWN_WINDOW`             | `4`                        | Consecutive idle cleanup observations required before load-triggered mode drains idle spillover capacity.                                                        |
| `ROOM_MAX_ACTIVE_AUDIO_SPEAKERS`             | `4`                        | Maximum active audio speakers forwarded by room media policy.                                                                                                     |
| `ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER`      | `10`                       | Maximum active video source downloads one receiver may keep at once.                                                                                              |
| `AUTHENTICATION_TIMEOUT_MS`                  | `10000`                    | Timeout for user authentication in milliseconds.                                                                                                                  |
| `MAX_PRE_AUTH_WEBSOCKET_SESSIONS`            | `512`                      | Process-wide cap for upgraded WebSockets waiting for the first authenticated frame. Excess upgrades receive HTTP 503 before a socket task is spawned.             |
| `MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN` | `16`                       | Per-origin cap for upgraded WebSockets waiting for authentication. The origin is the direct peer IP unless `PROXY=true` trusts `x-forwarded-for`.                 |
| `USER_TIMEOUT_MS`                            | `10000`                    | Timeout for idle users in milliseconds.                                                                                                                           |
| `PING_INTERVAL_MS`                           | `60000`                    | Interval for signaling pings in milliseconds.                                                                                                                     |
| `USER_OUTBOUND_QUEUE_CAPACITY`               | `128`                      | Per-user WebSocket room-event queue depth. Overflow marks the user as a slow consumer and closes that WebSocket.                                                  |
| `USER_OUTBOUND_QUEUE_BYTE_CAPACITY`          | `2097152`                  | Per-user WebSocket room-event queued-byte budget. Broadcast payloads are capped at 16 KiB and are charged against this budget before enqueue.                     |
| `ROOM_SIZE`                                  | `100`                      | Maximum amount of concurrent users per room.                                                                                                                      |
| `RUST_LOG`                                   | `info`                     | SFU log level and filtering (standard `tracing-subscriber` env filter).                                                                                           |
| `TELEMETRY_LOG_FORMAT`                       | `compact`                  | Runtime log output mode (`compact` or `json`).                                                                                                                    |
| `TELEMETRY_SERVICE_NAME`                     | `o-sfu`                    | Service name attached to runtime telemetry metadata.                                                                                                              |
| `TELEMETRY_DEPLOYMENT_ENVIRONMENT`           | `local`                    | Deployment environment name attached to runtime telemetry metadata.                                                                                               |
| `TELEMETRY_SERVICE_INSTANCE_ID`              | `pid-<pid>`                | Optional stable instance identifier for logs and future traces.                                                                                                   |
| `TELEMETRY_MEDIA_QUALITY_INTERVAL_MS`        | `5000`                     | str0m transport-quality stats interval in milliseconds. Set to `0` to disable sampled media-quality telemetry.                                                   |
| `TELEMETRY_OTLP_ENDPOINT`                    | disabled                   | Optional OTLP/HTTP traces endpoint (for example `http://collector:4318` or `http://collector:4318/v1/traces`). Requires the default `otel-tracing` cargo feature. |
| `FEATURE_TRANSCRIPTION`                      | `false`                    | Enable transcription intent flags. WIP.                                                                                                                           |
| `FEATURE_AUDIO_RECORDING`                    | `false`                    | Enable audio recording intent flags. WIP.                                                                                                                         |
| `FEATURE_VIDEO_RECORDING`                    | `false`                    | Enable video recording intent flags. WIP.                                                                                                                         |
| `CODEC_OPUS`                                 | `true`                     | Enable Opus audio codec.                                                                                                                                          |
| `CODEC_PCMU`                                 | `false`                    | Enable G.711 mu-law audio codec.                                                                                                                                  |
| `CODEC_PCMA`                                 | `false`                    | Enable G.711 a-law audio codec.                                                                                                                                   |
| `CODEC_VP8`                                  | `true`                     | Enable VP8 video codec.                                                                                                                                           |
| `CODEC_H264`                                 | `false`                    | Enable H.264 video codec.                                                                                                                                         |
| `CODEC_H265`                                 | `false`                    | Enable H.265 video codec.                                                                                                                                         |
| `CODEC_VP9`                                  | `false`                    | Enable VP9 video codec.                                                                                                                                           |
| `CODEC_AV1`                                  | `false`                    | Enable AV1 video codec.                                                                                                                                           |
| `CODEC_AUDIO_PREFERENCE`                     | `opus,PCMU,PCMA`           | Optional comma-separated audio codec preference order. Missing codecs keep their default relative order.                                                          |
| `CODEC_VIDEO_PREFERENCE`                     | `VP8,H264,H265,VP9,AV1`    | Optional comma-separated video codec preference order. Missing codecs keep their default relative order.                                                          |
| `MAX_BITRATE_IN`                             | `8000000`                  | Maximum incoming bitrate in bps per user (upload).                                                                                                                |
| `MAX_BITRATE_OUT`                            | `10000000`                 | WebRTC desired-send-bitrate and BWE ceiling in bps per user (download). It is not a strict packet-forwarding cap.                                                 |
| `MAX_VIDEO_BITRATE`                          | `4000000`                  | Maximum bitrate in bps for the highest default simulcast video layer metadata.                                                                                    |

### WORK IN PROGRESS: Control-plane env variables

The control plane is experimental. The normal media-server Docker image does not
copy or expose it. Build it explicitly with `Dockerfile.control-plane` or run
`cargo run --bin o-sfu-control-plane` when testing scalable-topology work.
The control-plane HTTP API does not authenticate requests yet, security specifications are 
still to decide.

| Variable                     | Default          | Description |
| :--------------------------- | :--------------- | :---------- |
| `CONTROL_PLANE_BIND_ADDRESS` | `127.0.0.1:8071` | Socket address for the experimental control-plane listener. The control-plane image overrides it to `0.0.0.0:8071` for container-network tests. |


## Running the server and contributing

See [CONTRIBUTING.md](https://github.com/ThanhDodeurOdoo/o-sfu/blob/master/.github/CONTRIBUTING.md)


## Future work:

### Recording:

the o-sfu architecture helps a lot with recording compared to the previous version, since we now have complete control over the rtp packet dispatch, don't have to pipe streams through a transport layer and use ports and ffmpeg (at the real time recording step). we can just write packet frames to the disk directly and bypass all that old boilerplate.
another advantage is the router/recording topology, we have recording nodes that should just act as "opaque" media consuming "entities" and their locality shouldn't matter much so recording and forwarding could be physically separated.

also the recording feature on the official repo is still in active development so the API may change. This repo
will adapt accordingly.

### scalability (sharding)

rooms can have multiple workers and the load will be sharded across them (logic still wip). In the long term an optional controller server will allow the SFUs to share shards between them.

### Simulcast/SVC

Partial coverage

| Codec path                    | Support status                                                                                                            |
| :---------------------------- | :------------------------------------------------------------------------------------------------------------------------ |
| VP8 RID simulcast             | Production path, enabled by default with `CODEC_VP8=true`.                                                                |
| H.264 RID simulcast           | Production-ready for Chromium constrained baseline (`packetization-mode=1`, `profile-level-id=42e01f`) with RTX disabled. |
| VP9 hybrid/layered forwarding | WIP. `CODEC_VP9=true` is codec negotiation only.                                                                          |
| AV1 hybrid/layered forwarding | WIP. `CODEC_AV1=true` is codec negotiation only.                                                                          |

The browser bundle configures RID send encodings only for upload slots that
match a production simulcast path. Unsupported H.264 profiles, unsupported
browsers and optional codec-only configurations fall back to single-encoding
publication

## Tooling

## Monitoring

The `telemetry/` crate (sub dir) contain the telemetry tooling and serialization formats,
runtime log and trace setup, event and field schema, diagnostics DTOs and recent
event storage, the runtime metrics catalog, Prometheus text rendering and
Grafana node-graph JSON formatting.

you can check
https://github.com/ThanhDodeurOdoo/o-sfu-telemetry
it is an example of how to read and exploit the telemetry api

## Benchmarking

if you want to play with benchmarking you can fork
https://github.com/ThanhDodeurOdoo/o-sfu-benchmarks
