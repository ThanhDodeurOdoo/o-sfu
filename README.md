[![Tests](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml)
[![Client](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml)
[![Client Browser](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client-browser.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client-browser.yml)
[![Fuzzing](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/fuzzing.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/fuzzing.yml)
[![Formal Verification](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml)
[![CodeQL](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql)

# o-sfu

> [!WARNING]  
> NOT PRODUCTION READY! This repo is mostly made for experimenting with ideas. The readme may not be up to date, or be incorrect.
> Everything is up for refactor, some files are just testing prototypes.

MISSING FEATURES [Odoo SFU](https://github.com/odoo/sfu):
- Recording
- Local sharding
- Multi-server sharding

Comments may be a bit lacking (although I added some for the most important parts in recent commits) because I don't want to write big comments when the code is still changing a lot (the code could get outdated and I forget to change the comments).


## Env variables (based on odoo/sfu)

| Variable                         | Default                        | Required | Implemented | Description                                                                        |
| :------------------------------- | :----------------------------- | :------: | :---------: | :--------------------------------------------------------------------------------- |
| `PUBLIC_IP`                      | -                              |   Yes    |      ✅      | Used to establish WebRTC connections to the server.                                |
| `AUTH_KEY`                       | -                              |   Yes    |      ✅      | The base64 encoded encryption key used for JWT authentication.                     |
| `BIND_ADDRESS`                   | `0.0.0.0:8080`                 |    No    |      ✅      | HTTP and WebSocket listening address.                                              |
| `PROXY`                          | `false`                        |    No    |      ✅      | Set to true if behind a proxy to trust forwarding headers.                         |
| `RTC_MIN_PORT`                   | `40000`                        |    No    |      ✅      | Lower bound for the range of ports used by the RTC server (UDP).                   |
| `RTC_MAX_PORT`                   | `49999`                        |    No    |      ✅      | Upper bound for the range of ports used by the RTC server (UDP).                   |
| `RTC_MEDIA_WORKER_COUNT`         | `1`                            |    No    |      ✅      | Number of RTC media workers to spawn.                                              |
| `AUTHENTICATION_TIMEOUT_MS`      | `10000`                        |    No    |      ✅      | Timeout for session authentication in milliseconds.                                |
| `SESSION_TIMEOUT_MS`             | `10000`                        |    No    |      ✅      | Timeout for idle sessions in milliseconds.                                         |
| `PING_INTERVAL_MS`               | `60000`                        |    No    |      ✅      | Interval for signaling pings in milliseconds.                                      |
| `CHANNEL_SIZE`                   | `100`                          |    No    |      ✅      | Maximum amount of concurrent users per channel.                                    |
| `RUST_LOG`                       | `o_sfu=info,o_sfu_router=info` |    No    |      ✅      | SFU log level and filtering (standard `tracing-subscriber` env filter).            |
| `ENABLE_FEATURE_TRANSCRIPTION`   | `false`                        |    No    |      ✅      | Enable transcription feature flags.                                                |
| `ENABLE_FEATURE_AUDIO_RECORDING` | `false`                        |    No    |      ✅      | Enable audio recording feature flags.                                              |
| `ENABLE_FEATURE_VIDEO_RECORDING` | `false`                        |    No    |      ✅      | Enable video recording feature flags.                                              |
| `ENABLE_CODEC_OPUS`              | `true`                         |    No    |      ✅      | Enable Opus audio codec.                                                           |
| `ENABLE_CODEC_PCMU`              | `false`                        |    No    |      ✅      | Enable G.711 mu-law audio codec.                                                   |
| `ENABLE_CODEC_PCMA`              | `false`                        |    No    |      ✅      | Enable G.711 a-law audio codec.                                                    |
| `ENABLE_CODEC_VP8`               | `true`                         |    No    |      ✅      | Enable VP8 video codec.                                                            |
| `ENABLE_CODEC_H264`              | `false`                        |    No    |      ✅      | Enable H.264 video codec.                                                          |
| `ENABLE_CODEC_H265`              | `false`                        |    No    |      ✅      | Enable H.265 video codec.                                                          |
| `ENABLE_CODEC_VP9`               | `false`                        |    No    |      ✅      | Enable VP9 video codec.                                                            |
| `ENABLE_CODEC_AV1`               | `false`                        |    No    |      ✅      | Enable AV1 video codec.                                                            |
| `MAX_BUF_IN`                     | `0` (unlimited)                |    No    |      ❌      | Maximum incoming buffer size in bytes for SCTP messages per session.               |
| `MAX_BUF_OUT`                    | `0` (unlimited)                |    No    |      ❌      | Maximum outgoing buffer size in bytes for SCTP messages per session.               |
| `MAX_BITRATE_IN`                 | `8000000`                      |    No    |      ❌      | Maximum incoming bitrate in bps per session (upload).                              |
| `MAX_BITRATE_OUT`                | `10000000`                     |    No    |      ❌      | Maximum outgoing bitrate in bps per session (download).                            |
| `MAX_VIDEO_BITRATE`              | `4000000`                      |    No    |      ❌      | Maximum bitrate in bps for the highest simulcast video layer.                      |
| `LOG_TIMESTAMP`                  | `true`                         |    No    |      ❌      | Prefix timestamps to log lines.                                                    |
| `LOG_COLOR`                      | TTY detection                  |    No    |      ❌      | Colors log lines based on their level.                                             |
| `DEBUG`                          | -                              |    No    |      ❌      | Used by the [debug](https://www.npmjs.com/package/debug) module (e.g., `DEBUG=*`). |
| `WORKER_LOG_LEVEL`               | `none`                         |    No    |      ❌      | Mediasoup worker log level. Requires `DEBUG` to be active.                         |
| `DATA_PATH`                      | `/tmp/odoo_sfu`                |    No    |      ❌      | Base path for SFU local storage (`recordings`, `resources`, `debug` subfolders).   |


## random thoughts

## Recording:

the o-sfu architecture helps a lot with recording compared to the previous version, since we now have complete control over the rtp packet dispatch, don't have to pipe streams through a transport layer and use ports and ffmpeg (at the real time recording step). we can just write packet frames to the disk directly and bypass all that old boilerplate.
another advantage is the router/recording topology, we have recording nodes that should just act as "opaque" media consuming "entities" and their locality shouldn't matter much so recording and forwarding could be physically separated.

also the recording feature on the official repo is still in active development so the API may change, and this repo
will adapt accordingly.

## scalability (sharding)

channels will have multiple routers and the load will be sharded across them. In the long term an optional controller server will
allow the SFUs to share shards between them.

## Observability

There is already groundwork done for observability with runtime/metrics, but there is still a lot to do (like connect to real end-oints), and build a deeper observability system, some random thoughts:
- Metrics, logs, traces, and diagnostics must live at runtime boundaries, not in `router/`.
- `router/` may expose events or state needed by outer layres, but it must not know about Prometheus, OTLP, log shipping, or collector protocols.
- Call sites must speak in domain terms such as "join accepted", "offer applied", or "relay overload dropped", not in backend-specific terms such as "increment counter X".
- No single type may simultaneously own metric storage, log formatting, OTLP export wiring, and subsystem-specific business semantics.
- `/metrics` and `/v1/stats` keep distinct roles:
  - `/metrics` is the authoritative low-cardinality time-series surface.
  - `/v1/stats` remains a compatibility snapshot surface.


## crypto

investigate chacha20 instead of classical dtls/srtp

## API documentation

Can copy the one form odoo/sfu since it's roughly the same (Bundle API and http API)
