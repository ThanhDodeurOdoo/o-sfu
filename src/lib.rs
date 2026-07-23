//! o-sfu is a Selective Forwading Unit for audio/video calls
//!
//! This handles room admission, routing topology, media policy, packet
//! forwarding, signaling and telemetry for applications that need a dedicated SFU server.
//!
//! root crate (`o-sfu/src`) contains:
//!
//! - configuration loading through [`config::Config`] and [`Runtime`]
//!   construction through [`Runtime::new`]
//! - room provisioning, disconnect, statistics, metrics and diagnostics
//!   through [`http`]
//! - room admission through [`core::prelude::SfuCore`] and user-session
//!   orchestration through [`core::prelude::MediaSession`]
//! - process lifecycle through [`run`], [`Runtime::serve_listener`],
//!   [`core::server::metrics::RuntimeMetrics`] and structured tracing
//!
//!
//! # architecture
//!
//! ```text
//!     +---------------------+   +---------------------------+
//!     | HTTP control API    |   |  WebSocket user sessions  |
//!     +-----------+---------+   +---------+-----------------+
//!                 |                       |
//!                 v                       v
//!            RoomManager          application::User / MediaSession
//!                 |                       |
//!                 |                       |
//!                 |                       |
//!                 +----------+------------+
//!                            |
//!                            v
//!                       core::Room <--------> router::Router
//!                            |
//!                            v
//!                  core::MediaTransport
//!                          | | |
//!                          | | |  (multi-threaded)
//!                          | | |
//!                          v v v
//!                       core::Worker
//!  UDP Socket IN ----->  RTP fanout ------>  relays / sinks / UDP socket OUT
//! ```
//!
//! follow the steps through [`Runtime`],
//! [`core::server::room::RoomManager`], [`core::prelude::MediaSession`],
//! [`core::server::room::Room`], [`o_sfu_router::Router`]
//! and [`core::server::transport::MediaTransport`]
//!
//! room media control reaches `MediaTransport` as one semantic plan, then runs
//! through bounded receiver-BWE, producer, source-gate and consumer follow-up
//! worker phases while relay control and resolved teardown keep separate paths
//!
//! ## sub crates
//!
//! | crate | role |
//! | --- | --- |
//! | [`o_sfu_rfc`] | RFC-backed JWT, RTP, RTCP, SDP and WebRTC consts/types |
//! | `o-sfu-model` | shared call data surfaced through [`o_sfu_protocol::wire::UserId`], [`o_sfu_protocol::wire::StreamType`], [`o_sfu_protocol::wire::RecordingState`] and [`o_sfu_protocol::wire::WebSocketCloseCode`] |
//! | [`o_sfu_router`] | sans-I/O [`o_sfu_router::Router`] facade for room placement and routed media lifetimes |
//! | [`o_sfu_core`] | room engine, [`core::prelude::SourcePolicy`], recording taps, resolved teardown effects and [`core::server::transport::MediaTransport`] projection |
//! | [`o_sfu_protocol`] | sans-I/O [`o_sfu_protocol::host::ProtocolCore`] and typed [`o_sfu_protocol::host::Command`] values |
//! | [`o_sfu_telemetry`] | tracing setup, [`o_sfu_telemetry::metrics::RuntimeMetrics`], diagnostics response types, [`o_sfu_telemetry::prometheus::render_prometheus`] and graph payloads |
//!
//! ## client bundle
//!
//! The server that implements the call (like odoo) imports the generated browser bundle from `crates/client` (part of the release artifacts)
//! and implements the calls features with `SfuClient`.
//!
//! `SfuClient` exposes a small and simple API with `connect`, `publish`, `subscribe`,
//! recording, stats and update events.
//! signaling state stays in [`o_sfu_protocol::host::ProtocolCore`] and returns
//! ordered [`o_sfu_protocol::host::CommandBatch`] values
//! `BrowserRuntime` executes the
//! [`o_sfu_protocol::host::HostCommand`] values against browser `WebSocket`,
//! `RTCPeerConnection` and timer APIs
//!
//! ```text
//! SfuClient public API
//!   connect, disconnect, updateUpload, updateDownload, updateInfo
//!        |
//!        v
//! BrowserRuntime
//!        |
//!        v
//! ProtocolCore
//!   input envelope -> CommandBatch -> HostCommand
//!        |
//!        v
//! WebSocket, RTCPeerConnection, timers
//! ```
//!
//! read `crates/client/API.md` for the public TypeScript surface and
//! `crates/client/README.md` for the client file map
//!
//! # runtime
//!
//! [`Runtime`] manages the full process lifecycle,
//! request handlers receive a smaller internal state handle so HTTP extractors
//! and WebSocket loops cannot depend on boot details or shutdown ownership
//!
//! # shutdown and teardown
//!
//! [`Runtime::serve_listener`] stops listener acceptance when its shutdown
//! future resolves. It then closes the session tracker, sends close code 1001,
//! drains every tracked WebSocket and stops background tasks plus RTC workers within
//! [`config::HttpConfig::shutdown_timeout_ms`]. Dropping the server future
//! cancels the same runtime tokens.
//!
//! ```text
//! Runtime::serve_listener
//!     |
//!     +-> stop listener
//!     +-> close tracker and cancel sessions
//!     +-> wait for tracker emptiness
//!     +-> stop source-policy sync and media workers
//!     |
//!     +-> return or ServeError::ShutdownIncomplete
//! ```
//!
//! user and media teardown is explicit async work,
//! closing a WebSocket user awaits resolved teardown through
//! [`core::server::room::RoomManager`], [`core::server::room::UserCloseReason`]
//! and [`core::server::transport::MediaTransport`]
//! the current room is removed immediately once final teardown and its
//! lifecycle lease finish
//! drop paths are used to detect missed teardown, not to complete normal media
//! teardown
//!
//! # HTTP and WebSocket admission
//!
//! applications create rooms through HTTP while clients join through
//! the WebSocket,
//! both paths are authenticated before they reach room state
//!
//!
//! the HTTP controller parses server-to-server requests using [`http::CreateRoomQuery`]
//! and validates authorization via [`auth::HttpRoomClaims`] before returning a [`http::RoomResponse`].
//!
//! the WebSocket client connection frame is parsed by [`websocket::decode_auth_payload_text`],
//! enforcing bounds like [`auth::MAX_JWT_TOKEN_BYTES`] and validating user authorization
//! via [`auth::WebSocketConnectClaims`] before establishing a session.
//!
//! decoded JWT claims are not trusted until the token is verified with the key
//! selected for that room,
//! an unsigned room hint may select verification material, but it does not
//! become authenticated identity or room access
//!
//! # room, router and transport ownership
//!
//! room transitions are planned while holding short exclusive room state locks,
//! async transport and diagnostics work is executed later through effect plans
//!
//! ```text
//! room state lock held                  lock released
//! +--------------------------------+    +------------------------------+
//! | validate user and connection   |    | MediaTransport commands      |
//! | commit room topology           |    | diagnostics and metrics      |
//! | capture RoomEffects            |--->| websocket output             |
//! |                                |    | idempotent teardown           |
//! +--------------------------------+    +------------------------------+
//! ```
//!
//! [`o_sfu_router::Router`] is the sole pure, sans-I/O, synchronous router facade.
//! it owns exact user-to-connection placement plus private local-router graphs
//! keyed by connection and canonical producer or consumer ids. cross-router
//! receiver shadows are foreign local sessions derived from active consumer
//! dependencies and disappear with their final consumer.
//!
//! [`core::prelude::SfuCore`] owns admitted media-session construction and
//! [`core::prelude::MediaSession`] owns the bridge from room intent to
//! transport effects,
//! they decide which [`core::prelude::SourcePublishIntent`] values exist, which
//! receivers want them, which [`core::server::transport::SourcePacketGate`]
//! should be installed and which resolved teardown operations must run
//!
//! [`core::server::transport::SessionOffer`],
//! [`core::server::transport::SessionUploadSlot`] and
//! [`core::server::transport::SessionUploadEncoding`] are the canonical public
//! offer family
//! [`core::prelude::NegotiationOffer`], [`core::prelude::UploadSlot`] and
//! [`core::prelude::UploadEncoding`] are renamed re-exports of those same types
//! only `WaitingForAnswer(InFlightOffer)` owns queued publishes plus the
//! follow-up latch
//! parsed client capability presence is the direct room publish and consume
//! readiness fact with its first accepted commit named `became_ready`
//! `ServerMediaNegotiation`, room-staged reservations and worker-pinned
//! `SdpPendingOffer` keep their separate failure boundaries
//!
//! [`core::server::transport::MediaTransport`] owns final teardown semantics
//! missing workers or resources are successful no-ops
//! ownership and invariant failures are terminal and trigger an awaited
//! session-close fallback
//! no room or runtime teardown retry state exists
//!
//! # packet path
//!
//! the [`core::server::transport::MediaTransport`] owns worker-local packet loops.
//! those loops receive UDP datagrams, drive WebRTC state, apply route tables,
//! forward RTP and publish bounded metrics
//!
//! ```text
//! UDP datagram
//!     |
//!     v
//! worker ingress
//!     |
//!     +-> remote-address cache
//!     +-> bounded fallback
//!     +-> source pin
//!     |
//!     v
//! str0m input and output drain
//!     |
//!     v
//! RouteTable
//!     |
//!     +-> packet gate
//!     |     +-> RID or layer policy
//!     |     +-> source activity
//!     |
//!     +-> local fanout
//!     +-> relay fanout
//!     +-> packet sinks
//! ```
//!
//! packet loops do not own room membership,
//! they consume stable transport keys and route controls projected from room and router state,
//! that split keeps hot packet work close to worker-local state while keeping
//! policy changes in the room engine
//!
//! # observability
//!
//! for documentation on the operator-facing telemetry http API, see: [`http::telemetry`]
//!
//! managed by the [`o_sfu_telemetry`] sub crate.
//!
//! the telemetry crate handles:
//!
//! - low-cardinality [`o_sfu_telemetry::metrics::RuntimeMetrics`] and
//!   [`o_sfu_telemetry::prometheus::render_prometheus`]
//! - structured event names in [`o_sfu_telemetry::schema`] used by tracing
//! - current room, user, worker and media diagnostics response types
//! - structured lifecycle events retained by the configured log sink
//! - [`o_sfu_telemetry::graph`] payloads used by the diagnostics UI and
//!   Grafana-style views
//!
//! provisioned dashboards and Prometheus rules are maintained in the sibling
//! `o-sfu-telemetry` repository
//! terminal teardown observability requires a separate delivery there
//!
//! # scaling
//!
//! rooms use one [`o_sfu_router::Router`] facade and can opt into additional
//! same-process local routers through [`config::RoomWorkerPolicy`]
//!
//! note: it is later possible to extend the SFU for cross server scaling, but it's not a priority.
//!
//! # feature flags
//!
//! the root package has a small feature surface,
//! the default feature enables `otel-tracing`, which turns on OpenTelemetry
//! tracing support through [`o_sfu_telemetry::TraceExportConfig`].
//! other features are for tests/bemchanarking.
//! core media behavior is configured at runtime through [`config`], not cargo
//! features
//!
//! # reading map
//!
//! - [`run`] and [`Runtime`] own boot, serving, background tasks and shutdown
//! - [`config`] is the environment-to-runtime boundary
//!   lower crates receive typed options, not raw environment state
//! - [`auth`], [`http`] and [`websocket`] are the admision edge
//!   they bound request shape, frame size and identity before room state is
//!   touched
//! - [`crate::core`] turns accepted control-plane intent into room mutations
//!   and transport effects
//! - [`o_sfu_protocol::host::ProtocolCore`] keeps browser signaling sans-I/O
//!   host commands are ordered effects for the browser integration
//! - [`core::server::transport::MediaTransport`] is the packet boundary
//!   workers consume route state that room and router state already approved
//!
//! start with [`Runtime`] when following process startup, HTTP serving or
//! shutdown
//! then read [`http`] for the HTTP control-plane contract and [`websocket`] for
//! frame decoding
//!
//! for media behavior,read [`core::prelude::SfuCore`],
//! [`core::prelude::MediaSession`], [`core::prelude::SourcePolicy`],
//! [`core::server::room::RoomManager`] and
//! [`core::server::transport::MediaTransport`]
//! for pure routing invariants, read [`o_sfu_router::Router`]
//! for browser signaling, read [`o_sfu_protocol::host::ProtocolCore`],
//! [`o_sfu_protocol::host::CommandBatch`] and
//! [`o_sfu_protocol::host::HostCommand`]

pub mod config;
pub mod core {
    pub use o_sfu_core::{prelude, server};
}
pub(crate) mod application;
mod runtime;

pub mod auth {
    pub use crate::runtime::auth::{
        AuthenticationError, HttpDisconnectClaims, HttpRoomClaims, MAX_JWT_TOKEN_BYTES,
        RegisteredJwtClaims, WebSocketConnectClaims, sign, verify,
    };
}

pub mod http {
    pub use crate::runtime::{
        http_server::contract::{
            CreateRoomQuery, IncomingBitRateStatsResponse, NoopResponse, RoomResponse,
            StatsResponse, route,
        },
        request_origin::{RequestOrigin, resolve_request_origin},
    };

    /// operator-facing metrics and diagnostics contracts.
    pub mod telemetry {
        /// Prometheus metric scrape contract.
        ///
        /// `GET` [`metrics::PATH`] returns `200 OK` Prometheus text exposition
        /// with [`metrics::CONTENT_TYPE`] and requires no application-layer
        /// authentication.
        /// operators should restrict access in the deployment boundary.
        ///
        /// this is a scrape endpoint.
        /// configure Prometheus to scrape [`metrics::PATH`], then issue `PromQL`
        /// queries to Prometheus.
        /// histogram families render `<name>_bucket` with the additional `le`
        /// label plus `<name>_sum` and `<name>_count`.
        ///
        /// # scrape and query
        ///
        /// Prometheus scrapes o-sfu directly.
        ///
        /// ```yaml
        /// scrape_configs:
        ///   - job_name: o-sfu
        ///     metrics_path: /metrics
        ///     static_configs:
        ///       - targets: ["o-sfu:8070"]
        /// ```
        ///
        /// the endpoint returns Prometheus text exposition.
        ///
        /// ```text
        /// # HELP osfu_rooms_active Current number of active rooms owned by this runtime.
        /// # TYPE osfu_rooms_active gauge
        /// osfu_rooms_active 3
        /// # TYPE osfu_worker_rtp_packets_total counter
        /// osfu_worker_rtp_packets_total{media_worker_id="0",direction="ingress"} 1240
        /// ```
        ///
        /// Grafana then sends `PromQL` to Prometheus, rather than to o-sfu.
        /// these examples cover a gauge, counter and histogram.
        ///
        /// ```promql
        /// sum(osfu_users_active)
        /// sum by (stage) (rate(osfu_ws_connections_total[5m]))
        /// histogram_quantile(
        ///   0.95,
        ///   sum by (le, route) (rate(osfu_http_request_duration_seconds_bucket[10m]))
        /// )
        /// ```
        ///
        /// read the [`metrics::MetricName`] variants below to find every
        /// exported name and its meaning.
        /// query [`metrics::PATH`] to see each family's `HELP`, `TYPE`, label
        /// keys and current label values before building selectors.
        pub mod metrics {
            pub use o_sfu_telemetry::{
                metrics::MetricName, prometheus::PROMETHEUS_CONTENT_TYPE as CONTENT_TYPE,
            };

            pub use crate::http::route::METRICS as PATH;
        }

        /// JSON diagnostics contract.
        ///
        /// every constant in [`diagnostics::route`] is a `GET` endpoint.
        /// when a diagnostics token is configured, requests must send it as an
        /// `Authorization: Bearer <token>` header.
        /// without a configured token, the server permits access only when its
        /// HTTP listener is bound to a loopback address.
        /// successful requests return `200 OK` JSON.
        ///
        /// # routes and parameters
        ///
        /// | request | JSON response | parameter source |
        /// | --- | --- | --- |
        /// | `GET /internal/diagnostics/summary` | one [`diagnostics::DiagnosticsSummaryResponse`] | none |
        /// | `GET /internal/diagnostics/rooms` | array of [`diagnostics::DiagnosticsRoomSummary`] | none |
        /// | `GET /internal/diagnostics/workers` | array of [`diagnostics::DiagnosticsWorkerSummary`] | none |
        /// | `GET /internal/diagnostics/rooms/{uuid}` | one [`diagnostics::DiagnosticsRoomDetail`] | `uuid` from the rooms response |
        /// | `GET /internal/diagnostics/rooms/{uuid}/users` | array of [`diagnostics::DiagnosticsUserSummary`] | `uuid` from the rooms response |
        /// | `GET /internal/diagnostics/rooms/{uuid}/users/{id}` | one [`diagnostics::DiagnosticsUserDetail`] | `uuid` from rooms and `userKey` from room users |
        /// | `GET /internal/diagnostics/node-graph/rooms/{uuid}` | `{ "nodes": [], "edges": [] }` | `uuid` from the rooms response |
        /// | `GET /internal/diagnostics/node-graph/rooms/{uuid}/users/{id}` | `{ "nodes": [], "edges": [] }` | `uuid` from rooms and `userKey` from room users |
        ///
        /// `userId` may be a JSON number or string.
        /// `userKey` is always the string to put into `{id}`.
        /// URL-encode both path values before substitution.
        ///
        /// # summary request and response
        ///
        /// ```text
        /// GET /internal/diagnostics/summary HTTP/1.1
        /// Host: o-sfu:8070
        /// Authorization: Bearer <diagnostics-token>
        /// Accept: application/json
        ///
        /// HTTP/1.1 200 OK
        /// Content-Type: application/json
        ///
        /// {
        ///   "roomsActive": 1,
        ///   "publicationsActive": 1,
        ///   "recordingRoomsActive": 0,
        ///   "usersActive": 2,
        ///   "subscriptionsActive": 1,
        ///   "transport": {
        ///     "connectedUsers": 2,
        ///     "disconnectedUsers": 0,
        ///     "totalUsers": 2,
        ///     "unknownUsers": 0
        ///   }
        /// }
        /// ```
        ///
        /// # JavaScript fetch example
        ///
        /// ```javascript
        /// const origin = "http://o-sfu:8070";
        /// const headers = {
        ///   Authorization: `Bearer ${process.env.DIAGNOSTICS_AUTH_TOKEN}`,
        /// };
        ///
        /// async function getJson(path) {
        ///   const response = await fetch(`${origin}${path}`, { headers });
        ///   if (!response.ok) {
        ///     throw new Error(`${response.status} ${await response.text()}`);
        ///   }
        ///   return response.json();
        /// }
        ///
        /// async function main() {
        ///   const rooms = await getJson("/internal/diagnostics/rooms");
        ///   const roomUuid = encodeURIComponent(rooms[0].uuid);
        ///   const room = await getJson(`/internal/diagnostics/rooms/${roomUuid}`);
        ///   const users = await getJson(`/internal/diagnostics/rooms/${roomUuid}/users`);
        ///   const userKey = encodeURIComponent(users[0].userKey);
        ///   const graph = await getJson(
        ///     `/internal/diagnostics/node-graph/rooms/${roomUuid}/users/${userKey}`,
        ///   );
        ///
        ///   console.log(room.summary, room.users, room.sources);
        ///   console.log(graph.nodes, graph.edges);
        /// }
        ///
        /// main().catch((error) => {
        ///   console.error(error);
        ///   process.exitCode = 1;
        /// });
        /// ```
        ///
        /// the rooms response has this shape.
        ///
        /// ```json
        /// [
        ///   {
        ///     "createDate": "2026-07-15T10:20:30.000Z",
        ///     "mediaWorkerId": 0,
        ///     "publicationCount": 1,
        ///     "recordingState": {
        ///       "recording": false,
        ///       "audio": false,
        ///       "transcription": false,
        ///       "video": false
        ///     },
        ///     "remoteAddress": "203.0.113.10",
        ///     "sourceCount": 1,
        ///     "userCount": 2,
        ///     "subscriptionCount": 1,
        ///     "transport": {
        ///       "connectedUsers": 2,
        ///       "disconnectedUsers": 0,
        ///       "totalUsers": 2,
        ///       "unknownUsers": 0
        ///     },
        ///     "uuid": "550e8400-e29b-41d4-a716-446655440000",
        ///     "webRtcEnabled": true
        ///   }
        /// ]
        /// ```
        ///
        /// the room users response has this shape.
        ///
        /// ```json
        /// [
        ///   {
        ///     "audioIncomingBitrateBps": 32000,
        ///     "cameraIncomingBitrateBps": 600000,
        ///     "connectionId": 91,
        ///     "health": "connected",
        ///     "incomingBitrateBps": 632000,
        ///     "mediaWorkerId": 0,
        ///     "publicationCount": 2,
        ///     "roomId": "550e8400-e29b-41d4-a716-446655440000",
        ///     "screenIncomingBitrateBps": 0,
        ///     "subscriptionCount": 1,
        ///     "userId": 42,
        ///     "userKey": "42"
        ///   }
        /// ]
        /// ```
        ///
        /// the response structs below list every field in each payload.
        /// wire names are `camelCase`, unless a field documents an exception.
        ///
        /// # Grafana Infinity (plugin)
        ///
        /// the Grafana Infinity data source plugin queries these HTTP JSON routes.
        /// keep the bearer token in the datasource's secure header.
        ///
        /// ```yaml
        /// jsonData:
        ///   httpHeaderName1: Authorization
        ///   allowedHosts:
        ///     - http://o-sfu:8070
        /// secureJsonData:
        ///   httpHeaderValue1: "Bearer <diagnostics-token>"
        /// ```
        ///
        /// a panel target percent-encodes both path variables.
        ///
        /// ```json
        /// [
        ///   {
        ///     "refId": "A",
        ///     "type": "json",
        ///     "source": "url",
        ///     "format": "node-graph-nodes",
        ///     "url": "http://o-sfu:8070/internal/diagnostics/node-graph/rooms/${room_uuid:percentencode}/users/${user_key:percentencode}",
        ///     "url_options": { "method": "GET", "data": "" },
        ///     "root_selector": "nodes"
        ///   },
        ///   {
        ///     "refId": "B",
        ///     "type": "json",
        ///     "source": "url",
        ///     "format": "node-graph-edges",
        ///     "url": "http://o-sfu:8070/internal/diagnostics/node-graph/rooms/${room_uuid:percentencode}/users/${user_key:percentencode}",
        ///     "url_options": { "method": "GET", "data": "" },
        ///     "root_selector": "edges"
        ///   }
        /// ]
        /// ```
        ///
        /// populate `room_uuid` from the rooms response `uuid` field.
        /// populate `user_key` from the room users response `userKey` field.
        ///
        /// User detail is room-scoped because the same user key can be active
        /// in several rooms.
        pub mod diagnostics {
            pub use o_sfu_telemetry::diagnostics::{
                DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSummaryResponse,
                DiagnosticsUserDetail, DiagnosticsUserSummary, DiagnosticsWorkerSummary,
            };

            pub use crate::http::route::diagnostics as route;
        }
    }
}

pub mod websocket {
    pub use crate::runtime::websocket_server::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
        MAX_CLIENT_FRAME_BYTES, decode_auth_payload_text, decode_client_batch,
    };
}

pub use self::runtime::{Runtime, ServeError, run};
