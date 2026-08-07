//! A Selective Forwarding Unit (SFU) for audio/video calls.
//!
//! A SFU receives each participant's media once and selectively forwards it to
//! the others, so an N-party call costs one upload per sender rather than one
//! per listener and no stream is transcoded or mixed. `o-sfu` runs this model as
//! a dedicated server that handles room admission, routing topology, media
//! policy, packet forwarding, signaling and telemetry. Applications provision
//! rooms over HTTP, browsers connect over WebSocket and media travels over UDP
//! with `str0m` terminating ICE, DTLS and SRTP.
//!
//! # High-Level Features
//!
//! - **Separation of concerns**: Pure sans-I/O routing policy separated from worker-local packet loops.
//! - **Robust state management**: Room transitions run under short exclusive locks. Side effects are planned then executed asynchronously.
//! - **Deterministic signaling**: Browser signaling remains in a sans-I/O `ProtocolCore`, yielding ordered commands for predictable WebRTC orchestration.
//! - **Predictable teardown**: Explicit async cleanup ensures all resources are released or forcefully drained.
//!
//! # Core Concepts
//!
//! `o-sfu` is built around a strict separation of three planes: the **control plane** (admission and room policy), the **routing plane** (the placement graph that maps users to connections) and the **packet plane** (RTP forwarding on worker loops).
//!
//! - **[`Runtime`]**: Owns the process lifecycle, the HTTP/WebSocket servers and graceful shutdown.
//! - **[`core::server::room::Room`]**: The control plane boundary for a set of participants. It commits membership and media relationships.
//! - **[`o_sfu_router::Router`]**: The routing plane, a pure, sans-I/O engine owning the placement graph that maps users to connections.
//! - **[`core::prelude::MediaSession`]**: Orchestrates a user's connection, bridging room intent to transport effects.
//! - **[`core::server::transport::MediaTransport`]**: Owns the media workers and hides their threading model. Each worker holds a packet loop and applies projected routes to incoming datagrams.
//!
//! # Architecture
//!
//! Control and routing decisions happen in the upper half. Packet loops execute them in the lower half against UDP datagrams.
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
//! # Admission Edge
//!
//! Applications provision rooms via HTTP, while clients join via WebSocket. Both paths require JWT authentication before admission.
//!
//! ```text
//! App HTTP POST /room/create -> verify auth -> [RoomManager] -> alloc Room
//! WebSocket Client Connect -> verify auth -> [Room] -> admit User
//! ```
//!
//! - **HTTP**: Parses server-to-server requests using [`http::CreateRoomQuery`]. Verifies [`auth::HttpRoomClaims`]. The first verified request for an issuer fixes the room's signing key.
//! - **WebSocket**: Client connection frames are decoded by [`websocket::decode_auth_payload_text`]. A hint selects a candidate key. Claims are verified, normalized into [`auth::WebSocketConnectClaims`] and trusted for access.
//!
//! # Security Model
//!
//! `o-sfu` secures two planes independently. Application-layer JWTs gate room
//! admission on the control plane. `str0m` encrypts media on the packet plane
//! with DTLS-SRTP. Signaling transport confidentiality is terminated at the
//! deployment edge rather than in process.
//!
//! ```text
//! control plane    JWT HS256           admission trust
//! packet plane     DTLS-SRTP (str0m)   media confidentiality
//! signaling wire   TLS at edge         transport confidentiality
//! ```
//!
//! ## JWT Admission
//!
//! Tokens are `HS256` only. [`auth::verify`] rejects any other `alg`, checks the
//! HMAC in constant time and enforces `exp`, `nbf` plus an `iat` future-skew
//! bound. It caps token size at [`auth::MAX_JWT_TOKEN_BYTES`]. Two keys scope
//! trust:
//!
//! - **Server-to-server key**: `AUTH_KEY` (base64, at least 32 bytes) verifies
//!   the HTTP [`http::CreateRoomQuery`] path through [`auth::HttpRoomClaims`] and
//!   [`auth::HttpDisconnectClaims`]. See [`config`].
//! - **Per-room key**: the first verified create-room request pins the room
//!   signing key from the `key` claim in [`auth::HttpRoomClaims`]. WebSocket
//!   [`auth::WebSocketConnectClaims`] verify against that room key, never against
//!   `AUTH_KEY`.
//!
//! Token carriage differs per surface: HTTP room creation uses the
//! `Authorization` header, HTTP disconnect uses the request body and the
//! WebSocket client sends a first-frame auth envelope decoded by
//! [`websocket::decode_auth_payload_text`]. An unverified decode selects the
//! candidate room key, then the same token is re-verified against it. A verified
//! [`auth::WebSocketConnectClaims`] must carry the `room_id` of its target room,
//! which blocks replay of a token minted for another room.
//!
//! Admission establishes identity and room scope. It does not enforce the
//! per-user `permissions` claim, which room state collapses to a marker.
//!
//! ## Signaling Ingress
//!
//! Every authenticated client frame passes through the same decoder as the auth
//! frame, [`websocket::decode_client_batch`], which bounds parser work with
//! static caps: [`websocket::MAX_CLIENT_FRAME_BYTES`] per frame and
//! [`websocket::MAX_CLIENT_BATCH_ENVELOPES`] per batch. The Axum upgrade applies
//! the frame cap at the socket and the decoder re-checks it. Oversized frames,
//! oversized batches, malformed JSON, ambiguous routing metadata and unknown
//! protocol tags reject as [`websocket::ClientBatchDecodeError`] and close the
//! socket with a protocol-error code.
//!
//! Two further bounds guard against resource exhaustion:
//!
//! - **Pre-auth admission**: global and per-origin permits cap concurrent
//!   unauthenticated sockets and return `503` once exhausted. See [`config`].
//! - **Outbound backpressure**: per-user fanout is a bounded queue by message
//!   count and by bytes. A consumer that falls behind is closed rather than
//!   buffered without limit.
//!
//! A first-frame auth timeout rejects clients that never authenticate. A
//! ping/pong health loop closes clients that stop responding. There is no
//! per-session request-rate budget: the size, count and backpressure caps bound
//! the work rather than metering a rate.
//!
//! ## Media Transport
//!
//! `str0m` terminates ICE, DTLS and SRTP over UDP with the `aws-lc-rs` crypto
//! backend. `o-sfu` builds and drives the `str0m` session but implements no DTLS
//! or SRTP itself: it forwards already-decrypted RTP between sessions and hands
//! outbound RTP back to `str0m` for SRTP protection.
//!
//! - **Keying**: the DTLS handshake derives SRTP keys per RFC 5764 DTLS-SRTP.
//! - **Certificate**: `str0m` generates a self-signed certificate at session
//!   build and advertises `a=fingerprint:sha-256` in the SDP offer. It binds and
//!   verifies the remote fingerprint when the answer is accepted.
//! - **ICE**: `o-sfu` runs ICE-lite with `a=setup:actpass` and advertises
//!   `ANNOUNCED_IP`, so media UDP must reach the host directly.
//!
//! ## Signaling Transport
//!
//! HTTP and WebSocket are served in plaintext in process. HTTPS and WSS are
//! terminated by an external reverse proxy, so a forwarded scheme and client
//! address are trusted only when the proxy is trusted through [`config`]
//! (`PROXY`). Diagnostics require a bearer token or a loopback listener. The
//! metrics endpoint carries no application authentication and must be restricted
//! at the deployment boundary.
//!
//! # Room and Router Ownership
//!
//! Room transitions are planned while holding short exclusive room state locks. Async transport and diagnostics work is executed later through effect plans.
//!
//! ```text
//! room state lock held                  lock released
//! +--------------------------------+    +------------------------------+
//! | validate user and connection   |    | MediaTransport commands      |
//! | commit room topology           |    | diagnostics and metrics      |
//! | capture RoomEffects            |--->| websocket output             |
//! |                                |    | idempotent teardown          |
//! +--------------------------------+    +------------------------------+
//! ```
//!
//! [`o_sfu_router::Router`] owns exact user-to-connection placement. Receiver shadows are foreign local sessions derived from active consumer dependencies, disappearing with their final consumer.
//!
//! # Signaling and Client Bundle
//!
//! Browsers interact with `o-sfu` through a minimal `SfuClient` API (`connect`, `publish`, `subscribe`).
//! Signaling state stays in [`o_sfu_protocol::host::ProtocolCore`] and yields ordered [`o_sfu_protocol::host::CommandBatch`] values. The WASM bridge projects these into `HostCommand` values to drive browser `WebSocket` and `RTCPeerConnection` APIs.
//!
//! ```text
//! SfuClient (public API)
//!        |
//!        v
//! BrowserRuntime
//!        |
//!        v
//! ProtocolCore (sans-I/O) -> CommandBatch
//!        |
//!        v
//! WASM projection -> HostCommand
//!        |
//!        v
//! BrowserRuntime -> WebSocket, RTCPeerConnection, timers
//!        ^                                        |
//!        +--------------- browser events ---------+
//! ```
//!
//! # Packet Path
//!
//! [`core::server::transport::MediaTransport`] owns the media workers, which hold the packet loops. These loops receive UDP datagrams, drive WebRTC state (`str0m`), apply route tables and forward RTP.
//!
//! ```text
//! UDP datagram
//!     |
//!     v
//! worker ingress (fallback, source pin)
//!     |
//!     v
//! str0m input and output drain
//!     |
//!     v
//! RouteTable
//!     |
//!     +-> packet gate (RID/layer policy, source activity)
//!     |
//!     +-> local fanout
//!     +-> relay fanout
//!     +-> packet sinks
//! ```
//!
//! `str0m` handles ICE, DTLS and SRTP. `o-sfu` resolves source facts, then plans origin packet sinks before applying aggregate demand and source policy. Receiver and relay gates narrow each destination. Same-process relay passes shared payload data to another worker for local delivery.
//!
//! Worker BWE and audio observations feed into room source policy, which updates route gates for later packets.
//!
//! ```text
//! worker BWE and audio observations
//!                |
//!                v
//!        room source policy
//!                |
//!                v
//!   route gates for later packets
//! ```
//!
//! # Shutdown and Teardown
//!
//! Teardown is explicit async work. [`Runtime::serve_listener`] stops listener acceptance, drains tracked web sockets and stops background tasks within [`config::HttpConfig::shutdown_timeout_ms`].
//!
//! ```text
//! Runtime::serve_listener
//!     |
//!     +-> stop listener
//!     +-> close tracker and cancel sessions
//!     +-> wait for tracker emptiness
//!     +-> stop source-policy sync and media workers
//! ```
//!
//! Missing worker-local sessions or media during teardown are successful no-ops. Unavailable workers or ownership mismatches are terminal.
//!
//! # Observability
//!
//! Monitored through the [`o_sfu_telemetry`] sub-crate. See [`http::telemetry`] for the HTTP contracts.
//!
//! - **Metrics**: [`http::telemetry::metrics`] exposes Prometheus text exposition.
//! - **Diagnostics**: [`http::telemetry::diagnostics`] exposes JSON state summaries.
//!
//! # Scaling
//!
//! Rooms use one [`o_sfu_router::Router`] facade and can opt into additional same-process local routers through [`config::RoomWorkerPolicy`].
//! Joins stay on an assigned healthy packet loop. When no assigned worker has a
//! known delay below the configured threshold, a join can attach a healthy
//! worker not yet assigned to the room.
//!
//! # Feature Flags
//!
//! Core media behavior is configured at runtime through [`config`], not Cargo features.
//! The default feature `otel-tracing` enables OpenTelemetry tracing support through [`o_sfu_telemetry::TraceExportConfig`]. Other features are used strictly for tests and benchmarking.
//!
//! # Sub-crates
//!
//! | Crate | Role |
//! | --- | --- |
//! | [`o_sfu_rfc`] | RFC-backed JWT, RTP, RTCP, SDP and WebRTC consts/types |
//! | `o-sfu-model` | Shared call data ([`o_sfu_protocol::wire::UserId`], etc.) |
//! | [`o_sfu_router`] | Sans-I/O [`o_sfu_router::Router`] facade for room placement and routed media lifetimes |
//! | [`o_sfu_core`] | Room engine, [`core::prelude::SourcePolicy`], recording taps and [`core::server::transport::MediaTransport`] projection |
//! | [`o_sfu_protocol`] | Sans-I/O [`o_sfu_protocol::host::ProtocolCore`] and typed commands |
//! | [`o_sfu_telemetry`] | Tracing setup, metrics, diagnostics response types and graph payloads |
//!
//! # Reading Map
//!
//! - [`run`] and [`Runtime`] own boot, serving, background tasks and shutdown.
//! - [`config`] is the environment-to-runtime boundary.
//! - [`auth`], [`http`] and [`websocket`] form the admission edge.
//! - [`crate::core`] turns accepted control-plane intent into room mutations and transport effects.
//! - [`o_sfu_protocol::host::ProtocolCore`] keeps browser signaling sans-I/O.
//! - [`core::server::transport::MediaTransport`] owns the media workers and hides their threading model.
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

    /// Operator-facing metrics and diagnostics contracts.
    pub mod telemetry {
        /// Prometheus metric scrape contract.
        ///
        /// `GET` [`metrics::PATH`] returns `200 OK` Prometheus text exposition
        /// with [`metrics::CONTENT_TYPE`] and requires no application-layer
        /// authentication.
        /// Operators should restrict access at the deployment boundary.
        ///
        /// This is a scrape endpoint.
        /// Configure Prometheus to scrape [`metrics::PATH`], then issue `PromQL`
        /// queries to Prometheus.
        /// Histogram families render `<name>_bucket` with the additional `le`
        /// label plus `<name>_sum` and `<name>_count`.
        ///
        /// # Scrape and Query
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
        /// The endpoint returns Prometheus text exposition.
        ///
        /// ```text
        /// # HELP osfu_rooms_active Current number of active rooms owned by this runtime.
        /// # TYPE osfu_rooms_active gauge
        /// osfu_rooms_active 3
        /// # TYPE osfu_worker_rtp_packets_total counter
        /// osfu_worker_rtp_packets_total{media_worker_id="0",direction="ingress"} 1240
        /// ```
        ///
        /// Query clients send `PromQL` to the Prometheus-compatible backend
        /// rather than to o-sfu.
        /// These examples cover a gauge, counter and histogram.
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
        /// Read the [`metrics::MetricName`] variants below to find every
        /// exported name and its meaning.
        /// Query [`metrics::PATH`] to see each family's `HELP`, `TYPE`, label
        /// keys and current label values before building selectors.
        pub mod metrics {
            pub use o_sfu_telemetry::{
                metrics::MetricName, prometheus::PROMETHEUS_CONTENT_TYPE as CONTENT_TYPE,
            };

            pub use crate::http::route::METRICS as PATH;
        }

        /// JSON diagnostics contract.
        ///
        /// Every constant in [`diagnostics::route`] is a `GET` endpoint.
        /// When a diagnostics token is configured, requests must send it as an
        /// `Authorization: Bearer <token>` header.
        /// Without a configured token, the server permits access only when its
        /// HTTP listener is bound to a loopback address.
        /// Successful requests return `200 OK` JSON.
        ///
        /// # Routes and Parameters
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
        /// # Summary Request and Response
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
        /// # JavaScript Fetch Example
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
        /// The rooms response has this shape.
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
        /// The room users response has this shape.
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
        /// The response structs below list every field in each payload.
        /// Wire names are `camelCase` unless a field documents an exception.
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
