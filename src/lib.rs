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
//!   [`core::server::metrics::RuntimeMetrics`] and
//!   [`core::server::diagnostics::DiagnosticsStore`]
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
//! ## sub crates
//!
//! | crate | role |
//! | --- | --- |
//! | [`o_sfu_rfc`] | RFC-backed JWT, RTP, RTCP, SDP and WebRTC consts/types |
//! | `o-sfu-model` | shared call data surfaced through [`o_sfu_protocol::wire::UserId`], [`o_sfu_protocol::wire::StreamType`], [`o_sfu_protocol::wire::RecordingState`] and [`o_sfu_protocol::wire::WebSocketCloseCode`] |
//! | [`o_sfu_router`] | sans-I/O [`o_sfu_router::Router`] facade for room placement and routed media lifetimes |
//! | [`o_sfu_core`] | room engine, [`core::prelude::SourcePolicy`], recording taps, cleanup effects and [`core::server::transport::MediaTransport`] projection |
//! | [`o_sfu_protocol`] | sans-I/O [`o_sfu_protocol::host::ProtocolCore`] and typed [`o_sfu_protocol::host::Command`] values |
//! | [`o_sfu_telemetry`] | tracing setup, [`o_sfu_telemetry::metrics::RuntimeMetrics`], [`o_sfu_telemetry::diagnostics::DiagnosticsStore`], [`o_sfu_telemetry::prometheus::render_prometheus`] and graph payloads |
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
//! # shutdown and cleanup
//!
//! [`Runtime::serve_listener`] scopes background tasks to the server future
//! normal shutdown cancels the shared token and waits for those tasks to finish
//! if the server future is dropped first, the drop path cancels the same token
//! and aborts remaining task handles
//!
//! ```text
//! Runtime::serve_listener
//!     |
//!     +-> spawn RuntimeTasks
//!     |     +-> source-policy sync
//!     |     +-> cleanup retry drain
//!     |
//!     +-> serve HTTP and WebSocket
//!     |
//!     +-> cancel task token
//!     |
//!     +-> join background tasks
//!     |
//!     +-> return server result
//! ```
//!
//! user and media cleanup is explicit async work,
//! closing a WebSocket user drains room cleanup through
//! [`core::server::room::RoomManager`], [`core::server::room::UserCloseReason`]
//! and [`core::server::transport::MediaTransport`]
//! drop paths are used to detect missed cleanup, not to complete normal media
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
//! asyn transport and diagnostics work is executed later through effect plans
//!
//! ```text
//! room state lock held                  lock released
//! +--------------------------------+    +------------------------------+
//! | validate user and connection   |    | MediaTransport commands      |
//! | commit room topology           |    | diagnostics and metrics      |
//! | capture RoomEffects            |--->| websocket output             |
//! |                                |    | cleanup retry reconciliation |
//! +--------------------------------+    +------------------------------+
//! ```
//!
//! [`o_sfu_router::Router`] is the sole pure, sans-I/O, synchronous router facade.
//! it owns exact user-to-connection placement plus private local-router graphs
//! keyed by connection and canonical producer or consumer ids. cross-router
//! receiver shadows are foreign local sessions derived from live consumer
//! dependencies and disappear with their final consumer.
//!
//! [`core::prelude::SfuCore`] owns admitted media-session construction and
//! [`core::prelude::MediaSession`] owns the bridge from room intent to
//! transport effects,
//! they decide which [`core::prelude::SourcePublishIntent`] values exist, which
//! receivers want them, which [`core::server::transport::SourcePacketGate`]
//! should be installed and which cleanup work must be retried after transport
//! failure
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
//! managed by the [`o_sfu_telemetry`] sub crate.
//!
//! the telemetry crate owns:
//!
//! - low-cardinality [`o_sfu_telemetry::metrics::RuntimeMetrics`] and
//!   [`o_sfu_telemetry::prometheus::render_prometheus`]
//! - structured event names in [`o_sfu_telemetry::schema`] used by tracing
//! - [`o_sfu_telemetry::diagnostics::DiagnosticsStore`] snapshots for rooms,
//!   users, workers and media paths
//! - recent-event storage for operator investigation
//! - [`o_sfu_telemetry::graph`] payloads used by the diagnostics UI and
//!   Grafana-style views
//!
//! diagnostics routes are separate from metrics,
//! metrics are intended for time series,
//! diagnostics expose live room and transport state.
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
}

pub mod websocket {
    pub use crate::runtime::websocket_server::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
        MAX_CLIENT_FRAME_BYTES, decode_auth_payload_text, decode_client_batch,
    };
}

pub use self::runtime::{Runtime, run};
