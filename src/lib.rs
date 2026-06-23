//! o-sfu is a Selective Forwading Unit for audio/video calls
//!
//! This handles room admission, routing topology, media policy, packet
//! forwarding, diagnostics and verification for applications that need a dedicated SFU server.
//!
//! root crate (`o-sfu/src`) contains:
//!
//! - configuration loading through [`config::Config`] and [`Runtime`]
//!   construction through [`Runtime::new`]
//! - room provisioning, bulk disconnect, statistics, metrics and diagnostics
//!   through [`http`]
//! - browser bundle orchestration for Odoo through `SfuClient`,
//!   `BrowserRuntime` and [`o_sfu_protocol::host::ProtocolCore`]
//! - room membership through [`core::server::room::RoomManager`] and
//!   user-session orchestration through [`core::prelude::MediaSession`]
//! - process lifecycle through [`run`], [`Runtime::serve_listener`],
//!   [`core::server::metrics::RuntimeMetrics`] and
//!   [`core::server::diagnostics::DiagnosticsStore`]
//!
//! media routing and packet forwarding live below this crate in
//! [`core::server::transport::MediaTransport`] and `o-sfu-router`
//!
//! browser signaling state is in [`o_sfu_protocol::host::ProtocolCore`]
//! and the browser integration (client crate) executes the returned
//! [`o_sfu_protocol::host::HostCommand`] values with WebSocket and
//! `RTCPeerConnection` APIs
//!
//! # architecture
//!
//! ```text
//! HTTP control API
//!     -> Runtime
//!     -> RoomManager
//!     -> MediaSession
//!     -> Room
//!     -> RoutingTopology
//!     -> MediaTransport
//!     -> UDP, RTP fanout, relays and packet sinks
//! ```
//!
//! follow the steps through [`Runtime`],
//! [`core::server::room::RoomManager`], [`core::prelude::MediaSession`],
//! [`core::server::room::Room`], [`o_sfu_router::topology::RoutingTopology`]
//! and [`core::server::transport::MediaTransport`]
//!
//! ## sub crates
//!
//! | crate | role |
//! | --- | --- |
//! | [`o_sfu_rfc`] | RFC-backed JWT, RTP, RTCP, SDP and WebRTC vocabulary |
//! | `o-sfu-model` | shared call data surfaced through [`o_sfu_protocol::wire::UserId`], [`o_sfu_protocol::wire::StreamType`], [`o_sfu_protocol::wire::RecordingState`] and [`o_sfu_protocol::wire::WebSocketCloseCode`] |
//! | [`o_sfu_router`] | sans-I/O [`o_sfu_router::Router`] state for sessions, transports, producers, consumers and [`o_sfu_router::topology::RoutingTopology`] |
//! | [`o_sfu_core`] | room engine, [`core::prelude::SourcePolicy`], recording taps, cleanup effects and [`core::server::transport::MediaTransport`] projection |
//! | [`o_sfu_protocol`] | sans-I/O [`o_sfu_protocol::host::ProtocolCore`] and typed [`o_sfu_protocol::host::Command`] values |
//! | [`o_sfu_telemetry`] | tracing setup, [`o_sfu_telemetry::metrics::RuntimeMetrics`], [`o_sfu_telemetry::diagnostics::DiagnosticsStore`], [`o_sfu_telemetry::prometheus::render_prometheus`] and graph payloads |
//!
//! ## client bundle
//!
//! The server that implements the call (like odoo) imports the generated browser bundle from `crates/client` (part of the release artifacts)
//! and implements the calls features with `SfuClient`.
//!
//! ```text
//! SfuClient
//!     -> BrowserRuntime
//!     -> ProtocolCore
//!     -> HostCommand
//!     -> WebSocket, RTCPeerConnection and timers
//! ```
//!
//! `SfuClient` exposes a small and simple API with `connect`, `publish`, `subscribe`,
//! recording, stats and update events.
//! signaling state stays in [`o_sfu_protocol::host::ProtocolCore`] and returns
//! ordered [`o_sfu_protocol::host::CommandBatch`] values
//! `BrowserRuntime` executes the projected
//! [`o_sfu_protocol::host::HostCommand`] values against browser `WebSocket`,
//! `RTCPeerConnection` and timer APIs
//!
//! read `crates/client/API.md` for the public TypeScript surface and
//! `crates/client/README.md` for the client file map
//!
//! the idea is that room and router state approve topology
//! before packet-loop workers forward media
//! hot packet code applies verififid route state, packet gates and
//! recording taps rather than making room-level policy decisions
//!
//! # runtime
//!
//! [`Runtime`] owns the full process lifecycle
//! request handlers receive a smaller internal state handle so HTTP extractors
//! and WebSocket loops cannot depend on boot details or shutdown ownership
//!
//! # shutdown and cleanup
//!
//! serving owns background tasks for the lfietime of the server future
//! normal shutdown cancels those tasks and waits for completion
//! cancelling or dropping the server future cancels the same token and aborts
//! remaining work so embedders cannot detach process workers by accident
//!
//! ```text
//! serve HTTP
//!     -> spawn runtime tasks
//!     -> wait for server exit
//!     -> cancel task token
//!     -> join background tasks
//!     -> return server result
//! ```
//!
//! user and media cleanup is explicit async work
//! closing a WebSocket user drains room cleanup through
//! [`core::server::room::RoomManager`], [`core::server::room::UserCloseReason`]
//! and [`core::server::transport::MediaTransport`]
//! drop paths are used to detect missed cleanup, not to complete normal media
//! teardown
//!
//! # HTTP and WebSocket admission
//!
//! applications create rooms through HTTP and clients join through
//! the WebSocket
//! both paths are authenticated before they reach room state
//!
//! ```text
//! POST /v1/channel
//!     -> verify HttpRoomClaims with the HTTP key
//!     -> require issuer and room key claim
//!     -> create or reuse keyed Room
//!     -> return RoomResponse
//!
//! WebSocket upgrade
//!     -> reserve pre-auth capacity
//!     -> read exactly one first-frame auth envelope
//!     -> select candidate room
//!     -> verify WebSocketConnectClaims with that room key
//!     -> admit the user into room state
//! ```
//!
//! the HTTP path is repersented by [`http::CHANNEL_PATH`],
//! [`http::CreateRoomQuery`], [`auth::HttpRoomClaims`] and
//! [`http::RoomResponse`]
//! the WebSocket path is represented by [`websocket::decode_auth_payload_text`],
//! [`auth::WebSocketConnectClaims`], [`auth::MAX_JWT_TOKEN_BYTES`] and
//! [`websocket::ClientBatchDecodeError`]
//!
//! decoded JWT claims are not trusted until the token is verified with the key
//! selected for that room
//! an unsigned room hint may select verification material, but it does not
//! become authenticated identity or room access
//!
//! # room, router and transport ownership
//!
//! room transitions are planned while holding short exclusive room state
//! locks
//! asyn transport and diagnostics work is executed later through effect plans
//!
//! ```text
//! room mutation
//!     -> validate user and connection ownership
//!     -> update room media graph
//!     -> update `o-sfu-router` topology
//!     -> return transport and output effects
//!     -> release room lock
//!     -> execute effects
//! ```
//!
//! [`o_sfu_router::Router`] is pure and synchronous
//! it owns router topology facts such as sessions, receive transports,
//! producers, send transports, consumers, reverse indexes and cross-router
//! receiver shadows
//!
//! [`o_sfu_router::topology::RoutingTopology`] composes routers into room-local
//! placement
//! [`core::prelude::SfuCore`] and [`core::prelude::MediaSession`] own the
//! bridge from room intent to transport effects
//! they decide which [`core::prelude::SourcePublishIntent`] values exist, which
//! receivers want them, which [`core::server::transport::SourcePacketGate`]
//! should be installed and which cleanup work must be retried after transport
//! failure
//!
//! # packet path
//!
//! the [`core::server::transport::MediaTransport`] owns
//! worker-local packet loops
//! those loops receive UDP datagrams, drive WebRTC state, apply route tables,
//! forward RTP and publish bounded metrics
//!
//! ```text
//! UDP datagram
//!     -> worker ingress
//!     -> session lookup and source pin
//!     -> str0m input
//!     -> route lookup
//!     -> packet gate
//!     -> local fanout, relay fanout and packet sinks
//! ```
//!
//! packet loops do not own room membership
//! they consume stable transport keys and route controls projected from room
//! and router state
//! that split keeps hot packet work close to worker-local state while keeping
//! policy changes in the room engine
//!
//! # observability
//!
//! observability is part of the architecture, not an adapter around it
//! runtime and media code emit typed observations into [`o_sfu_telemetry`]
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
//! diagnostics routes are separate from metrics
//! metrics are intended for time series
//! diagnostics expose live room and transport state
//!
//! # scaling
//!
//! rooms use one local [`o_sfu_router::Router`] by default and can opt into
//! same-process local spillover through [`config::RoomWorkerPolicy`]
//!
//! it is later possible to extend the SFU for cross server scaling, but it's not a priority.
//!
//! # feature flags
//!
//! the root package has a small feature surface
//! the default feature enables `otel-tracing`, which turns on OpenTelemetry
//! tracing support through [`o_sfu_telemetry::TraceExportConfig`]
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
//! for pure routing invariants, read [`o_sfu_router::Router`] and
//! [`o_sfu_router::topology::RoutingTopology`]
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
            CHANNEL_PATH, CreateRoomQuery, DIAGNOSTICS_ROOMS_PATH, DIAGNOSTICS_SUMMARY_PATH,
            DISCONNECT_PATH, IncomingBitRateStatsResponse, METRICS_PATH, NOOP_PATH, NoopResponse,
            RoomResponse, STATS_PATH, StatsResponse,
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
