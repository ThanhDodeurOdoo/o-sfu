//! Media routing packet loop (one per worker (tokio task) is ran)
//!
//! This module implements the "hot-path" of the SFU, responsible for receiving,
//! routing, and forwading RTP/RTCP packets with minimal latency and overhead.
//!
//! ### Architecture & Efficiency
//!
//! The packet loop is designed for high throughput and predictable latency:
//!
//! * **Zero-Allocation Hot-Path**: All packet processing happens using pre-allocated
//!   buffers (`PacketLoopBuffers`). Payloads are forward with atomic reference counting
//!   when forwarding to multiple destinations (to avoid clones/copies).
//! * **Biased Event Loop**: The main loop (`run_packet_loop`) uses a biased selection
//!   strategy to prioritize control commands and shutdown signals.
//! * **Efficient incoming Routing**: Incoming UDP datagrams are routed to users
//!   using a milti tier approach:
//!     1. **Fast-Path**: A direct map from source `SocketAddr` to user key.
//!     2. **Recovery-Path**: On cache misses, packets are inspected for STUN/ICE
//!        attributes to recover the routing state.
//!     3. **Negative Caching**: Proved misses are cached to prevent repeated,
//!        expensive scans from unknown sources
//! * **Batched Processing**: Instead of processing one packet at a time from start
//!   to finish, the loop drains pending producers, receives socket data, and
//!   flushes all transmissions in coordinated batches to optimize I/O and cache locallity.
//!
//! ### sub files
//!
//! * [`loop_driver`]: The main loop and state machine transition logic.
//! * [`ingress_routing`]: Logic for mapping raw UDP datagrams to the correct RTC user.
//! * [`forward_flush`]: Handles the actual transmission of packets to their destinations
//!   (other users, recorders, or relay nodes).
//! * [`buffers`]: Reusable memory pools for pending transmissions and batching.
//! * [`session_drain`]: Pulls media from producer users into the packet loop.
//! * [`keyframe_requests`]: Manages the lifecycle and flushing of RTCP keyframe requests (PLI/FIR).
//! * [`event_observation`]: Translates low-level transport events into health and state metrics.

mod buffers;
mod event_observation;
mod forward_flush;
mod ingress_routing;
mod keyframe_requests;
mod loop_driver;
mod session_drain;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use event_observation::{transport_health_from_event, transport_ice_state};
pub(crate) use loop_driver::{PacketLoopConfig, run_packet_loop};
