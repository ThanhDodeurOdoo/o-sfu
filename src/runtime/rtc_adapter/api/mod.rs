//! External API and orchestration layer for the RTC transport adapter.
//!
//! This module provide the interface through which the rest of the SFU interacts
//! with the WebRTC backend. It acts as the bridge between the high-level
//! transport orchestration and the low-level media packet loops.
//!
//! role of the dir:
//!
//! * **Facade Implementation**: provide a set of scoped facades (`Negotiation`,
//!   `Media`, `Session`, `Observability`) that define how callers interact with
//!   the adapter while maintaining encapsulation.
//! * **Worker Lifecycle**: Manages the lazy bootstrapping and shutdown of the
//!   [`packet_loop`] workers.
//! * **Command Dispatch**: Translates facade method cals into `RtcWorkerCommand`
//!   messages and handles the asynchronous coordination (request/response) with
//!   the workers.
//! * **Observability Bridge**: Projects the internal state of the packet loops
//!   (bitrates, health, active speakers) back to the caller-facing facades.
//!
//! ### Sub-Modules
//!
//! * [`facade`]: Defines the public `RtcTransportAdapter` struc and its concern-scoped
//!   facades.
//! * [`runtime`]: Implement the worker communication logic, lazy-boot orchestration,
//!   and command-dispatching helpers.
//! * [`test_support`]: test "utils"/public exports

mod facade;
mod runtime;

#[cfg(feature = "internal-benchmarks")]
mod benchmarks;

#[cfg(test)]
mod test_support;

pub(crate) use facade::RtcTransportAdapter;
