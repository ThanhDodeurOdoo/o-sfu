//! External API and orchestration layer for the RTC transport adapter.
//!
//! This module provide the interface through which the rest of the SFU interacts
//! with the WebRTC backend. It acts as the bridge between the high-level
//! transport orchestration and the low-level media packet loops.
//!
//! role of the dir:
//!
//! * **Backend Service Surface**: provide the concrete RTC backend methods the
//!   runtime transport boundary uses for negotiation, media, user cleanup,
//!   and observability without adding a second selector-facing service layer.
//! * **Worker Lifecycle**: Manages the lazy bootstrapping and shutdown of the
//!   [`packet_loop`] workers.
//! * **Command Dispatch**: Translates facade method cals into `RtcWorkerCommand`
//!   messages and handles the asynchronous coordination (request/response) with
//!   the workers.
//! * **Observability Bridge**: Projects the internal state of the packet loops
//!   (bitrates, health, active speakers, speaker-expiry deadlines) back to the
//!   caller-facing backend surface.
//!
//! ### Sub-Modules
//!
//! * [`facade`]: Defines the public `RtcTransportAdapter` struct plus the
//!   concern-scoped backend methods that sit directly above the worker mailbox.
//! * [`runtime`]: Implement the worker communication logic, lazy-boot orchestration,
//!   and command-dispatching helpers.
//! * [`test_support`]: test "utils"/public exports

mod facade;
mod runtime;

#[cfg(test)]
mod test_support;

pub(crate) use facade::RtcTransportAdapter;
pub use runtime::WorkerHandleSlot;
