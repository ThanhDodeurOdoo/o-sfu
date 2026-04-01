mod consumer;
mod error;
mod ids;
mod producer;
mod router;
mod session;
#[cfg(test)]
mod tests;
mod transport;
#[cfg_attr(
    not(any(test, kani)),
    allow(
        dead_code,
        reason = "The invariant surface is intentionally only used by tests and proofs."
    )
)]
pub(crate) mod verification;

pub use self::consumer::Consumer;
pub use self::error::{ResourceKind, RouterError};
pub use self::ids::{ConsumerId, ProducerId, RouterId, SessionId, TransportId};
pub use self::producer::Producer;
pub use self::router::{Router, RouterModel};
pub use self::session::Session;
pub use self::transport::Transport;
