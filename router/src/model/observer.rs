use super::{MediaKind, ProducerId, SessionId, TransportId};

/// Observer contract for router lifecycle notifications.
///
/// This boundary exists so downstream features (for example recording) can
/// subscribe to router-side lifecycle events without requiring router-core
/// refactors.
///
/// Example:
///
/// ```rust
/// use o_sfu_router::{RouterEvent, RouterObserver};
///
/// struct RecordingObserver;
///
/// impl RouterObserver for RecordingObserver {
///     fn on_event(&mut self, event: RouterEvent) {
///         if let RouterEvent::ProducerAdded { producer_id, .. } = event {
///             let _ = producer_id;
///         }
///     }
/// }
/// ```
pub trait RouterObserver {
    fn on_event(&mut self, event: RouterEvent);
}

/// Events emitted when router-owned lifecycle changes matter to outer systems.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterEvent {
    SessionJoined {
        session_id: SessionId,
    },
    SessionLeft {
        session_id: SessionId,
    },
    ProducerAdded {
        session_id: SessionId,
        transport_id: TransportId,
        producer_id: ProducerId,
        media_kind: MediaKind,
    },
    ProducerRemoved {
        session_id: SessionId,
        transport_id: TransportId,
        producer_id: ProducerId,
        media_kind: MediaKind,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoopRouterObserver;

impl RouterObserver for NoopRouterObserver {
    #[inline]
    fn on_event(&mut self, _event: RouterEvent) {}
}
