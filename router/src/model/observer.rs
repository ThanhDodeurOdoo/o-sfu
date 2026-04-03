use super::{MediaKind, ProducerId, SessionId, StreamType, TransportId};

/// Observer contract for router lifecycle notifications.
///
/// This boundary exists so downstream features (for example recording) can
/// subscribe to router-side lifecycle events without requiring router-core
/// refactors.
pub trait RouterObserver {
    fn on_event(&mut self, event: RouterEvent);
}

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
        stream_type: StreamType,
    },
    ProducerRemoved {
        session_id: SessionId,
        transport_id: TransportId,
        producer_id: ProducerId,
        media_kind: MediaKind,
        stream_type: StreamType,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoopRouterObserver;

impl RouterObserver for NoopRouterObserver {
    #[inline]
    fn on_event(&mut self, _event: RouterEvent) {}
}
