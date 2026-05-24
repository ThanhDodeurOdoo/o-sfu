use std::{fmt, sync::Arc};

use o_sfu_router::{RouterEvent, RouterObserver};

/// room-owned sink for pure-router topology events
///
/// the router emits topology facts without knowing which room subsystem will
/// consume them. recording is the only sink today, but the trait keeps router
/// state independent from recording capture and any later diagnostics sink
pub(in crate::runtime) trait RoomRouterEventSink: Send + Sync {
    fn handle_room_router_event(&self, event: RouterEvent);
}

#[derive(Clone)]
pub(in crate::runtime) struct RoomRouterObserver {
    sink: Arc<dyn RoomRouterEventSink>,
}

impl RoomRouterObserver {
    /// adapts one room sink into the pure-router observer callback
    pub fn new(sink: Arc<dyn RoomRouterEventSink>) -> Self {
        Self { sink }
    }
}

impl RouterObserver for RoomRouterObserver {
    fn on_event(&mut self, event: RouterEvent) {
        self.sink.handle_room_router_event(event);
    }
}

impl fmt::Debug for RoomRouterObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoomRouterObserver")
            .finish_non_exhaustive()
    }
}
