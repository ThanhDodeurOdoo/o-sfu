use std::{fmt, sync::Arc};

use o_sfu_router::{RouterEvent, RouterObserver};

pub(in crate::runtime) trait RoomRouterEventSink: Send + Sync {
    fn handle_room_router_event(&self, event: RouterEvent);
}

#[derive(Clone)]
pub(in crate::runtime) struct RoomRouterObserver {
    sink: Arc<dyn RoomRouterEventSink>,
}

impl RoomRouterObserver {
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
