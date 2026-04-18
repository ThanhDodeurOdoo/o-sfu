#[cfg(test)]
mod test_support;

#[cfg(test)]
pub(crate) use test_support::SessionTransportReady;

/// Returned after each state transition to tell the caller what changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionNegotiationUpdate {
    /// Whether the session was found and the transition was applied.
    pub(crate) session_present: bool,
    /// True only on the exact transition that crosses the `can_consume()` threshold,
    /// so the channel knows to start creating consumers for this session.
    pub(crate) became_consumer_ready: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct SessionNegotiation {
    publish_transport_ready: bool,
    consume_transport_ready: bool,
    capabilities_received: bool,
}

impl SessionNegotiation {
    #[must_use]
    pub(super) const fn can_publish(&self) -> bool {
        self.publish_transport_ready
    }

    #[must_use]
    pub(super) const fn can_consume(&self) -> bool {
        self.consume_transport_ready && self.capabilities_received
    }

    pub(super) fn set_session_negotiated(&mut self) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.publish_transport_ready = true;
        self.consume_transport_ready = true;
        self.capabilities_received = true;
        self.readiness_update(was_consumer_ready)
    }

    fn readiness_update(&self, was_consumer_ready: bool) -> SessionNegotiationUpdate {
        SessionNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }
}
