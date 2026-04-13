use crate::runtime::transport_adapter::TransportConnectDirection;

/// Tracks the two independent axes of session readiness: **transport connections**
/// (upload / download ICE) and **RTP capability exchange**.
///
/// A session can only publihs once its upload transport is conected, and can only
/// consume once *both* the download transport is conected *and* RTP capabilities
/// have been received. The two axes can advance in any order; the state machine
/// merges them into a single enum so every legal combination is represented.
///
/// Each state is a combination of two independents axes:
/// 1. Transport Connection: None, Upload only, Download only, or Both.
/// 2. RTP Capabilities: Not yet received, or Ready.
///
/// ```text
///                      TRANSPORT CONNECTION
///               None        Upload (P)  Download    Both (P)
///            ┌─────────────┬───────────┬────────────┬────────────┐
/// NO CAPS    │ `Awaiting`  │ `UpConn`  │ `DownConn` │ `TransConn`│
///            ├─────────────┼───────────┼────────────┼────────────┤
/// CAPS READY │ `CapsReady` │ `UpReady` │ `DownReady`│  `Ready`   │
///            └─────────────┴───────────┴────────────┴────────────┘
///                            (P)        (C)        (P, C)
/// ```
///
/// (P) = `can_publish()` is true
/// (C) = `can_consume()` is true
///
/// **Gate conditions:**
/// - `can_publish()`: true when the upload transport is connected (regardeless of whether capabilities have arrived).
/// - `can_consume()`: true only when *both* the download transport is connected *and* capabilities are available (`DownloadReady` or `Ready`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SessionNegotiationState {
    /// Neither transport connected nor capabilities received.
    AwaitingCapabilities,
    /// Capabilities received, but no transport connected yet.
    CapabilitiesReady,
    /// Upload transport connected; still waiting for capabilities.
    UploadConnectedAwaitingCapabilities,
    /// Download transport connected; still waiting for capabilities.
    DownloadConnectedAwaitingCapabilities,
    /// Both transports connected; still waiting for capabilities.
    TransportsConnectedAwaitingCapabilities,
    /// Upload transport connected and capabilities received; download pending.
    UploadReady,
    /// Download transport connected and capabilities received; upload pending.
    DownloadReady,
    /// Fully negotiated: both transports conected and capabilities received.
    Ready,
}

/// Returned after each state transition to tell the caller what changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionNegotiationUpdate {
    /// Whether the session was found and the transition was applied.
    pub(crate) session_present: bool,
    /// True only on the exact transition that crosses the `can_consume()` threshold,
    /// so the channel knows to start creating consumers for this session.
    pub(crate) became_consumer_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionNegotiation {
    state: SessionNegotiationState,
}

impl Default for SessionNegotiation {
    fn default() -> Self {
        Self {
            state: SessionNegotiationState::AwaitingCapabilities,
        }
    }
}

impl SessionNegotiation {
    #[cfg(test)]
    #[must_use]
    pub(super) fn state(&self) -> &SessionNegotiationState {
        &self.state
    }

    #[must_use]
    pub(super) fn can_publish(&self) -> bool {
        matches!(
            self.state,
            SessionNegotiationState::UploadConnectedAwaitingCapabilities
                | SessionNegotiationState::TransportsConnectedAwaitingCapabilities
                | SessionNegotiationState::UploadReady
                | SessionNegotiationState::Ready
        )
    }

    #[must_use]
    pub(super) fn can_consume(&self) -> bool {
        matches!(
            self.state,
            SessionNegotiationState::DownloadReady | SessionNegotiationState::Ready
        )
    }

    pub(super) fn set_client_rtp_capabilities(&mut self) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = match &self.state {
            SessionNegotiationState::AwaitingCapabilities
            | SessionNegotiationState::CapabilitiesReady => {
                SessionNegotiationState::CapabilitiesReady
            }
            SessionNegotiationState::UploadConnectedAwaitingCapabilities => {
                SessionNegotiationState::UploadReady
            }
            SessionNegotiationState::DownloadConnectedAwaitingCapabilities
            | SessionNegotiationState::DownloadReady => SessionNegotiationState::DownloadReady,
            SessionNegotiationState::TransportsConnectedAwaitingCapabilities => {
                SessionNegotiationState::Ready
            }
            SessionNegotiationState::UploadReady => SessionNegotiationState::UploadReady,
            SessionNegotiationState::Ready => SessionNegotiationState::Ready,
        };
        SessionNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }

    pub(super) fn set_transport_connected(
        &mut self,
        direction: TransportConnectDirection,
    ) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = match (&self.state, direction) {
            (SessionNegotiationState::AwaitingCapabilities, TransportConnectDirection::Upload) => {
                SessionNegotiationState::UploadConnectedAwaitingCapabilities
            }
            (
                SessionNegotiationState::AwaitingCapabilities,
                TransportConnectDirection::Download,
            ) => SessionNegotiationState::DownloadConnectedAwaitingCapabilities,
            (SessionNegotiationState::CapabilitiesReady, TransportConnectDirection::Upload) => {
                SessionNegotiationState::UploadReady
            }
            (SessionNegotiationState::CapabilitiesReady, TransportConnectDirection::Download) => {
                SessionNegotiationState::DownloadReady
            }
            (
                SessionNegotiationState::UploadConnectedAwaitingCapabilities,
                TransportConnectDirection::Download,
            )
            | (
                SessionNegotiationState::DownloadConnectedAwaitingCapabilities,
                TransportConnectDirection::Upload,
            ) => SessionNegotiationState::TransportsConnectedAwaitingCapabilities,
            (SessionNegotiationState::UploadReady, TransportConnectDirection::Download)
            | (SessionNegotiationState::DownloadReady, TransportConnectDirection::Upload) => {
                SessionNegotiationState::Ready
            }
            _ => self.state.clone(),
        };
        SessionNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }

    pub(super) fn set_session_negotiated(&mut self) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = SessionNegotiationState::Ready;
        SessionNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionNegotiation, SessionNegotiationState};
    use crate::runtime::transport_adapter::TransportConnectDirection;

    #[test]
    fn session_negotiation_transitions_to_ready_when_capabilities_follow_connections() {
        let mut negotiation = SessionNegotiation::default();

        let upload_update = negotiation.set_transport_connected(TransportConnectDirection::Upload);
        let download_update =
            negotiation.set_transport_connected(TransportConnectDirection::Download);
        let capabilities_update = negotiation.set_client_rtp_capabilities();

        assert!(upload_update.session_present);
        assert!(!upload_update.became_consumer_ready);
        assert!(download_update.session_present);
        assert!(!download_update.became_consumer_ready);
        assert!(capabilities_update.session_present);
        assert!(capabilities_update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state(), &SessionNegotiationState::Ready);
    }

    #[test]
    fn session_negotiation_transitions_to_download_ready_when_download_follows_capabilities() {
        let mut negotiation = SessionNegotiation::default();

        let capabilities_update = negotiation.set_client_rtp_capabilities();
        let download_update =
            negotiation.set_transport_connected(TransportConnectDirection::Download);

        assert!(capabilities_update.session_present);
        assert!(!capabilities_update.became_consumer_ready);
        assert!(download_update.session_present);
        assert!(download_update.became_consumer_ready);
        assert!(!negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state(), &SessionNegotiationState::DownloadReady);
    }

    #[test]
    fn session_negotiation_set_session_negotiated_jumps_directly_to_ready() {
        let mut negotiation = SessionNegotiation::default();

        let update = negotiation.set_session_negotiated();

        assert!(update.session_present);
        assert!(update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state(), &SessionNegotiationState::Ready);
    }
}
