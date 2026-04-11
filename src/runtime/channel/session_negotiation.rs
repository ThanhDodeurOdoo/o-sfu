use crate::runtime::transport_adapter::TransportConnectDirection;
use crate::signaling::webrtc::RtpCapabilities as SignalingRtpCapabilities;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SessionNegotiationState {
    AwaitingCapabilities,
    CapabilitiesReady {
        client_rtp_capabilities: SignalingRtpCapabilities,
    },
    UploadConnectedAwaitingCapabilities,
    DownloadConnectedAwaitingCapabilities,
    TransportsConnectedAwaitingCapabilities,
    UploadReady {
        client_rtp_capabilities: SignalingRtpCapabilities,
    },
    DownloadReady {
        client_rtp_capabilities: SignalingRtpCapabilities,
    },
    Ready {
        client_rtp_capabilities: SignalingRtpCapabilities,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionNegotiationUpdate {
    pub(crate) session_present: bool,
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
    pub(super) fn client_rtp_capabilities(&self) -> Option<&SignalingRtpCapabilities> {
        match &self.state {
            SessionNegotiationState::AwaitingCapabilities
            | SessionNegotiationState::UploadConnectedAwaitingCapabilities
            | SessionNegotiationState::DownloadConnectedAwaitingCapabilities
            | SessionNegotiationState::TransportsConnectedAwaitingCapabilities => None,
            SessionNegotiationState::CapabilitiesReady {
                client_rtp_capabilities,
            }
            | SessionNegotiationState::UploadReady {
                client_rtp_capabilities,
            }
            | SessionNegotiationState::DownloadReady {
                client_rtp_capabilities,
            }
            | SessionNegotiationState::Ready {
                client_rtp_capabilities,
            } => Some(client_rtp_capabilities),
        }
    }

    #[must_use]
    pub(super) fn can_publish(&self) -> bool {
        matches!(
            self.state,
            SessionNegotiationState::UploadConnectedAwaitingCapabilities
                | SessionNegotiationState::TransportsConnectedAwaitingCapabilities
                | SessionNegotiationState::UploadReady { .. }
                | SessionNegotiationState::Ready { .. }
        )
    }

    #[must_use]
    pub(super) fn can_consume(&self) -> bool {
        matches!(
            self.state,
            SessionNegotiationState::DownloadReady { .. } | SessionNegotiationState::Ready { .. }
        )
    }

    pub(super) fn set_client_rtp_capabilities(
        &mut self,
        client_rtp_capabilities: SignalingRtpCapabilities,
    ) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = match &self.state {
            SessionNegotiationState::AwaitingCapabilities
            | SessionNegotiationState::CapabilitiesReady { .. } => {
                SessionNegotiationState::CapabilitiesReady {
                    client_rtp_capabilities,
                }
            }
            SessionNegotiationState::UploadConnectedAwaitingCapabilities => {
                SessionNegotiationState::UploadReady {
                    client_rtp_capabilities,
                }
            }
            SessionNegotiationState::DownloadConnectedAwaitingCapabilities
            | SessionNegotiationState::DownloadReady { .. } => {
                SessionNegotiationState::DownloadReady {
                    client_rtp_capabilities,
                }
            }
            SessionNegotiationState::TransportsConnectedAwaitingCapabilities => {
                SessionNegotiationState::Ready {
                    client_rtp_capabilities,
                }
            }
            SessionNegotiationState::UploadReady { .. } => SessionNegotiationState::UploadReady {
                client_rtp_capabilities,
            },
            SessionNegotiationState::Ready { .. } => SessionNegotiationState::Ready {
                client_rtp_capabilities,
            },
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
            (
                SessionNegotiationState::CapabilitiesReady {
                    client_rtp_capabilities,
                },
                TransportConnectDirection::Upload,
            ) => SessionNegotiationState::UploadReady {
                client_rtp_capabilities: client_rtp_capabilities.clone(),
            },
            (
                SessionNegotiationState::CapabilitiesReady {
                    client_rtp_capabilities,
                },
                TransportConnectDirection::Download,
            ) => SessionNegotiationState::DownloadReady {
                client_rtp_capabilities: client_rtp_capabilities.clone(),
            },
            (
                SessionNegotiationState::UploadConnectedAwaitingCapabilities,
                TransportConnectDirection::Download,
            )
            | (
                SessionNegotiationState::DownloadConnectedAwaitingCapabilities,
                TransportConnectDirection::Upload,
            ) => SessionNegotiationState::TransportsConnectedAwaitingCapabilities,
            (
                SessionNegotiationState::UploadReady {
                    client_rtp_capabilities,
                },
                TransportConnectDirection::Download,
            )
            | (
                SessionNegotiationState::DownloadReady {
                    client_rtp_capabilities,
                },
                TransportConnectDirection::Upload,
            ) => SessionNegotiationState::Ready {
                client_rtp_capabilities: client_rtp_capabilities.clone(),
            },
            _ => self.state.clone(),
        };
        SessionNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{SessionNegotiation, SessionNegotiationState};
    use crate::runtime::transport_adapter::TransportConnectDirection;
    use crate::signaling::webrtc::RtpCapabilities as SignalingRtpCapabilities;

    fn test_client_rtp_capabilities() -> SignalingRtpCapabilities {
        SignalingRtpCapabilities(json!({
            "codecs": [{
                "mimeType": "audio/opus",
                "kind": "audio",
                "preferredPayloadType": 111,
                "clockRate": 48000,
                "channels": 2,
                "parameters": { "useinbandfec": "1" },
                "rtcpFeedback": [{ "type": "transport-cc" }]
            }],
            "headerExtensions": [{
                "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
                "preferredId": 1,
                "preferredEncrypt": false,
                "kind": "audio",
                "direction": "sendrecv"
            }]
        }))
    }

    #[test]
    fn session_negotiation_transitions_to_ready_when_capabilities_follow_connections() {
        let mut negotiation = SessionNegotiation::default();

        let upload_update = negotiation.set_transport_connected(TransportConnectDirection::Upload);
        let download_update =
            negotiation.set_transport_connected(TransportConnectDirection::Download);
        let capabilities_update =
            negotiation.set_client_rtp_capabilities(test_client_rtp_capabilities());

        assert!(upload_update.session_present);
        assert!(!upload_update.became_consumer_ready);
        assert!(download_update.session_present);
        assert!(!download_update.became_consumer_ready);
        assert!(capabilities_update.session_present);
        assert!(capabilities_update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert!(matches!(
            negotiation.state(),
            SessionNegotiationState::Ready { .. }
        ));
    }

    #[test]
    fn session_negotiation_transitions_to_download_ready_when_download_follows_capabilities() {
        let mut negotiation = SessionNegotiation::default();

        let capabilities_update =
            negotiation.set_client_rtp_capabilities(test_client_rtp_capabilities());
        let download_update =
            negotiation.set_transport_connected(TransportConnectDirection::Download);

        assert!(capabilities_update.session_present);
        assert!(!capabilities_update.became_consumer_ready);
        assert!(download_update.session_present);
        assert!(download_update.became_consumer_ready);
        assert!(!negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert!(matches!(
            negotiation.state(),
            SessionNegotiationState::DownloadReady { .. }
        ));
    }
}
