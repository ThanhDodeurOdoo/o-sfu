use crate::signaling::shared::{AvailableFeatures, RecordingState};

/// Connection lifecycle states exposed by the browser bundle to Odoo code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleConnectionState {
    Disconnected,
    Connecting,
    Authenticated,
    Connected,
    Recovering,
    Closed,
}

/// Update categories emitted by the browser bundle to Odoo code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleUpdateKind {
    Track,
    Broadcast,
    Disconnect,
    SessionInfoChange,
    ChannelInfoChange,
}

/// Public session-level properties surfaced by the bundle once a connection is established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleSessionSnapshot {
    pub available_features: AvailableFeatures,
    pub recording_state: RecordingState,
}
