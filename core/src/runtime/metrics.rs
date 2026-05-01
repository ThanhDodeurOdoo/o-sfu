pub use o_sfu_telemetry::metrics::*;

use crate::runtime::{
    rtc_adapter::TransportSessionHealth,
    source_model::{SourceRoomPolicySelector, SourceSelector},
};

pub(crate) const fn transport_health_state(health: TransportSessionHealth) -> TransportHealthState {
    match health {
        TransportSessionHealth::Connected => TransportHealthState::Connected,
        TransportSessionHealth::Disconnected => TransportHealthState::Disconnected,
    }
}

pub(crate) const fn source_selection_kind(selector: SourceSelector) -> SourceSelectionKind {
    match selector {
        SourceSelector::Open => SourceSelectionKind::Open,
        SourceSelector::Encoding(_) => SourceSelectionKind::Encoding,
        SourceSelector::OperatingPoint(_) => SourceSelectionKind::OperatingPoint,
        SourceSelector::RoomPolicy(
            SourceRoomPolicySelector::Pinned
            | SourceRoomPolicySelector::Featured
            | SourceRoomPolicySelector::ScreenShare
            | SourceRoomPolicySelector::ActiveSpeaker,
        ) => SourceSelectionKind::RoomPolicyFeatured,
        SourceSelector::RoomPolicy(
            SourceRoomPolicySelector::VisibleThumbnail
            | SourceRoomPolicySelector::Hidden
            | SourceRoomPolicySelector::Overflow,
        ) => SourceSelectionKind::RoomPolicyThumbnail,
    }
}
