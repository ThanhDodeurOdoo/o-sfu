use diagnostics::{DiagnosticsRoomInstanceId, DiagnosticsTransportHealth};
use metrics::{SourceSelectionKind, TransportHealthState};
pub use o_sfu_telemetry::{diagnostics, metrics};
use serde_json::{Value, json};

use crate::engine::{
    RoomInstanceId, media_transport::TransportSessionHealth, source_model::SourceSelector,
};

pub(crate) const fn diagnostics_room_instance_id(
    room_instance_id: RoomInstanceId,
) -> DiagnosticsRoomInstanceId {
    DiagnosticsRoomInstanceId::from_raw(room_instance_id.as_u64())
}

pub(crate) const fn diagnostics_transport_health(
    health: TransportSessionHealth,
) -> DiagnosticsTransportHealth {
    match health {
        TransportSessionHealth::Connected => DiagnosticsTransportHealth::Connected,
        TransportSessionHealth::Disconnected => DiagnosticsTransportHealth::Disconnected,
    }
}

pub(crate) fn health_json_value(health: TransportSessionHealth) -> Value {
    json!(diagnostics_transport_health(health))
}

pub(crate) fn maybe_health_json_value(health: Option<TransportSessionHealth>) -> Value {
    health.map_or(Value::Null, health_json_value)
}

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
    }
}
