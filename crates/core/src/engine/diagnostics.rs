pub use o_sfu_telemetry::diagnostics::*;
use serde_json::{Value, json};

use crate::engine::{RoomInstanceId, rtc::TransportSessionHealth};

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
