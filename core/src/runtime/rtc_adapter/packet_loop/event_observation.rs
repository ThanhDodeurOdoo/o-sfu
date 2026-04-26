use std::sync::{Arc, Mutex};

use str0m::{Event, IceConnectionState, bwe::BweKind};
use tracing::{debug, trace};

use super::super::state::{RtcSnapshotState, TransportSessionHealth};
use crate::runtime::{
    diagnostics::{DiagnosticsStore, health_json_value, maybe_health_json_value},
    metrics::{RuntimeMetrics, TransportIceState},
    telemetry::schema,
    transport_adapter::{SourcePolicySignal, TransportSessionKey},
};

pub(super) fn log_rtc_event(session_key: &TransportSessionKey, event: &Event) {
    match event {
        Event::IceConnectionStateChange(state) => {
            debug!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                ?state,
                "rtc ICE connection state transition"
            );
        }
        Event::Connected => {
            debug!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                "rtc DTLS transport reached connected state"
            );
        }
        _ => {
            trace!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                ?event,
                "rtc packet loop event"
            );
        }
    }
}

pub(super) fn observe_rtc_event(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    diagnostics: &Arc<DiagnosticsStore>,
    metrics: &RuntimeMetrics,
    source_policy_signal: &SourcePolicySignal,
    session_key: &TransportSessionKey,
    event: &Event,
) {
    match event {
        Event::IceConnectionStateChange(state) => {
            metrics.record_transport_ice_state_change(transport_ice_state(*state));
        }
        Event::Connected => {
            metrics.record_transport_dtls_connected();
        }
        Event::EgressBitrateEstimate(kind) => {
            observe_receiver_bandwidth(snapshot_state, source_policy_signal, session_key, kind);
        }
        _ => {}
    }
    let Some(health) = transport_health_from_event(event) else {
        return;
    };
    let Ok(mut snapshot_state) = snapshot_state.lock() else {
        return;
    };
    let previous = snapshot_state.set_transport_health(session_key, health);
    metrics.record_transport_health_transition(previous, Some(health));
    if previous == Some(health) {
        return;
    }
    let mut fields = serde_json::Map::new();
    fields.insert(String::from("from"), maybe_health_json_value(previous));
    fields.insert(String::from("to"), health_json_value(health));
    diagnostics.record_transport_user_event(
        session_key.room_instance_id(),
        session_key.user_id(),
        schema::event::TRANSPORT_HEALTH_CHANGED,
        session_key.media_worker_id(),
        fields,
    );
}

fn observe_receiver_bandwidth(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_policy_signal: &SourcePolicySignal,
    session_key: &TransportSessionKey,
    kind: &BweKind,
) {
    let Some(estimate_bps) = (match kind {
        BweKind::Twcc(bitrate) | BweKind::Remb(_, bitrate) => Some(bitrate.as_u64()),
        _ => None,
    }) else {
        return;
    };
    let Ok(mut snapshot_state) = snapshot_state.lock() else {
        return;
    };
    if snapshot_state.set_receiver_bandwidth(session_key, estimate_bps) == Some(estimate_bps) {
        return;
    }
    source_policy_signal.mark_dirty(session_key.room_instance_id());
}

pub fn transport_ice_state(state: IceConnectionState) -> TransportIceState {
    match state {
        IceConnectionState::New => TransportIceState::New,
        IceConnectionState::Checking => TransportIceState::Checking,
        IceConnectionState::Connected => TransportIceState::Connected,
        IceConnectionState::Completed => TransportIceState::Completed,
        IceConnectionState::Disconnected => TransportIceState::Disconnected,
    }
}

pub fn transport_health_from_event(event: &Event) -> Option<TransportSessionHealth> {
    match event {
        Event::Connected => Some(TransportSessionHealth::Connected),
        Event::IceConnectionStateChange(state) => transport_health_from_ice_state(*state),
        _ => None,
    }
}

fn transport_health_from_ice_state(state: IceConnectionState) -> Option<TransportSessionHealth> {
    if state.is_connected() {
        Some(TransportSessionHealth::Connected)
    } else if state.is_disconnected() {
        Some(TransportSessionHealth::Disconnected)
    } else {
        None
    }
}
