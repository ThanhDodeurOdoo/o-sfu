//! Transport event observation for packet-loop sessions.
//!
//! `str0m` emits many events while the packet loop drains a session. Only a
//! small subset changes the SFU's observable transport state. This module contain
//! that translation so session draining can stay focused on moving `str0m`
//! outputs into packet-loop buffers.

use std::sync::{Arc, Mutex};

use o_sfu_telemetry::schema;
use str0m::{Event, IceConnectionState, bwe::BweKind};
use tracing::{debug, trace};

use super::super::state::{RtcSnapshotState, TransportSessionHealth};
use crate::{
    Bitrate,
    runtime::{
        diagnostics::{
            DiagnosticsStore, diagnostics_room_instance_id, health_json_value,
            maybe_health_json_value,
        },
        media_transport::{SourcePolicySignal, TransportSessionKey},
        metrics::{self, RuntimeMetrics, TransportIceState},
    },
};

/// Log a transport event at the level useful for packet-loop diagnostics.
///
/// Connection transitions are debug-level because they describe lifecycle.
/// Other events are trace-level to avoid turning the media path into a logging
/// hot spot during normal calls.
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

/// Project selected `str0m` events into metrics, snapshots and diagnostics.
///
/// Snapshot writes are best-effort. If the snapshot lock is unavailable, the
/// packet loop keeps running because the worker-owned `PacketLoopState`
/// remains authoritative for media behavior.
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
    metrics.record_transport_health_transition(
        previous.map(metrics::transport_health_state),
        Some(metrics::transport_health_state(health)),
    );
    if previous == Some(health) {
        return;
    }
    let mut fields = serde_json::Map::new();
    fields.insert(String::from("from"), maybe_health_json_value(previous));
    fields.insert(String::from("to"), health_json_value(health));
    diagnostics.record_transport_user_event(
        diagnostics_room_instance_id(session_key.room_instance_id()),
        session_key.user_id(),
        schema::event::TRANSPORT_HEALTH_CHANGED,
        session_key.media_worker_id(),
        fields,
    );
}

/// Store receiver bandwidth estimates and wake source-policy recomputation.
///
/// Bandwidth estimates can change source selection, but that policy is owned by
/// the room layer. The packet loop records the latest estimate in the snapshot
/// and marks the room dirty only when the value changed.
fn observe_receiver_bandwidth(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_policy_signal: &SourcePolicySignal,
    session_key: &TransportSessionKey,
    kind: &BweKind,
) {
    let Some(estimate) = (match kind {
        BweKind::Twcc(bitrate) | BweKind::Remb(_, bitrate) => {
            Some(Bitrate::from_bps(bitrate.as_u64()))
        }
        _ => None,
    }) else {
        return;
    };
    let Ok(mut snapshot_state) = snapshot_state.lock() else {
        return;
    };
    if snapshot_state.set_receiver_bandwidth(session_key, estimate) == Some(estimate) {
        return;
    }
    source_policy_signal.mark_dirty(session_key.room_instance_id());
}

/// Convert `str0m` ICE connection state into the metrics enum.
pub fn transport_ice_state(state: IceConnectionState) -> TransportIceState {
    match state {
        IceConnectionState::New => TransportIceState::New,
        IceConnectionState::Checking => TransportIceState::Checking,
        IceConnectionState::Connected => TransportIceState::Connected,
        IceConnectionState::Completed => TransportIceState::Completed,
        IceConnectionState::Disconnected => TransportIceState::Disconnected,
    }
}

/// Convert a `str0m` event into the public transport-health snapshot value.
///
/// Not every event is a health transition. `None` means the event should not
/// update the session health snapshot.
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
