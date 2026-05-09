//! Transport event observation for packet-loop sessions.
//!
//! `str0m` emits many events while the packet loop drains a session. Only a
//! small subset changes the SFU's observable transport state. This module contain
//! that translation so session draining can stay focused on moving `str0m`
//! outputs into packet-loop scratch.

use str0m::{Event, IceConnectionState, bwe::BweKind};
use tracing::{debug, trace};

use super::{
    super::state::TransportSessionHealth,
    machine::effect::{PacketLoopEffect, PacketLoopEffects, PacketLoopMetricEffect},
};
use crate::runtime::{media_transport::TransportSessionKey, metrics::TransportIceState};

/// Project one `str0m` event into host effects and diagnostics logging.
pub(super) fn observe_rtc_event(
    session_key: &TransportSessionKey,
    event: &Event,
    effects: &mut PacketLoopEffects,
) {
    match event {
        Event::IceConnectionStateChange(state) => {
            observe_ice_connection_state(session_key, effects, *state);
        }
        Event::Connected => observe_dtls_connected(session_key, effects),
        Event::EgressBitrateEstimate(kind) => {
            observe_egress_bitrate(session_key, effects, event, kind);
        }
        _ => trace_rtc_event(session_key, event),
    }
    observe_transport_health(session_key, effects, event);
}

fn observe_ice_connection_state(
    session_key: &TransportSessionKey,
    effects: &mut PacketLoopEffects,
    state: IceConnectionState,
) {
    effects.record_metric(PacketLoopMetricEffect::TransportIceStateChange(
        transport_ice_state(state),
    ));
    debug!(
        user_id = ?session_key.user_id(),
        media_worker_id = session_key.media_worker_id(),
        ?state,
        "rtc ICE connection state transition"
    );
}

fn observe_dtls_connected(session_key: &TransportSessionKey, effects: &mut PacketLoopEffects) {
    effects.record_metric(PacketLoopMetricEffect::TransportDtlsConnected);
    debug!(
        user_id = ?session_key.user_id(),
        media_worker_id = session_key.media_worker_id(),
        "rtc DTLS transport reached connected state"
    );
}

fn observe_egress_bitrate(
    session_key: &TransportSessionKey,
    effects: &mut PacketLoopEffects,
    event: &Event,
    kind: &BweKind,
) {
    if let Some(estimate_bps) = receiver_bandwidth_estimate(kind) {
        effects.push(PacketLoopEffect::SetReceiverBandwidth {
            session_key: session_key.clone(),
            estimate_bps,
        });
    }
    trace!(
        user_id = ?session_key.user_id(),
        media_worker_id = session_key.media_worker_id(),
        ?event,
        "rtc packet loop event"
    );
}

fn trace_rtc_event(session_key: &TransportSessionKey, event: &Event) {
    trace!(
        user_id = ?session_key.user_id(),
        media_worker_id = session_key.media_worker_id(),
        ?event,
        "rtc packet loop event"
    );
}

fn observe_transport_health(
    session_key: &TransportSessionKey,
    effects: &mut PacketLoopEffects,
    event: &Event,
) {
    let Some(health) = transport_health_from_event(event) else {
        return;
    };
    effects.push(PacketLoopEffect::SetTransportHealth {
        session_key: session_key.clone(),
        health,
    });
}

fn receiver_bandwidth_estimate(kind: &BweKind) -> Option<u64> {
    match kind {
        BweKind::Twcc(bitrate) | BweKind::Remb(_, bitrate) => Some(bitrate.as_u64()),
        _ => None,
    }
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
