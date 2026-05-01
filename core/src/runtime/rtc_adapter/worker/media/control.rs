//! Worker-local route control for declared media.
//!
//! This module exists because consumer-route mutation touches several pieces of
//! state that must stay consistent:
//!
//! - `media_route_index` records which consumer transports depend on a source
//! - remote-source registrations track cross-worker ownership and relay control
//! - `route_control` keeps the effective local, relay and server-owned gates
//! - relay cleanup must be emitted when the last remote-backed route disappears
//!
//! `lifecycle.rs` owns media declaration and teardown against `RtcSessionState`.
//! Once a producer or consumer handle exists, this module owns the routing-side
//! bookkeeping that validates sources, registers or removes consumer routes and
//! recomputes packet-gate state.
//!
//! Small ownership graph:
//!
//! ```text
//! lifecycle.rs
//!   |-- declare/remove str0m media
//!   |-- register/remove media handles
//!   `-- call control.rs when route ownership changes
//!
//! control.rs
//!   |-- validate source ownership (local vs remote)
//!   |-- mutate media_route_index
//!   |-- refresh route_control packet gates
//!   `-- propagate remote relay state / cleanup
//!
//! keyframe.rs
//!   `-- reads the same source-ownership rules for feedback routing
//! ```
//!
//! The `respond_*` functions at the top are command-adapter entry points for the
//! worker dispatcher. The lower worker functions keep the ownership checks close
//! to the state they protect.

use std::time::{Duration, Instant};

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{KeyframeRequestKind, Mid, Pt, Rid};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use super::{
    super::super::{
        commands::{ConsumerPacketGateCommand, RelayCleanup, RemoteSourceControl},
        demux::{MediaRouteDestination, MediaRouteEntry},
        media_registry::RegisteredMediaHandle,
        relay_registry::{RelayRegistry, RelayTargetId},
        route_control::{PacketLayerGate, aggregate_packet_gates},
        state::RtcBootstrapState,
    },
    keyframe::request_keyframe_for_source,
    types::RouteSourceKind,
};
use crate::runtime::{
    metrics::{RtcRouteControlOutcome, RuntimeMetrics},
    rtc_adapter::route_control::KeyframeRequestDecision,
    transport_adapter::{
        TransportAdapterError, TransportMediaId, TransportResult, TransportSessionKey,
    },
};

/// Controls whether route-source lookup may create a remote-source entry.
///
/// Registering a consumer route is the only path allowed to install remote
/// source control. Later active, gate and keyframe changes must find an existing
/// registration so stale commands cannot recreate a removed remote route.
enum RouteSourceAccess {
    Existing,
    Register(Option<RemoteSourceControl>),
}

const SELECTED_RID_READY_MAX_AGE: Duration = Duration::from_secs(2);
const SELECTED_RID_KEYFRAME_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_millis(1_100),
    Duration::from_millis(2_500),
    Duration::from_millis(5_000),
    Duration::from_millis(8_000),
    Duration::from_millis(13_000),
];

pub fn respond_set_producer_active(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_producer_active(
        state,
        session_key,
        transport_media_id,
        active,
    ));
}

pub fn respond_set_consumer_active(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_consumer_active(
        state,
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
        active,
    ));
}

/// Command adapter for receiver-driven layer updates.
///
/// The dispatcher already owns the mutable worker state. This wrapper keeps the
/// oneshot response boundary at the command edge while the worker function
/// revalidates the route before mutating packet-gate state.
pub fn respond_set_consumer_packet_gate(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    packet_gate: PacketLayerGate,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_consumer_packet_gate(
        state,
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
        packet_gate,
    ));
}

pub fn respond_set_consumer_packet_gates(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    updates: Vec<ConsumerPacketGateCommand>,
    response: oneshot::Sender<TransportResult<Vec<TransportResult<()>>>>,
) {
    let _ = response.send(Ok(worker_set_consumer_packet_gates(
        state,
        source_session_key,
        source_transport_media_id,
        updates,
    )));
}

pub fn respond_request_consumer_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_request_consumer_keyframe(
        state,
        metrics,
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
    ));
}

pub fn respond_set_remote_source_route_active(
    state: &RtcBootstrapState,
    relay_registry: &RelayRegistry,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
    active: bool,
) {
    if owned_local_producer_mid(state, source_session_key, source_transport_media_id).is_none() {
        return;
    }
    relay_registry.set_source_target_active(source_transport_media_id, target_id, active);
}

pub fn respond_set_remote_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    if owned_local_producer_mid(state, source_session_key, source_transport_media_id).is_none() {
        return;
    }
    state
        .route_control
        .set_relay_packet_gate(source_transport_media_id, target_id, packet_gate);
}

/// Update packet-path readiness for one incoming producer RID.
///
/// This is the bridge between RTP observation and route control. It may move
/// pending consumer gates to their selected RID, install a single fallback RID
/// while the selected layer waits for a keyframe, or suspend a stale strict gate
/// that no longer has fresh packets. It only mutates transport state and queues
/// keyframe refreshes. Room policy remains the owner of which RID is selected.
pub fn observe_source_rid_readiness(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    is_keyframe: bool,
    now: Instant,
) -> bool {
    let first_observed = state.observe_producer_rid_packet(source_transport_media_id, rid, now);
    if first_observed {
        debug!(
            user_id = ?source_session_key.user_id(),
            media_worker_id = source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            is_keyframe,
            "observed first live RTP for producer RID"
        );
    }
    let suspended_stale_gate = suspend_stale_packet_gates(
        state,
        metrics,
        source_session_key,
        source_transport_media_id,
        rid,
        now,
    );
    let has_pending_selected_gate =
        has_pending_packet_gate_for_rid(state, source_transport_media_id, rid);
    let activated_pending_gate =
        is_keyframe && activate_pending_packet_gates_for_rid(state, source_transport_media_id, rid);
    let activated_bootstrap_fallback_gate = !activated_pending_gate
        && is_keyframe
        && activate_bootstrap_fallback_packet_gates_for_rid(state, source_transport_media_id, rid);
    if activated_pending_gate {
        refresh_source_packet_gate(state, source_transport_media_id);
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
        schedule_live_rid_keyframe_retries(state, source_transport_media_id, rid, now);
    } else if activated_bootstrap_fallback_gate {
        refresh_source_packet_gate(state, source_transport_media_id);
        request_pending_selected_rid_keyframes(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            now,
        );
    } else if has_pending_selected_gate {
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
    } else if suspended_stale_gate {
        refresh_source_packet_gate(state, source_transport_media_id);
    }
    drain_live_rid_keyframe_retries(
        state,
        metrics,
        source_session_key,
        source_transport_media_id,
        rid,
        now,
    );
    activated_pending_gate || activated_bootstrap_fallback_gate || suspended_stale_gate
}

/// Drain selected-RID keyframe retries whose packet-loop deadlines have passed.
///
/// Retries live in `RtcBootstrapState` so they can fire even if the selected
/// RID does not keep sending packets. Missing source ownership is expected
/// after teardown and is handled as a dropped best-effort refresh.
pub fn drain_due_rid_keyframe_refreshes(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    now: Instant,
) {
    for (source_transport_media_id, rid) in state.drain_due_rid_keyframe_refreshes_for_all(now) {
        let Some(source_session_key) =
            keyframe_refresh_source_session(state, source_transport_media_id)
        else {
            warn!(
                ?source_transport_media_id,
                ?rid,
                "dropped selected RID keyframe refresh because source ownership is unavailable"
            );
            continue;
        };
        debug!(
            user_id = ?source_session_key.user_id(),
            media_worker_id = source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            "draining scheduled selected RID keyframe refresh"
        );
        request_live_rid_keyframe(
            state,
            metrics,
            &source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
    }
}

/// Reduces negotiated consumer RTP parameters into the route-level packet gate.
///
/// A selected RID means this route should forward only that simulcast layer.
///
/// Simulcast sources are declared as one rid-less downstream browser stream,
/// so the initial transport route must not open every publisher RID while room
/// policy catches up. When several RID encodings are present, the route starts
/// on the lowest advertised bitrate, falling back to declaration order when the
/// browser did not preserve bitrate metadata. Mixed or RID-less encodings stay
/// open so non-simulcast streams and broader compatibility cases keep the
/// previous behavior.
pub(super) fn consumer_packet_gate(
    consumer_rtp_parameters: &RouterRtpParameters,
) -> PacketLayerGate {
    let mut first_rid: Option<Rid> = None;
    let mut lowest_bitrate_rid: Option<(Rid, u64)> = None;
    let mut all_encodings_have_bitrate = true;
    for encoding in consumer_rtp_parameters.encodings() {
        let Some(rid) = encoding.rid().map(Rid::from) else {
            return PacketLayerGate::Open;
        };
        if first_rid.is_none() {
            first_rid = Some(rid);
        }
        let bitrate = encoding.max_bitrate();
        all_encodings_have_bitrate &= bitrate.is_some();
        if let Some(bitrate) = bitrate {
            match lowest_bitrate_rid.as_mut() {
                Some((selected_rid, selected_bitrate)) if bitrate < *selected_bitrate => {
                    *selected_rid = rid;
                    *selected_bitrate = bitrate;
                }
                Some(_) => {}
                None => lowest_bitrate_rid = Some((rid, bitrate)),
            }
        }
    }
    if all_encodings_have_bitrate && let Some((rid, _bitrate)) = lowest_bitrate_rid {
        return PacketLayerGate::Rid(rid);
    }
    first_rid.map_or(PacketLayerGate::Open, PacketLayerGate::Rid)
}

fn consumer_packet_gate_for_source(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    consumer_rtp_parameters: &RouterRtpParameters,
) -> (PacketLayerGate, Option<PacketLayerGate>) {
    guarded_packet_gate(
        state,
        source_transport_media_id,
        consumer_packet_gate(consumer_rtp_parameters),
    )
}

/// Split a selected-RID gate into effective and pending transport state.
///
/// The effective gate is what the packet loop enforces now. The pending gate is
/// the target selected by room policy. Keeping both lets bootstrap forwarding
/// stay decodable without losing the receiver's intended layer.
fn guarded_packet_gate(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    packet_gate: PacketLayerGate,
) -> (PacketLayerGate, Option<PacketLayerGate>) {
    let Some(rid) = packet_gate_rid(&packet_gate) else {
        return (packet_gate, None);
    };
    if state.producer_rid_is_ready(
        source_transport_media_id,
        rid,
        Instant::now(),
        SELECTED_RID_READY_MAX_AGE,
    ) {
        return (packet_gate, None);
    }
    debug!(
        ?source_transport_media_id,
        ?rid,
        requested_packet_gate = ?packet_gate,
        "blocked selected RID route until selected producer RID has live RTP"
    );
    (PacketLayerGate::Block, Some(packet_gate))
}

/// Ensure the source side of a route is valid before adding a destination.
///
/// Local sources must still be owned by the requesting session. Remote sources
/// are registered with relay control so later source-gate updates can be pushed
/// back to the worker that owns the producer.
pub(super) fn ensure_route_source_registered(
    state: &mut RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    remote_source_control: Option<RemoteSourceControl>,
) -> Result<RouteSourceKind, TransportAdapterError> {
    ensure_route_source(
        state,
        route_owner_session_key,
        source_session_key,
        source_transport_media_id,
        RouteSourceAccess::Register(remote_source_control),
    )
}

/// Register one consumer route in the worker-local forwarding index.
///
/// This binds the consumer transport media to its source route, installs the
/// negotiated packet gate that drives the initial RID selection and updates any
/// remote-source relay activity that depends on this route existing. Room policy
/// may later replace the gate for this one consumer without changing other
/// destinations on the same source.
///
/// A strict selected-RID route is not made effective until packet-path
/// liveness proves that RID exists for this producer. The pending gate keeps
/// the room-selected target attached to the destination while the effective
/// gate remains blocked or temporarily points at one decodable fallback RID.
pub(super) fn register_consumer_route(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    consumer_mid: Mid,
    source_transport_media_id: TransportMediaId,
    route_source: RouteSourceKind,
    consumer_rtp_parameters: &RouterRtpParameters,
) {
    let (packet_gate, pending_packet_gate) =
        consumer_packet_gate_for_source(state, source_transport_media_id, consumer_rtp_parameters);
    let dest_payload_type = consumer_payload_type(consumer_rtp_parameters);
    state
        .media_route_index
        .entry(source_transport_media_id)
        .or_insert_with(|| MediaRouteEntry {
            source_active: true,
            destinations: Vec::new(),
        })
        .destinations
        .push(MediaRouteDestination {
            dest_session: consumer_session_key.clone(),
            dest_transport_media_id: consumer_transport_media_id,
            dest_mid: consumer_mid,
            dest_payload_type,
            active: true,
            packet_gate,
            pending_packet_gate,
        });
    refresh_source_packet_gate(state, source_transport_media_id);
    if matches!(route_source, RouteSourceKind::Remote) {
        update_remote_route_active(state, source_transport_media_id, true);
    }
}

pub(super) fn consumer_payload_type(consumer_rtp_parameters: &RouterRtpParameters) -> Option<Pt> {
    consumer_rtp_parameters
        .encodings()
        .find_map(|encoding| encoding.payload_type().map(Pt::from))
        .or_else(|| {
            consumer_rtp_parameters
                .formats()
                .find(|format| !format.codec().is_rtx())
                .map(|format| Pt::from(format.payload_type()))
        })
}

pub(super) fn remove_consumer_route(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_transport_media_id: TransportMediaId,
) -> Option<RelayCleanup> {
    let relay_cleanup = state
        .remote_source_registration(source_transport_media_id)
        .map(|registration| {
            RelayCleanup::new(
                registration.source_session_key().clone(),
                source_transport_media_id,
            )
        });
    let Some(route_entry) = state.media_route_index.get_mut(&source_transport_media_id) else {
        state.prune_remote_source_if_unrouted(source_transport_media_id);
        return relay_cleanup;
    };
    let (removed_active_route, remove_route_entry) = {
        let Some(position) = route_entry.destinations.iter().position(|destination| {
            destination.dest_session == *consumer_session_key
                && destination.dest_transport_media_id == consumer_transport_media_id
        }) else {
            return relay_cleanup;
        };
        let removed_active_route = route_entry
            .destinations
            .get(position)
            .is_some_and(|destination| destination.active);
        route_entry.destinations.remove(position);
        (removed_active_route, route_entry.destinations.is_empty())
    };
    if removed_active_route {
        update_remote_route_active(state, source_transport_media_id, false);
    }
    if remove_route_entry {
        state.media_route_index.remove(&source_transport_media_id);
    }
    refresh_source_packet_gate(state, source_transport_media_id);
    state.prune_remote_source_if_unrouted(source_transport_media_id);
    relay_cleanup
}

/// Recompute the effective packet gate for one source after consumer-route
/// changes.
///
/// Join resume, pause and removal all converge here so local forwarding and
/// remote relay control immediately see the same effective route selection.
pub fn refresh_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) {
    let route_entry = state.media_route_index.get(&source_transport_media_id);
    let local_packet_gate = route_entry.and_then(local_source_packet_gate);
    let remote_packet_gate =
        remote_source_packet_gate_for_route(route_entry, local_packet_gate.clone());
    state
        .route_control
        .set_local_packet_gate(source_transport_media_id, local_packet_gate);
    if let Some(remote_source_registration) =
        state.remote_source_registration(source_transport_media_id)
    {
        remote_source_registration.source_control().set_packet_gate(
            remote_source_registration.source_session_key().clone(),
            source_transport_media_id,
            remote_packet_gate,
        );
    }
}

/// Derive the gate sent to the producer worker for a remote source route.
///
/// RID and operating-point gates are enforced on the consumer worker. The
/// producer worker must keep the relay open for those routes because consumer
/// workers need to observe non-selected RID packets for bootstrap fallback and
/// stale-layer recovery. A local `Block` is forwarded only when it is a real
/// block, not when it is the temporary state used while a selected RID is still
/// pending.
fn remote_source_packet_gate_for_route(
    route_entry: Option<&MediaRouteEntry>,
    local_packet_gate: Option<PacketLayerGate>,
) -> PacketLayerGate {
    match (route_entry, local_packet_gate) {
        (
            Some(_),
            Some(
                PacketLayerGate::Open
                | PacketLayerGate::Rid(_)
                | PacketLayerGate::OperatingPoint(_),
            ),
        ) => PacketLayerGate::Open,
        (Some(route_entry), Some(PacketLayerGate::Block))
            if route_entry
                .destinations
                .iter()
                .any(|destination| destination.pending_packet_gate.is_some()) =>
        {
            PacketLayerGate::Open
        }
        (_route_entry, Some(packet_gate)) => packet_gate,
        (None | Some(_), None) => PacketLayerGate::Block,
    }
}

pub(super) fn owned_local_producer_mid(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Option<Mid> {
    ensure_owned_local_producer_mid(state, source_session_key, source_transport_media_id).ok()
}

fn worker_set_producer_active(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    ensure_owned_local_producer_mid(state, session_key, transport_media_id)?;
    let route_entry = state
        .media_route_index
        .get_mut(&transport_media_id)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    route_entry.source_active = active;
    Ok(())
}

fn worker_set_consumer_active(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    ensure_route_source(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
        RouteSourceAccess::Existing,
    )?;
    match state
        .mid_registry
        .get(&consumer_transport_media_id.as_u64())
    {
        Some(RegisteredMediaHandle::Consumer {
            session_key,
            source_transport_media_id: consumer_source_transport_media_id,
            ..
        }) if session_key == consumer_session_key
            && *consumer_source_transport_media_id == source_transport_media_id => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    let mut route_changed = false;
    {
        let route_entry = state
            .media_route_index
            .get_mut(&source_transport_media_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let destination = route_entry
            .destinations
            .iter_mut()
            .find(|destination| {
                destination.dest_session == *consumer_session_key
                    && destination.dest_transport_media_id == consumer_transport_media_id
            })
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        if destination.active != active {
            destination.active = active;
            route_changed = true;
        }
    }
    if !route_changed {
        return Ok(());
    }
    refresh_source_packet_gate(state, source_transport_media_id);
    update_remote_route_active(state, source_transport_media_id, active);
    Ok(())
}

/// Replaces the packet gate for exactly one consumer route.
///
/// Source ownership is checked first because a stale room effect may arrive
/// after user replacement or route cleanup. The source aggregate is refreshed
/// only when the destination gate actually changed
fn worker_set_consumer_packet_gate(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    packet_gate: PacketLayerGate,
) -> Result<(), TransportAdapterError> {
    if update_consumer_packet_gate(
        state,
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
        packet_gate,
    )? {
        refresh_source_packet_gate(state, source_transport_media_id);
    }
    Ok(())
}

fn worker_set_consumer_packet_gates(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    updates: Vec<ConsumerPacketGateCommand>,
) -> Vec<TransportResult<()>> {
    let mut route_changed = false;
    let mut results = Vec::with_capacity(updates.len());
    for update in updates {
        let (consumer_session_key, consumer_transport_media_id, packet_gate) = update.into_parts();
        match update_consumer_packet_gate(
            state,
            &consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            packet_gate,
        ) {
            Ok(changed) => {
                route_changed |= changed;
                results.push(Ok(()));
            }
            Err(error) => results.push(Err(error)),
        }
    }
    if route_changed {
        refresh_source_packet_gate(state, source_transport_media_id);
    }
    results
}

fn update_consumer_packet_gate(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    packet_gate: PacketLayerGate,
) -> Result<bool, TransportAdapterError> {
    ensure_route_source(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
        RouteSourceAccess::Existing,
    )?;
    match state
        .mid_registry
        .get(&consumer_transport_media_id.as_u64())
    {
        Some(RegisteredMediaHandle::Consumer {
            session_key,
            source_transport_media_id: consumer_source_transport_media_id,
            ..
        }) if session_key == consumer_session_key
            && *consumer_source_transport_media_id == source_transport_media_id => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    let (packet_gate, pending_packet_gate) =
        guarded_packet_gate(state, source_transport_media_id, packet_gate);
    {
        let route_entry = state
            .media_route_index
            .get_mut(&source_transport_media_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let destination = route_entry
            .destinations
            .iter_mut()
            .find(|destination| {
                destination.dest_session == *consumer_session_key
                    && destination.dest_transport_media_id == consumer_transport_media_id
            })
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        if destination.packet_gate != packet_gate
            || destination.pending_packet_gate != pending_packet_gate
        {
            destination.packet_gate = packet_gate;
            destination.pending_packet_gate = pending_packet_gate;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Request a refresh frame for an already-declared consumer route.
///
/// The worker revalidates consumer/source ownership and skips paused routes.
/// RID-gated destinations are mapped back into the keyframe target before the
/// local source is marked dirty or the remote keyframe request is forwarded with
/// the normal coalescing rules.
fn worker_request_consumer_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    let route_source = ensure_route_source(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
        RouteSourceAccess::Existing,
    )?;
    match state
        .mid_registry
        .get(&consumer_transport_media_id.as_u64())
    {
        Some(RegisteredMediaHandle::Consumer {
            session_key,
            source_transport_media_id: consumer_source_transport_media_id,
            ..
        }) if session_key == consumer_session_key
            && *consumer_source_transport_media_id == source_transport_media_id => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    let (destination_active, destination_rid) = state
        .media_route_index
        .get(&source_transport_media_id)
        .and_then(|route_entry| {
            route_entry.destinations.iter().find(|destination| {
                destination.dest_session == *consumer_session_key
                    && destination.dest_transport_media_id == consumer_transport_media_id
            })
        })
        .map(|destination| (destination.active, keyframe_request_rid(destination)))
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if !destination_active {
        return Ok(());
    }
    let now = Instant::now();
    match route_source {
        RouteSourceKind::Local => {
            request_keyframe_for_source(
                state,
                metrics,
                source_session_key,
                source_transport_media_id,
                destination_rid,
                KeyframeRequestKind::Pli,
                now,
            );
        }
        RouteSourceKind::Remote => {
            let Some((source_session_key, source_control)) = state
                .remote_source_registration(source_transport_media_id)
                .map(|registration| {
                    (
                        registration.source_session_key().clone(),
                        registration.source_control().clone(),
                    )
                })
            else {
                return Err(TransportAdapterError::TransportUnavailable);
            };
            match state.route_control.decide_keyframe_request_for_rid(
                source_transport_media_id,
                destination_rid,
                now,
            ) {
                KeyframeRequestDecision::Forward => {
                    source_control.request_keyframe(
                        source_session_key,
                        source_transport_media_id,
                        destination_rid,
                        KeyframeRequestKind::Pli,
                    );
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
                }
                KeyframeRequestDecision::Absorb => {
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
                }
            }
        }
    }
    Ok(())
}

fn keyframe_request_rid(destination: &MediaRouteDestination) -> Option<Rid> {
    destination
        .pending_packet_gate
        .as_ref()
        .and_then(packet_gate_rid)
        .or_else(|| packet_gate_rid(&destination.packet_gate))
}

fn packet_gate_rid(packet_gate: &PacketLayerGate) -> Option<Rid> {
    match packet_gate {
        PacketLayerGate::Rid(rid) => Some(*rid),
        PacketLayerGate::OperatingPoint(operating_point) => operating_point.rid(),
        PacketLayerGate::Open | PacketLayerGate::Block => None,
    }
}

fn has_pending_packet_gate_for_rid(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    live_rid: Rid,
) -> bool {
    state
        .media_route_index
        .get(&source_transport_media_id)
        .is_some_and(|route_entry| {
            route_entry.destinations.iter().any(|destination| {
                destination
                    .pending_packet_gate
                    .as_ref()
                    .and_then(packet_gate_rid)
                    == Some(live_rid)
            })
        })
}

/// Make the selected strict RID gate effective after a keyframe for that RID.
///
/// A RID can become live on delta frames before it is decodable by a receiver.
/// Activation waits for the VP8 keyframe check performed by the caller.
fn activate_pending_packet_gates_for_rid(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    live_rid: Rid,
) -> bool {
    let Some(route_entry) = state.media_route_index.get_mut(&source_transport_media_id) else {
        return false;
    };
    let mut changed = false;
    for destination in &mut route_entry.destinations {
        if destination
            .pending_packet_gate
            .as_ref()
            .and_then(packet_gate_rid)
            != Some(live_rid)
        {
            continue;
        }
        if let Some(packet_gate) = destination.pending_packet_gate.take() {
            debug!(
                ?source_transport_media_id,
                consumer_session_key = ?destination.dest_session,
                consumer_transport_media_id = ?destination.dest_transport_media_id,
                ?live_rid,
                activated_packet_gate = ?packet_gate,
                "activated deferred strict RID packet gate after producer RID became live"
            );
            destination.packet_gate = packet_gate;
            changed = true;
        }
    }
    changed
}

/// Allow one decodable fallback RID while the selected RID remains pending.
///
/// This is a bootstrap-only compromise. It avoids black screens when Chrome
/// starts `lo` first, but still keeps each consumer on exactly one publisher RID
/// at a time.
fn activate_bootstrap_fallback_packet_gates_for_rid(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    live_rid: Rid,
) -> bool {
    let Some(route_entry) = state.media_route_index.get_mut(&source_transport_media_id) else {
        return false;
    };
    let mut changed = false;
    for destination in &mut route_entry.destinations {
        let Some(selected_rid) = destination
            .pending_packet_gate
            .as_ref()
            .and_then(packet_gate_rid)
        else {
            continue;
        };
        if selected_rid == live_rid || !matches!(destination.packet_gate, PacketLayerGate::Block) {
            continue;
        }
        debug!(
            ?source_transport_media_id,
            consumer_session_key = ?destination.dest_session,
            consumer_transport_media_id = ?destination.dest_transport_media_id,
            fallback_rid = ?live_rid,
            pending_selected_rid = ?selected_rid,
            "activated bootstrap fallback RID packet gate while selected producer RID is pending"
        );
        destination.packet_gate = PacketLayerGate::Rid(live_rid);
        changed = true;
    }
    changed
}

/// Move stale strict gates back to pending when their selected RID goes quiet.
///
/// Browser encoders can pause a simulcast layer after it was once live. When
/// packets arrive for another RID and the selected RID is no longer fresh, this
/// prevents the consumer route from staying strict on a silent layer.
fn suspend_stale_packet_gates(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    incoming_rid: Rid,
    now: Instant,
) -> bool {
    let Some(route_entry) = state.media_route_index.get(&source_transport_media_id) else {
        return false;
    };
    let stale_rids = route_entry
        .destinations
        .iter()
        .filter(|destination| destination.pending_packet_gate.is_none())
        .filter_map(|destination| packet_gate_rid(&destination.packet_gate))
        .filter(|selected_rid| *selected_rid != incoming_rid)
        .filter(|selected_rid| {
            !state.producer_rid_is_ready(
                source_transport_media_id,
                *selected_rid,
                now,
                SELECTED_RID_READY_MAX_AGE,
            )
        })
        .fold(Vec::new(), |mut stale_rids, selected_rid| {
            if !stale_rids.contains(&selected_rid) {
                stale_rids.push(selected_rid);
            }
            stale_rids
        });
    if stale_rids.is_empty() {
        return false;
    }
    let Some(route_entry) = state.media_route_index.get_mut(&source_transport_media_id) else {
        return false;
    };
    let mut changed = false;
    for destination in &mut route_entry.destinations {
        if destination.pending_packet_gate.is_some() {
            continue;
        }
        let Some(selected_rid) = packet_gate_rid(&destination.packet_gate) else {
            continue;
        };
        if !stale_rids.contains(&selected_rid) {
            continue;
        }
        let selected_packet_gate = destination.packet_gate.clone();
        debug!(
            ?source_transport_media_id,
            consumer_session_key = ?destination.dest_session,
            consumer_transport_media_id = ?destination.dest_transport_media_id,
            ?incoming_rid,
            stale_rid = ?selected_rid,
            pending_packet_gate = ?selected_packet_gate,
            "blocked stale selected RID route until selected producer RID resumes"
        );
        destination.packet_gate = PacketLayerGate::Block;
        destination.pending_packet_gate = Some(selected_packet_gate);
        changed = true;
    }
    for stale_rid in stale_rids {
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            stale_rid,
            now,
        );
    }
    changed
}

fn request_pending_selected_rid_keyframes(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    now: Instant,
) {
    for rid in pending_selected_rids(state, source_transport_media_id) {
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
    }
}

fn pending_selected_rids(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> Vec<Rid> {
    state
        .media_route_index
        .get(&source_transport_media_id)
        .map(|route_entry| {
            route_entry
                .destinations
                .iter()
                .filter_map(|destination| {
                    destination
                        .pending_packet_gate
                        .as_ref()
                        .and_then(packet_gate_rid)
                })
                .fold(Vec::new(), |mut rids, rid| {
                    if !rids.contains(&rid) {
                        rids.push(rid);
                    }
                    rids
                })
        })
        .unwrap_or_default()
}

fn request_live_rid_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    debug!(
        user_id = ?source_session_key.user_id(),
        media_worker_id = source_session_key.media_worker_id(),
        ?source_transport_media_id,
        ?rid,
        "requesting selected RID producer keyframe"
    );
    if owned_local_producer_mid(state, source_session_key, source_transport_media_id).is_some() {
        request_keyframe_for_source(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            Some(rid),
            KeyframeRequestKind::Pli,
            now,
        );
        return;
    }
    let Some((registered_source_session_key, source_control)) = state
        .remote_source_registration(source_transport_media_id)
        .map(|registration| {
            (
                registration.source_session_key().clone(),
                registration.source_control().clone(),
            )
        })
    else {
        warn!(
            user_id = ?source_session_key.user_id(),
            media_worker_id = source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            "could not request selected RID keyframe because source ownership is unavailable"
        );
        return;
    };
    if registered_source_session_key != *source_session_key {
        warn!(
            observed_source_user_id = ?source_session_key.user_id(),
            observed_media_worker_id = source_session_key.media_worker_id(),
            registered_source_user_id = ?registered_source_session_key.user_id(),
            registered_media_worker_id = registered_source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            "could not request selected RID keyframe because source ownership changed"
        );
        return;
    }
    match state.route_control.decide_keyframe_request_for_rid(
        source_transport_media_id,
        Some(rid),
        now,
    ) {
        KeyframeRequestDecision::Forward => {
            source_control.request_keyframe(
                registered_source_session_key,
                source_transport_media_id,
                Some(rid),
                KeyframeRequestKind::Pli,
            );
            metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
        }
        KeyframeRequestDecision::Absorb => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
        }
    }
}

fn schedule_live_rid_keyframe_retries(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    for delay in SELECTED_RID_KEYFRAME_RETRY_DELAYS {
        state.schedule_rid_keyframe_refresh(source_transport_media_id, rid, now + delay);
    }
    debug!(
        ?source_transport_media_id,
        ?rid,
        retry_count = SELECTED_RID_KEYFRAME_RETRY_DELAYS.len(),
        "scheduled follow-up selected RID keyframe refreshes"
    );
}

fn drain_live_rid_keyframe_retries(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    let due_count = state.drain_due_rid_keyframe_refreshes(source_transport_media_id, rid, now);
    for _ in 0..due_count {
        debug!(
            user_id = ?source_session_key.user_id(),
            media_worker_id = source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            "draining follow-up selected RID keyframe refresh"
        );
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
    }
}

fn keyframe_refresh_source_session(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> Option<TransportSessionKey> {
    match state.media_handle(source_transport_media_id) {
        Some(RegisteredMediaHandle::Producer { session_key, .. }) => Some(session_key.clone()),
        Some(RegisteredMediaHandle::Consumer { .. }) | None => state
            .remote_source_registration(source_transport_media_id)
            .map(|registration| registration.source_session_key().clone()),
    }
}

fn ensure_route_source(
    state: &mut RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    access: RouteSourceAccess,
) -> Result<RouteSourceKind, TransportAdapterError> {
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        ensure_owned_local_producer_mid(state, source_session_key, source_transport_media_id)?;
        return Ok(RouteSourceKind::Local);
    }
    match access {
        RouteSourceAccess::Existing => {
            match state.remote_source_registration(source_transport_media_id) {
                Some(registration) if registration.source_session_key() == source_session_key => {
                    Ok(RouteSourceKind::Remote)
                }
                Some(_) => Err(TransportAdapterError::InvalidInput),
                None => Err(TransportAdapterError::TransportUnavailable),
            }
        }
        RouteSourceAccess::Register(remote_source_control) => {
            let Some(remote_source_control) = remote_source_control else {
                return Err(TransportAdapterError::InvalidInput);
            };
            let _previous_registration = state.register_remote_source(
                source_transport_media_id,
                source_session_key,
                remote_source_control,
            )?;
            Ok(RouteSourceKind::Remote)
        }
    }
}

fn update_remote_route_active(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    active: bool,
) {
    if let Some(remote_source_registration) =
        state.remote_source_registration(source_transport_media_id)
    {
        remote_source_registration
            .source_control()
            .set_route_active(
                remote_source_registration.source_session_key().clone(),
                source_transport_media_id,
                active,
            );
    }
}

fn ensure_owned_local_producer_mid(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<Mid, TransportAdapterError> {
    match state.media_handle(source_transport_media_id) {
        Some(RegisteredMediaHandle::Producer { session_key, mid })
            if session_key == source_session_key =>
        {
            Ok(*mid)
        }
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            Err(TransportAdapterError::InvalidInput)
        }
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}

fn local_source_packet_gate(route_entry: &MediaRouteEntry) -> Option<PacketLayerGate> {
    aggregate_packet_gates(
        route_entry
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .map(|destination| &destination.packet_gate),
    )
}
