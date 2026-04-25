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

use std::time::Instant;

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{KeyframeRequestKind, Mid, Rid};
use tokio::sync::oneshot;

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

pub(crate) fn respond_set_producer_active(
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

pub(crate) fn respond_set_consumer_active(
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
pub(crate) fn respond_set_consumer_packet_gate(
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

pub(crate) fn respond_set_consumer_packet_gates(
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

pub(crate) fn respond_request_consumer_keyframe(
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

pub(crate) fn respond_set_remote_source_route_active(
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

pub(crate) fn respond_set_remote_source_packet_gate(
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

/// Reduces negotiated consumer RTP parameters into the route-level packet gate.
///
/// A single selected RID means this route should forward only that simulcast
/// layer. Mixed or RID-less encodings stay open so non-simulcast streams and
/// broader compatibility cases keep the previous behavior.
pub(super) fn consumer_packet_gate(
    consumer_rtp_parameters: &RouterRtpParameters,
) -> PacketLayerGate {
    let mut selected_rid: Option<Rid> = None;
    for encoding in consumer_rtp_parameters.encodings() {
        let Some(rid) = encoding.rid().map(Rid::from) else {
            return PacketLayerGate::Open;
        };
        if let Some(current_rid) = selected_rid.as_ref() {
            if current_rid != &rid {
                return PacketLayerGate::Open;
            }
        } else {
            selected_rid = Some(rid);
        }
    }
    selected_rid.map_or(PacketLayerGate::Open, PacketLayerGate::Rid)
}

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
pub(super) fn register_consumer_route(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    consumer_mid: Mid,
    source_transport_media_id: TransportMediaId,
    route_source: RouteSourceKind,
    consumer_rtp_parameters: &RouterRtpParameters,
) {
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
            active: true,
            packet_gate: consumer_packet_gate(consumer_rtp_parameters),
        });
    refresh_source_packet_gate(state, source_transport_media_id);
    if matches!(route_source, RouteSourceKind::Remote) {
        update_remote_route_active(state, source_transport_media_id, true);
    }
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
pub(crate) fn refresh_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) {
    let local_packet_gate = state
        .media_route_index
        .get(&source_transport_media_id)
        .and_then(local_source_packet_gate);
    state
        .route_control
        .set_local_packet_gate(source_transport_media_id, local_packet_gate.clone());
    if let Some(remote_source_registration) =
        state.remote_source_registration(source_transport_media_id)
    {
        remote_source_registration.source_control().set_packet_gate(
            remote_source_registration.source_session_key().clone(),
            source_transport_media_id,
            local_packet_gate.unwrap_or(PacketLayerGate::Block),
        );
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
/// Source ownership is checked first because a stale channel effect may arrive
/// after session replacement or route cleanup. The source aggregate is refreshed
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
        if destination.packet_gate != packet_gate {
            destination.packet_gate = packet_gate;
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
            match state
                .route_control
                .decide_keyframe_request(source_transport_media_id, now)
            {
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
    match &destination.packet_gate {
        PacketLayerGate::Rid(rid) => Some(*rid),
        PacketLayerGate::OperatingPoint(operating_point) => operating_point.rid(),
        PacketLayerGate::Open | PacketLayerGate::Block => None,
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
