//! Consumer-route registration, source validation and packet-gate mutation.

use std::time::Instant;

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{Mid, Pt, Rid};

use super::{
    super::types::{ConsumerPacketGateRequest, RouteSourceKind},
    remote_source, selected_rid,
};
use crate::runtime::{
    media_transport::{
        TransportAdapterError, TransportMediaId, TransportResult, TransportSessionKey,
    },
    rtc_engine::{
        commands::{ConsumerPacketGateCommand, RemoteSourceControl},
        demux::{MediaRouteDestination, MediaRouteEntry},
        media_registry::RegisteredMediaHandle,
        route_control::{PacketLayerGate, aggregate_packet_gates},
        simulcast,
        state::RtcBootstrapState,
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

#[derive(Clone, Copy)]
pub(in crate::runtime::rtc_engine::worker::media) struct ConsumerRouteRegistration<'a> {
    pub(in crate::runtime::rtc_engine::worker::media) consumer_session_key: &'a TransportSessionKey,
    pub(in crate::runtime::rtc_engine::worker::media) consumer_transport_media_id: TransportMediaId,
    pub(in crate::runtime::rtc_engine::worker::media) consumer_mid: Mid,
    pub(in crate::runtime::rtc_engine::worker::media) source_transport_media_id: TransportMediaId,
    pub(in crate::runtime::rtc_engine::worker::media) consumer_rtp_parameters:
        &'a RouterRtpParameters,
    pub(in crate::runtime::rtc_engine::worker::media) now: Instant,
}

fn consumer_packet_gate_for_source(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    consumer_rtp_parameters: &RouterRtpParameters,
    now: Instant,
) -> (PacketLayerGate, Option<PacketLayerGate>) {
    selected_rid::guarded_packet_gate(
        state,
        source_transport_media_id,
        simulcast::initial_consumer_packet_gate(consumer_rtp_parameters),
        now,
    )
}

/// Ensure the source side of a route is valid before adding a destination.
///
/// Local sources must still be owned by the requesting session. Remote sources
/// are registered with relay control so later source-gate updates can be pushed
/// back to the worker that owns the producer.
pub(in crate::runtime::rtc_engine::worker::media) fn ensure_route_source_registered(
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

pub(in crate::runtime::rtc_engine::worker::media) fn ensure_existing_route_source(
    state: &mut RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<RouteSourceKind, TransportAdapterError> {
    ensure_route_source(
        state,
        route_owner_session_key,
        source_session_key,
        source_transport_media_id,
        RouteSourceAccess::Existing,
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
pub(in crate::runtime::rtc_engine::worker::media) fn register_consumer_route(
    state: &mut RtcBootstrapState,
    registration: ConsumerRouteRegistration<'_>,
) {
    let ConsumerRouteRegistration {
        consumer_session_key,
        consumer_transport_media_id,
        consumer_mid,
        source_transport_media_id,
        consumer_rtp_parameters,
        now,
    } = registration;
    let (packet_gate, pending_packet_gate) = consumer_packet_gate_for_source(
        state,
        source_transport_media_id,
        consumer_rtp_parameters,
        now,
    );
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
}

pub(in crate::runtime::rtc_engine::worker::media) fn consumer_payload_type(
    consumer_rtp_parameters: &RouterRtpParameters,
) -> Option<Pt> {
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

pub(in crate::runtime::rtc_engine::worker::media) fn remove_consumer_route(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_transport_media_id: TransportMediaId,
) {
    let Some(route_entry) = state.media_route_index.get_mut(&source_transport_media_id) else {
        state.prune_remote_source_if_unrouted(source_transport_media_id);
        return;
    };
    let remove_route_entry = {
        let Some(position) = route_entry.destinations.iter().position(|destination| {
            destination.dest_session == *consumer_session_key
                && destination.dest_transport_media_id == consumer_transport_media_id
        }) else {
            return;
        };
        route_entry.destinations.remove(position);
        route_entry.destinations.is_empty()
    };
    if remove_route_entry {
        state.media_route_index.remove(&source_transport_media_id);
    }
    refresh_source_packet_gate(state, source_transport_media_id);
    state.prune_remote_source_if_unrouted(source_transport_media_id);
}

/// Recompute the effective packet gate for one source after consumer-route
/// changes.
///
/// Join resume, pause and removal all converge here so local forwarding and
/// remote relay control immediately see the same effective route selection.
pub(in crate::runtime::rtc_engine::worker) fn refresh_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) {
    let route_entry = state.media_route_index.get(&source_transport_media_id);
    let local_packet_gate = route_entry.and_then(local_source_packet_gate);
    let remote_packet_gate =
        remote_source::remote_source_packet_gate_for_route(route_entry, local_packet_gate.clone());
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

pub(in crate::runtime::rtc_engine::worker::media) fn owned_local_producer_mid(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Option<Mid> {
    ensure_owned_local_producer_mid(state, source_session_key, source_transport_media_id).ok()
}

pub(super) fn worker_set_producer_active(
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

pub(super) fn worker_set_consumer_active(
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
    Ok(())
}

/// Replaces the packet gate for exactly one consumer route.
///
/// Source ownership is checked first because a stale room effect may arrive
/// after user replacement or route cleanup. The source aggregate is refreshed
/// only when the destination gate actually changed
pub(super) fn worker_set_consumer_packet_gate(
    state: &mut RtcBootstrapState,
    request: ConsumerPacketGateRequest<'_>,
    now: Instant,
) -> Result<(), TransportAdapterError> {
    if update_consumer_packet_gate(
        state,
        request.consumer_session_key,
        request.consumer_transport_media_id,
        request.source_session_key,
        request.source_transport_media_id,
        request.packet_gate,
        now,
    )? {
        refresh_source_packet_gate(state, request.source_transport_media_id);
    }
    Ok(())
}

pub(super) fn worker_set_consumer_packet_gates(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    updates: Vec<ConsumerPacketGateCommand>,
    now: Instant,
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
            now,
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
    now: Instant,
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
        selected_rid::guarded_packet_gate(state, source_transport_media_id, packet_gate, now);
    {
        let route_entry = state
            .media_route_index
            .get_mut(&source_transport_media_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        // PERFORMANCE: The linear scan is intentionally kept for now. Benchmarks showed
        // that a destination side index only wins clearly in larger rooms, about 4.15x at
        // 128 consumers and 12.91x at 512 consumers, while tiny rooms do not justify the
        // extra route maintenance cost. Add the index when dense-room packet-gate batches
        // become a production priority.
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

pub(in crate::runtime::rtc_engine::worker::media) fn packet_gate_rid(
    packet_gate: &PacketLayerGate,
) -> Option<Rid> {
    match packet_gate {
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

pub(super) fn ensure_owned_local_producer_mid(
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
