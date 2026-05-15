//! worker-local consumer-route control for media fanout
//!
//! this file is the mutation boundary for `media_route_index`
//! lifecycle code creates and removes browser media handles, command adapters
//! deliver room effects and selected-rid readiness turns packet observations
//! into effective gates
//! this module keeps those paths on the same ownership checks before the packet
//! loop can observe a route
//!
//! route ownership has two valid shapes:
//!
//! ```text
//! same media worker
//!   source id -> RegisteredMediaHandle::Producer
//!
//! different media worker
//!   source id -> RemoteSourceRegistration
//! ```
//!
//! only consumer-route registration may create a remote-source entry
//! later active state, packet-gate and keyframe paths must find the existing
//! registration so stale room effects cannot recreate a removed remote source
//!
//! destination gates live on the route entry
//! [`refresh_source_packet_gate`] projects active destination gates into
//! source-local route control and mirrors the remote-source aggregate back to
//! the worker that owns the producer when the source is remote

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
        state::PacketLoopState,
    },
};

/// access mode for source validation
///
/// `Register` is restricted to route creation because it may install the
/// command path used to relay gates and keyframe requests back to the producer
/// worker
/// `Existing` is used by later mutations so they cannot recreate deleted remote
/// source state
enum RouteSourceAccess {
    /// require that local or remote source state already exists
    Existing,
    /// allow remote-source registration while the consumer route is created
    ///
    /// local sources ignore the control handle and are validated through media
    /// ownership
    Register(Option<RemoteSourceControl>),
}

/// route data captured after consumer media declaration
///
/// lifecycle builds this only after the source has been validated and the
/// consumer media handle has been registered
/// the values stay borrowed so route setup cannot outlive the command state or
/// copy negotiated RTP parameters just to pass them across this boundary
#[derive(Clone, Copy)]
pub(in crate::runtime::rtc_engine::worker::media) struct ConsumerRouteRegistration<'a> {
    /// session that owns the destination `Rtc`
    pub consumer_session_key: &'a TransportSessionKey,
    /// registered consumer media handle that receives forwarded packets
    pub consumer_transport_media_id: TransportMediaId,
    /// consumer MID used when packets are rewritten for local egress
    pub consumer_mid: Mid,
    /// producer or remote-source media id that feeds this destination
    pub source_transport_media_id: TransportMediaId,
    /// router-negotiated consumer stream used for payload rewriting and the
    /// initial packet gate
    pub consumer_rtp_parameters: &'a RouterRtpParameters,
    /// packet-loop clock sample used for selected-rid freshness checks
    pub now: Instant,
}

/// projects consumer RTP parameters into the gate installed on a new route
///
/// strict selected-rid gates may stay pending until packet-path liveness proves
/// that the selected rid can be decoded
fn consumer_packet_gate_for_source(
    state: &PacketLoopState,
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

/// validates source ownership for a route that is about to be created
///
/// local sources must still be producer handles owned by the declared source
/// session
/// remote sources require `remote_source_control` because later route refreshes
/// need a command path back to the producer worker
///
/// # errors
///
/// returns `InvalidInput` when a source id belongs to another owner, when the
/// media id names a consumer or when a remote source is missing its control path
/// returns `TransportUnavailable` when the local source media id does not exist
pub(in crate::runtime::rtc_engine::worker::media) fn ensure_route_source_registered(
    state: &mut PacketLoopState,
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

/// validates source ownership for a mutation on an existing route
///
/// this is used by active-state, packet-gate and keyframe paths
/// it never creates remote-source state, so cleanup cannot be undone by a late
/// command that still carries an old source id
///
/// # errors
///
/// returns `InvalidInput` when the media id exists but belongs to a different
/// source owner or is not a producer source for a local route
/// returns `TransportUnavailable` when the expected local producer or remote
/// source registration is gone
pub(in crate::runtime::rtc_engine::worker::media) fn ensure_existing_route_source(
    state: &mut PacketLoopState,
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

/// registers one consumer route in the worker-local forwarding index
///
/// this binds the consumer transport media to its source route, installs the
/// negotiated packet gate that drives the initial RID selection and updates any
/// remote-source relay activity that depends on the route existing
/// room policy may later replace the gate for this one consumer without
/// changing other destinations on the same source
///
/// a strict selected-rid route is not made effective until packet-path liveness
/// proves that the rid exists for this producer
/// the pending gate keeps the room-selected target attached to the destination
/// while the effective gate remains blocked or temporarily points at one
/// decodable fallback rid
///
/// callers must validate the source with [`ensure_route_source_registered`] and
/// register the consumer media handle before calling this function
pub(in crate::runtime::rtc_engine::worker::media) fn register_consumer_route(
    state: &mut PacketLoopState,
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

/// returns the payload type to write on packets sent to a consumer
///
/// encoding-level payload types are preferred because they are the most
/// specific negotiated value
/// when encodings do not carry one, the first primary codec supplies the
/// rewrite target
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

/// removes one consumer destination from a source route
///
/// teardown may call this after partial cleanup, so missing route state is
/// treated as already removed
/// when the last destination disappears, remote-source placeholders and their
/// packet-loop side tables are pruned with the route
pub(in crate::runtime::rtc_engine::worker::media) fn remove_consumer_route(
    state: &mut PacketLoopState,
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

/// recomputes the effective packet gate for one source after route changes
///
/// route creation, resume, pause, selected-rid activation and removal all
/// converge here so local forwarding and remote relay control observe the same
/// route selection
/// the local route-control gate is the union of active destination gates
/// remote sources receive the producer-worker gate derived from that union
pub(in crate::runtime::rtc_engine::worker) fn refresh_source_packet_gate(
    state: &mut PacketLoopState,
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

/// returns the MID for a local producer when ownership still matches
///
/// this is the best-effort form of [`ensure_owned_local_producer_mid`]
/// callers use it when teardown or feedback should ignore a source that already
/// disappeared
pub(in crate::runtime::rtc_engine::worker::media) fn owned_local_producer_mid(
    state: &PacketLoopState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Option<Mid> {
    ensure_owned_local_producer_mid(state, source_session_key, source_transport_media_id).ok()
}

/// updates the source-wide activity gate for one local producer route
///
/// source activity is enforced by the forwarding planner through
/// `MediaRouteEntry::source_active`
/// it is intentionally separate from packet-layer gates because producer
/// activity is a route-lifecycle fact, not a layer-selection predicate
///
/// # errors
///
/// returns `InvalidInput` when the media id does not name a producer owned by
/// `session_key`
/// returns `TransportUnavailable` when the producer or route entry is gone
pub(super) fn worker_set_producer_active(
    state: &mut PacketLoopState,
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

/// updates the destination activity gate for one existing consumer route
///
/// consumer activity changes the route aggregate only when the visible active
/// state changes
/// the source is revalidated before mutation so stale room effects cannot touch
/// a route after replacement or teardown
///
/// # errors
///
/// returns `InvalidInput` when the source or consumer media id belongs to a
/// different owner
/// returns `TransportUnavailable` when the source, consumer handle or route
/// destination is gone
pub(super) fn worker_set_consumer_active(
    state: &mut PacketLoopState,
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

/// replaces the packet gate for exactly one consumer route
///
/// source ownership is checked first because a stale room effect may arrive
/// after user replacement or route cleanup
/// the source aggregate is refreshed only when the effective or pending
/// destination gate actually changed
///
/// # errors
///
/// returns `InvalidInput` when the source or consumer media id belongs to a
/// different owner
/// returns `TransportUnavailable` when the source, consumer handle or route
/// destination is gone
pub(super) fn worker_set_consumer_packet_gate(
    state: &mut PacketLoopState,
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

/// applies a batch of packet-gate updates for one source route
///
/// each update is validated independently so callers receive one result per
/// requested consumer route
/// the source aggregate is refreshed once after the batch when at least one
/// destination gate changed
pub(super) fn worker_set_consumer_packet_gates(
    state: &mut PacketLoopState,
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

/// mutates one destination packet gate and reports whether the route changed
///
/// selected-rid gates pass through readiness guarding before they are written
/// to the destination
/// this keeps room policy attached to the route as `pending_packet_gate` until
/// the packet loop observes a decodable rid
///
/// # errors
///
/// returns `InvalidInput` when the source or consumer media id belongs to a
/// different owner
/// returns `TransportUnavailable` when the source, consumer handle or route
/// destination is gone
fn update_consumer_packet_gate(
    state: &mut PacketLoopState,
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
        // linear scan stays until dense-room packet-gate batches become a
        // production priority
        // benchmarks showed a destination-side index wins clearly only in
        // larger rooms, about 4.15x at 128 consumers and 12.91x at 512 consumers
        // tiny rooms do not justify the extra route-maintenance cost
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

/// extracts the rid restriction carried by a packet-layer gate
///
/// keyframe routing and selected-rid readiness use this to map destination
/// policy back to producer-side refresh targets
pub(in crate::runtime::rtc_engine::worker::media) fn packet_gate_rid(
    packet_gate: &PacketLayerGate,
) -> Option<Rid> {
    match packet_gate {
        PacketLayerGate::Rid(rid) => Some(*rid),
        PacketLayerGate::OperatingPoint(operating_point) => operating_point.rid(),
        PacketLayerGate::Open | PacketLayerGate::Block => None,
    }
}

/// validates the source side of a route and optionally registers remote state
///
/// local sources are resolved through worker media handles because they must be
/// producer media owned by the source session
/// remote sources are resolved through `remote_source_registry` because this
/// worker only owns a command path back to the producer worker
///
/// # errors
///
/// returns `InvalidInput` for owner mismatches, consumer media ids used as
/// sources or remote registration without a control handle
/// returns `TransportUnavailable` when the expected source state is absent
fn ensure_route_source(
    state: &mut PacketLoopState,
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

/// returns the MID for a local producer after enforcing source ownership
///
/// this is the strict form used by command paths that must reject stale or
/// misaddressed producer effects
///
/// # errors
///
/// returns `InvalidInput` when the media id exists but is not owned by
/// `source_session_key` as a producer
/// returns `TransportUnavailable` when the media id is not registered
pub(super) fn ensure_owned_local_producer_mid(
    state: &PacketLoopState,
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

/// computes the source-local packet gate required by active destinations
///
/// `source_active` is not part of this aggregate because the forwarding planner
/// applies that route-lifecycle gate before destination fanout
fn local_source_packet_gate(route_entry: &MediaRouteEntry) -> Option<PacketLayerGate> {
    aggregate_packet_gates(
        route_entry
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .map(|destination| &destination.packet_gate),
    )
}
