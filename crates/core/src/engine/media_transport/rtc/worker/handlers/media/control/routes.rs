use std::time::Instant;

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{MediaKind, Mid, Pt};

use super::{super::types::RouteSourceKind, selected_rid};
use crate::engine::media_transport::{
    TransportAdapterError, TransportConsumerRoute, TransportMediaId, TransportResult,
    TransportSessionKey, TransportSourceKey,
    rtc::{
        commands::{ConsumerPacketGateCommand, RemoteSourceControl},
        demux::MediaRouteDestination,
        local_send_rewrite::forget_transport_media_stream,
        media_registry::RegisteredMediaHandle,
        route_control::PacketLayerGate,
        simulcast,
        slots::ConsumerStreamHandle,
        state::PacketLoopState,
    },
};

#[derive(Clone, Copy)]
pub(in crate::engine::media_transport::rtc::worker::handlers::media) struct ConsumerRouteRegistration<
    'a,
> {
    pub consumer_session_key: &'a TransportSessionKey,
    pub consumer_transport_media_id: TransportMediaId,
    pub consumer_stream: ConsumerStreamHandle,
    pub consumer_mid: Mid,
    pub consumer_media_kind: MediaKind,
    pub source_transport_media_id: TransportMediaId,
    pub consumer_rtp_parameters: &'a RouterRtpParameters,
    pub active: bool,
    pub now: Instant,
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
pub(in crate::engine::media_transport::rtc::worker::handlers::media) fn ensure_route_source_registered(
    state: &mut PacketLoopState,
    route_owner_session_key: &TransportSessionKey,
    source: &TransportSourceKey,
    remote_source_control: Option<RemoteSourceControl>,
) -> Result<RouteSourceKind, TransportAdapterError> {
    let source_session_key = source.session_key();
    let source_id = source.transport_media_id();
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        ensure_owned_local_producer_mid(state, source_session_key, source_id)?;
        return Ok(RouteSourceKind::Local);
    }
    let Some(remote_source_control) = remote_source_control else {
        return Err(TransportAdapterError::InvalidInput);
    };
    state
        .routes
        .register_remote_source(source, remote_source_control)?;
    Ok(RouteSourceKind::Remote)
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
pub(in crate::engine::media_transport::rtc::worker::handlers::media) fn ensure_existing_route_source(
    state: &PacketLoopState,
    route_owner_session_key: &TransportSessionKey,
    source: &TransportSourceKey,
) -> Result<RouteSourceKind, TransportAdapterError> {
    let source_session_key = source.session_key();
    let source_id = source.transport_media_id();
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        ensure_owned_local_producer_mid(state, source_session_key, source_id)?;
        return Ok(RouteSourceKind::Local);
    }
    match state.routes.remote_source(source_id) {
        Some(registration) if registration.source_session_key() == source_session_key => {
            Ok(RouteSourceKind::Remote)
        }
        Some(_) => Err(TransportAdapterError::InvalidInput),
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}

pub(in crate::engine::media_transport::rtc::worker::handlers::media) fn register_consumer_route(
    state: &mut PacketLoopState,
    registration: ConsumerRouteRegistration<'_>,
) {
    let ConsumerRouteRegistration {
        consumer_session_key,
        consumer_transport_media_id,
        consumer_stream,
        consumer_mid,
        consumer_media_kind,
        source_transport_media_id,
        consumer_rtp_parameters,
        active,
        now,
    } = registration;
    let (packet_gate, pending_packet_gate) = selected_rid::guarded_packet_gate(
        state,
        source_transport_media_id,
        simulcast::initial_consumer_packet_gate(consumer_rtp_parameters),
        now,
    );
    let dest_payload_type = consumer_payload_type(consumer_rtp_parameters);
    let destination_index = state.routes.add_consumer_route(
        source_transport_media_id,
        MediaRouteDestination {
            dest_session: consumer_session_key.clone(),
            dest_transport_media_id: consumer_transport_media_id,
            dest_stream: consumer_stream,
            dest_mid: consumer_mid,
            dest_payload_type,
            nackable: !consumer_media_kind.is_audio(),
            active,
            packet_gate,
            pending_packet_gate,
        },
    );
    state.set_consumer_destination_index(
        consumer_session_key,
        consumer_mid,
        consumer_transport_media_id,
        source_transport_media_id,
        Some(destination_index),
    );
}

pub(in crate::engine::media_transport::rtc::worker::handlers::media) fn consumer_payload_type(
    consumer_rtp_parameters: &RouterRtpParameters,
) -> Option<Pt> {
    consumer_rtp_parameters
        .bindings()
        .find_map(|encoding| encoding.payload_type().map(Pt::from))
        .or_else(|| {
            consumer_rtp_parameters
                .formats()
                .find(|format| !format.codec().is_rtx())
                .map(|format| Pt::from(format.payload_type()))
        })
}

pub(in crate::engine::media_transport::rtc::worker::handlers::media) fn remove_consumer_route(
    state: &mut PacketLoopState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_transport_media_id: TransportMediaId,
) {
    let Some(removed) = state.routes.remove_consumer_route(
        source_transport_media_id,
        consumer_session_key,
        consumer_transport_media_id,
    ) else {
        state
            .routes
            .prune_remote_source_if_unrouted(source_transport_media_id);
        return;
    };
    let (removed, moved) = removed;
    state.set_consumer_destination_index(
        &removed.dest_session,
        removed.dest_mid,
        removed.dest_transport_media_id,
        source_transport_media_id,
        None,
    );
    if let Some((session, mid, media_id, index)) = moved {
        // `swap_remove` can move another consumer into `position`
        // repair that consumer's feedback index before the route is reused
        state.set_consumer_destination_index(
            &session,
            mid,
            media_id,
            source_transport_media_id,
            Some(index),
        );
    }
    release_destination_stream(state, &removed);
}

pub(in crate::engine::media_transport::rtc::worker) fn remove_source_route(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
) {
    let Some(route_entry) = state.routes.take_route(source_transport_media_id) else {
        return;
    };
    for destination in route_entry.destinations {
        state.set_consumer_destination_index(
            &destination.dest_session,
            destination.dest_mid,
            destination.dest_transport_media_id,
            source_transport_media_id,
            None,
        );
        release_destination_stream(state, &destination);
    }
}

fn release_destination_stream(state: &mut PacketLoopState, destination: &MediaRouteDestination) {
    if let Some(session_state) = state.users.get_mut(&destination.dest_session) {
        forget_transport_media_stream(&mut session_state.consumer_streams, destination.dest_stream);
    }
}

pub(super) fn worker_set_producer_active(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    active: bool,
) -> Result<(), TransportAdapterError> {
    let source_transport_media_id = source.transport_media_id();
    ensure_owned_local_producer_mid(state, source.session_key(), source_transport_media_id)?;
    state
        .routes
        .set_source_active(source_transport_media_id, active)
}

pub(super) fn worker_set_consumer_active(
    state: &mut PacketLoopState,
    route: &TransportConsumerRoute,
    active: bool,
) -> Result<(), TransportAdapterError> {
    update_consumer_route(state, route, ConsumerRouteMutation::Active(active)).map(|_| ())
}

pub(super) fn worker_set_consumer_packet_gate(
    state: &mut PacketLoopState,
    route: &TransportConsumerRoute,
    packet_gate: PacketLayerGate,
    now: Instant,
) -> Result<(), TransportAdapterError> {
    update_consumer_route(
        state,
        route,
        ConsumerRouteMutation::PacketGate(packet_gate, now, true),
    )
    .map(|_| ())
}

pub(super) fn worker_set_consumer_packet_gates(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    updates: Vec<ConsumerPacketGateCommand>,
    now: Instant,
) -> Vec<TransportResult<()>> {
    let source_transport_media_id = source.transport_media_id();
    let mut changed = false;
    let mut results = Vec::with_capacity(updates.len());
    for update in updates {
        let (consumer_session_key, consumer_transport_media_id, packet_gate) = update.into_parts();
        let route = TransportConsumerRoute::new(
            consumer_session_key,
            consumer_transport_media_id,
            source.clone(),
        );
        match update_consumer_route(
            state,
            &route,
            ConsumerRouteMutation::PacketGate(packet_gate, now, false),
        ) {
            Ok(route_changed) => {
                changed |= route_changed;
                results.push(Ok(()));
            }
            Err(error) => results.push(Err(error)),
        }
    }
    if changed {
        state
            .routes
            .refresh_source_packet_gate(source_transport_media_id);
    }
    results
}

#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) fn worker_set_consumer_packet_gates_for_benchmark(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    updates: Vec<ConsumerPacketGateCommand>,
    now: Instant,
) -> Vec<TransportResult<()>> {
    worker_set_consumer_packet_gates(state, source, updates, now)
}

#[derive(Clone, Copy)]
enum ConsumerRouteMutation {
    Active(bool),
    PacketGate(PacketLayerGate, Instant, bool),
}

fn update_consumer_route(
    state: &mut PacketLoopState,
    route: &TransportConsumerRoute,
    mutation: ConsumerRouteMutation,
) -> Result<bool, TransportAdapterError> {
    let consumer_session_key = route.consumer_session_key();
    let consumer_transport_media_id = route.consumer_transport_media_id();
    let source_transport_media_id = route.source_transport_media_id();
    ensure_existing_route_source(state, consumer_session_key, route.source())?;
    let RegisteredMediaHandle::Consumer {
        session_key,
        mid,
        source_transport_media_id: consumer_source_transport_media_id,
        ..
    } = state
        .media_handle(consumer_transport_media_id)
        .ok_or(TransportAdapterError::TransportUnavailable)?
    else {
        return Err(TransportAdapterError::InvalidInput);
    };
    if session_key != consumer_session_key
        || *consumer_source_transport_media_id != source_transport_media_id
    {
        return Err(TransportAdapterError::InvalidInput);
    }
    let destination_index = state
        .consumer_destination_index(
            consumer_session_key,
            *mid,
            consumer_transport_media_id,
            source_transport_media_id,
        )
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    match mutation {
        ConsumerRouteMutation::Active(active) => state.routes.set_consumer_active(
            source_transport_media_id,
            destination_index,
            consumer_session_key,
            consumer_transport_media_id,
            active,
        ),
        ConsumerRouteMutation::PacketGate(packet_gate, now, refresh) => {
            let (packet_gate, pending_packet_gate) = selected_rid::guarded_packet_gate(
                state,
                source_transport_media_id,
                packet_gate,
                now,
            );
            if refresh {
                state.routes.set_consumer_packet_gate(
                    source_transport_media_id,
                    destination_index,
                    consumer_session_key,
                    consumer_transport_media_id,
                    packet_gate,
                    pending_packet_gate,
                )
            } else {
                state.routes.set_consumer_packet_gate_in_batch(
                    source_transport_media_id,
                    destination_index,
                    consumer_session_key,
                    consumer_transport_media_id,
                    packet_gate,
                    pending_packet_gate,
                )
            }
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
pub(in crate::engine::media_transport::rtc::worker::handlers::media) fn ensure_owned_local_producer_mid(
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
