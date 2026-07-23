use std::time::Instant;

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::media::{MediaKind, Mid, Pt};

use super::{super::RouteSourceKind, selected_rid};
use crate::engine::media_transport::{
    SourceActivityUpdate, TransportAdapterError, TransportConsumerRoute, TransportMediaId,
    TransportResult, TransportSessionKey, TransportSourceKey,
    rtc::{
        commands::RemoteSourceControl,
        media_registry::RegisteredMediaHandle,
        relay_registry::{RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
        simulcast,
        slots::ConsumerStreamHandle,
        source_route::MediaRouteDestination,
        state::PacketLoopState,
    },
};

#[derive(Clone, Copy)]
pub struct ConsumerRouteRegistration<'a> {
    pub consumer_key: &'a TransportSessionKey,
    pub consumer_media: TransportMediaId,
    pub consumer_stream: ConsumerStreamHandle,
    pub consumer_mid: Mid,
    pub consumer_media_kind: MediaKind,
    pub src_media: TransportMediaId,
    pub consumer_rtp: &'a RouterRtpParameters,
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
pub fn ensure_route_src_registered(
    state: &mut PacketLoopState,
    route_owner_session_key: &TransportSessionKey,
    source: &TransportSourceKey,
    remote_source_control: Option<RemoteSourceControl>,
) -> Result<RouteSourceKind, TransportAdapterError> {
    let src_key = source.session_key();
    let src_media = source.transport_media_id();
    if src_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        ensure_local_producer_mid(state, src_key, src_media)?;
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
pub fn ensure_existing_route_src(
    state: &PacketLoopState,
    route_owner_session_key: &TransportSessionKey,
    source: &TransportSourceKey,
) -> Result<RouteSourceKind, TransportAdapterError> {
    let src_key = source.session_key();
    let src_media = source.transport_media_id();
    if src_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        ensure_local_producer_mid(state, src_key, src_media)?;
        return Ok(RouteSourceKind::Local);
    }
    match state.routes.remote_source(src_media) {
        Some(registration) if registration.source().session_key() == src_key => {
            Ok(RouteSourceKind::Remote)
        }
        Some(_) => Err(TransportAdapterError::InvalidInput),
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}

pub fn register_consumer_route(
    state: &mut PacketLoopState,
    registration: ConsumerRouteRegistration<'_>,
) {
    let ConsumerRouteRegistration {
        consumer_key,
        consumer_media,
        consumer_stream,
        consumer_mid,
        consumer_media_kind,
        src_media,
        consumer_rtp,
        active,
        now,
    } = registration;
    let (packet_gate, pending_gate) = selected_rid::guarded_pkt_gate(
        state,
        src_media,
        simulcast::initial_consumer_packet_gate(consumer_rtp),
        now,
    );
    let dest_payload_type = consumer_payload_type(consumer_rtp);
    let dst_idx = state.routes.add_consumer_route(
        src_media,
        MediaRouteDestination {
            dest_session: consumer_key.clone(),
            dest_transport_media_id: consumer_media,
            dest_stream: consumer_stream,
            dest_mid: consumer_mid,
            dest_payload_type,
            nackable: !consumer_media_kind.is_audio(),
            active,
            packet_gate,
            pending_gate,
        },
    );
    state.set_consumer_dst_idx(
        consumer_key,
        consumer_mid,
        consumer_media,
        src_media,
        Some(dst_idx),
    );
}

pub fn consumer_payload_type(consumer_rtp: &RouterRtpParameters) -> Option<Pt> {
    consumer_rtp
        .bindings()
        .find_map(|encoding| encoding.payload_type().map(Pt::from))
        .or_else(|| {
            consumer_rtp
                .formats()
                .find(|format| !format.codec().is_rtx())
                .map(|format| Pt::from(format.payload_type()))
        })
}

pub(super) fn set_remote_src_pkt_gate(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    let src_media = source.transport_media_id();
    if ensure_local_producer_mid(state, source.session_key(), src_media).is_err() {
        return;
    }
    state
        .routes
        .set_relay_pkt_gate(src_media, target_id, packet_gate);
}

pub(super) fn worker_add_relay_target(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    target: RelayPacketMailbox,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    ensure_local_producer_mid(state, source.session_key(), src_media)?;
    state.routes.add_relay_target(src_media, target_id, target);
    Ok(())
}

pub(super) fn worker_remove_relay_target(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    match ensure_local_producer_mid(state, source.session_key(), src_media) {
        Ok(_) => {}
        Err(TransportAdapterError::TransportUnavailable) => return Ok(()),
        Err(error) => return Err(error),
    }
    state.routes.remove_relay_target(src_media, target_id);
    Ok(())
}

pub(super) fn worker_set_relay_target_active(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    if active {
        ensure_local_producer_mid(state, source.session_key(), src_media)?;
    }
    state
        .routes
        .set_relay_target_active(src_media, target_id, active);
    Ok(())
}

pub fn remove_consumer_route(
    state: &mut PacketLoopState,
    consumer_key: &TransportSessionKey,
    consumer_media: TransportMediaId,
    src_media: TransportMediaId,
) {
    let Some(removed) = state
        .routes
        .remove_consumer_route(src_media, consumer_key, consumer_media)
    else {
        state.routes.prune_unrouted_remote_src(src_media);
        return;
    };
    state.set_consumer_dst_idx(
        &removed.destination.dest_session,
        removed.destination.dest_mid,
        removed.destination.dest_transport_media_id,
        src_media,
        None,
    );
    if let Some(moved) = &removed.moved {
        state.set_consumer_dst_idx(
            &moved.session_key,
            moved.mid,
            moved.media_id,
            src_media,
            Some(moved.dst_idx),
        );
    }
    release_dst_stream(state, &removed.destination);
}

pub fn remove_source_route(state: &mut PacketLoopState, src_media: TransportMediaId) {
    let Some(route_entry) = state.routes.take_route(src_media) else {
        return;
    };
    for destination in route_entry.destinations {
        state.set_consumer_dst_idx(
            &destination.dest_session,
            destination.dest_mid,
            destination.dest_transport_media_id,
            src_media,
            None,
        );
        release_dst_stream(state, &destination);
    }
}

fn release_dst_stream(state: &mut PacketLoopState, destination: &MediaRouteDestination) {
    if let Some(session_state) = state.users.get_mut(&destination.dest_session) {
        session_state
            .consumer_streams
            .release(destination.dest_stream);
    }
}

pub(super) fn worker_apply_producer_activity(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    update: SourceActivityUpdate,
) -> Result<bool, TransportAdapterError> {
    let src_media = source.transport_media_id();
    ensure_local_producer_mid(state, source.session_key(), src_media)?;
    state.routes.apply_source_activity(src_media, update)
}

pub(super) fn worker_set_remote_source_activity(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    update: SourceActivityUpdate,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    match state.routes.remote_source(src_media) {
        Some(registration) if registration.source() == source => {}
        Some(_) => return Err(TransportAdapterError::InvalidInput),
        None => return Ok(()),
    }
    state
        .routes
        .apply_source_activity(src_media, update)
        .map(|_| ())
}

pub(super) fn worker_set_consumer_active(
    state: &mut PacketLoopState,
    route: &TransportConsumerRoute,
    active: bool,
) -> Result<(), TransportAdapterError> {
    update_consumer_route(state, route, ConsumerRouteMutation::Active(active)).map(|_| ())
}

pub(super) fn worker_set_consumer_pkt_gates(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    updates: Vec<(usize, TransportConsumerRoute, PacketLayerGate)>,
    now: Instant,
) -> Vec<TransportResult<()>> {
    let src_media = source.transport_media_id();
    let mut changed = false;
    let mut results = Vec::with_capacity(updates.len());
    for (_, route, packet_gate) in updates {
        if route.source() != source {
            results.push(Err(TransportAdapterError::InvalidInput));
            continue;
        }
        let result = update_consumer_route(
            state,
            &route,
            ConsumerRouteMutation::PacketGate { packet_gate, now },
        );
        results.push(
            result
                .inspect(|route_changed| changed |= *route_changed)
                .map(|_| ()),
        );
    }
    if changed {
        state.routes.refresh_src_pkt_gate(src_media);
    }
    results
}

#[derive(Clone, Copy)]
enum ConsumerRouteMutation {
    Active(bool),
    PacketGate {
        packet_gate: PacketLayerGate,
        now: Instant,
    },
}

fn update_consumer_route(
    state: &mut PacketLoopState,
    route: &TransportConsumerRoute,
    mutation: ConsumerRouteMutation,
) -> Result<bool, TransportAdapterError> {
    let consumer_key = route.consumer_session_key();
    let consumer_media = route.consumer_transport_media_id();
    let src_media = route.source_transport_media_id();
    ensure_existing_route_src(state, consumer_key, route.source())?;
    let RegisteredMediaHandle::Consumer {
        session_key,
        mid,
        src_media: consumer_src_media,
        ..
    } = state
        .media_handle(consumer_media)
        .ok_or(TransportAdapterError::TransportUnavailable)?
    else {
        return Err(TransportAdapterError::InvalidInput);
    };
    if session_key != consumer_key || *consumer_src_media != src_media {
        return Err(TransportAdapterError::InvalidInput);
    }
    let dst_idx = state
        .consumer_dst_idx(consumer_key, *mid, consumer_media, src_media)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    match mutation {
        ConsumerRouteMutation::Active(active) => state.routes.set_consumer_active(
            src_media,
            dst_idx,
            consumer_key,
            consumer_media,
            active,
        ),
        ConsumerRouteMutation::PacketGate { packet_gate, now } => {
            let (packet_gate, pending_gate) =
                selected_rid::guarded_pkt_gate(state, src_media, packet_gate, now);
            state.routes.set_consumer_pkt_gate(
                src_media,
                dst_idx,
                consumer_key,
                consumer_media,
                packet_gate,
                pending_gate,
            )
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
/// returns `InvalidInput` when the media id exists without a live producer
/// owned by `src_key`
/// returns `TransportUnavailable` when the media id is not registered
pub fn ensure_local_producer_mid(
    state: &PacketLoopState,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
) -> Result<Mid, TransportAdapterError> {
    match state.media_handle(src_media) {
        Some(RegisteredMediaHandle::Producer { session_key, mid })
            if session_key == src_key && state.users.contains_key(src_key) =>
        {
            Ok(*mid)
        }
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            Err(TransportAdapterError::InvalidInput)
        }
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}
