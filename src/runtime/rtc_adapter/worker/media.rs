use o_sfu_router::RtpParameters as RouterRtpParameters;
use std::time::Instant;
use str0m::media::{Direction, KeyframeRequestKind, MediaKind, Mid, Rid};
use str0m::rtp::Ssrc;
use tokio::sync::oneshot;
use tracing::debug;

use crate::runtime::metrics::{RtcRouteControlOutcome, RuntimeMetrics};
use crate::runtime::transport_adapter::{
    SourceMediaRoutingPolicy, TransportAdapterError, TransportMediaId, TransportSessionKey,
};

use super::super::{
    commands::{RelayCleanup, RemoteSourceControl, RemoveMediaOutcome},
    demux::{MediaRouteDestination, MediaRouteEntry},
    media_registry::RegisteredMediaHandle,
    relay_registry::RelayRegistry,
    route_control::{PacketLayerGate, aggregate_packet_gates},
    state::RtcBootstrapState,
};

enum RouteSourceKind {
    Local { source_mid: Mid },
    Remote,
}

pub(super) struct AddSendMediaRequest<'a> {
    pub(super) consumer_session_key: &'a TransportSessionKey,
    pub(super) media_kind: MediaKind,
    pub(super) source_session_key: &'a TransportSessionKey,
    pub(super) source_transport_media_id: TransportMediaId,
    pub(super) remote_source_control: Option<RemoteSourceControl>,
    pub(super) consumer_rtp_parameters: &'a RouterRtpParameters,
}

pub(super) fn respond_remove_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    response: oneshot::Sender<Result<RemoveMediaOutcome, TransportAdapterError>>,
) {
    let _ = response.send(worker_remove_media(state, session_key, transport_media_id));
}

pub(super) fn respond_add_recv_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
) {
    let _ = response.send(worker_add_recv_media(
        state,
        session_key,
        media_kind,
        rtp_parameters,
    ));
}

pub(super) fn respond_add_send_media(
    state: &mut RtcBootstrapState,
    request: AddSendMediaRequest<'_>,
    response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
) {
    let _ = response.send(worker_add_send_media(
        state,
        request.consumer_session_key,
        request.media_kind,
        request.source_session_key,
        request.source_transport_media_id,
        request.remote_source_control,
        request.consumer_rtp_parameters,
    ));
}

pub(super) fn respond_request_remote_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    relay_registry: &RelayRegistry,
    request: &RemoteKeyframeRequest<'_>,
) {
    if !relay_registry.is_source_target_active(request.source_transport_media_id, request.target_id)
    {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
        return;
    }
    request_keyframe_for_source(
        state,
        metrics,
        request.source_session_key,
        request.source_transport_media_id,
        request.rid,
        request.kind,
        Instant::now(),
    );
}

pub(super) fn respond_set_remote_source_route_active(
    state: &RtcBootstrapState,
    relay_registry: &RelayRegistry,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::relay_registry::RelayTargetId,
    active: bool,
) {
    set_remote_source_route_active(
        state,
        relay_registry,
        source_session_key,
        source_transport_media_id,
        target_id,
        active,
    );
}

pub(super) fn respond_set_remote_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::relay_registry::RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    set_remote_source_packet_gate(
        state,
        source_session_key,
        source_transport_media_id,
        target_id,
        packet_gate,
    );
}

pub(super) struct RemoteKeyframeRequest<'a> {
    pub(super) source_session_key: &'a TransportSessionKey,
    pub(super) source_transport_media_id: TransportMediaId,
    pub(super) target_id: super::super::relay_registry::RelayTargetId,
    pub(super) rid: Option<Rid>,
    pub(super) kind: KeyframeRequestKind,
}

pub(super) fn respond_set_producer_active(
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

pub(super) fn respond_set_consumer_active(
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

pub(super) fn respond_set_source_routing_policy(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    policy: SourceMediaRoutingPolicy,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_source_routing_policy(
        state,
        session_key,
        source_transport_media_id,
        policy,
    ));
}

fn worker_remove_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<RemoveMediaOutcome, TransportAdapterError> {
    let Some(handle) = state.remove_media_handle(transport_media_id) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if handle.session_key() != session_key {
        return Err(TransportAdapterError::InvalidInput);
    }
    match handle {
        RegisteredMediaHandle::Producer { session_key, mid } => {
            let should_remove_media = !state.session_has_mid(&session_key, mid);
            if let Some(session_state) = state.sessions.get_mut(&session_key) {
                session_state
                    .sdp_negotiation
                    .negotiated_producer_parameters
                    .remove(&mid);
            }
            if let Some(session_state) = state.sessions.get_mut(&session_key)
                && should_remove_media
            {
                if session_state.sdp_negotiation.initial_offer_applied {
                    worker_stage_native_media_removal(session_state, mid)?;
                } else {
                    session_state.rtc.direct_api().remove_media(mid);
                }
            }
            state.media_route_index.remove(&transport_media_id);
            state.mark_session_dirty(&session_key);
            Ok(RemoveMediaOutcome::without_relay_cleanup())
        }
        RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            source_transport_media_id,
        } => {
            let relay_cleanup = relay_cleanup_for_source(state, source_transport_media_id);
            let remote_source_registration = state
                .remote_source_registration(source_transport_media_id)
                .cloned();
            let should_remove_media = !state.session_has_mid(&session_key, mid);
            if let Some(session_state) = state.sessions.get_mut(&session_key)
                && should_remove_media
            {
                if session_state.sdp_negotiation.initial_offer_applied {
                    worker_stage_native_media_removal(session_state, mid)?;
                } else {
                    session_state.rtc.direct_api().remove_media(mid);
                }
            }
            if let Some(route_entry) = state.media_route_index.get_mut(&source_transport_media_id) {
                if let Some(position) = route_entry.destinations.iter().position(|destination| {
                    destination.dest_session == session_key
                        && destination.dest_transport_media_id == transport_media_id
                }) {
                    let destination_was_active = route_entry
                        .destinations
                        .get(position)
                        .is_some_and(|destination| destination.active);
                    if destination_was_active
                        && let Some(remote_source_registration) =
                            remote_source_registration.as_ref()
                    {
                        remote_source_registration
                            .source_control()
                            .set_route_active(
                                remote_source_registration.source_session_key().clone(),
                                source_transport_media_id,
                                false,
                            );
                    }
                    route_entry.destinations.remove(position);
                }
                if route_entry.destinations.is_empty() {
                    state.media_route_index.remove(&source_transport_media_id);
                }
            }
            refresh_source_packet_gate(state, source_transport_media_id);
            state.prune_remote_source_if_unrouted(source_transport_media_id);
            state.mark_session_dirty(&session_key);
            relay_cleanup.map_or_else(
                || Ok(RemoveMediaOutcome::without_relay_cleanup()),
                |cleanup| {
                    Ok(RemoveMediaOutcome::with_relay_cleanup(
                        cleanup.source_session_key().clone(),
                        source_transport_media_id,
                    ))
                },
            )
        }
    }
}

fn relay_cleanup_for_source(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> Option<RelayCleanup> {
    state
        .remote_source_registration(source_transport_media_id)
        .map(|registration| {
            RelayCleanup::new(
                registration.source_session_key().clone(),
                source_transport_media_id,
            )
        })
}

fn worker_stage_native_media_removal(
    session_state: &mut super::super::state::RtcSessionState,
    mid: Mid,
) -> Result<(), TransportAdapterError> {
    if session_state.sdp_negotiation.pending_offer.is_some()
        && session_state.sdp_negotiation.staged_offer_sdp.is_none()
    {
        session_state
            .sdp_negotiation
            .queued_removal_mids
            .insert(mid);
        return Ok(());
    }
    if session_state.rtc.media(mid).is_none() {
        return Err(TransportAdapterError::InvalidInput);
    }

    let existing_pending_offer = session_state.sdp_negotiation.pending_offer.take();
    let mut sdp_api = session_state.rtc.sdp_api();
    if let Some(pending_offer) = existing_pending_offer {
        sdp_api.merge(pending_offer);
    }
    sdp_api.set_direction(mid, Direction::Inactive);
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    session_state
        .sdp_negotiation
        .queued_removal_mids
        .remove(&mid);
    Ok(())
}

fn worker_add_recv_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> Result<TransportMediaId, TransportAdapterError> {
    let Some(session_state) = state.sessions.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = if session_state.sdp_negotiation.initial_offer_applied {
        worker_stage_native_recv_media(session_state, media_kind, rtp_parameters)?
    } else {
        let mid = transport_mid(rtp_parameters).unwrap_or_default();
        let has_media = session_state.rtc.media(mid).is_some();
        {
            let mut api = session_state.rtc.direct_api();
            if !has_media {
                api.declare_media(mid, media_kind);
            }
            if let Some((ssrc, rid)) = primary_encoding_identity(rtp_parameters) {
                api.expect_stream_rx(ssrc, None, mid, rid);
            }
        }
        state.mark_session_dirty(session_key);
        mid
    };
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid,
    });
    debug!(
        session_id = ?session_key.session_id(),
        channel_runtime_id = session_key.channel_runtime_id(),
        ?transport_media_id,
        ?media_kind,
        "declared recv-only media on rtc session for incoming producer RTP"
    );
    Ok(transport_media_id)
}

fn worker_stage_native_recv_media(
    session_state: &mut super::super::state::RtcSessionState,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> Result<Mid, TransportAdapterError> {
    if session_state.sdp_negotiation.pending_offer.is_some()
        && session_state.sdp_negotiation.staged_offer_sdp.is_none()
    {
        return Err(TransportAdapterError::InvalidInput);
    }

    let existing_pending_offer = session_state.sdp_negotiation.pending_offer.take();
    let mut sdp_api = session_state.rtc.sdp_api();
    if let Some(pending_offer) = existing_pending_offer {
        sdp_api.merge(pending_offer);
    }
    let mid = sdp_api.add_media(media_kind, Direction::RecvOnly, None, None, None);
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    if let Some((ssrc, rid)) = primary_encoding_identity(rtp_parameters) {
        session_state
            .sdp_negotiation
            .pending_recv_streams
            .insert(mid, super::super::state::PendingRecvStream { ssrc, rid });
    } else {
        session_state
            .sdp_negotiation
            .pending_recv_streams
            .remove(&mid);
    }
    Ok(mid)
}

fn worker_add_send_media(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    media_kind: MediaKind,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    remote_source_control: Option<RemoteSourceControl>,
    consumer_rtp_parameters: &RouterRtpParameters,
) -> Result<TransportMediaId, TransportAdapterError> {
    let route_source = ensure_route_source_registered(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
        remote_source_control,
    )?;
    let Some(session_state) = state.sessions.get_mut(consumer_session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = if session_state.sdp_negotiation.initial_offer_applied {
        worker_stage_native_send_media(session_state, media_kind)?
    } else {
        let mid = transport_mid(consumer_rtp_parameters).unwrap_or_default();
        declare_direct_send_media(session_state, mid, media_kind, consumer_rtp_parameters);
        state.mark_session_dirty(consumer_session_key);
        mid
    };
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: consumer_session_key.clone(),
        mid,
        source_transport_media_id,
    });
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
            dest_transport_media_id: transport_media_id,
            dest_mid: mid,
            active: true,
            packet_gate: consumer_packet_gate(consumer_rtp_parameters),
        });
    refresh_source_packet_gate(state, source_transport_media_id);
    debug!(
        consumer_session_id = ?consumer_session_key.session_id(),
        consumer_channel_runtime_id = consumer_session_key.channel_runtime_id(),
        source_session_id = ?source_session_key.session_id(),
        source_channel_runtime_id = source_session_key.channel_runtime_id(),
        ?source_transport_media_id,
        source_mid = ?route_source.source_mid(),
        source_route_kind = route_source.label(),
        ?transport_media_id,
        ?media_kind,
        "declared send-only media and registered media route for consumer"
    );
    if matches!(route_source, RouteSourceKind::Remote)
        && let Some(remote_source_registration) =
            state.remote_source_registration(source_transport_media_id)
    {
        remote_source_registration
            .source_control()
            .set_route_active(
                remote_source_registration.source_session_key().clone(),
                source_transport_media_id,
                true,
            );
    }
    Ok(transport_media_id)
}

fn worker_stage_native_send_media(
    session_state: &mut super::super::state::RtcSessionState,
    media_kind: MediaKind,
) -> Result<Mid, TransportAdapterError> {
    if session_state.sdp_negotiation.pending_offer.is_some()
        && session_state.sdp_negotiation.staged_offer_sdp.is_none()
    {
        return Err(TransportAdapterError::InvalidInput);
    }

    let existing_pending_offer = session_state.sdp_negotiation.pending_offer.take();
    let mut sdp_api = session_state.rtc.sdp_api();
    if let Some(pending_offer) = existing_pending_offer {
        sdp_api.merge(pending_offer);
    }
    let mid = sdp_api.add_media(media_kind, Direction::SendOnly, None, None, None);
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    Ok(mid)
}

fn declare_direct_send_media(
    session_state: &mut super::super::state::RtcSessionState,
    mid: Mid,
    media_kind: MediaKind,
    consumer_rtp_parameters: &RouterRtpParameters,
) {
    let has_media = session_state.rtc.media(mid).is_some();
    let mut api = session_state.rtc.direct_api();
    if !has_media {
        api.declare_media(mid, media_kind);
    }
    let (ssrc, rid) = primary_encoding_identity(consumer_rtp_parameters)
        .unwrap_or_else(|| (api.new_ssrc(), None));
    api.declare_stream_tx(ssrc, None, mid, rid);
}

fn worker_set_producer_active(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    match state.mid_registry.get(&transport_media_id.as_u64()) {
        Some(RegisteredMediaHandle::Producer {
            session_key: owner_session_key,
            ..
        }) if owner_session_key == session_key => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
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
    ensure_route_source_exists(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
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
    if destination.active == active {
        return Ok(());
    }
    destination.active = active;
    refresh_source_packet_gate(state, source_transport_media_id);
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
    Ok(())
}

fn worker_set_source_routing_policy(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    policy: SourceMediaRoutingPolicy,
) -> Result<(), TransportAdapterError> {
    match state.mid_registry.get(&source_transport_media_id.as_u64()) {
        Some(RegisteredMediaHandle::Producer {
            session_key: owner_session_key,
            ..
        }) if owner_session_key == session_key => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    state.route_control.set_policy_packet_gate(
        source_transport_media_id,
        match policy {
            SourceMediaRoutingPolicy::Default => None,
            SourceMediaRoutingPolicy::Suppress => Some(PacketLayerGate::Block),
        },
    );
    Ok(())
}

pub(super) fn refresh_source_packet_gate(
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

pub(super) fn local_source_packet_gate(route_entry: &MediaRouteEntry) -> Option<PacketLayerGate> {
    aggregate_packet_gates(
        route_entry
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .map(|destination| &destination.packet_gate),
    )
}

fn consumer_packet_gate(consumer_rtp_parameters: &RouterRtpParameters) -> PacketLayerGate {
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

fn transport_mid(rtp_parameters: &RouterRtpParameters) -> Option<Mid> {
    rtp_parameters.mid().map(Into::into)
}

fn primary_encoding_identity(rtp_parameters: &RouterRtpParameters) -> Option<(Ssrc, Option<Rid>)> {
    let encoding = rtp_parameters
        .encodings()
        .find(|encoding| encoding.ssrc().is_some() || encoding.rid().is_some())?;
    let ssrc = encoding.ssrc().map(Ssrc::from)?;
    let rid = encoding.rid().map(Into::into);
    Some((ssrc, rid))
}

impl RouteSourceKind {
    fn source_mid(&self) -> Option<Mid> {
        match self {
            Self::Local { source_mid } => Some(*source_mid),
            Self::Remote => None,
        }
    }

    const fn label(&self) -> &'static str {
        match self {
            Self::Local { .. } => "local",
            Self::Remote => "remote",
        }
    }
}

fn ensure_route_source_registered(
    state: &mut RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    remote_source_control: Option<RemoteSourceControl>,
) -> Result<RouteSourceKind, TransportAdapterError> {
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        if let Some(handle) = state.mid_registry.get(&source_transport_media_id.as_u64()) {
            return match handle {
                RegisteredMediaHandle::Producer {
                    session_key: owner_session_key,
                    mid,
                } if owner_session_key == source_session_key => {
                    Ok(RouteSourceKind::Local { source_mid: *mid })
                }
                RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. } => {
                    Err(TransportAdapterError::InvalidInput)
                }
            };
        }
        return Err(TransportAdapterError::TransportUnavailable);
    }
    let Some(remote_source_control) = remote_source_control else {
        return Err(TransportAdapterError::InvalidInput);
    };
    state.register_remote_source(
        source_transport_media_id,
        source_session_key,
        remote_source_control,
    )?;
    Ok(RouteSourceKind::Remote)
}

fn ensure_route_source_exists(
    state: &RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        if let Some(handle) = state.mid_registry.get(&source_transport_media_id.as_u64()) {
            return match handle {
                RegisteredMediaHandle::Producer {
                    session_key: owner_session_key,
                    ..
                } if owner_session_key == source_session_key => Ok(()),
                RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. } => {
                    Err(TransportAdapterError::InvalidInput)
                }
            };
        }
        return Err(TransportAdapterError::TransportUnavailable);
    }
    match state.remote_source_registration(source_transport_media_id) {
        Some(registration) if registration.source_session_key() == source_session_key => Ok(()),
        Some(_registration) => Err(TransportAdapterError::InvalidInput),
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}

pub(crate) fn request_keyframe_for_source(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    now: Instant,
) {
    let Some(handle) = state
        .mid_registry
        .get(&source_transport_media_id.as_u64())
        .cloned()
    else {
        return;
    };
    let RegisteredMediaHandle::Producer { session_key, mid } = handle else {
        return;
    };
    if &session_key != source_session_key {
        return;
    }
    let Some(session_state) = state.sessions.get_mut(&session_key) else {
        return;
    };
    if session_state
        .rtc
        .direct_api()
        .stream_rx_by_mid(mid, rid)
        .is_none()
    {
        return;
    }
    if matches!(
        state
            .route_control
            .decide_keyframe_request(source_transport_media_id, now),
        super::super::route_control::KeyframeRequestDecision::Absorb
    ) {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
        return;
    }
    let Some(session_state) = state.sessions.get_mut(&session_key) else {
        return;
    };
    let mut direct_api = session_state.rtc.direct_api();
    let Some(stream_rx) = direct_api.stream_rx_by_mid(mid, rid) else {
        return;
    };
    stream_rx.request_keyframe(kind);
    state.mark_session_dirty(&session_key);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
}

fn set_remote_source_route_active(
    state: &RtcBootstrapState,
    relay_registry: &RelayRegistry,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::relay_registry::RelayTargetId,
    active: bool,
) {
    let Some(handle) = state
        .mid_registry
        .get(&source_transport_media_id.as_u64())
        .cloned()
    else {
        return;
    };
    let RegisteredMediaHandle::Producer { session_key, .. } = handle else {
        return;
    };
    if &session_key != source_session_key {
        return;
    }
    relay_registry.set_source_target_active(source_transport_media_id, target_id, active);
}

fn set_remote_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::relay_registry::RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    let Some(handle) = state
        .mid_registry
        .get(&source_transport_media_id.as_u64())
        .cloned()
    else {
        return;
    };
    let RegisteredMediaHandle::Producer { session_key, .. } = handle else {
        return;
    };
    if &session_key != source_session_key {
        return;
    }
    state
        .route_control
        .set_relay_packet_gate(source_transport_media_id, target_id, packet_gate);
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use str0m::media::MediaKind;

    use super::*;
    use crate::config::MediaCodecFlags;
    use crate::runtime::rtc_adapter::{
        bootstrap,
        media_registry::RegisteredMediaHandle,
        relay_registry::{RelayPacketMailbox, RelayTargetId},
    };
    use crate::signaling::shared::SessionId;

    fn prepare_source_session(
        state: &mut RtcBootstrapState,
        source_session: &TransportSessionKey,
        source_mid: Mid,
        ssrc: u32,
    ) -> TransportMediaId {
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 47_000));
        assert!(
            bootstrap::ensure_session_rtc_state(
                &mut state.sessions,
                source_session,
                candidate_addr,
                MediaCodecFlags::default(),
            )
            .is_ok()
        );
        let Some(source_session_state) = state.sessions.get_mut(source_session) else {
            return TransportMediaId::default();
        };
        let mut direct_api = source_session_state.rtc.direct_api();
        direct_api.declare_media(source_mid, MediaKind::Video);
        direct_api.expect_stream_rx(Ssrc::from(ssrc), None, source_mid, None);
        state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: source_session.clone(),
            mid: source_mid,
        })
    }

    #[test]
    fn remote_keyframe_requests_drop_when_the_relay_target_is_inactive() {
        let source_session = TransportSessionKey::new(101, 0, 102, SessionId::Integer(103));
        let source_mid = Mid::from("cam-up");
        let mut state = RtcBootstrapState::default();
        let metrics = RuntimeMetrics::default();
        let relay_registry = RelayRegistry::default();
        let (_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id =
            prepare_source_session(&mut state, &source_session, source_mid, 66_666);

        respond_request_remote_keyframe(
            &mut state,
            &metrics,
            &relay_registry,
            &RemoteKeyframeRequest {
                source_session_key: &source_session,
                source_transport_media_id,
                target_id: RelayTargetId::new(7),
                rid: None,
                kind: KeyframeRequestKind::Pli,
            },
        );

        assert!(!state.dirty_sessions.contains(&source_session));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_route_control_forwarded, 0);
        assert_eq!(snapshot.rtc_route_control_absorbed, 0);
        assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops, 1);
    }

    #[test]
    fn remote_keyframe_requests_forward_once_and_then_absorb_within_the_window() {
        let source_session = TransportSessionKey::new(111, 0, 112, SessionId::Integer(113));
        let source_mid = Mid::from("cam-up");
        let mut state = RtcBootstrapState::default();
        let metrics = RuntimeMetrics::default();
        let relay_registry = RelayRegistry::default();
        let (mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id =
            prepare_source_session(&mut state, &source_session, source_mid, 77_777);
        let relay_target_id = RelayTargetId::new(8);

        relay_registry.activate_source_target(
            source_session.channel_runtime_id(),
            source_transport_media_id,
            relay_target_id,
            mailbox,
        );
        relay_registry.set_source_target_active(source_transport_media_id, relay_target_id, true);

        respond_request_remote_keyframe(
            &mut state,
            &metrics,
            &relay_registry,
            &RemoteKeyframeRequest {
                source_session_key: &source_session,
                source_transport_media_id,
                target_id: relay_target_id,
                rid: None,
                kind: KeyframeRequestKind::Pli,
            },
        );
        respond_request_remote_keyframe(
            &mut state,
            &metrics,
            &relay_registry,
            &RemoteKeyframeRequest {
                source_session_key: &source_session,
                source_transport_media_id,
                target_id: relay_target_id,
                rid: None,
                kind: KeyframeRequestKind::Fir,
            },
        );

        assert!(state.dirty_sessions.contains(&source_session));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_route_control_forwarded, 1);
        assert_eq!(snapshot.rtc_route_control_absorbed, 1);
        assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops, 0);
    }
}
