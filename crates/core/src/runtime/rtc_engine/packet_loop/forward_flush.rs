//! Packet observation and forwarding flush for the packet loop.
//!
//! This module handles packet-derived updates, bounded relay intake and
//! destination sends during the pump phase. Local session output and relay
//! packets are batched before planning, packet observations update worker state
//! before planning, and planned destinations are flushed before the worker
//! returns to async waiting:
//!
//! - learn producer MID, SSRC and RID bindings from RTP headers
//! - update active-speaker and incoming bitrate observations
//! - request first video keyframes when ingress starts
//! - drain a bounded number of relay packets into the local batch
//! - send each planned packet to local RTC, packet sink or relay destinations
//!
//! Room policy decisions must already be projected into route-control state.
//! This module observes packets and executes planned sends. It does not decide
//! subscriptions or room membership.

use std::{mem::take, time::Instant};

use str0m::media::{KeyframeRequestKind, MediaKind};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::{
    super::{
        forwarded_packet::ForwardedPacket,
        forwarding_destination::{ForwardSendOutcome, ForwardingDestination},
        media_registry::RegisteredMediaHandle,
        state::RtcBootstrapState,
        worker::{apply_source_rid_readiness, request_keyframe_for_source},
    },
    buffers::PacketLoopBuffers,
};
use crate::runtime::{
    hot_path::unlikely,
    media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
    metrics::{RtcRouteControlMetrics, RtpMetricsRecorder, RuntimeMetrics},
};

/// Observe packet-path metadata before packets are forwarded.
///
/// This is where producer SSRC bindings, audio activity, RID readiness and
/// incoming bitrate become worker state or metrics. The function also coalesces
/// source-policy wakeups so the room layer is notified once per changed room
/// after the batch has been inspected.
pub(super) fn record_incoming_stats(
    state: &mut RtcBootstrapState,
    source_policy_signal: &SourcePolicySignal,
    metrics: &impl RtcRouteControlMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    let mut pending_packets = take(&mut buffers.pending_packets);
    for packet in &mut pending_packets {
        if let Some(transport_media_id) = packet.resolve_source_transport_media_id(state) {
            if packet.route_control_mid().is_some() {
                // MID proves which producer this packet belongs to. Persist the
                // SSRC before the browser stops sending MID/RID extensions.
                state.learn_producer_ssrc_binding(
                    packet.source_session_key(),
                    transport_media_id,
                    packet.route_control_ssrc(),
                    packet.route_control_rid_extension(),
                );
            }
            let payload_len = packet.payload_len();
            let voice_activity = packet.route_control_voice_activity();
            let audio_level = packet.route_control_audio_level();
            let audio_policy_changed = state.route_control.observe_audio_activity(
                transport_media_id,
                voice_activity,
                audio_level,
                packet.received_at(),
            );
            if unlikely(audio_policy_changed) {
                buffers
                    .dirty_source_policy_channel_ids
                    .push(packet.source_session_key().room_instance_id());
            }
            let metadata = packet.resolve_route_control_layer_metadata(state);
            if let Some(rid) = metadata.rid() {
                state.observe_producer_rid_packet(transport_media_id, rid, packet.received_at());
                let is_keyframe = packet.route_control_decoder_refresh(state, transport_media_id);
                buffers.push_rid_readiness(
                    packet.source_session_key(),
                    transport_media_id,
                    rid,
                    is_keyframe,
                    packet.received_at(),
                );
            }
            let first_ingress = state
                .record_incoming_bitrate(transport_media_id, packet.received_at(), payload_len)
                .unwrap_or(false);
            if unlikely(first_ingress) {
                debug!(
                    user_id = ?packet.source_session_key().user_id(),
                    media_worker_id = packet.source_session_key().media_worker_id(),
                    ?transport_media_id,
                    payload_bytes = payload_len,
                    "observed first RTP ingress for published media"
                );
                buffers.push_first_video_keyframe(
                    packet.source_session_key(),
                    transport_media_id,
                    packet.received_at(),
                );
            }
            rtp_metrics.record_ingress(payload_len);
        }
    }
    buffers.pending_packets = pending_packets;
    flush_pending_rid_readiness(state, metrics, buffers);
    flush_pending_first_video_keyframes(state, metrics, buffers);
    buffers.flush_source_policy_dirty(source_policy_signal);
}

fn flush_pending_rid_readiness(
    state: &mut RtcBootstrapState,
    metrics: &impl RtcRouteControlMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    for pending in buffers.pending_rid_readiness.drain(..) {
        if apply_source_rid_readiness(
            state,
            metrics,
            &pending.source_session_key,
            pending.source_transport_media_id,
            pending.rid,
            pending.is_keyframe,
            pending.observed_at,
        ) {
            buffers
                .rid_readiness_changed_sources
                .push(pending.source_transport_media_id);
        }
    }
}

fn flush_pending_first_video_keyframes(
    state: &mut RtcBootstrapState,
    metrics: &impl RtcRouteControlMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    for pending in buffers.pending_first_video_keyframes.drain(..) {
        if buffers
            .rid_readiness_changed_sources
            .contains(&pending.source_transport_media_id)
        {
            continue;
        }
        request_first_video_keyframe(
            state,
            metrics,
            &pending.source_session_key,
            pending.source_transport_media_id,
            pending.observed_at,
        );
    }
    buffers.rid_readiness_changed_sources.clear();
}

/// Request a keyframe when the first packet for a video producer appears.
///
/// First ingress proves the producer is alive, but the first observed packet may
/// not be decodable by new consumers. Asking for a PLI here helps strict
/// selected-RID gates and late subscribers converge without making room policy
/// inspect packet payloads.
fn request_first_video_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &impl RtcRouteControlMetrics,
    source_session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    now: Instant,
) {
    if !source_is_video(state, source_session_key, transport_media_id) {
        return;
    }
    request_keyframe_for_source(
        state,
        metrics,
        source_session_key,
        transport_media_id,
        None,
        KeyframeRequestKind::Pli,
        now,
    );
}

fn source_is_video(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> bool {
    let Some(RegisteredMediaHandle::Producer { session_key, mid }) =
        state.media_handle(transport_media_id)
    else {
        return false;
    };
    if session_key != source_session_key {
        return false;
    }
    state
        .users
        .get(source_session_key)
        .and_then(|session_state| session_state.rtc.media(*mid))
        .is_some_and(|media| matches!(media.kind(), MediaKind::Video))
}

/// Drain relay packets without letting relay bursts monopolize one loop turn.
///
/// Relay messages are already decoded as `ForwardedPacket` values by their
/// source worker. The cap keeps command handling and UDP receive responsive
/// under cross-worker fanout spikes.
pub(super) fn drain_relay_packets(
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    pending_packets: &mut Vec<ForwardedPacket>,
    max_packets: usize,
) -> usize {
    let mut drained_packets = 0;
    while drained_packets < max_packets {
        match relay_rx.try_recv() {
            Ok(packet) => {
                pending_packets.push(packet);
                drained_packets += 1;
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
    drained_packets
}

/// Execute planned forwarding destinations for all staged packets.
///
/// # Payload lifetime
///
/// The planner orders forwards by packet index. For relay destinations, this
/// function promotes the source packet to a shared relay packet once and reuses
/// it for every relay destination for that packet. For local RTC destinations,
/// the destination can move the payload when it is the last destination and
/// clone only when an earlier destination still needs the bytes.
///
/// # Error handling
///
/// A missing local destination is treated as an empty local send because routes
/// can change while packets are already batched. Relay overload is counted and
/// dropped. Other destination errors are logged and the loop continues flushing
/// the remaining planned destinations.
pub(super) fn flush_forward_routes(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    let (forwards, pending_packets) = (&buffers.forwards, &mut buffers.pending_packets);
    buffers.relay_packet = None;
    buffers.relay_packet_idx = None;
    for (forward_idx, forward) in forwards.iter().enumerate() {
        let is_last_destination = forwards
            .get(forward_idx + 1)
            .is_none_or(|next_forward| next_forward.packet_idx() != forward.packet_idx());
        let packet_idx = forward.packet_idx();
        let Some(packet) = pending_packets.get_mut(packet_idx) else {
            continue;
        };
        let destination = forward.destination();
        let destination_kind = destination.metrics_kind();
        let payload_len = packet.payload_len();
        let relay_packet = match destination {
            ForwardingDestination::IntraNodeRelay(_) | ForwardingDestination::InterNodeRelay(_) => {
                let Some(source_transport_media_id) =
                    packet.resolve_source_transport_media_id(state)
                else {
                    continue;
                };
                if buffers.relay_packet_idx != Some(packet_idx) {
                    buffers.relay_packet = Some(packet.share_for_relay(source_transport_media_id));
                    buffers.relay_packet_idx = Some(packet_idx);
                }
                buffers.relay_packet.as_mut()
            }
            ForwardingDestination::LocalRtc(_) | ForwardingDestination::PacketSink(_) => None,
        };
        let packet = relay_packet.unwrap_or(packet);
        match destination.send(state, packet, is_last_destination) {
            Ok(ForwardSendOutcome::LocalRtc {
                payload_bytes: Some(payload_len),
            }) => {
                rtp_metrics.record_egress(payload_len);
                rtp_metrics.record_forwarded(destination_kind, payload_len);
            }
            Ok(ForwardSendOutcome::SideEffect)
                if matches!(
                    destination,
                    ForwardingDestination::PacketSink(_)
                        | ForwardingDestination::IntraNodeRelay(_)
                        | ForwardingDestination::InterNodeRelay(_)
                ) =>
            {
                rtp_metrics.record_forwarded(destination_kind, payload_len);
            }
            Ok(ForwardSendOutcome::OverloadedRelay) => {
                if let Some(destination_kind) = destination.relay_drop_kind() {
                    metrics.record_rtp_relay_overload_drop(destination_kind);
                }
            }
            Ok(
                ForwardSendOutcome::SideEffect
                | ForwardSendOutcome::LocalRtc {
                    payload_bytes: None,
                },
            ) => {}
            Err(error) => {
                warn!(
                    ?destination,
                    ?error,
                    "failed to write media to destination user"
                );
            }
        }
    }
}
