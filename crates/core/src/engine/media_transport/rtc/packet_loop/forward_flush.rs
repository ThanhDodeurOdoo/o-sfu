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

use core::hint::cold_path;
use std::{mem::take, time::Instant};

use str0m::media::{KeyframeRequestKind, MediaKind};
use tokio::sync::mpsc;
use tracing::debug;

use super::{
    super::{
        forwarded_packet::{ForwardedPacket, ForwardedPacketSource},
        forwarding_destination::{ForwardSendOutcome, ForwardingDestination, relay_enqueue_result},
        media_registry::RegisteredMediaHandle,
        relay_registry::RelayEnqueueOutcome,
        state::PacketLoopState,
        worker::{
            KeyframeRequestMode, KeyframeRequestTarget, apply_src_rid_ready, request_kf_for_target,
        },
    },
    buffers::PacketLoopBuffers,
};
use crate::engine::{
    media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
    metrics::{
        RtcKeyframeRequestOutcome, RtcMetricsRecorder, RtcRouteControlMetrics,
        RtpDecoderRefreshScope, RtpMetricsRecorder, RuntimeMetrics,
    },
};

/// observe packet-path metadata before packets are forwarded
///
/// This is where producer SSRC bindings, audio activity, RID readiness and
/// incoming bitrate become worker state or metrics. The function also coalesces
/// source-policy wakeups so the room layer is notified once per changed room
/// after the batch has been inspected.
pub(super) fn record_incoming_stats(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    metrics: &impl RtcRouteControlMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    let mut pending_packets = take(&mut buffers.pending_packets);
    for packet in &mut pending_packets {
        if let Some(facts) = packet.resolve_facts(state) {
            let transport_media_id = facts.src_media;
            if packet.route_control_mid().is_some() {
                // MID proves which producer this packet belongs to. Persist the
                // SSRC before the browser stops sending MID/RID extensions.
                state.learn_producer_ssrc_from_pkt(
                    packet.source(),
                    transport_media_id,
                    packet.route_control_ssrc(),
                    packet.route_control_rid_extension(),
                );
            }
            let audio_policy_changed = state.routes.observe_audio_activity(
                transport_media_id,
                facts.voice_activity,
                facts.audio_level,
                packet.received_at(),
            );
            if facts.decoder_refresh {
                let cleared = state
                    .routes
                    .observe_decoder_refresh(transport_media_id, facts.rid);
                for _ in 0..cleared {
                    metrics.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Cleared);
                }
            }
            if audio_policy_changed {
                cold_path();
                buffers
                    .dirty_source_policy_channel_ids
                    .push(facts.room_instance_id);
            }
            let packet_rid = facts.rid;
            if packet_rid.is_some() || facts.decoder_refresh {
                state.routes.observe_producer_packet(
                    transport_media_id,
                    packet_rid,
                    facts.decoder_refresh,
                    packet.received_at(),
                );
            }
            if facts.decoder_refresh {
                let scope = if packet_rid.is_some() {
                    RtpDecoderRefreshScope::Rid
                } else {
                    RtpDecoderRefreshScope::Source
                };
                rtp_metrics.record_decoder_refresh(scope);
            }
            if let Some(rid) = packet_rid {
                buffers.push_rid_readiness(
                    packet.source(),
                    transport_media_id,
                    rid,
                    facts.decoder_refresh,
                    packet.received_at(),
                );
            }
            let first_ingress = state
                .record_incoming_bitrate(
                    transport_media_id,
                    packet.received_at(),
                    facts.payload_len,
                )
                .unwrap_or(false);
            if first_ingress {
                cold_path();
                let Some(src_key) = packet.src_key(state) else {
                    continue;
                };
                debug!(
                    user_id = ?src_key.user_id(),
                    media_worker_id = src_key.media_worker_id().as_usize(),
                    ?transport_media_id,
                    payload_bytes = facts.payload_len,
                    "observed first RTP ingress for published media"
                );
                buffers.push_first_video_keyframe(
                    packet.source(),
                    transport_media_id,
                    packet.received_at(),
                );
            }
            rtp_metrics.record_ingress(facts.payload_len);
        }
    }
    buffers.pending_packets = pending_packets;
    flush_pending_rid_readiness(state, metrics, buffers);
    flush_first_video_kfs(state, metrics, buffers);
    buffers.flush_source_policy_dirty(source_policy_signal);
}

#[cfg(feature = "internal-benchmarks")]
pub fn record_incoming_stats_for_benchmark(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    metrics: &impl RtcRouteControlMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    record_incoming_stats(state, source_policy_signal, metrics, rtp_metrics, buffers);
}

fn flush_pending_rid_readiness(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    for pending in buffers.pending_rid_readiness.drain(..) {
        let changed = match &pending.source {
            ForwardedPacketSource::Relayed(src_key) => apply_src_rid_ready(
                state,
                metrics,
                src_key,
                pending.src_media,
                pending.rid,
                pending.is_keyframe,
                pending.observed_at,
            ),
            ForwardedPacketSource::Local(session_handle) => {
                let Some(src_key) = state.users.key_for_handle(*session_handle).cloned() else {
                    continue;
                };
                apply_src_rid_ready(
                    state,
                    metrics,
                    &src_key,
                    pending.src_media,
                    pending.rid,
                    pending.is_keyframe,
                    pending.observed_at,
                )
            }
        };
        if changed {
            buffers
                .rid_readiness_changed_sources
                .push(pending.src_media);
        }
    }
}

fn flush_first_video_kfs(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    for pending in buffers.pending_first_video_keyframes.drain(..) {
        if buffers
            .rid_readiness_changed_sources
            .contains(&pending.src_media)
        {
            continue;
        }
        request_first_video_kf(
            state,
            metrics,
            &pending.source,
            pending.src_media,
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
fn request_first_video_kf(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source: &ForwardedPacketSource,
    transport_media_id: TransportMediaId,
    now: Instant,
) {
    match source {
        ForwardedPacketSource::Relayed(src_key) => {
            request_first_video_kf_for_session(state, metrics, src_key, transport_media_id, now);
        }
        ForwardedPacketSource::Local(session_handle) => {
            let Some(src_key) = state.users.key_for_handle(*session_handle).cloned() else {
                return;
            };
            request_first_video_kf_for_session(state, metrics, &src_key, transport_media_id, now);
        }
    }
}

fn request_first_video_kf_for_session(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    now: Instant,
) {
    if source_is_video(state, src_key, transport_media_id) {
        request_kf_for_target(
            state,
            metrics,
            KeyframeRequestTarget::Local(src_key, transport_media_id),
            None,
            KeyframeRequestKind::Pli,
            KeyframeRequestMode::Track(now),
        );
    }
}

fn source_is_video(
    state: &PacketLoopState,
    src_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> bool {
    let Some(RegisteredMediaHandle::Producer { session_key, mid }) =
        state.media_handle(transport_media_id)
    else {
        return false;
    };
    if session_key != src_key {
        return false;
    }
    state
        .users
        .get(src_key)
        .and_then(|session_state| session_state.rtc.media(*mid))
        .is_some_and(|media| matches!(media.kind(), MediaKind::Video))
}

/// Drain relay packets without letting relay bursts monopolize one loop turn.
///
/// Relay messages are already decoded as `ForwardedPacket` values by their
/// source worker. The cap keeps command handling and UDP receive responsive
/// under cross-worker fanout spikes.
pub fn drain_relay_packets(
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    pending_packets: &mut Vec<ForwardedPacket>,
    max_packets: usize,
    metrics: &RtcMetricsRecorder,
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
    let cap_hit = max_packets > 0 && drained_packets == max_packets && !relay_rx.is_empty();
    if drained_packets > 0 {
        metrics.record_rtc_relay_drain_batch(drained_packets, cap_hit);
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
pub fn flush_forward_routes(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    rtc_recorder: &RtcMetricsRecorder,
    buffers: &PacketLoopBuffers,
) {
    let (forwards, pending_packets) = (&buffers.forwards, &buffers.pending_packets);
    for forward in forwards {
        let Some(packet) = pending_packets.get(forward.pkt_idx) else {
            continue;
        };
        let destination = &forward.destination;
        let destination_kind = destination.metrics_kind();
        let payload_len = packet.payload_len();
        match destination.send(state, packet) {
            ForwardSendOutcome::LocalRtc {
                payload_bytes: Some(payload_len),
            } => {
                rtp_metrics.record_egress(payload_len);
                rtp_metrics.record_forwarded(destination_kind, payload_len);
            }
            ForwardSendOutcome::SideEffect
                if matches!(destination, ForwardingDestination::PacketSink(_)) =>
            {
                rtp_metrics.record_forwarded(destination_kind, payload_len);
            }
            ForwardSendOutcome::RelayEnqueue(report) => {
                rtc_recorder.record_rtc_relay_enqueue(relay_enqueue_result(report));
                rtc_recorder.record_rtc_relay_mailbox_depth(report.mailbox_depth);
                match report.outcome {
                    RelayEnqueueOutcome::Enqueued => {
                        rtp_metrics.record_forwarded(destination_kind, payload_len);
                    }
                    RelayEnqueueOutcome::Overloaded => {
                        if let Some(destination_kind) = destination.relay_drop_kind() {
                            metrics.record_rtp_relay_overload_drop(destination_kind);
                        }
                    }
                    RelayEnqueueOutcome::Closed => {}
                }
            }
            ForwardSendOutcome::SideEffect
            | ForwardSendOutcome::LocalRtc {
                payload_bytes: None,
            } => {}
        }
    }
}
