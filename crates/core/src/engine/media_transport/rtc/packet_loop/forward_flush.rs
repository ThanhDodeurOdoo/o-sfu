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
use std::{mem::take, ops::Range, time::Instant};

use str0m::{
    media::{KeyframeRequestKind, MediaKind, Rid},
    rtp::Ssrc,
};
use tokio::sync::mpsc;
use tracing::debug;

use super::{
    super::{
        decoder_refresh::{
            DecoderRefreshActivation, DecoderRefreshAdmission, DecoderRefreshAdmissionInput,
            DecoderRefreshNeed, DecoderRefreshPacket,
        },
        forwarded_packet::{ForwardedPacket, ForwardedPacketSource, PacketFacts},
        forwarding_destination::{
            ForwardSendOutcome, ForwardingDestination, PacketForward, relay_enqueue_result,
        },
        forwarding_planner::plan_forwards,
        media_registry::RegisteredMediaHandle,
        relay_registry::RelayEnqueueOutcome,
        state::PacketLoopState,
        worker::{
            KeyframeRequestMode, KeyframeRequestTarget, apply_src_decoder_ready,
            request_kf_for_target, request_src_decoder_refresh,
        },
    },
    buffers::PacketLoopBuffers,
};
use crate::engine::{
    RoomInstanceId,
    media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
    metrics::{
        RtcKeyframeRequestOutcome, RtcMetricsRecorder, RtpDecoderRefreshScope, RtpMetricsRecorder,
        RuntimeMetrics,
    },
    packet_sink_registry::PacketSinkRouteCache,
};

/// observe packet-path metadata before packets are forwarded
///
/// This is where producer SSRC bindings, audio activity, RID readiness and
/// incoming bitrate become worker state or metrics. The function also coalesces
/// source-policy wakeups so the room layer is notified once per changed room
/// after the batch has been inspected.
#[cfg(any(test, feature = "internal-benchmarks"))]
pub(super) fn record_incoming_stats(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    observe_incoming_packets(state, source_policy_signal, control, rtp, None, buffers);
}

pub(super) fn observe_and_plan_incoming_packets(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    packet_sinks: &PacketSinkRouteCache,
    buffers: &mut PacketLoopBuffers,
) {
    observe_incoming_packets(
        state,
        source_policy_signal,
        control,
        rtp,
        Some(packet_sinks),
        buffers,
    );
}

fn observe_incoming_packets(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    packet_sinks: Option<&PacketSinkRouteCache>,
    buffers: &mut PacketLoopBuffers,
) {
    let mut incoming_packets = take(&mut buffers.pending_packets);
    let mut ready_packets = take(&mut buffers.observed_packets);
    plan_decoder_refresh_releases(
        state,
        control,
        rtp,
        packet_sinks,
        buffers,
        &mut ready_packets,
    );
    for mut packet in incoming_packets.drain(..) {
        let facts = packet.resolve_facts(state);
        let observed = match observe_packet_metadata(state, rtp, buffers, &mut packet, facts) {
            PacketMetadataObservation::Observed(observed) => observed,
            PacketMetadataObservation::Unresolved => {
                let pkt_idx = ready_packets.len();
                ready_packets.push(packet);
                plan_ready_packets(
                    state,
                    control,
                    packet_sinks,
                    pkt_idx..ready_packets.len(),
                    &mut ready_packets,
                    &mut buffers.forwards,
                );
                continue;
            }
        };
        if observed.decoder_refresh_packet.codec.is_some()
            && state.pending_decoder_refreshes.has_matching_release(
                observed.room_instance_id,
                observed.src_media,
                observed.rid,
                observed.decoder_refresh_packet,
            )
        {
            update_decoder_readiness(state, control, buffers, &observed);
            let deferred = state.pending_decoder_refreshes.defer_behind_release(
                observed.room_instance_id,
                observed.src_media,
                observed.rid,
                observed.decoder_refresh_packet,
                packet,
            );
            if !deferred {
                let src_key = match &observed.source {
                    ForwardedPacketSource::Relayed(src_key) => Some(src_key.clone()),
                    ForwardedPacketSource::Local(session_handle) => {
                        state.users.key_for_handle(*session_handle).cloned()
                    }
                };
                if let Some(src_key) = src_key {
                    request_src_decoder_refresh(
                        state,
                        control,
                        &src_key,
                        observed.src_media,
                        observed.rid,
                        observed.received_at,
                    );
                }
            }
            continue;
        }
        let admission = admit_observed_packet(state, &observed, packet, &mut ready_packets);
        let packet_range = match admission {
            DecoderRefreshAdmission::Held => None,
            DecoderRefreshAdmission::Ready { packet_range } => Some(packet_range),
        };
        update_decoder_readiness(state, control, buffers, &observed);
        if let Some(packet_range) = packet_range {
            plan_ready_packets(
                state,
                control,
                packet_sinks,
                packet_range,
                &mut ready_packets,
                &mut buffers.forwards,
            );
        }
    }
    buffers.pending_packets = ready_packets;
    buffers.observed_packets = incoming_packets;
    buffers.pending_rid_readiness.clear();
    flush_first_video_kfs(state, control, buffers);
    buffers.flush_source_policy_dirty(source_policy_signal);
}

fn plan_decoder_refresh_releases(
    state: &mut PacketLoopState,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    packet_sinks: Option<&PacketSinkRouteCache>,
    buffers: &mut PacketLoopBuffers,
    ready_packets: &mut [ForwardedPacket],
) {
    let mut releases = take(&mut buffers.decoder_refresh_releases);
    let mut failed_sources = Vec::new();
    for release in releases.drain(..) {
        if failed_sources.contains(&release.source_id) {
            continue;
        }
        let may_plan = match release.activation {
            None => true,
            Some(activation) => {
                let source_id = activation.source_id;
                let activated =
                    activate_decoder_refresh_release(state, control, rtp, buffers, &activation);
                if !activated {
                    state.pending_decoder_refreshes.remove_source(source_id);
                    failed_sources.push(source_id);
                }
                activated
            }
        };
        if may_plan {
            plan_ready_packets(
                state,
                control,
                packet_sinks,
                release.packet_range,
                ready_packets,
                &mut buffers.forwards,
            );
        }
    }
    buffers.decoder_refresh_releases = releases;
}

fn activate_decoder_refresh_release(
    state: &mut PacketLoopState,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
    activation: &DecoderRefreshActivation,
) -> bool {
    if !observe_complete_decoder_refresh(
        state,
        control,
        rtp,
        activation.source_id,
        activation.rid,
        activation.ssrc,
        activation.observed_at,
    ) {
        return false;
    }
    let Some(readiness) = buffers.push_rid_readiness(
        &activation.source,
        activation.source_id,
        activation.rid,
        true,
        activation.observed_at,
    ) else {
        return true;
    };
    if apply_pending_rid_readiness(state, control, &readiness) {
        buffers
            .rid_readiness_changed_sources
            .push(activation.source_id);
    }
    true
}

struct ObservedIncomingPacket {
    source: ForwardedPacketSource,
    room_instance_id: RoomInstanceId,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    received_at: Instant,
    decoder_refresh_need: DecoderRefreshNeed,
    decoder_refresh_packet: DecoderRefreshPacket,
}

enum PacketMetadataObservation {
    Observed(ObservedIncomingPacket),
    Unresolved,
}

fn observe_packet_metadata(
    state: &mut PacketLoopState,
    rtp: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
    packet: &mut ForwardedPacket,
    facts: Option<PacketFacts>,
) -> PacketMetadataObservation {
    let Some(facts) = facts else {
        return PacketMetadataObservation::Unresolved;
    };
    let payload_len = packet.payload().len();
    let src_media = facts.src_media;
    let source = packet.source().clone();
    let received_at = packet.received_at();
    let decoder_capable = facts.decoder_refresh_packet.codec.is_some();
    let producer_observation = state.routes.observe_producer_rtp(
        src_media,
        facts.rid,
        facts.decoder_refresh_packet.ssrc,
        decoder_capable,
        received_at,
    );
    state.learn_producer_ssrc_from_pkt(
        packet.source(),
        src_media,
        packet.route_control_ssrc(),
        facts.rid,
    );
    observe_audio_filter(state, buffers, packet, facts, received_at);
    if producer_observation.first_rid_packet {
        debug!(?src_media, ?facts.rid, "observed first RTP for producer RID");
    }
    if state.record_incoming_bitrate(src_media, received_at, payload_len) == Some(true) {
        cold_path();
        if let Some(src_key) = packet.src_key(state) {
            debug!(
                user_id = ?src_key.user_id(),
                media_worker_id = src_key.media_worker_id().as_usize(),
                ?src_media,
                payload_bytes = payload_len,
                "observed first RTP ingress for published media"
            );
            buffers.push_first_video_keyframe(&source, src_media, received_at);
        }
    }
    rtp.record_ingress(payload_len);
    PacketMetadataObservation::Observed(ObservedIncomingPacket {
        source,
        room_instance_id: facts.room_instance_id,
        src_media,
        rid: facts.rid,
        received_at,
        decoder_refresh_need: producer_observation.decoder_refresh_need,
        decoder_refresh_packet: facts.decoder_refresh_packet,
    })
}

fn observe_audio_filter(
    state: &mut PacketLoopState,
    buffers: &mut PacketLoopBuffers,
    packet: &mut ForwardedPacket,
    facts: PacketFacts,
    received_at: Instant,
) {
    let filter_at_source = packet.visits_origin_sinks();
    let activity_changed = state.routes.observe_audio_activity_with_filter(
        facts.src_media,
        facts.voice_activity,
        facts.audio_level,
        filter_at_source,
        received_at,
    );
    if filter_at_source
        && let Some(generation) = state.routes.source_filter_generation(facts.src_media)
    {
        packet.replace_source_filter_generation(generation);
    }
    if activity_changed {
        cold_path();
        buffers
            .dirty_source_policy_channel_ids
            .push(facts.room_instance_id);
    }
}

fn admit_observed_packet(
    state: &mut PacketLoopState,
    observed: &ObservedIncomingPacket,
    packet: ForwardedPacket,
    ready_packets: &mut Vec<ForwardedPacket>,
) -> DecoderRefreshAdmission {
    if observed.decoder_refresh_packet.codec.is_none() {
        let start = ready_packets.len();
        ready_packets.push(packet);
        return DecoderRefreshAdmission::Ready {
            packet_range: start..ready_packets.len(),
        };
    }
    state.pending_decoder_refreshes.admit(
        DecoderRefreshAdmissionInput {
            room_instance_id: observed.room_instance_id,
            source_id: observed.src_media,
            rid: observed.rid,
            need: observed.decoder_refresh_need,
            packet: observed.decoder_refresh_packet,
            observed_at: observed.received_at,
        },
        packet,
        ready_packets,
    )
}

fn update_decoder_readiness(
    state: &mut PacketLoopState,
    control: &RtcMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
    observed: &ObservedIncomingPacket,
) {
    let Some(readiness) = buffers.push_rid_readiness(
        &observed.source,
        observed.src_media,
        observed.rid,
        false,
        observed.received_at,
    ) else {
        return;
    };
    if apply_pending_rid_readiness(state, control, &readiness) {
        buffers
            .rid_readiness_changed_sources
            .push(observed.src_media);
    }
}

fn plan_ready_packets(
    state: &PacketLoopState,
    control: &RtcMetricsRecorder,
    packet_sinks: Option<&PacketSinkRouteCache>,
    packet_range: Range<usize>,
    ready_packets: &mut [ForwardedPacket],
    forwards: &mut Vec<PacketForward>,
) {
    let Some(packet_sinks) = packet_sinks else {
        return;
    };
    for pkt_idx in packet_range {
        if let Some(packet) = ready_packets.get_mut(pkt_idx) {
            plan_forwards(state, packet_sinks, control, pkt_idx, packet, forwards);
        }
    }
}

fn observe_complete_decoder_refresh(
    state: &mut PacketLoopState,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    ssrc: Ssrc,
    observed_at: Instant,
) -> bool {
    let Some(cleared) =
        state
            .routes
            .observe_decoder_refresh(transport_media_id, rid, ssrc, observed_at)
    else {
        return false;
    };
    for _ in 0..cleared {
        control.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Cleared);
    }
    let scope = if rid.is_some() {
        RtpDecoderRefreshScope::Rid
    } else {
        RtpDecoderRefreshScope::Source
    };
    rtp.record_decoder_refresh(scope);
    true
}

#[cfg(feature = "internal-benchmarks")]
pub fn record_incoming_stats_for_benchmark(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    record_incoming_stats(state, source_policy_signal, control, rtp, buffers);
}

fn apply_pending_rid_readiness(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    pending: &super::buffers::PendingRidReadiness,
) -> bool {
    match &pending.source {
        ForwardedPacketSource::Relayed(src_key) => apply_src_decoder_ready(
            state,
            metrics,
            src_key,
            pending.src_media,
            pending.rid,
            pending.complete_refresh,
            pending.observed_at,
        ),
        ForwardedPacketSource::Local(session_handle) => {
            let Some(src_key) = state.users.key_for_handle(*session_handle).cloned() else {
                return false;
            };
            apply_src_decoder_ready(
                state,
                metrics,
                &src_key,
                pending.src_media,
                pending.rid,
                pending.complete_refresh,
                pending.observed_at,
            )
        }
    }
}

fn flush_first_video_kfs(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
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
    metrics: &RtcMetricsRecorder,
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
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    now: Instant,
) {
    if state.routes.source_is_active(transport_media_id)
        && source_is_video(state, src_key, transport_media_id)
    {
        request_kf_for_target(
            state,
            metrics,
            KeyframeRequestTarget::Local(src_key, transport_media_id),
            None,
            KeyframeRequestKind::Pli,
            KeyframeRequestMode::for_recovery(
                now,
                state
                    .routes
                    .decoder_refresh_is_observable(transport_media_id),
            ),
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
/// The planner orders forwards by packet index. Each relay destination receives
/// a distinct packet wrapper whose payload is shared through `Arc<[u8]>`.
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
        let payload_len = packet.payload().len();
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
