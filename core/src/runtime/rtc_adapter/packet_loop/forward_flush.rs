use std::time::Instant;

use str0m::media::{KeyframeRequestKind, MediaKind};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::{
    super::{
        forwarded_packet::ForwardedPacket,
        forwarding_destination::{ForwardSendOutcome, ForwardingDestination},
        media_registry::RegisteredMediaHandle,
        state::RtcBootstrapState,
        worker::request_keyframe_for_source,
    },
    buffers::PacketLoopBuffers,
};
use crate::runtime::{
    metrics::RuntimeMetrics,
    transport_adapter::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
};

pub(super) fn record_incoming_stats(
    state: &mut RtcBootstrapState,
    source_policy_signal: &SourcePolicySignal,
    metrics: &RuntimeMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    let (pending_packets, dirty_source_policy_channel_ids) = (
        &mut buffers.pending_packets,
        &mut buffers.dirty_source_policy_channel_ids,
    );
    for packet in pending_packets {
        if let Some(transport_media_id) = packet.resolve_source_transport_media_id(state) {
            let payload_len = packet.payload_len();
            let voice_activity = packet.route_control_voice_activity();
            let audio_level = packet.route_control_audio_level();
            let audio_policy_changed = state.route_control.observe_audio_activity(
                transport_media_id,
                voice_activity,
                audio_level,
                packet.received_at(),
            );
            if audio_policy_changed {
                dirty_source_policy_channel_ids
                    .push(packet.source_session_key().room_instance_id());
            }
            let first_ingress = state
                .record_incoming_bitrate(transport_media_id, packet.received_at(), payload_len)
                .unwrap_or(false);
            if first_ingress {
                debug!(
                    user_id = ?packet.source_session_key().user_id(),
                    media_worker_id = packet.source_session_key().media_worker_id(),
                    ?transport_media_id,
                    payload_bytes = payload_len,
                    "observed first RTP ingress for published media"
                );
                request_first_video_keyframe(
                    state,
                    metrics,
                    packet.source_session_key(),
                    transport_media_id,
                    packet.received_at(),
                );
            }
            metrics.record_rtp_ingress(payload_len);
        }
    }
    buffers.flush_source_policy_dirty(source_policy_signal);
}

fn request_first_video_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    now: Instant,
) {
    if !source_is_video(state, source_session_key, transport_media_id) {
        return;
    }
    // FIXME(simulcast): replace this first-RTP retry with a pending per-RID
    // keyframe refresh queue once live layer detection owns RID readiness.
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

pub(super) fn flush_forward_routes(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    let (forwards, pending_packets) = (&buffers.forwards, &mut buffers.pending_packets);
    buffers.relay_packets.clear();
    buffers
        .relay_packets
        .resize_with(pending_packets.len(), || None);
    let relay_packets = &mut buffers.relay_packets;
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
                let Some(shared_packet) = relay_packets.get_mut(packet_idx) else {
                    continue;
                };
                Some(
                    shared_packet
                        .get_or_insert_with(|| packet.share_for_relay(source_transport_media_id)),
                )
            }
            ForwardingDestination::LocalRtc(_) | ForwardingDestination::PacketSink(_) => None,
        };
        let packet = relay_packet.unwrap_or(packet);
        match destination.send(state, packet, is_last_destination) {
            Ok(ForwardSendOutcome::LocalRtc {
                payload_bytes: Some(payload_len),
            }) => {
                metrics.record_rtp_egress(payload_len);
                metrics.record_rtp_forwarded(destination_kind, payload_len);
            }
            Ok(ForwardSendOutcome::SideEffect)
                if matches!(
                    destination,
                    ForwardingDestination::PacketSink(_)
                        | ForwardingDestination::IntraNodeRelay(_)
                        | ForwardingDestination::InterNodeRelay(_)
                ) =>
            {
                metrics.record_rtp_forwarded(destination_kind, payload_len);
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
