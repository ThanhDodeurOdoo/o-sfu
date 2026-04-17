use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::super::{
    forwarded_packet::ForwardedPacket,
    forwarding_destination::ForwardingDestination,
    state::{RtcBootstrapState, RtcSnapshotState},
};
use super::buffers::PacketLoopBuffers;
use crate::runtime::metrics::{RtpForwardDestinationKind, RuntimeMetrics};

pub(super) fn record_incoming_stats(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    metrics: &RuntimeMetrics,
    buffers: &PacketLoopBuffers,
) {
    for packet in &buffers.pending_packets {
        if let Some(transport_media_id) = packet.resolve_source_transport_media_id(state) {
            let payload_len = packet.payload_len();
            state.route_control.observe_audio_activity(
                transport_media_id,
                packet.route_control_voice_activity(),
                packet.route_control_audio_level(),
                packet.received_at(),
            );
            let first_ingress = snapshot_state.lock().is_ok_and(|mut snapshot| {
                snapshot.record_incoming_media(
                    packet.source_session_key(),
                    transport_media_id,
                    packet.received_at(),
                    payload_len,
                )
            });
            if first_ingress {
                debug!(
                    session_id = ?packet.source_session_key().session_id(),
                    media_worker_id = packet.source_session_key().media_worker_id(),
                    ?transport_media_id,
                    payload_bytes = payload_len,
                    "observed first RTP ingress for published media"
                );
            }
            metrics.record_rtp_ingress(payload_len);
        }
    }
}

pub(super) fn drain_relay_packets(
    relay_rx: &mut mpsc::UnboundedReceiver<ForwardedPacket>,
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
    let mut relay_packets = Vec::with_capacity(pending_packets.len());
    relay_packets.resize_with(pending_packets.len(), || None);
    for (forward_idx, forward) in forwards.iter().enumerate() {
        let is_last_destination = forwards
            .get(forward_idx + 1)
            .is_none_or(|next_forward| next_forward.packet_idx() != forward.packet_idx());
        let packet_idx = forward.packet_idx();
        let Some(packet) = pending_packets.get_mut(packet_idx) else {
            continue;
        };
        let destination = forward.destination();
        let destination_kind = match destination {
            ForwardingDestination::LocalRtc(_) => RtpForwardDestinationKind::LocalRtc,
            ForwardingDestination::Recording(_) => RtpForwardDestinationKind::Recording,
            ForwardingDestination::IntraNodeRelay(_) => RtpForwardDestinationKind::IntraNodeRelay,
            ForwardingDestination::InterNodeRelay(_) => RtpForwardDestinationKind::InterNodeRelay,
        };
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
            ForwardingDestination::LocalRtc(_) | ForwardingDestination::Recording(_) => None,
        };
        let packet = relay_packet.unwrap_or(packet);
        match destination.send(state, packet, is_last_destination) {
            Ok(Some(payload_len)) => {
                metrics.record_rtp_egress(payload_len);
                metrics.record_rtp_forwarded(destination_kind, payload_len);
            }
            Ok(None)
                if matches!(
                    destination,
                    ForwardingDestination::Recording(_)
                        | ForwardingDestination::IntraNodeRelay(_)
                        | ForwardingDestination::InterNodeRelay(_)
                ) =>
            {
                metrics.record_rtp_forwarded(destination_kind, payload_len);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    ?destination,
                    ?error,
                    "failed to write media to destination session"
                );
            }
        }
    }
}
