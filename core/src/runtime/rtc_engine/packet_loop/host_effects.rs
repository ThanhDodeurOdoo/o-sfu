//! Host execution for typed packet-loop effects.

use std::sync::{Arc, Mutex};

use tracing::warn;

use super::{
    super::{
        forwarded_packet::ForwardedPacket,
        forwarding_destination::{ForwardSendOutcome, ForwardingDestination, PacketForward},
        state::{RtcBootstrapState, RtcSnapshotState},
        worker::request_keyframe_for_source,
    },
    machine::{
        effect::{HotRtpMetricEffect, PacketLoopEffect, PacketLoopEffects, PacketLoopMetricEffect},
        scratch::PacketLoopScratch,
    },
    route_snapshot::PacketLoopRouteSnapshot,
};
use crate::runtime::{
    diagnostics::{
        DiagnosticsStore, diagnostics_room_instance_id, health_json_value, maybe_health_json_value,
    },
    media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
    metrics::{self, RtpMetricsRecorder, RuntimeMetrics},
    rtc_engine::TransportSessionHealth,
    telemetry::schema,
};

pub(super) struct PacketLoopHostEffectContext<'a> {
    pub(super) snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub(super) diagnostics: &'a Arc<DiagnosticsStore>,
    pub(super) metrics: &'a RuntimeMetrics,
    pub(super) source_policy_signal: &'a SourcePolicySignal,
    pub(super) rtp_metrics: &'a RtpMetricsRecorder,
}

pub(super) fn execute_packet_loop_effects(
    state: &mut RtcBootstrapState,
    scratch: &PacketLoopScratch,
    context: &PacketLoopHostEffectContext<'_>,
    effects: &PacketLoopEffects,
) {
    scratch.with_pending_packets(|pending_packets| {
        for effect in effects.iter() {
            execute_effect(state, pending_packets, context, effect);
        }
    });
}

pub(super) fn flush_packet_loop_forwards(
    state: &mut RtcBootstrapState,
    scratch: &mut PacketLoopScratch,
    routes: &PacketLoopRouteSnapshot,
    context: &PacketLoopHostEffectContext<'_>,
) {
    scratch.with_forwarding_buffers(|forwards, pending_packets, relay_packets| {
        flush_forwarded_packets(
            state,
            forwards,
            pending_packets,
            relay_packets,
            routes,
            context,
        );
    });
}

fn execute_effect(
    state: &mut RtcBootstrapState,
    pending_packets: &[ForwardedPacket],
    context: &PacketLoopHostEffectContext<'_>,
    effect: &PacketLoopEffect,
) {
    match effect {
        PacketLoopEffect::RecordIncomingBitrate {
            packet_idx,
            transport_media_id,
            payload_bytes,
        } => execute_incoming_bitrate_effect(
            state,
            pending_packets,
            *packet_idx,
            *transport_media_id,
            *payload_bytes,
        ),
        PacketLoopEffect::RecordHotRtpMetric(effect) => execute_hot_rtp_metric(context, *effect),
        PacketLoopEffect::RecordMetric(effect) => execute_metric(context.metrics, *effect),
        PacketLoopEffect::MarkSourcePolicyDirty(room_instance_id) => {
            context.source_policy_signal.mark_dirty(*room_instance_id);
        }
        PacketLoopEffect::RememberSnapshotRemoteAddr {
            source_addr,
            session_key,
        } => {
            if let Ok(mut snapshot) = context.snapshot_state.lock() {
                let _ = snapshot
                    .remote_addr_demux
                    .remember_remote_addr(*source_addr, session_key);
            }
        }
        PacketLoopEffect::ForgetSnapshotRemoteAddr(source_addr) => {
            if let Ok(mut snapshot) = context.snapshot_state.lock() {
                snapshot.remote_addr_demux.forget_remote_addr(*source_addr);
            }
        }
        PacketLoopEffect::SetReceiverBandwidth {
            session_key,
            estimate_bps,
        } => execute_receiver_bandwidth_effect(context, session_key, *estimate_bps),
        PacketLoopEffect::SetTransportHealth {
            session_key,
            health,
        } => execute_transport_health_effect(context, session_key, *health),
        PacketLoopEffect::RequestLocalKeyframe {
            source_session_key,
            source_transport_media_id,
            rid,
            kind,
            now,
        } => request_keyframe_for_source(
            state,
            context.metrics,
            source_session_key,
            *source_transport_media_id,
            *rid,
            *kind,
            *now,
        ),
        PacketLoopEffect::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id,
            source_control,
            rid,
            kind,
        } => {
            source_control.request_keyframe(
                source_session_key.clone(),
                *source_transport_media_id,
                *rid,
                *kind,
            );
        }
    }
}

fn flush_forwarded_packets(
    state: &mut RtcBootstrapState,
    forwards: &[PacketForward],
    pending_packets: &mut [ForwardedPacket],
    relay_packets: &mut [Option<ForwardedPacket>],
    routes: &PacketLoopRouteSnapshot,
    context: &PacketLoopHostEffectContext<'_>,
) {
    for (forward_idx, forward) in forwards.iter().enumerate() {
        let is_last_destination = forwards
            .get(forward_idx + 1)
            .is_none_or(|next_forward| next_forward.packet_idx != forward.packet_idx);
        flush_forwarded_packet(
            state,
            context,
            pending_packets,
            relay_packets,
            routes,
            forward,
            is_last_destination,
        );
    }
}

fn execute_incoming_bitrate_effect(
    state: &RtcBootstrapState,
    pending_packets: &[ForwardedPacket],
    packet_idx: usize,
    transport_media_id: TransportMediaId,
    payload_bytes: usize,
) {
    if let Some(packet) = pending_packets.get(packet_idx) {
        let _ =
            state.record_incoming_bitrate(transport_media_id, packet.received_at(), payload_bytes);
    }
}

fn execute_receiver_bandwidth_effect(
    context: &PacketLoopHostEffectContext<'_>,
    session_key: &TransportSessionKey,
    estimate_bps: u64,
) {
    if let Ok(mut snapshot_state) = context.snapshot_state.lock()
        && snapshot_state.set_receiver_bandwidth(session_key, estimate_bps) != Some(estimate_bps)
    {
        context
            .source_policy_signal
            .mark_dirty(session_key.room_instance_id());
    }
}

fn flush_forwarded_packet(
    state: &mut RtcBootstrapState,
    context: &PacketLoopHostEffectContext<'_>,
    pending_packets: &mut [ForwardedPacket],
    relay_packets: &mut [Option<ForwardedPacket>],
    routes: &PacketLoopRouteSnapshot,
    forward: &PacketForward,
    is_last_destination: bool,
) {
    let Some(packet) = pending_packets.get_mut(forward.packet_idx) else {
        return;
    };
    let destination = &forward.destination;
    let destination_kind = destination.metrics_kind();
    let payload_len = packet.payload_len();
    let relay_packet = match destination {
        ForwardingDestination::Relay { .. } => {
            let Some(source_transport_media_id) =
                packet.resolve_source_transport_media_id(&state.packet_loop)
            else {
                return;
            };
            let Some(shared_packet) = relay_packets.get_mut(forward.packet_idx) else {
                return;
            };
            Some(
                shared_packet
                    .get_or_insert_with(|| packet.share_for_relay(source_transport_media_id)),
            )
        }
        ForwardingDestination::LocalRtc { .. } | ForwardingDestination::PacketSink { .. } => None,
    };
    let packet = relay_packet.unwrap_or(packet);
    match destination.send(state, routes, packet, is_last_destination) {
        Ok(ForwardSendOutcome::LocalRtc {
            payload_bytes: Some(payload_len),
        }) => {
            execute_hot_rtp_metric(
                context,
                HotRtpMetricEffect::Egress {
                    payload_bytes: payload_len,
                },
            );
            execute_hot_rtp_metric(
                context,
                HotRtpMetricEffect::Forwarded {
                    destination: destination_kind,
                    payload_bytes: payload_len,
                },
            );
        }
        Ok(ForwardSendOutcome::SideEffect)
            if matches!(
                destination,
                ForwardingDestination::PacketSink { .. } | ForwardingDestination::Relay { .. }
            ) =>
        {
            execute_hot_rtp_metric(
                context,
                HotRtpMetricEffect::Forwarded {
                    destination: destination_kind,
                    payload_bytes: payload_len,
                },
            );
        }
        Ok(ForwardSendOutcome::OverloadedRelay) => {
            if let Some(destination_kind) = destination.relay_drop_kind() {
                execute_metric(
                    context.metrics,
                    PacketLoopMetricEffect::RtpRelayOverloadDrop(destination_kind),
                );
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
                packet_idx = forward.packet_idx,
                destination = ?destination,
                ?error,
                "failed to write media to destination user"
            );
        }
    }
}

fn execute_hot_rtp_metric(context: &PacketLoopHostEffectContext<'_>, effect: HotRtpMetricEffect) {
    match effect {
        HotRtpMetricEffect::Ingress { payload_bytes } => {
            context.rtp_metrics.record_ingress(payload_bytes);
        }
        HotRtpMetricEffect::Egress { payload_bytes } => {
            context.rtp_metrics.record_egress(payload_bytes);
        }
        HotRtpMetricEffect::Forwarded {
            destination,
            payload_bytes,
        } => {
            context
                .rtp_metrics
                .record_forwarded(destination, payload_bytes);
        }
    }
}

fn execute_metric(metrics: &RuntimeMetrics, effect: PacketLoopMetricEffect) {
    match effect {
        PacketLoopMetricEffect::RtcDatagramRoute(path) => {
            metrics.record_rtc_datagram_route(path);
        }
        PacketLoopMetricEffect::RtcDatagramDrop(reason) => {
            metrics.record_rtc_datagram_drop(reason);
        }
        PacketLoopMetricEffect::RtcDatagramFallbackScan(examined_sessions) => {
            metrics.record_rtc_datagram_fallback_scan(examined_sessions);
        }
        PacketLoopMetricEffect::RtcRouteControl(outcome) => {
            metrics.record_rtc_route_control(outcome);
        }
        PacketLoopMetricEffect::RtpRelayOverloadDrop(destination) => {
            metrics.record_rtp_relay_overload_drop(destination);
        }
        PacketLoopMetricEffect::TransportIceStateChange(state) => {
            metrics.record_transport_ice_state_change(state);
        }
        PacketLoopMetricEffect::TransportDtlsConnected => {
            metrics.record_transport_dtls_connected();
        }
    }
}

fn execute_transport_health_effect(
    context: &PacketLoopHostEffectContext<'_>,
    session_key: &TransportSessionKey,
    health: TransportSessionHealth,
) {
    let Ok(mut snapshot_state) = context.snapshot_state.lock() else {
        return;
    };
    let previous = snapshot_state.set_transport_health(session_key, health);
    context.metrics.record_transport_health_transition(
        previous.map(metrics::transport_health_state),
        Some(metrics::transport_health_state(health)),
    );
    if previous == Some(health) {
        return;
    }
    let mut fields = serde_json::Map::new();
    fields.insert(String::from("from"), maybe_health_json_value(previous));
    fields.insert(String::from("to"), health_json_value(health));
    context.diagnostics.record_transport_user_event(
        diagnostics_room_instance_id(session_key.room_instance_id()),
        session_key.user_id(),
        schema::event::TRANSPORT_HEALTH_CHANGED,
        session_key.media_worker_id(),
        fields,
    );
}
