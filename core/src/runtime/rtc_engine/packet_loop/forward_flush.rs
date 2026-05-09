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

use str0m::media::KeyframeRequestKind;
use tracing::debug;

use super::{
    machine::{
        effect::{HotRtpMetricEffect, PacketLoopEffect, PacketLoopEffects},
        scratch::PacketLoopScratch,
        source_facts::PacketLoopSourceFacts,
        state::PacketLoopState,
    },
    selected_rid::{SourceRidReadinessObservation, observe_source_rid_readiness},
    time::PacketLoopTime,
};
use crate::runtime::media_transport::{TransportMediaId, TransportSessionKey};

/// Observe packet-path metadata before packets are forwarded.
///
/// This is where producer SSRC bindings, audio activity, RID readiness and
/// incoming bitrate become worker state or metrics. The function also coalesces
/// source-policy wakeups so the room layer is notified once per changed room
/// after the batch has been inspected.
pub(super) fn record_incoming_stats(
    state: &mut PacketLoopState,
    effects: &mut PacketLoopEffects,
    scratch: &mut PacketLoopScratch,
    now: PacketLoopTime,
) {
    scratch.observe_pending_packets(|packet_idx, packet, observation_scratch| {
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
            if audio_policy_changed {
                observation_scratch
                    .mark_source_policy_dirty(packet.source_session_key().room_instance_id());
            }
            let metadata = packet.resolve_route_control_layer_metadata(state);
            let activated_selected_rid = metadata.rid().is_some_and(|rid| {
                observe_source_rid_readiness(
                    state,
                    SourceRidReadinessObservation {
                        effects,
                        scratch: observation_scratch.rid_readiness(),
                        source_session_key: packet.source_session_key(),
                        source_transport_media_id: transport_media_id,
                        rid,
                        is_keyframe: packet
                            .route_control_decoder_refresh(state.source_facts(transport_media_id)),
                        now,
                    },
                )
            });
            effects.push(PacketLoopEffect::RecordIncomingBitrate {
                packet_idx,
                transport_media_id,
                payload_bytes: payload_len,
            });
            let first_ingress = state.observe_incoming_media(transport_media_id);
            if first_ingress {
                debug!(
                    user_id = ?packet.source_session_key().user_id(),
                    media_worker_id = packet.source_session_key().media_worker_id(),
                    ?transport_media_id,
                    payload_bytes = payload_len,
                    "observed first RTP ingress for published media"
                );
                if !activated_selected_rid {
                    request_first_video_keyframe(
                        effects,
                        packet.source_session_key(),
                        transport_media_id,
                        state.source_facts(transport_media_id),
                        now,
                    );
                }
            }
            effects.record_hot_rtp(HotRtpMetricEffect::Ingress {
                payload_bytes: payload_len,
            });
        }
    });
    emit_source_policy_dirty_effects(scratch, effects);
}

/// Request a keyframe when the first packet for a video producer appears.
///
/// First ingress proves the producer is alive, but the first observed packet may
/// not be decodable by new consumers. Asking for a PLI here helps strict
/// selected-RID gates and late subscribers converge without making room policy
/// inspect packet payloads.
fn request_first_video_keyframe(
    effects: &mut PacketLoopEffects,
    source_session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    source_facts: PacketLoopSourceFacts,
    now: PacketLoopTime,
) {
    if !source_facts.is_video() {
        return;
    }
    effects.push(PacketLoopEffect::RequestLocalKeyframe {
        source_session_key: source_session_key.clone(),
        source_transport_media_id: transport_media_id,
        rid: None,
        kind: KeyframeRequestKind::Pli,
        now,
    });
}

fn emit_source_policy_dirty_effects(
    scratch: &mut PacketLoopScratch,
    effects: &mut PacketLoopEffects,
) {
    let dirty_rooms = scratch.dirty_source_policy_rooms();
    if dirty_rooms.is_empty() {
        return;
    }
    for room_instance_id in dirty_rooms.iter().copied() {
        effects.push(PacketLoopEffect::MarkSourcePolicyDirty(room_instance_id));
    }
    scratch.clear_dirty_source_policy_rooms();
}
