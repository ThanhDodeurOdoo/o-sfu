//! command dispatcher for worker-local RTC state.
//!
//! This module exists to keep the mailbox match in one place while the actual
//! state mutation lives in focused submodules. It should stay simple:
//!  decode one worker command, forward it to the owning module, and passe
//! through the immutable runtime context that those handlers need.

use std::{
    collections::BTreeSet,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::sync::oneshot;

#[cfg(test)]
use super::publication;
use super::{
    super::{
        bitrate::RtcBitrateState,
        commands::RtcWorkerCommand,
        state::{RtcBootstrapState, RtcSnapshotState},
    },
    media,
    negotiation::{self, OfferBootstrapConfig},
    session,
};
use crate::{
    CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits,
    runtime::{
        RoomInstanceId,
        media_transport::{
            ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportAdapterError,
        },
        metrics::RuntimeMetrics,
    },
};

pub struct WorkerCommandContext<'a> {
    pub bitrate_state: &'a Arc<Mutex<RtcBitrateState>>,
    pub snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub now: Instant,
    pub public_ip: IpAddr,
    pub max_bitrate_in_bps: u64,
    pub max_bitrate_out_bps: u64,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
    pub codec_flags: MediaCodecFlags,
    pub codec_preferences: CodecPreferences,
    pub metrics: &'a RuntimeMetrics,
}

/// Dispatch one production worker command against the shard-local RTC state.
///
/// Callers must already serialize access to `state`, this function assumes it
/// runs on the packet-loop task that owns the shard.
pub fn handle_worker_command(
    state: &mut RtcBootstrapState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::CreateInitialSessionOffer { .. }
        | RtcWorkerCommand::ActiveSpeakerSourceSnapshot { .. }
        | RtcWorkerCommand::ActiveSpeakerDiagnosticSnapshot { .. }
        | RtcWorkerCommand::NextActiveSpeakerDeadline { .. }
        | RtcWorkerCommand::ExpiredActiveSpeakerRoomInstanceIds { .. }
        | RtcWorkerCommand::CreateSessionRenegotiationOffer { .. }
        | RtcWorkerCommand::ApplySessionAnswer { .. } => {
            handle_negotiation_command(state, context, command);
        }
        RtcWorkerCommand::CloseSession {
            session_key,
            response,
        } => session::respond_close_session(
            state,
            context.bitrate_state,
            context.snapshot_state,
            &session_key,
            context.metrics,
            response,
        ),
        #[cfg(test)]
        RtcWorkerCommand::ResolveNegotiatedProducerParameters {
            session_key,
            transport_media_id,
            response,
        } => publication::respond_resolve_negotiated_producer_parameters(
            state,
            &session_key,
            transport_media_id,
            response,
        ),
        RtcWorkerCommand::ResolveMediaMid {
            transport_media_id,
            response,
        } => media::respond_resolve_media_mid(state, transport_media_id, response),
        RtcWorkerCommand::RemoveMedia { .. }
        | RtcWorkerCommand::AddRecvMedia { .. }
        | RtcWorkerCommand::AddSendMedia { .. }
        | RtcWorkerCommand::AddRelayTarget { .. }
        | RtcWorkerCommand::RemoveRelayTarget { .. }
        | RtcWorkerCommand::SetRelayTargetActive { .. }
        | RtcWorkerCommand::RequestRemoteKeyframe { .. }
        | RtcWorkerCommand::SetRemoteSourceRouteActive { .. }
        | RtcWorkerCommand::SetRemoteSourcePacketGate { .. }
        | RtcWorkerCommand::SetProducerActive { .. }
        | RtcWorkerCommand::SetConsumerActive { .. }
        | RtcWorkerCommand::SetConsumerPacketGate { .. }
        | RtcWorkerCommand::SetConsumerPacketGateBatch { .. }
        | RtcWorkerCommand::RequestConsumerKeyframe { .. } => {
            handle_media_command(
                state,
                context.bitrate_state,
                media::RecvMediaPolicy {
                    max_bitrate_in_bps: context.max_bitrate_in_bps,
                    video_bitrate_limits: context.video_bitrate_limits,
                    codec_flags: context.codec_flags,
                    codec_preferences: context.codec_preferences,
                },
                context.metrics,
                context.now,
                command,
            );
        }
    }
}

fn handle_negotiation_command(
    state: &mut RtcBootstrapState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::CreateInitialSessionOffer {
            session_key,
            response,
        } => negotiation::respond_create_initial_session_offer(
            state,
            context.bitrate_state,
            context.snapshot_state,
            OfferBootstrapConfig {
                public_ip: context.public_ip,
                max_bitrate_out_bps: context.max_bitrate_out_bps,
                video_bitrate_limits: context.video_bitrate_limits,
                rtc_port_range: context.rtc_port_range,
                codec_flags: context.codec_flags,
                codec_preferences: context.codec_preferences,
                metrics: context.metrics,
            },
            &session_key,
            response,
        ),
        RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response } => {
            respond_active_speaker_source_snapshot(state, response);
        }
        RtcWorkerCommand::ActiveSpeakerDiagnosticSnapshot { response } => {
            respond_active_speaker_diagnostic_snapshot(state, response);
        }
        RtcWorkerCommand::NextActiveSpeakerDeadline { response } => {
            respond_next_active_speaker_deadline(state, response);
        }
        RtcWorkerCommand::ExpiredActiveSpeakerRoomInstanceIds { now, response } => {
            respond_expired_active_speaker_room_instance_ids(state, now, response);
        }
        RtcWorkerCommand::CreateSessionRenegotiationOffer {
            session_key,
            response,
        } => negotiation::respond_create_session_renegotiation_offer(state, &session_key, response),
        RtcWorkerCommand::ApplySessionAnswer {
            session_key,
            answer_sdp,
            response,
        } => negotiation::respond_apply_session_answer(
            state,
            context.max_bitrate_in_bps,
            &session_key,
            &answer_sdp,
            response,
        ),
        _ => {}
    }
}

fn respond_active_speaker_source_snapshot(
    state: &RtcBootstrapState,
    response: oneshot::Sender<Result<Vec<ActiveSpeakerSource>, TransportAdapterError>>,
) {
    let snapshot = state.active_speaker_source_snapshot(Instant::now());
    let _ = response.send(Ok(snapshot));
}

fn respond_active_speaker_diagnostic_snapshot(
    state: &RtcBootstrapState,
    response: oneshot::Sender<Result<Vec<ActiveSpeakerSourceDiagnostic>, TransportAdapterError>>,
) {
    let snapshot = state.active_speaker_diagnostic_snapshot(Instant::now());
    let _ = response.send(Ok(snapshot));
}

fn respond_next_active_speaker_deadline(
    state: &RtcBootstrapState,
    response: oneshot::Sender<Result<Option<Instant>, TransportAdapterError>>,
) {
    let deadline = state
        .route_control
        .next_active_speaker_deadline(Instant::now());
    let _ = response.send(Ok(deadline));
}

fn respond_expired_active_speaker_room_instance_ids(
    state: &RtcBootstrapState,
    now: Instant,
    response: oneshot::Sender<Result<BTreeSet<RoomInstanceId>, TransportAdapterError>>,
) {
    let room_instance_ids = state.expired_active_speaker_room_instance_ids(now);
    let _ = response.send(Ok(room_instance_ids));
}

fn handle_media_command(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    recv_media_policy: media::RecvMediaPolicy,
    metrics: &RuntimeMetrics,
    now: Instant,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::RemoveMedia {
            session_key,
            transport_media_id,
            response,
        } => media::respond_remove_media(
            state,
            bitrate_state,
            &session_key,
            transport_media_id,
            response,
        ),
        RtcWorkerCommand::AddRecvMedia {
            session_key,
            media_kind,
            rtp_parameters,
            response,
        } => media::respond_add_recv_media(
            state,
            bitrate_state,
            recv_media_policy,
            &session_key,
            media_kind,
            &rtp_parameters,
            response,
        ),
        RtcWorkerCommand::AddSendMedia {
            consumer_session_key,
            media_kind,
            source_session_key,
            source_transport_media_id,
            remote_source_control,
            consumer_rtp_parameters,
            response,
        } => media::respond_add_send_media(
            state,
            media::AddSendMediaRequest {
                consumer_session_key: &consumer_session_key,
                media_kind,
                source_session_key: &source_session_key,
                source_transport_media_id,
                remote_source_control,
                consumer_rtp_parameters: &consumer_rtp_parameters,
            },
            now,
            response,
        ),
        RtcWorkerCommand::AddRelayTarget { .. }
        | RtcWorkerCommand::RemoveRelayTarget { .. }
        | RtcWorkerCommand::SetRelayTargetActive { .. }
        | RtcWorkerCommand::RequestRemoteKeyframe { .. }
        | RtcWorkerCommand::SetRemoteSourceRouteActive { .. }
        | RtcWorkerCommand::SetRemoteSourcePacketGate { .. }
        | RtcWorkerCommand::SetProducerActive { .. }
        | RtcWorkerCommand::SetConsumerActive { .. }
        | RtcWorkerCommand::SetConsumerPacketGate { .. }
        | RtcWorkerCommand::SetConsumerPacketGateBatch { .. }
        | RtcWorkerCommand::RequestConsumerKeyframe { .. } => {
            handle_media_route_control_command(state, metrics, now, command);
        }
        _ => {}
    }
}

fn handle_media_route_control_command(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    now: Instant,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::AddRelayTarget { .. }
        | RtcWorkerCommand::RemoveRelayTarget { .. }
        | RtcWorkerCommand::SetRelayTargetActive { .. } => {
            handle_relay_route_control_command(state, command);
        }
        RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id,
            target_id,
            rid,
            kind,
        } => media::respond_request_remote_keyframe(
            state,
            metrics,
            &media::RemoteKeyframeRequest {
                source_session_key: &source_session_key,
                source_transport_media_id,
                target_id,
                rid,
                kind,
            },
        ),
        RtcWorkerCommand::SetRemoteSourceRouteActive {
            source_session_key,
            source_transport_media_id,
            target_id,
            active,
        } => media::respond_set_remote_source_route_active(
            state,
            &source_session_key,
            source_transport_media_id,
            target_id,
            active,
        ),
        RtcWorkerCommand::SetRemoteSourcePacketGate {
            source_session_key,
            source_transport_media_id,
            target_id,
            packet_gate,
        } => media::respond_set_remote_source_packet_gate(
            state,
            &source_session_key,
            source_transport_media_id,
            target_id,
            packet_gate,
        ),
        RtcWorkerCommand::SetProducerActive {
            session_key,
            transport_media_id,
            active,
            response,
        } => media::respond_set_producer_active(
            state,
            &session_key,
            transport_media_id,
            active,
            response,
        ),
        RtcWorkerCommand::SetConsumerActive {
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            active,
            response,
        } => media::respond_set_consumer_active(
            state,
            &consumer_session_key,
            consumer_transport_media_id,
            &source_session_key,
            source_transport_media_id,
            active,
            response,
        ),
        RtcWorkerCommand::SetConsumerPacketGate { .. } => {
            handle_consumer_packet_gate_update(state, command, now);
        }
        RtcWorkerCommand::SetConsumerPacketGateBatch { .. } => {
            handle_consumer_packet_gate_batch_update(state, command, now);
        }
        RtcWorkerCommand::RequestConsumerKeyframe { .. } => {
            handle_consumer_keyframe_request(state, metrics, command);
        }
        _ => {}
    }
}

fn handle_relay_route_control_command(state: &mut RtcBootstrapState, command: RtcWorkerCommand) {
    match command {
        RtcWorkerCommand::AddRelayTarget {
            source_session_key,
            source_transport_media_id,
            target_id,
            target,
            response,
        } => media::respond_add_relay_target(
            state,
            &source_session_key,
            source_transport_media_id,
            target_id,
            target,
            response,
        ),
        RtcWorkerCommand::RemoveRelayTarget {
            source_transport_media_id,
            target_id,
            response,
        } => media::respond_remove_relay_target(
            state,
            source_transport_media_id,
            target_id,
            response,
        ),
        RtcWorkerCommand::SetRelayTargetActive {
            source_session_key,
            source_transport_media_id,
            target_id,
            active,
            response,
        } => media::respond_set_relay_target_active(
            state,
            &source_session_key,
            source_transport_media_id,
            target_id,
            active,
            response,
        ),
        _ => {}
    }
}

fn handle_consumer_packet_gate_update(
    state: &mut RtcBootstrapState,
    command: RtcWorkerCommand,
    now: Instant,
) {
    if let RtcWorkerCommand::SetConsumerPacketGate {
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
        packet_gate,
        response,
    } = command
    {
        media::respond_set_consumer_packet_gate(
            state,
            media::ConsumerPacketGateRequest {
                consumer_session_key: &consumer_session_key,
                consumer_transport_media_id,
                source_session_key: &source_session_key,
                source_transport_media_id,
                packet_gate,
            },
            now,
            response,
        );
    }
}

fn handle_consumer_packet_gate_batch_update(
    state: &mut RtcBootstrapState,
    command: RtcWorkerCommand,
    now: Instant,
) {
    if let RtcWorkerCommand::SetConsumerPacketGateBatch {
        source_session_key,
        source_transport_media_id,
        updates,
        response,
    } = command
    {
        media::respond_set_consumer_packet_gates(
            state,
            &source_session_key,
            source_transport_media_id,
            updates,
            now,
            response,
        );
    }
}

fn handle_consumer_keyframe_request(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    command: RtcWorkerCommand,
) {
    if let RtcWorkerCommand::RequestConsumerKeyframe {
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
        response,
    } = command
    {
        media::respond_request_consumer_keyframe(
            state,
            metrics,
            &consumer_session_key,
            consumer_transport_media_id,
            &source_session_key,
            source_transport_media_id,
            response,
        );
    }
}
