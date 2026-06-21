//! command dispatcher for worker-local RTC state.
//!
//! This module exists to keep the mailbox match in one place while the actual
//! state mutation lives in focused submodules. It should stay simple:
//!  decode one worker command, forward it to the owning module, and pass
//! through the immutable runtime context that those handlers need.

use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::oneshot;

#[cfg(test)]
use super::publication;
use super::{
    super::super::{
        bitrate::BitrateRegistry,
        commands::{CloseSessionState, RtcMediaControlCommand, RtcWorkerCommand},
        state::{PacketLoopState, RtcSnapshotState},
    },
    bwe, media,
    negotiation::{self, OfferBootstrapConfig},
    session,
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, RtcUdpIoBackend, VideoBitrateLimits,
    engine::{
        media_transport::{
            TransportMediaId, TransportResult, TransportSessionKey, TransportSourceActivitySnapshot,
        },
        metrics::RuntimeMetrics,
    },
};

pub struct WorkerCommandContext<'a> {
    pub bitrate_registry: &'a Arc<Mutex<BitrateRegistry>>,
    pub snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub now: Instant,
    pub public_ip: IpAddr,
    pub max_bitrate_in: Bitrate,
    pub max_bitrate_out: Bitrate,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
    pub rtc_udp_io_backend: RtcUdpIoBackend,
    pub codec_flags: MediaCodecFlags,
    pub codec_preferences: CodecPreferences,
    pub media_quality_interval: Option<Duration>,
    pub metrics: &'a RuntimeMetrics,
}

impl WorkerCommandContext<'_> {
    fn offer_bootstrap_config(&self) -> OfferBootstrapConfig<'_> {
        OfferBootstrapConfig {
            public_ip: self.public_ip,
            max_bitrate_out: self.max_bitrate_out,
            video_bitrate_limits: self.video_bitrate_limits,
            rtc_port_range: self.rtc_port_range,
            rtc_udp_io_backend: self.rtc_udp_io_backend,
            codec_flags: self.codec_flags,
            codec_preferences: self.codec_preferences,
            media_quality_interval: self.media_quality_interval,
            metrics: self.metrics,
        }
    }

    fn recv_media_policy(&self) -> media::RecvMediaPolicy {
        media::RecvMediaPolicy {
            max_bitrate_in: self.max_bitrate_in,
            video_bitrate_limits: self.video_bitrate_limits,
            codec_flags: self.codec_flags,
            codec_preferences: self.codec_preferences,
        }
    }
}

/// Dispatch one production worker command against the worker-local RTC state.
///
/// Callers must already serialize access to `state`, this function assumes it
/// runs on the packet-loop task that owns the worker.
pub fn handle_worker_command(
    state: &mut PacketLoopState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::CreateInitialSessionOffer {
            session_key,
            response,
        } => respond(
            response,
            negotiation::worker_create_initial_session_offer(
                state,
                context.bitrate_registry,
                context.snapshot_state,
                context.offer_bootstrap_config(),
                &session_key,
            ),
        ),
        RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response } => respond(
            response,
            Ok(state.routes.active_speaker_sources(context.now)),
        ),
        RtcWorkerCommand::ActiveSpeakerDiagnosticSnapshot { response } => respond(
            response,
            Ok(state.routes.active_speaker_diagnostics(context.now)),
        ),
        RtcWorkerCommand::SourceActivitySnapshot {
            transport_media_ids,
            response,
        } => respond_source_activity_snapshot(state, &transport_media_ids, context.now, response),
        RtcWorkerCommand::NextActiveSpeakerDeadline { response } => respond(
            response,
            Ok(state.routes.next_active_speaker_deadline(context.now)),
        ),
        RtcWorkerCommand::ExpiredActiveSpeakerRoomInstanceIds { now, response } => {
            respond(response, Ok(state.expired_active_speaker_rooms(now)));
        }
        RtcWorkerCommand::SetReceiverBweTargetBatch { updates, response } => respond(
            response,
            Ok(bwe::worker_set_receiver_bwe_targets(
                state,
                context.max_bitrate_out,
                &updates,
            )),
        ),
        RtcWorkerCommand::CreateSessionRenegotiationOffer {
            session_key,
            response,
        } => respond(
            response,
            negotiation::worker_create_session_renegotiation_offer(state, &session_key),
        ),
        RtcWorkerCommand::ApplySessionAnswer {
            session_key,
            answer_sdp,
            response,
        } => respond(
            response,
            negotiation::worker_apply_session_answer(
                state,
                context.max_bitrate_in,
                &session_key,
                &answer_sdp,
            ),
        ),
        RtcWorkerCommand::CloseSession {
            session_key,
            response,
        } => respond(response, Ok(close_session(state, context, &session_key))),
        #[cfg(test)]
        RtcWorkerCommand::ResolveNegotiatedProducerParameters {
            session_key,
            transport_media_id,
            response,
        } => respond(
            response,
            publication::worker_resolve_negotiated_producer_parameters(
                state,
                &session_key,
                transport_media_id,
            ),
        ),
        RtcWorkerCommand::ResolveMediaMid {
            transport_media_id,
            response,
        } => respond(
            response,
            Ok(state
                .resolve_mid(transport_media_id)
                .map(|mid| mid.to_string())),
        ),
        RtcWorkerCommand::RemoveMedia { .. }
        | RtcWorkerCommand::AddRecvMedia { .. }
        | RtcWorkerCommand::AddSendMedia { .. }
        | RtcWorkerCommand::MediaControl(_) => {
            handle_media_command(state, context, command);
        }
    }
}

fn respond<T>(response: oneshot::Sender<TransportResult<T>>, result: TransportResult<T>) {
    let _ = response.send(result);
}

fn respond_source_activity_snapshot(
    state: &PacketLoopState,
    transport_media_ids: &[TransportMediaId],
    now: Instant,
    response: oneshot::Sender<TransportResult<TransportSourceActivitySnapshot>>,
) {
    respond(
        response,
        Ok(state.routes.source_activity_snapshot(
            transport_media_ids,
            now,
            &state.incoming_bitrate_counters,
        )),
    );
}

fn close_session(
    state: &mut PacketLoopState,
    context: &WorkerCommandContext<'_>,
    session_key: &TransportSessionKey,
) -> CloseSessionState {
    session::worker_close_session(
        state,
        context.bitrate_registry,
        context.snapshot_state,
        session_key,
        context.metrics,
    )
}

fn handle_media_command(
    state: &mut PacketLoopState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::RemoveMedia {
            session_key,
            transport_media_id,
            response,
        } => respond(
            response,
            media::worker_remove_media(
                state,
                context.bitrate_registry,
                &session_key,
                transport_media_id,
            ),
        ),
        RtcWorkerCommand::AddRecvMedia {
            session_key,
            media_kind,
            rtp_parameters,
            response,
        } => respond(
            response,
            media::worker_add_recv_media(
                state,
                context.bitrate_registry,
                context.recv_media_policy(),
                &session_key,
                media_kind,
                &rtp_parameters,
            ),
        ),
        RtcWorkerCommand::AddSendMedia {
            consumer_key,
            media_kind,
            source,
            remote_source_control,
            consumer_rtp_parameters,
            active,
            response,
        } => respond(
            response,
            media::worker_add_send_media(
                state,
                media::AddSendMediaRequest {
                    consumer_key: &consumer_key,
                    media_kind,
                    source: &source,
                    remote_source_control,
                    consumer_rtp_parameters: &consumer_rtp_parameters,
                    active,
                },
                context.now,
            ),
        ),
        RtcWorkerCommand::MediaControl(command) => {
            handle_media_route_control_command(state, context.metrics, context.now, command);
        }
        _ => {}
    }
}

fn handle_media_route_control_command(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    now: Instant,
    command: RtcMediaControlCommand,
) {
    match command {
        RtcMediaControlCommand::Apply { request, response } => {
            media::apply_route_control_request(state, metrics, request, response);
        }
        RtcMediaControlCommand::SetConsumerPacketGateBatch {
            source,
            updates,
            response,
        } => media::respond_set_consumer_pkt_gates(state, &source, updates, now, response),
    }
}
