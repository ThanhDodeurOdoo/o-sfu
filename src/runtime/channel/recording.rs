use o_sfu_protocol::{
    shared::{RecordingState, SessionId, StopCode},
    signaling::RecordingOptions,
};

use super::{Channel, ChannelSessionPermissions};
use crate::runtime::{
    ConnectionId, diagnostics::DiagnosticsEventData, telemetry::schema::event as telemetry_event,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordingPermissions {
    audio: bool,
    video: bool,
    transcription: bool,
}

impl RecordingPermissions {
    #[must_use]
    const fn any(self) -> bool {
        self.audio || self.video || self.transcription
    }
}

impl Channel {
    /// Validate and apply a recording-start request for one live session
    /// without exposing recording-service details to the signaling edge.
    pub(crate) async fn start_recording_runtime(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        options: RecordingOptions,
    ) -> bool {
        let request_context = {
            let state = self.state.read().await;
            state.recording_request_context(session_id, connection_id)
        };
        let Some(request_context) = request_context else {
            self.metrics.record_recording_start_rejected();
            return false;
        };
        let permissions = self.recording_permissions(request_context.permissions());
        if !permissions.any() {
            self.metrics.record_recording_start_rejected();
            return false;
        }

        let current_state = request_context.recording_state();
        let is_recording = current_state.recording == Some(true);
        if is_recording {
            if options.audio.is_some() || options.video.is_some() {
                self.metrics.record_recording_start_rejected();
                return false;
            }
            let Some(transcription) = options.transcription else {
                self.metrics.record_recording_start_rejected();
                return false;
            };
            if !permissions.transcription {
                self.metrics.record_recording_start_rejected();
                return false;
            }
            let mut next_state = current_state.clone();
            next_state.transcription = Some(transcription);
            let fanout = {
                let mut state = self.state.write().await;
                state.apply_recording_state_update(next_state, None)
            };
            if let Some(fanout) = fanout {
                fanout.emit();
            }
            self.metrics.record_recording_start_accepted();
            self.diagnostics.record(
                DiagnosticsEventData::for_session(
                    self.uuid(),
                    session_id,
                    telemetry_event::RECORDING_STARTED,
                )
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(self.media_worker_id())
                .insert_field("transcription", transcription),
            );
            return true;
        }

        let wants_audio = options.audio.unwrap_or(false);
        let wants_video = options.video.unwrap_or(false);
        let wants_transcription = options.transcription.unwrap_or(false);
        if (!wants_audio && !wants_video && !wants_transcription)
            || (wants_audio && !permissions.audio)
            || (wants_video && !permissions.video)
            || (wants_transcription && !permissions.transcription)
        {
            self.metrics.record_recording_start_rejected();
            return false;
        }

        if self.recording_service.start().is_err() {
            self.metrics.record_recording_start_rejected();
            return false;
        }

        let fanout = {
            let mut state = self.state.write().await;
            state.apply_recording_state_update(
                RecordingState {
                    recording: Some(true),
                    audio: Some(wants_audio),
                    transcription: Some(wants_transcription),
                    video: Some(wants_video),
                },
                None,
            )
        };
        if let Some(fanout) = fanout {
            fanout.emit();
        }
        self.metrics.record_recording_start_accepted();
        self.metrics.add_active_recording_channels(1);
        self.diagnostics.record(
            DiagnosticsEventData::for_session(
                self.uuid(),
                session_id,
                telemetry_event::RECORDING_STARTED,
            )
            .with_connection_id(connection_id.as_u64())
            .with_media_worker_id(self.media_worker_id())
            .insert_field("audio", wants_audio)
            .insert_field("transcription", wants_transcription)
            .insert_field("video", wants_video),
        );
        true
    }

    /// Validate and apply a recording-stop request for one live session.
    pub(crate) async fn stop_recording_runtime(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> bool {
        let request_context = {
            let state = self.state.read().await;
            state.recording_request_context(session_id, connection_id)
        };
        let Some(request_context) = request_context else {
            self.metrics.record_recording_stop_rejected();
            return false;
        };
        if !self
            .recording_permissions(request_context.permissions())
            .any()
        {
            self.metrics.record_recording_stop_rejected();
            return false;
        }
        let current_state = request_context.recording_state();
        if current_state.recording != Some(true) {
            self.metrics.record_recording_stop_accepted();
            return true;
        }
        if self.recording_service.stop().is_err() {
            self.metrics.record_recording_stop_rejected();
            return false;
        }
        let fanout = {
            let mut state = self.state.write().await;
            state.apply_recording_state_update(
                RecordingState {
                    recording: Some(false),
                    audio: Some(false),
                    transcription: Some(false),
                    video: Some(false),
                },
                Some(StopCode::UserRequest),
            )
        };
        if let Some(fanout) = fanout {
            fanout.emit();
        }
        self.metrics.record_recording_stop_accepted();
        self.metrics.add_active_recording_channels(-1);
        self.diagnostics.record(
            DiagnosticsEventData::for_session(
                self.uuid(),
                session_id,
                telemetry_event::RECORDING_STOPPED,
            )
            .with_connection_id(connection_id.as_u64())
            .with_media_worker_id(self.media_worker_id())
            .insert_field("stop_code", "user_request"),
        );
        true
    }

    fn recording_permissions(
        &self,
        permissions: ChannelSessionPermissions,
    ) -> RecordingPermissions {
        let feature_flags = self.feature_flags();
        let recording_enabled = self.recording_enabled();
        RecordingPermissions {
            audio: recording_enabled
                && feature_flags.audio_recording
                && permissions.audio_recording(),
            video: recording_enabled
                && feature_flags.video_recording
                && permissions.video_recording(),
            transcription: recording_enabled
                && feature_flags.transcription
                && permissions.transcription(),
        }
    }
}
