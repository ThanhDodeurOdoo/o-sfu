use o_sfu_protocol::{
    shared::{RecordingState, SessionId, SessionPermissions, StopCode},
    signaling::RecordingOptions,
};

use super::Channel;

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
    // TODO: needs documentation:
    pub(crate) async fn start_recording_runtime(
        &self,
        session_id: &SessionId,
        connection_id: u64,
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
        true
    }

    #[cfg(test)]
    pub async fn start_recording(&self, session_id: &SessionId, options: RecordingOptions) -> bool {
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            self.metrics.record_recording_start_rejected();
            return false;
        };
        self.start_recording_runtime(session_id, connection_id, options)
            .await
    }

    // TODO: needs documentation:
    pub(crate) async fn stop_recording_runtime(
        &self,
        session_id: &SessionId,
        connection_id: u64,
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
        true
    }

    #[cfg(test)]
    pub async fn stop_recording(&self, session_id: &SessionId) -> bool {
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            self.metrics.record_recording_stop_rejected();
            return false;
        };
        self.stop_recording_runtime(session_id, connection_id).await
    }

    fn recording_permissions(&self, permissions: &SessionPermissions) -> RecordingPermissions {
        let feature_flags = self.feature_flags();
        let recording_enabled = self.recording_enabled();
        RecordingPermissions {
            audio: recording_enabled
                && feature_flags.audio_recording
                && permissions.audio_recording.unwrap_or(false),
            video: recording_enabled
                && feature_flags.video_recording
                && permissions.video_recording.unwrap_or(false),
            transcription: recording_enabled
                && feature_flags.transcription
                && permissions.transcription.unwrap_or(false),
        }
    }
}
