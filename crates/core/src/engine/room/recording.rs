use o_sfu_telemetry::schema::event as telemetry_event;

use super::{Room, RoomUserPermissions};
use crate::engine::{
    ConnectionId, RecordingOptions, RecordingState, StopCode, UserId,
    diagnostics::DiagnosticsEventData,
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum StartRecordingPlan {
    Start {
        audio: bool,
        video: bool,
        transcription: bool,
    },
    UpdateTranscription(bool),
}

impl StartRecordingPlan {
    fn new(
        current: &RecordingState,
        permissions: RecordingPermissions,
        options: &RecordingOptions,
    ) -> Option<Self> {
        if !permissions.any() {
            return None;
        }
        if current.recording == Some(true) {
            if options.audio.is_some() || options.video.is_some() || !permissions.transcription {
                return None;
            }
            return Some(Self::UpdateTranscription(options.transcription?));
        }
        let audio = options.audio.unwrap_or(false);
        let video = options.video.unwrap_or(false);
        let transcription = options.transcription.unwrap_or(false);
        if (!audio && !video && !transcription)
            || (audio && !permissions.audio)
            || (video && !permissions.video)
            || (transcription && !permissions.transcription)
        {
            return None;
        }
        Some(Self::Start {
            audio,
            video,
            transcription,
        })
    }
}

impl Room {
    pub(crate) async fn apply_recording_start(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        options: RecordingOptions,
    ) -> bool {
        let Some((permissions, current_state)) =
            self.recording_request(user_id, connection_id).await
        else {
            return self.reject_recording_start();
        };
        let Some(plan) = StartRecordingPlan::new(&current_state, permissions, &options) else {
            return self.reject_recording_start();
        };
        let next_state = match &plan {
            StartRecordingPlan::Start {
                audio,
                video,
                transcription,
            } => {
                if self.recording_service.start().is_err() {
                    return self.reject_recording_start();
                }
                active_recording_state(*audio, *transcription, *video)
            }
            StartRecordingPlan::UpdateTranscription(transcription) => {
                let mut state = current_state;
                state.transcription = Some(*transcription);
                state
            }
        };

        self.apply_recording_state(next_state, None).await;
        self.metrics.record_recording_start_accepted();
        if matches!(plan, StartRecordingPlan::Start { .. }) {
            self.metrics.add_active_recording_rooms(1);
        }
        self.record_start_diagnostics(user_id, connection_id, &plan)
            .await;
        true
    }

    pub(crate) async fn apply_recording_stop(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> bool {
        let Some((permissions, current_state)) =
            self.recording_request(user_id, connection_id).await
        else {
            return self.reject_recording_stop();
        };
        if !permissions.any() {
            return self.reject_recording_stop();
        }
        if current_state.recording != Some(true) {
            self.metrics.record_recording_stop_accepted();
            return true;
        }
        if self.recording_service.stop().is_err() {
            return self.reject_recording_stop();
        }

        self.apply_recording_state(stopped_recording_state(), Some(StopCode::UserRequest))
            .await;
        self.metrics.record_recording_stop_accepted();
        self.metrics.add_active_recording_rooms(-1);
        self.record_stop_diagnostics(user_id, connection_id).await;
        true
    }

    async fn recording_request(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<(RecordingPermissions, RecordingState)> {
        let request = {
            let state = self.state.read().await;
            state.recording_request_context(user_id, connection_id)
        };
        let (permissions, state) = request?;
        Some((self.recording_permissions(permissions), state))
    }

    async fn apply_recording_state(&self, next: RecordingState, stop_code: Option<StopCode>) {
        let fanout = {
            let mut state = self.state.write().await;
            state.apply_recording_state_update(next, stop_code)
        };
        if let Some(fanout) = fanout {
            fanout.emit();
        }
    }

    async fn record_start_diagnostics(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        plan: &StartRecordingPlan,
    ) {
        let event = DiagnosticsEventData::for_user(
            self.uuid(),
            user_id,
            telemetry_event::RECORDING_STARTED,
        )
        .with_connection_id(connection_id.as_u64())
        .with_media_worker_id(self.media_worker_id_for_connection(connection_id).await);
        match plan {
            StartRecordingPlan::Start {
                audio,
                video,
                transcription,
            } => self.diagnostics.record(
                event
                    .insert_field("audio", *audio)
                    .insert_field("transcription", *transcription)
                    .insert_field("video", *video),
            ),
            StartRecordingPlan::UpdateTranscription(transcription) => self
                .diagnostics
                .record(event.insert_field("transcription", *transcription)),
        }
    }

    async fn record_stop_diagnostics(&self, user_id: &UserId, connection_id: ConnectionId) {
        let media_worker_id = self.media_worker_id_for_connection(connection_id).await;
        self.diagnostics.record(
            DiagnosticsEventData::for_user(
                self.uuid(),
                user_id,
                telemetry_event::RECORDING_STOPPED,
            )
            .with_connection_id(connection_id.as_u64())
            .with_media_worker_id(media_worker_id)
            .insert_field("stop_code", "user_request"),
        );
    }

    fn reject_recording_start(&self) -> bool {
        self.metrics.record_recording_start_rejected();
        false
    }

    fn reject_recording_stop(&self) -> bool {
        self.metrics.record_recording_stop_rejected();
        false
    }

    fn recording_permissions(&self, permissions: RoomUserPermissions) -> RecordingPermissions {
        let feature_flags = self.feature_flags();
        let recording_available = self.recording_available();
        RecordingPermissions {
            audio: recording_available
                && feature_flags.audio_recording
                && permissions.audio_recording(),
            video: recording_available
                && feature_flags.video_recording
                && permissions.video_recording(),
            transcription: recording_available
                && feature_flags.transcription
                && permissions.transcription(),
        }
    }

    async fn media_worker_id_for_connection(&self, connection_id: ConnectionId) -> usize {
        self.state
            .read()
            .await
            .media_worker_id_for_connection(connection_id)
            .as_usize()
    }
}

fn active_recording_state(audio: bool, transcription: bool, video: bool) -> RecordingState {
    RecordingState {
        recording: Some(true),
        audio: Some(audio),
        transcription: Some(transcription),
        video: Some(video),
    }
}

fn stopped_recording_state() -> RecordingState {
    RecordingState {
        recording: Some(false),
        audio: Some(false),
        transcription: Some(false),
        video: Some(false),
    }
}
