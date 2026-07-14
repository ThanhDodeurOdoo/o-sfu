#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeFeatureFlags {
    pub transcription: bool,
    pub audio_recording: bool,
    pub video_recording: bool,
}
