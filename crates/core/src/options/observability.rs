use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservabilityOptions {
    pub transport_diagnostics_enabled: bool,
    pub transport_metrics_enabled: bool,
    pub media_quality_interval: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeFeatureFlags {
    pub transcription: bool,
    pub audio_recording: bool,
    pub video_recording: bool,
}
