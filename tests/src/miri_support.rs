use o_sfu_protocol::{
    host::Command,
    wire::{
        AvailableFeatures, ClientEnvelope, EnvelopeBatch, RecordingState, ServerEnvelope,
        WelcomePayload,
    },
};

#[must_use]
pub fn empty_welcome_payload() -> WelcomePayload {
    WelcomePayload {
        features: AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: false,
            video_recording: true,
        },
        recording: RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        },
        peers: Vec::new(),
    }
}

#[must_use]
pub fn decode_sent_batch(commands: &[Command]) -> EnvelopeBatch {
    let Some(Command::SendWebSocket(frame)) = commands
        .iter()
        .find(|command| matches!(command, Command::SendWebSocket(_)))
    else {
        return Vec::new();
    };
    serde_json::from_str(frame).unwrap_or_default()
}

#[must_use]
pub fn decode_sent_client_envelopes(commands: &[Command]) -> Vec<ClientEnvelope> {
    decode_sent_batch(commands)
        .into_iter()
        .filter_map(|envelope| ClientEnvelope::decode(envelope).ok())
        .collect()
}

#[must_use]
pub fn encode_server_batch(envelope: ServerEnvelope) -> String {
    let Ok(envelope) = envelope.into_envelope() else {
        return String::new();
    };
    serde_json::to_string(&vec![envelope]).unwrap_or_default()
}
