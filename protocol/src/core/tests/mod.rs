use serde_json::json;

pub(super) use super::{
    BATCH_FLUSH_TIMER_ID, Command, ConnectionState, NegotiationKind, PendingRequestKind,
    ProtocolCore, ProtocolEvent, RECOVERY_TIMER_ID, REQUEST_TIMEOUT_MS,
};
pub(super) use crate::{
    shared::{
        AvailableFeatures, DownloadStates, RecordingState, RecordingStateUpdate, StopCode,
        StreamType, UserInfo,
    },
    signaling::{
        AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest,
        ClientResponse, EnvelopeBatch, PeerInfoPayload, PeerLeftPayload, PeerSnapshot,
        RecordingActionResult, RecordingOptions, RequestId, ServerBroadcastPayload, ServerEnvelope,
        ServerMessage, ServerRequest, ServerResponse, SessionDescriptionPayload, SourceDescriptor,
        SourceEncodingDescriptor, StreamIntentPayload, SubscribePayload, TrackBinding,
        WebSocketCloseCode, WelcomePayload,
    },
};

mod batching;
mod command_batch;
mod connection;
mod negotiation;
mod recovery;
mod requests;
mod server_messages;

pub(super) fn sample_welcome_payload() -> WelcomePayload {
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
        peers: vec![PeerSnapshot {
            user_id: 7_i64.into(),
            info: UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            },
        }],
    }
}

pub(super) fn decode_sent_batch(commands: &[Command]) -> EnvelopeBatch {
    let Some(Command::SendWebSocket(frame)) = commands
        .iter()
        .find(|command| matches!(command, Command::SendWebSocket(_)))
    else {
        return Vec::new();
    };
    serde_json::from_str(frame).unwrap_or_default()
}

pub(super) fn decode_sent_client_envelopes(commands: &[Command]) -> Vec<ClientEnvelope> {
    decode_sent_batch(commands)
        .into_iter()
        .filter_map(|envelope| ClientEnvelope::decode(envelope).ok())
        .collect()
}

pub(super) fn encode_server_batch(envelope: ServerEnvelope) -> String {
    let Ok(envelope) = envelope.into_envelope() else {
        return String::new();
    };
    serde_json::to_string(&vec![envelope]).unwrap_or_default()
}

pub(super) fn empty_recording_json() -> serde_json::Value {
    json!({})
}
