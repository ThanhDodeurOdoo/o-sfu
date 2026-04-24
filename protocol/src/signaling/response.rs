use super::{
    Envelope, PeerInfoPayload, PeerLeftPayload, RecordingActionResult, RequestId,
    ServerBroadcastPayload, SessionDescriptionPayload, TrackBinding, WelcomePayload,
};
use crate::shared::RecordingStateUpdate;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResponse {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    Welcome(WelcomePayload),
    Tracks(Vec<TrackBinding>),
    PeerInfo(PeerInfoPayload),
    PeerJoined(PeerInfoPayload),
    PeerLeft(PeerLeftPayload),
    Broadcast(ServerBroadcastPayload),
    RecordingChange(RecordingStateUpdate),
}

impl ServerMessage {
    fn tag(&self) -> &'static str {
        match self {
            Self::Welcome(_) => "welcome",
            Self::Tracks(_) => "tracks",
            Self::PeerInfo(_) => "peerinfo",
            Self::PeerJoined(_) => "peerjoined",
            Self::PeerLeft(_) => "peerleft",
            Self::Broadcast(_) => "broadcast",
            Self::RecordingChange(_) => "recordingchange",
        }
    }

    /// Serialize a server push message into the protocol websocket envelope shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
        Ok(Envelope::message(
            self.tag(),
            Some(match self {
                Self::Welcome(payload) => serde_json::to_value(payload)?,
                Self::Tracks(payload) => serde_json::to_value(payload)?,
                Self::PeerInfo(payload) | Self::PeerJoined(payload) => {
                    serde_json::to_value(payload)?
                }
                Self::PeerLeft(payload) => serde_json::to_value(payload)?,
                Self::Broadcast(payload) => serde_json::to_value(payload)?,
                Self::RecordingChange(payload) => serde_json::to_value(payload)?,
            }),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerResponse {
    StartRecording(RecordingActionResult),
    StopRecording(RecordingActionResult),
}

impl ServerResponse {
    fn tag(&self) -> &'static str {
        match self {
            Self::StartRecording(_) => "startrecording",
            Self::StopRecording(_) => "stoprecording",
        }
    }

    /// Serialize a server response into the protocol websocket envelope shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self, response_to: RequestId) -> Result<Envelope, serde_json::Error> {
        Ok(Envelope::response(
            self.tag(),
            response_to,
            Some(match self {
                Self::StartRecording(payload) | Self::StopRecording(payload) => {
                    serde_json::to_value(payload)?
                }
            }),
        ))
    }
}
