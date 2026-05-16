use super::{
    Envelope, PeerInfoPayload, PeerLeftPayload, RecordingActionResult, RequestId,
    ServerBroadcastPayload, SessionDescriptionPayload, TrackBinding, WelcomePayload, tags,
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
            Self::Welcome(_) => tags::WELCOME,
            Self::Tracks(_) => tags::TRACKS,
            Self::PeerInfo(_) => tags::PEER_INFO,
            Self::PeerJoined(_) => tags::PEER_JOINED,
            Self::PeerLeft(_) => tags::PEER_LEFT,
            Self::Broadcast(_) => tags::BROADCAST,
            Self::RecordingChange(_) => tags::RECORDING_CHANGE,
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
            Self::StartRecording(_) => tags::START_RECORDING,
            Self::StopRecording(_) => tags::STOP_RECORDING,
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
