use o_sfu_model::RecordingOptions;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{
    AuthPayload, ClientBroadcastPayload, Envelope, PeerInfoPayload, PeerLeftPayload,
    RecordingActionResult, RequestId, ServerBroadcastPayload, SessionDescriptionPayload,
    StreamIntentPayload, SubscribePayload, TrackBinding, WelcomePayload,
};
use crate::shared::{RecordingStateUpdate, UserInfo};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireTag {
    Auth,
    Broadcast,
    Info,
    Offer,
    PeerInfo,
    PeerJoined,
    PeerLeft,
    Publish,
    RecordingChange,
    Renegotiate,
    StartRecording,
    StopRecording,
    Subscribe,
    Tracks,
    Unpublish,
    Welcome,
}

impl WireTag {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Broadcast => "broadcast",
            Self::Info => "info",
            Self::Offer => "offer",
            Self::PeerInfo => "peerinfo",
            Self::PeerJoined => "peerjoined",
            Self::PeerLeft => "peerleft",
            Self::Publish => "publish",
            Self::RecordingChange => "recordingchange",
            Self::Renegotiate => "renegotiate",
            Self::StartRecording => "startrecording",
            Self::StopRecording => "stoprecording",
            Self::Subscribe => "subscribe",
            Self::Tracks => "tracks",
            Self::Unpublish => "unpublish",
            Self::Welcome => "welcome",
        }
    }
}

type EntryDecode<T> = fn(WireTag, Option<Value>) -> Result<T, EnvelopeDecodeError>;

#[derive(Clone, Copy)]
struct EnvelopeEntry<T> {
    tag: WireTag,
    decode: EntryDecode<T>,
}

impl<T> EnvelopeEntry<T> {
    const fn new(tag: WireTag, decode: EntryDecode<T>) -> Self {
        Self { tag, decode }
    }

    fn decode(&self, payload: Option<Value>) -> Result<T, EnvelopeDecodeError> {
        (self.decode)(self.tag, payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvelopeDecodeError {
    UnknownTag(String),
    InvalidPayload(String),
    UnexpectedPayload(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Auth(AuthPayload),
    Publish(StreamIntentPayload),
    Unpublish(StreamIntentPayload),
    Subscribe(SubscribePayload),
    Info(UserInfo),
    Broadcast(ClientBroadcastPayload),
}

impl ClientMessage {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::new(WireTag::Auth, |tag, payload| {
            decode_payload(tag, payload, Self::Auth)
        }),
        EnvelopeEntry::new(WireTag::Publish, |tag, payload| {
            decode_payload(tag, payload, Self::Publish)
        }),
        EnvelopeEntry::new(WireTag::Unpublish, |tag, payload| {
            decode_payload(tag, payload, Self::Unpublish)
        }),
        EnvelopeEntry::new(WireTag::Subscribe, |tag, payload| {
            decode_payload(tag, payload, Self::Subscribe)
        }),
        EnvelopeEntry::new(WireTag::Info, |tag, payload| {
            decode_payload(tag, payload, Self::Info)
        }),
        EnvelopeEntry::new(WireTag::Broadcast, |tag, payload| {
            decode_payload(tag, payload, Self::Broadcast)
        }),
    ];

    pub(crate) fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Auth(payload) => encode_message(WireTag::Auth, payload),
            Self::Publish(payload) => encode_message(WireTag::Publish, payload),
            Self::Unpublish(payload) => encode_message(WireTag::Unpublish, payload),
            Self::Subscribe(payload) => encode_message(WireTag::Subscribe, payload),
            Self::Info(payload) => encode_message(WireTag::Info, payload),
            Self::Broadcast(payload) => encode_message(WireTag::Broadcast, payload),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        decode_entry(tag, payload, Self::ENTRIES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {
    StartRecording(RecordingOptions),
    StopRecording,
}

impl ClientRequest {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::new(WireTag::StartRecording, |tag, payload| {
            decode_payload(tag, payload, Self::StartRecording)
        }),
        EnvelopeEntry::new(WireTag::StopRecording, |tag, payload| {
            decode_empty(tag, payload.as_ref(), Self::StopRecording)
        }),
    ];

    pub(crate) fn into_envelope(
        self,
        request_id: RequestId,
    ) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::StartRecording(payload) => {
                encode_request(WireTag::StartRecording, request_id, payload)
            }
            Self::StopRecording => Ok(Envelope::request(
                WireTag::StopRecording.as_str(),
                request_id,
                None,
            )),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        decode_entry(tag, payload, Self::ENTRIES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRequest {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
}

impl ServerRequest {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::new(WireTag::Offer, |tag, payload| {
            decode_payload(tag, payload, Self::Offer)
        }),
        EnvelopeEntry::new(WireTag::Renegotiate, |tag, payload| {
            decode_payload(tag, payload, Self::Renegotiate)
        }),
    ];

    /// encode one server-authored request into the Odoo wire envelope
    ///
    /// # Errors
    ///
    /// returns an error when the typed payload cannot be serialized into the
    /// JSON envelope payload
    pub fn into_envelope(self, request_id: RequestId) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Offer(payload) => encode_request(WireTag::Offer, request_id, payload),
            Self::Renegotiate(payload) => encode_request(WireTag::Renegotiate, request_id, payload),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        decode_entry(tag, payload, Self::ENTRIES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResponse {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
}

impl ClientResponse {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::new(WireTag::Offer, |tag, payload| {
            decode_payload(tag, payload, Self::Offer)
        }),
        EnvelopeEntry::new(WireTag::Renegotiate, |tag, payload| {
            decode_payload(tag, payload, Self::Renegotiate)
        }),
    ];

    pub(crate) fn into_envelope(
        self,
        response_to: RequestId,
    ) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Offer(payload) => encode_response(WireTag::Offer, response_to, payload),
            Self::Renegotiate(payload) => {
                encode_response(WireTag::Renegotiate, response_to, payload)
            }
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        decode_entry(tag, payload, Self::ENTRIES)
    }
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
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::new(WireTag::Welcome, |tag, payload| {
            decode_payload(tag, payload, Self::Welcome)
        }),
        EnvelopeEntry::new(WireTag::Tracks, |tag, payload| {
            decode_payload(tag, payload, Self::Tracks)
        }),
        EnvelopeEntry::new(WireTag::PeerInfo, |tag, payload| {
            decode_payload(tag, payload, Self::PeerInfo)
        }),
        EnvelopeEntry::new(WireTag::PeerJoined, |tag, payload| {
            decode_payload(tag, payload, Self::PeerJoined)
        }),
        EnvelopeEntry::new(WireTag::PeerLeft, |tag, payload| {
            decode_payload(tag, payload, Self::PeerLeft)
        }),
        EnvelopeEntry::new(WireTag::Broadcast, |tag, payload| {
            decode_payload(tag, payload, Self::Broadcast)
        }),
        EnvelopeEntry::new(WireTag::RecordingChange, |tag, payload| {
            decode_payload(tag, payload, Self::RecordingChange)
        }),
    ];

    /// encode one server message into the Odoo wire envelope
    ///
    /// # Errors
    ///
    /// returns an error when the typed payload cannot be serialized into the
    /// JSON envelope payload
    pub fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Welcome(payload) => encode_message(WireTag::Welcome, payload),
            Self::Tracks(payload) => encode_message(WireTag::Tracks, payload),
            Self::PeerInfo(payload) => encode_message(WireTag::PeerInfo, payload),
            Self::PeerJoined(payload) => encode_message(WireTag::PeerJoined, payload),
            Self::PeerLeft(payload) => encode_message(WireTag::PeerLeft, payload),
            Self::Broadcast(payload) => encode_message(WireTag::Broadcast, payload),
            Self::RecordingChange(payload) => encode_message(WireTag::RecordingChange, payload),
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        decode_entry(tag, payload, Self::ENTRIES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerResponse {
    StartRecording(RecordingActionResult),
    StopRecording(RecordingActionResult),
}

impl ServerResponse {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::new(WireTag::StartRecording, |tag, payload| {
            decode_payload(tag, payload, Self::StartRecording)
        }),
        EnvelopeEntry::new(WireTag::StopRecording, |tag, payload| {
            decode_payload(tag, payload, Self::StopRecording)
        }),
    ];

    /// encode one server response into the Odoo wire envelope
    ///
    /// # Errors
    ///
    /// returns an error when the typed payload cannot be serialized into the
    /// JSON envelope payload
    pub fn into_envelope(self, response_to: RequestId) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::StartRecording(payload) => {
                encode_response(WireTag::StartRecording, response_to, payload)
            }
            Self::StopRecording(payload) => {
                encode_response(WireTag::StopRecording, response_to, payload)
            }
        }
    }

    pub(crate) fn decode(tag: &str, payload: Option<Value>) -> Result<Self, EnvelopeDecodeError> {
        decode_entry(tag, payload, Self::ENTRIES)
    }
}

fn encode_message<T: Serialize>(tag: WireTag, payload: T) -> Result<Envelope, serde_json::Error> {
    Ok(Envelope::message(
        tag.as_str(),
        Some(serde_json::to_value(payload)?),
    ))
}

fn encode_request<T: Serialize>(
    tag: WireTag,
    request_id: RequestId,
    payload: T,
) -> Result<Envelope, serde_json::Error> {
    Ok(Envelope::request(
        tag.as_str(),
        request_id,
        Some(serde_json::to_value(payload)?),
    ))
}

fn encode_response<T: Serialize>(
    tag: WireTag,
    response_to: RequestId,
    payload: T,
) -> Result<Envelope, serde_json::Error> {
    Ok(Envelope::response(
        tag.as_str(),
        response_to,
        Some(serde_json::to_value(payload)?),
    ))
}

fn decode_entry<T>(
    tag: &str,
    payload: Option<Value>,
    entries: &[EnvelopeEntry<T>],
) -> Result<T, EnvelopeDecodeError> {
    entries
        .iter()
        .find(|entry| entry.tag.as_str() == tag)
        .ok_or_else(|| unknown_tag(tag))?
        .decode(payload)
}

fn decode_payload<T, P>(
    tag: WireTag,
    payload: Option<Value>,
    build: fn(P) -> T,
) -> Result<T, EnvelopeDecodeError>
where
    P: DeserializeOwned,
{
    parse_payload(tag.as_str(), payload).map(build)
}

fn decode_empty<T>(
    tag: WireTag,
    payload: Option<&Value>,
    value: T,
) -> Result<T, EnvelopeDecodeError> {
    ensure_empty_payload(tag.as_str(), payload)?;
    Ok(value)
}

fn unknown_tag(tag: &str) -> EnvelopeDecodeError {
    EnvelopeDecodeError::UnknownTag(tag.to_owned())
}

fn parse_payload<T: DeserializeOwned>(
    tag: &str,
    payload: Option<Value>,
) -> Result<T, EnvelopeDecodeError> {
    serde_json::from_value(
        payload.ok_or_else(|| EnvelopeDecodeError::InvalidPayload(tag.to_owned()))?,
    )
    .map_err(|_error| EnvelopeDecodeError::InvalidPayload(tag.to_owned()))
}

fn ensure_empty_payload(tag: &str, payload: Option<&Value>) -> Result<(), EnvelopeDecodeError> {
    if payload.is_some() {
        return Err(EnvelopeDecodeError::UnexpectedPayload(tag.to_owned()));
    }
    Ok(())
}
