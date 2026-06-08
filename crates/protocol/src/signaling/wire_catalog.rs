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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvelopeKind {
    Message,
    Request,
    Response,
}

#[derive(Clone, Copy)]
pub(crate) struct EnvelopeSpec {
    kind: EnvelopeKind,
    tag: WireTag,
}

impl EnvelopeSpec {
    pub(crate) const fn kind(self) -> EnvelopeKind {
        self.kind
    }

    pub(crate) const fn tag(self) -> &'static str {
        self.tag.as_str()
    }
}

type EntryDecode<T> = fn(WireTag, Option<Value>) -> Result<T, EnvelopeDecodeError>;

#[derive(Clone, Copy)]
struct EnvelopeEntry<T> {
    tag: WireTag,
    decode: EntryDecode<T>,
    kind: EnvelopeKind,
}

impl<T> EnvelopeEntry<T> {
    const fn message(tag: WireTag, decode: EntryDecode<T>) -> Self {
        Self::new(EnvelopeKind::Message, tag, decode)
    }

    const fn request(tag: WireTag, decode: EntryDecode<T>) -> Self {
        Self::new(EnvelopeKind::Request, tag, decode)
    }

    const fn empty_request(tag: WireTag, decode: EntryDecode<T>) -> Self {
        Self::request(tag, decode)
    }

    const fn response(tag: WireTag, decode: EntryDecode<T>) -> Self {
        Self::new(EnvelopeKind::Response, tag, decode)
    }

    const fn new(kind: EnvelopeKind, tag: WireTag, decode: EntryDecode<T>) -> Self {
        Self { tag, decode, kind }
    }

    fn decode(&self, payload: Option<Value>) -> Result<T, EnvelopeDecodeError> {
        (self.decode)(self.tag, payload)
    }

    const fn spec(&self) -> EnvelopeSpec {
        EnvelopeSpec {
            kind: self.kind,
            tag: self.tag,
        }
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
        EnvelopeEntry::message(WireTag::Auth, |tag, payload| {
            decode_payload(tag, payload, Self::Auth)
        }),
        EnvelopeEntry::message(WireTag::Publish, |tag, payload| {
            decode_payload(tag, payload, Self::Publish)
        }),
        EnvelopeEntry::message(WireTag::Unpublish, |tag, payload| {
            decode_payload(tag, payload, Self::Unpublish)
        }),
        EnvelopeEntry::message(WireTag::Subscribe, |tag, payload| {
            decode_payload(tag, payload, Self::Subscribe)
        }),
        EnvelopeEntry::message(WireTag::Info, |tag, payload| {
            decode_payload(tag, payload, Self::Info)
        }),
        EnvelopeEntry::message(WireTag::Broadcast, |tag, payload| {
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
    pub(crate) fn specs() -> impl Iterator<Item = EnvelopeSpec> {
        entry_specs(Self::ENTRIES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {
    StartRecording(RecordingOptions),
    StopRecording,
}

impl ClientRequest {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::request(WireTag::StartRecording, |tag, payload| {
            decode_payload(tag, payload, Self::StartRecording)
        }),
        EnvelopeEntry::empty_request(WireTag::StopRecording, |tag, payload| {
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
    pub(crate) fn specs() -> impl Iterator<Item = EnvelopeSpec> {
        entry_specs(Self::ENTRIES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRequest {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
}

impl ServerRequest {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::request(WireTag::Offer, |tag, payload| {
            decode_payload(tag, payload, Self::Offer)
        }),
        EnvelopeEntry::request(WireTag::Renegotiate, |tag, payload| {
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
    pub(crate) fn specs() -> impl Iterator<Item = EnvelopeSpec> {
        entry_specs(Self::ENTRIES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResponse {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
}

impl ClientResponse {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::response(WireTag::Offer, |tag, payload| {
            decode_payload(tag, payload, Self::Offer)
        }),
        EnvelopeEntry::response(WireTag::Renegotiate, |tag, payload| {
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
    pub(crate) fn specs() -> impl Iterator<Item = EnvelopeSpec> {
        entry_specs(Self::ENTRIES)
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
        EnvelopeEntry::message(WireTag::Welcome, |tag, payload| {
            decode_payload(tag, payload, Self::Welcome)
        }),
        EnvelopeEntry::message(WireTag::Tracks, |tag, payload| {
            decode_payload(tag, payload, Self::Tracks)
        }),
        EnvelopeEntry::message(WireTag::PeerInfo, |tag, payload| {
            decode_payload(tag, payload, Self::PeerInfo)
        }),
        EnvelopeEntry::message(WireTag::PeerJoined, |tag, payload| {
            decode_payload(tag, payload, Self::PeerJoined)
        }),
        EnvelopeEntry::message(WireTag::PeerLeft, |tag, payload| {
            decode_payload(tag, payload, Self::PeerLeft)
        }),
        EnvelopeEntry::message(WireTag::Broadcast, |tag, payload| {
            decode_payload(tag, payload, Self::Broadcast)
        }),
        EnvelopeEntry::message(WireTag::RecordingChange, |tag, payload| {
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
    pub(crate) fn specs() -> impl Iterator<Item = EnvelopeSpec> {
        entry_specs(Self::ENTRIES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerResponse {
    StartRecording(RecordingActionResult),
    StopRecording(RecordingActionResult),
}

impl ServerResponse {
    const ENTRIES: &'static [EnvelopeEntry<Self>] = &[
        EnvelopeEntry::response(WireTag::StartRecording, |tag, payload| {
            decode_payload(tag, payload, Self::StartRecording)
        }),
        EnvelopeEntry::response(WireTag::StopRecording, |tag, payload| {
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
    pub(crate) fn specs() -> impl Iterator<Item = EnvelopeSpec> {
        entry_specs(Self::ENTRIES)
    }
}

fn entry_specs<T: 'static>(
    entries: &'static [EnvelopeEntry<T>],
) -> impl Iterator<Item = EnvelopeSpec> {
    entries.iter().map(EnvelopeEntry::spec)
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
    entry_for_tag(entries, tag)
        .ok_or_else(|| unknown_tag(tag))?
        .decode(payload)
}

fn entry_for_tag<'a, T>(
    entries: &'a [EnvelopeEntry<T>],
    tag: &str,
) -> Option<&'a EnvelopeEntry<T>> {
    entries.iter().find(|entry| entry.tag.as_str() == tag)
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

#[cfg(test)]
#[path = "TESTS/wire_catalog.rs"]
mod tests;
