//! ordered websocket output for one user-session transition
//!
//! this module is the handoff from application orchestration to websocket IO
//! `User` decides the order of client-visible work, while the socket writer
//! decides how adjacent messages are batched into protocol envelopes

use o_sfu_protocol::wire::{RequestId, ServerMessage, ServerRequest, ServerResponse};

/// ordered signaling produced by one user-session transition
///
/// the application layer returns this value only after it has finished applying
/// a client or room event
/// plain messages may be batched together, while requests and responses stay
/// explicit so routing metadata remains attached to the right protocol envelope
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserOutput {
    signals: Vec<UserSignal>,
}

impl UserOutput {
    /// build an empty output for an accepted transition with no client-visible
    /// websocket work
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signals: Vec::new(),
        }
    }

    /// append one signal while preserving the application ordering decision
    #[must_use]
    pub fn with_signal(mut self, signal: UserSignal) -> Self {
        self.signals.push(signal);
        self
    }

    /// merge another transition output after this one
    ///
    /// use this when one high-level action first emits compatibility messages
    /// then discovers that negotiation work must follow
    pub fn extend(&mut self, other: Self) {
        self.signals.extend(other.signals);
    }

    /// consume the ordered signals for websocket serialization
    #[must_use]
    pub fn into_signals(self) -> Vec<UserSignal> {
        self.signals
    }

    /// create an output from a sequence of compatibility messages
    pub fn from_messages(messages: impl IntoIterator<Item = ServerMessage>) -> Self {
        messages.into_iter().map(UserSignal::from).collect()
    }
}

impl FromIterator<UserSignal> for UserOutput {
    fn from_iter<T: IntoIterator<Item = UserSignal>>(iter: T) -> Self {
        Self {
            signals: iter.into_iter().collect(),
        }
    }
}

/// one envelope-level signal that the websocket edge can serialize
///
/// each variant represents a different routing contract for the sender
/// plain messages may share one batch, server-authored requests must carry
/// their request id and client-request responses must carry the id they resolve
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserSignal {
    /// fire-and-forget server message
    ///
    /// adjacent message signals may share one websocket batch
    Message(ServerMessage),
    /// server-authored request that expects a later client response
    Request {
        /// server-local request id used to correlate the client answer
        request_id: RequestId,
        /// typed request payload sent to the browser
        request: ServerRequest,
    },
    /// response to a request previously authored by the client
    Response {
        /// client request id that this response resolves
        response_to: RequestId,
        /// typed response payload sent back to the browser
        response: ServerResponse,
    },
}

impl UserSignal {
    /// create a server-authored request signal
    #[must_use]
    pub const fn request(request_id: RequestId, request: ServerRequest) -> Self {
        Self::Request {
            request_id,
            request,
        }
    }

    /// create a response signal for a client-authored request
    #[must_use]
    pub const fn response(response_to: RequestId, response: ServerResponse) -> Self {
        Self::Response {
            response_to,
            response,
        }
    }
}

impl From<ServerMessage> for UserSignal {
    fn from(message: ServerMessage) -> Self {
        Self::Message(message)
    }
}
