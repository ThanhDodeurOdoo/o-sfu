use o_sfu_protocol::signaling::{RequestId, ServerMessage, ServerRequest, ServerResponse};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CallOutcome {
    signals: Vec<UserSignal>,
    end_user: Option<UserEndReason>,
}

impl CallOutcome {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signals: Vec::new(),
            end_user: None,
        }
    }

    #[must_use]
    pub fn with_signal(mut self, signal: UserSignal) -> Self {
        self.signals.push(signal);
        self
    }

    #[must_use]
    pub fn with_signals(mut self, signals: impl IntoIterator<Item = UserSignal>) -> Self {
        self.signals.extend(signals);
        self
    }

    pub fn extend(&mut self, other: Self) {
        self.signals.extend(other.signals);
        if other.end_user.is_some() {
            self.end_user = other.end_user;
        }
    }

    #[must_use]
    pub fn with_end_user(mut self, reason: UserEndReason) -> Self {
        self.end_user = Some(reason);
        self
    }

    #[must_use]
    pub fn signals(&self) -> &[UserSignal] {
        &self.signals
    }

    #[must_use]
    pub fn signal_count(&self) -> usize {
        self.signals.len()
    }

    #[must_use]
    pub const fn end_user(&self) -> Option<UserEndReason> {
        self.end_user
    }

    #[must_use]
    pub fn into_signals(self) -> Vec<UserSignal> {
        self.signals
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserSignal {
    Message(ServerMessage),
    Request {
        request_id: RequestId,
        request: ServerRequest,
    },
    Response {
        response_to: RequestId,
        response: ServerResponse,
    },
}

impl UserSignal {
    #[must_use]
    pub const fn request(request_id: RequestId, request: ServerRequest) -> Self {
        Self::Request {
            request_id,
            request,
        }
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserEndReason {
    Completed,
    Replaced,
    RemovedByRuntime,
    ProtocolViolation,
    TransportDisconnected,
    InternalError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserError {
    ProtocolViolation,
    Kicked,
    InternalError,
}

#[cfg(test)]
mod tests {
    use o_sfu_protocol::{
        shared::{AvailableFeatures, RecordingState},
        signaling::{RecordingActionResult, SessionDescriptionPayload, WelcomePayload},
    };

    use super::*;

    #[test]
    fn call_outcome_preserves_signal_order() {
        let first = ServerMessage::Welcome(WelcomePayload {
            peers: Vec::new(),
            features: AvailableFeatures {
                rtc: true,
                transcription: false,
                audio_recording: false,
                video_recording: false,
            },
            recording: RecordingState::default(),
        });
        let second = ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0"),
            upload_slots: Vec::new(),
        });

        let outcome = CallOutcome::new()
            .with_signal(first.clone().into())
            .with_signal(UserSignal::request(
                RequestId::new("server-1"),
                second.clone(),
            ));

        assert_eq!(
            outcome.signals(),
            &[
                UserSignal::Message(first),
                UserSignal::Request {
                    request_id: RequestId::new("server-1"),
                    request: second,
                },
            ]
        );
    }

    #[test]
    fn call_outcome_records_user_end_reason_without_dropping_signals() {
        let response = ServerResponse::StopRecording(RecordingActionResult { ok: true });

        let outcome = CallOutcome::new()
            .with_signal(UserSignal::response(
                RequestId::new("client-1"),
                response.clone(),
            ))
            .with_end_user(UserEndReason::Completed);

        assert_eq!(outcome.end_user(), Some(UserEndReason::Completed));
        assert_eq!(
            outcome.into_signals(),
            vec![UserSignal::Response {
                response_to: RequestId::new("client-1"),
                response,
            }]
        );
    }

    #[test]
    fn call_outcome_extends_ordered_signal_batches() {
        let first = ServerMessage::Welcome(WelcomePayload {
            peers: Vec::new(),
            features: AvailableFeatures {
                rtc: true,
                transcription: false,
                audio_recording: false,
                video_recording: false,
            },
            recording: RecordingState::default(),
        });
        let second = ServerResponse::StopRecording(RecordingActionResult { ok: true });

        let mut outcome = CallOutcome::new().with_signal(first.clone().into());
        outcome.extend(CallOutcome::new().with_signal(UserSignal::response(
            RequestId::new("client-1"),
            second.clone(),
        )));

        assert_eq!(
            outcome.into_signals(),
            vec![
                UserSignal::Message(first),
                UserSignal::Response {
                    response_to: RequestId::new("client-1"),
                    response: second,
                },
            ]
        );
    }
}
