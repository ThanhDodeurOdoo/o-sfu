use serde_json::Value;

use crate::signaling::{
    current_bus::{CurrentBusEnvelope, CurrentBusRequestId},
    current_protocol::{CurrentClientMessage, CurrentClientRequest},
};

#[derive(Debug)]
pub(super) enum DomainCommand {
    Response {
        response_to: CurrentBusRequestId,
        payload: Value,
    },
    Request {
        request_id: CurrentBusRequestId,
        request: CurrentClientRequest,
    },
    InvalidRequest {
        request_id: CurrentBusRequestId,
    },
    Message(CurrentClientMessage),
    InvalidMessage,
}

pub(super) fn decode_envelope(envelope: CurrentBusEnvelope) -> DomainCommand {
    let CurrentBusEnvelope {
        message,
        need_response,
        response_to,
    } = envelope;
    match (response_to, need_response) {
        (Some(response_to), _) => DomainCommand::Response {
            response_to,
            payload: message,
        },
        (None, Some(request_id)) => match serde_json::from_value::<CurrentClientRequest>(message) {
            Ok(request) => DomainCommand::Request {
                request_id,
                request,
            },
            Err(_error) => DomainCommand::InvalidRequest { request_id },
        },
        (None, None) => match serde_json::from_value::<CurrentClientMessage>(message) {
            Ok(message) => DomainCommand::Message(message),
            Err(_error) => DomainCommand::InvalidMessage,
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{DomainCommand, decode_envelope};
    use crate::signaling::{
        current_bus::{CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
        current_protocol::CurrentClientRequest,
        webrtc::DtlsParameters,
    };

    #[test]
    fn request_envelope_decodes_to_typed_domain_command() {
        let request_id = CurrentBusRequestId::new(CurrentBusOrigin::Client, 1, 2);
        let envelope = CurrentBusEnvelope {
            message: json!({
                "name": "CONNECT_CTS_TRANSPORT",
                "payload": {
                    "dtlsParameters": DtlsParameters {
                        role: String::from("client"),
                        fingerprints: vec![],
                    },
                },
            }),
            need_response: Some(request_id.clone()),
            response_to: None,
        };

        let command = decode_envelope(envelope);

        assert!(matches!(
            command,
            DomainCommand::Request {
                request_id: actual_request_id,
                request: CurrentClientRequest::ConnectUploadTransport(_),
            } if actual_request_id == request_id
        ));
    }

    #[test]
    fn invalid_request_envelope_keeps_request_identity() {
        let request_id = CurrentBusRequestId::new(CurrentBusOrigin::Client, 4, 8);
        let envelope = CurrentBusEnvelope {
            message: json!({
                "name": "CONNECT_CTS_TRANSPORT",
                "payload": "wrong-shape",
            }),
            need_response: Some(request_id.clone()),
            response_to: None,
        };

        let command = decode_envelope(envelope);

        assert!(matches!(
            command,
            DomainCommand::InvalidRequest {
                request_id: actual_request_id,
            } if actual_request_id == request_id
        ));
    }

    #[test]
    fn invalid_message_envelope_becomes_invalid_message_command() {
        let envelope = CurrentBusEnvelope {
            message: json!({
                "name": "UPDATE_INFO",
                "payload": false,
            }),
            need_response: None,
            response_to: None,
        };

        assert!(matches!(
            decode_envelope(envelope),
            DomainCommand::InvalidMessage
        ));
    }
}
