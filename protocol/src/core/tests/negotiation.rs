use super::*;

#[test]
fn protocol_core_emits_negotiation_command_and_accepts_matching_answer() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let offer_frame = encode_server_batch(ServerEnvelope::Request {
        request_id: RequestId::new("offer-1"),
        request: ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\ns=offer\r\n"),
            upload_slots: Vec::new(),
        }),
    });
    let offer_commands = core.on_ws_message(&offer_frame);

    assert_eq!(
        offer_commands,
        vec![
            Command::CreatePeerConnection,
            Command::ApplyNegotiation {
                request_id: RequestId::new("offer-1"),
                kind: NegotiationKind::Offer,
                sdp: String::from("v=0\r\ns=offer\r\n"),
                upload_slots: Vec::new(),
            },
        ]
    );

    let answer_commands = core.submit_negotiation_answer(
        &RequestId::new("offer-1"),
        NegotiationKind::Offer,
        "v=0\r\ns=answer\r\n",
    );
    let mut batch = decode_sent_batch(&answer_commands).into_iter();
    let Some(envelope) = batch.next() else {
        return;
    };

    assert_eq!(
        ClientEnvelope::decode(envelope),
        Ok(ClientEnvelope::Response {
            response_to: RequestId::new("offer-1"),
            response: ClientResponse::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0\r\ns=answer\r\n"),
                upload_slots: Vec::new(),
            }),
        })
    );
}

#[test]
fn protocol_core_rejects_overlapping_negotiation_requests() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());

    let first_offer = encode_server_batch(ServerEnvelope::Request {
        request_id: RequestId::new("offer-1"),
        request: ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\ns=offer-1\r\n"),
            upload_slots: Vec::new(),
        }),
    });
    let second_offer = encode_server_batch(ServerEnvelope::Request {
        request_id: RequestId::new("offer-2"),
        request: ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\ns=offer-2\r\n"),
            upload_slots: Vec::new(),
        }),
    });

    let _ = core.on_ws_message(&first_offer);
    let commands = core.on_ws_message(&second_offer);

    assert_eq!(
        commands,
        vec![Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
}

#[test]
fn protocol_core_rejects_initial_offer_after_transport_ready() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.on_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();

    let late_offer = encode_server_batch(ServerEnvelope::Request {
        request_id: RequestId::new("offer-1"),
        request: ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\ns=late-offer\r\n"),
            upload_slots: Vec::new(),
        }),
    });
    let commands = core.on_ws_message(&late_offer);

    assert_eq!(
        commands,
        vec![Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
}
