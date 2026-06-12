use o_sfu_protocol::wire::{ClientEnvelope, ClientMessage, StreamIntentPayload, StreamType};
use o_sfu_router::{
    ConsumerCapability, ConsumerId, ConsumerSpec, MediaKind, ProducerId, ProducerSpec, Router,
    RouterId, Session, SessionId as RouterSessionId, TransportId,
};

fn user(id: RouterSessionId) -> Session {
    Session::new(id)
}

#[test]
fn router_session_teardown_keeps_remaining_routing_consistent() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join(user(RouterSessionId(10))), Ok(()));
    assert_eq!(router.join(user(RouterSessionId(20))), Ok(()));
    assert_eq!(
        router
            .session(RouterSessionId(10))
            .and_then(|session| session.open_receive_transport(TransportId(100)))
            .map(|_| ()),
        Ok(()),
    );
    assert_eq!(
        router
            .session(RouterSessionId(20))
            .and_then(|session| session.open_send_transport(TransportId(200)))
            .map(|_| ()),
        Ok(()),
    );
    assert_eq!(
        router
            .receive_transport(TransportId(100))
            .and_then(|transport| {
                transport.publish(ProducerSpec::new(ProducerId(300), MediaKind::Audio))
            }),
        Ok(ProducerId(300))
    );
    assert_eq!(
        router
            .send_transport(TransportId(200))
            .and_then(|transport| {
                transport.consume(ConsumerSpec::new(
                    ConsumerId(400),
                    ProducerId(300),
                    ConsumerCapability::Compatible,
                ))
            }),
        Ok(ConsumerId(400)),
    );

    assert_eq!(router.remove_session(RouterSessionId(10)), Ok(()));
    assert_eq!(router.session_count(), 1);
    assert_eq!(router.sessions().count(), 1);
    assert_eq!(
        router
            .session(RouterSessionId(20))
            .and_then(|session| session.open_send_transport(TransportId(200)))
            .map(|_| ()),
        Err(o_sfu_router::RouterError::DuplicateTransport(TransportId(
            200
        )))
    );
    assert_eq!(
        router.remove_producer(ProducerId(300)),
        Err(o_sfu_router::RouterError::MissingProducer(ProducerId(300)))
    );
    assert_eq!(
        router.remove_consumer(ConsumerId(400)),
        Err(o_sfu_router::RouterError::MissingConsumer(ConsumerId(400)))
    );
}

#[test]
fn signaling_codec_round_trip_preserves_subscribe_payload() {
    let encoded = ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
        stream_type: StreamType::Screen,
    }))
    .into_envelope();
    assert!(encoded.is_ok());

    let Ok(encoded) = encoded else {
        return;
    };
    let decoded = ClientEnvelope::decode(encoded);
    assert_eq!(
        decoded,
        Ok(ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Screen,
            },
        )))
    );
}
