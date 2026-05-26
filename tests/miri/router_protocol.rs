use o_sfu_protocol::wire::{ClientEnvelope, ClientMessage, StreamIntentPayload, StreamType};
use o_sfu_router::{
    Consumer, ConsumerCapability, ConsumerId, MediaKind, Producer, ProducerId, Router, RouterId,
    Session, SessionId as RouterSessionId, Transport, TransportDirection, TransportId,
};

fn user(id: RouterSessionId) -> Session {
    Session::new(id)
}

#[test]
fn router_session_teardown_keeps_remaining_routing_consistent() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(user(RouterSessionId(10))), Ok(()));
    assert_eq!(router.join_session(user(RouterSessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            RouterSessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            RouterSessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.remove_session(RouterSessionId(10)), Ok(()));
    assert_eq!(router.session_count(), 1);
    assert_eq!(router.sessions().count(), 1);
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            RouterSessionId(20),
            TransportDirection::Send,
        )),
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
