use crate::Router;

pub(super) fn assert_router_is_consistent(router: &Router) {
    assert_session_transport_index(router);
    assert_transport_producer_index(router);
    assert_transport_consumer_index(router);
    assert_producer_consumer_index(router);
}

fn assert_session_transport_index(router: &Router) {
    for (session_id, transport_ids) in &router.session_transports {
        assert!(!transport_ids.is_empty());
        assert!(router.sessions.contains_key(session_id));
        for transport_id in transport_ids {
            let transport = router.transports.get(transport_id);
            assert!(
                transport.is_some(),
                "missing transport {transport_id:?} for session {session_id:?}"
            );
            let Some(transport) = transport else {
                return;
            };
            assert_eq!(transport.session_id(), *session_id);
        }
    }

    for (transport_id, transport) in &router.transports {
        assert!(router.sessions.contains_key(&transport.session_id()));
        let session_transport_ids = router.session_transports.get(&transport.session_id());
        assert!(
            session_transport_ids.is_some(),
            "missing session transport index for session {:?}",
            transport.session_id()
        );
        let Some(session_transport_ids) = session_transport_ids else {
            return;
        };
        assert!(session_transport_ids.contains(transport_id));
    }
}

fn assert_transport_producer_index(router: &Router) {
    for (transport_id, producer_ids) in &router.transport_producers {
        assert!(!producer_ids.is_empty());
        assert!(router.transports.contains_key(transport_id));
        for producer_id in producer_ids {
            let producer = router.producers.get(producer_id);
            assert!(
                producer.is_some(),
                "missing producer {producer_id:?} for transport {transport_id:?}"
            );
            let Some(producer) = producer else {
                return;
            };
            assert_eq!(producer.transport_id(), *transport_id);
        }
    }

    for (producer_id, producer) in &router.producers {
        assert!(router.transports.contains_key(&producer.transport_id()));
        let transport_producer_ids = router.transport_producers.get(&producer.transport_id());
        assert!(
            transport_producer_ids.is_some(),
            "missing transport producer index for transport {:?}",
            producer.transport_id()
        );
        let Some(transport_producer_ids) = transport_producer_ids else {
            return;
        };
        assert!(transport_producer_ids.contains(producer_id));
    }
}

fn assert_transport_consumer_index(router: &Router) {
    for (transport_id, consumer_ids) in &router.transport_consumers {
        assert!(!consumer_ids.is_empty());
        assert!(router.transports.contains_key(transport_id));
        for consumer_id in consumer_ids {
            let consumer = router.consumers.get(consumer_id);
            assert!(
                consumer.is_some(),
                "missing consumer {consumer_id:?} for transport {transport_id:?}"
            );
            let Some(consumer) = consumer else {
                return;
            };
            assert_eq!(consumer.transport_id(), *transport_id);
        }
    }

    for (consumer_id, consumer) in &router.consumers {
        assert!(router.transports.contains_key(&consumer.transport_id()));
        let transport_consumer_ids = router.transport_consumers.get(&consumer.transport_id());
        assert!(
            transport_consumer_ids.is_some(),
            "missing transport consumer index for transport {:?}",
            consumer.transport_id()
        );
        let Some(transport_consumer_ids) = transport_consumer_ids else {
            return;
        };
        assert!(transport_consumer_ids.contains(consumer_id));
    }
}

fn assert_producer_consumer_index(router: &Router) {
    for (producer_id, consumer_ids) in &router.producer_consumers {
        assert!(!consumer_ids.is_empty());
        assert!(router.producers.contains_key(producer_id));
        for consumer_id in consumer_ids {
            let consumer = router.consumers.get(consumer_id);
            assert!(
                consumer.is_some(),
                "missing consumer {consumer_id:?} for producer {producer_id:?}"
            );
            let Some(consumer) = consumer else {
                return;
            };
            assert_eq!(consumer.producer_id(), *producer_id);
        }
    }

    for (consumer_id, consumer) in &router.consumers {
        assert!(router.producers.contains_key(&consumer.producer_id()));
        let producer_consumer_ids = router.producer_consumers.get(&consumer.producer_id());
        assert!(
            producer_consumer_ids.is_some(),
            "missing producer consumer index for producer {:?}",
            consumer.producer_id()
        );
        let Some(producer_consumer_ids) = producer_consumer_ids else {
            return;
        };
        assert!(producer_consumer_ids.contains(consumer_id));
        let producer = router.producers.get(&consumer.producer_id());
        assert!(
            producer.is_some(),
            "missing producer {:?} for consumer {:?}",
            consumer.producer_id(),
            consumer_id
        );
        let Some(producer) = producer else {
            return;
        };
        assert_eq!(consumer.producer_paused(), producer.paused());
    }
}
