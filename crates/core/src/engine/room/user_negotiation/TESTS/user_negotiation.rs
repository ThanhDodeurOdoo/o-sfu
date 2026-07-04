use super::{UserNegotiation, UserNegotiationUpdate};

#[test]
fn user_negotiation_only_reports_consumer_readiness_once() {
    let mut negotiation = UserNegotiation::default();

    assert_eq!(
        negotiation.mark_ready(),
        UserNegotiationUpdate::BecameConsumerReady
    );

    assert!(negotiation.can_publish());
    assert!(negotiation.can_consume());

    assert_eq!(negotiation.mark_ready(), UserNegotiationUpdate::Applied);
}
