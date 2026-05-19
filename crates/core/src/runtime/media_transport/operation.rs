use super::{
    ConsumerActivity, ProducerActivity, SourcePacketGate, TransportConsumerRoute, TransportMediaId,
    TransportRelayRouteEffect, TransportSessionKey,
};

#[derive(Debug, Clone)]
pub(super) enum TransportControlOperation {
    RelayRouteEffect(TransportRelayRouteEffect),
    SetProducerActivity {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    },
    SetConsumerActivity {
        route: TransportConsumerRoute,
        activity: ConsumerActivity,
    },
    SetConsumerPacketGate {
        route: TransportConsumerRoute,
        packet_gate: SourcePacketGate,
    },
    RequestConsumerKeyframe {
        route: TransportConsumerRoute,
    },
}
