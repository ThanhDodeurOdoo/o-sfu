use o_sfu_telemetry::schema::event as telemetry_event;

use super::{
    RoomGaugeDelta,
    transport::{
        RoomTransportOutcome, execute_relay_route_effects, execute_setup_activity_correction,
    },
};
use crate::engine::{
    diagnostics::DiagnosticsEventData,
    media_transport::{MediaTransport, TransportConsumerRoute},
    room::{
        Room, UserOutbound,
        cleanup::TransportCleanupOperation,
        media_graph::{
            ConsumerSetupOrigin, ConsumerSetupOutcome, ConsumerSetupTarget, PendingConsumerSetup,
        },
    },
};

pub(super) async fn execute_receiver_route_setup(
    setup: PendingConsumerSetup,
    origin: ConsumerSetupOrigin,
    room: &Room,
    media_transport: &MediaTransport,
    outcome: &mut RoomTransportOutcome,
) {
    if !execute_relay_route_effects(room, media_transport, setup.relays()).await {
        release_pending_setup(room, setup, media_transport, outcome).await;
        return;
    }
    let setup = match setup.declare(media_transport, origin).await {
        Ok(setup) => setup,
        Err(setup) => {
            release_pending_setup(room, setup, media_transport, outcome).await;
            return;
        }
    };
    let (before, after, setup_outcome) = {
        let mut state = room.state.write().await;
        state.commit_declared_consumer_setup(setup)
    };
    outcome.gauges.push(RoomGaugeDelta::media(before, after));
    match setup_outcome {
        ConsumerSetupOutcome::Committed {
            target,
            route,
            sender,
            snapshot,
            transport_activity_update,
        } => {
            outcome
                .diagnostics
                .push(setup_diagnostics(room.uuid(), &target, origin, &route));
            if let Some(active) = transport_activity_update {
                execute_setup_activity_correction(route, target.kind, active, media_transport)
                    .await;
            }
            let _ = sender.send(UserOutbound::RemoteSources(snapshot));
        }
        ConsumerSetupOutcome::Released(route, relays) => {
            execute_relay_route_effects(room, media_transport, &relays).await;
            let cleanup = [TransportCleanupOperation::RemoveMedia {
                session_key: route.consumer_session_key().clone(),
                transport_media_id: route.consumer_transport_media_id(),
            }];
            room.execute_transport_cleanup_operations(media_transport, &cleanup)
                .await;
            outcome.source_policy.fanout_pressure_changed();
        }
    }
}

fn setup_diagnostics(
    room_id: &str,
    target: &ConsumerSetupTarget,
    origin: ConsumerSetupOrigin,
    route: &TransportConsumerRoute,
) -> DiagnosticsEventData {
    DiagnosticsEventData::for_user(room_id, &target.user, telemetry_event::SUBSCRIBE_SUCCEEDED)
        .with_connection_id(target.connection.as_u64())
        .with_media_worker_id(route.consumer_session_key().media_worker_id().as_usize())
        .with_transport_media_id(route.consumer_transport_media_id().as_u64())
        .insert_field(
            "producer_user_id",
            serde_json::to_value(&target.producer_user).unwrap_or(serde_json::Value::Null),
        )
        .insert_field("source_transport_media_id", target.media.as_u64())
        .insert_field("stream_id", target.stream.to_string())
        .insert_field("origin", origin.as_diagnostic_str())
}

async fn release_pending_setup(
    room: &Room,
    setup: PendingConsumerSetup,
    media_transport: &MediaTransport,
    outcome: &mut RoomTransportOutcome,
) {
    let (before, after, relays) = {
        let mut state = room.state.write().await;
        state.release_pending_consumer_setup(setup)
    };
    execute_relay_route_effects(room, media_transport, &relays).await;
    outcome.gauges.push(RoomGaugeDelta::media(before, after));
    outcome.source_policy.fanout_pressure_changed();
}
