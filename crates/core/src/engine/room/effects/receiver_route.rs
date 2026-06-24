use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::{
    batch::RoomGaugeDelta,
    transport::{RoomRouteBatch, RoomTransportOutcome, execute_relay_route_effects},
};
use crate::engine::{
    diagnostics::DiagnosticsEventData,
    media_transport::{ConsumerActivity, MediaTransport, TransportConsumerRoute},
    room::{
        Room, UserOutbound,
        cleanup::TransportCleanupOperation,
        media_graph::{
            ConsumerSetupOrigin, ConsumerSetupOutcome, ConsumerSetupTarget, PendingConsumerSetup,
        },
    },
};

#[derive(Debug)]
pub(super) struct ReceiverRouteSetup {
    setup: PendingConsumerSetup,
    origin: ConsumerSetupOrigin,
}

impl ReceiverRouteSetup {
    pub(super) fn new(setup: PendingConsumerSetup, origin: ConsumerSetupOrigin) -> Self {
        Self { setup, origin }
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        media_transport: &MediaTransport,
        outcome: &mut RoomTransportOutcome,
    ) {
        if !execute_relay_route_effects(room, media_transport, &self.setup.relays).await {
            release_pending_setup(room, self.setup, media_transport, outcome).await;
            return;
        }
        let target = self.setup.target.clone();
        let activity =
            ConsumerActivity::from_active(self.setup.reservation.selection().delivery_active());
        let Some((route, transport_mid)) = declare_consumer(
            &target,
            &self.setup.track.rtp,
            activity,
            self.origin,
            media_transport,
        )
        .await
        else {
            release_pending_setup(room, self.setup, media_transport, outcome).await;
            return;
        };
        let (before, after, setup_outcome) = {
            let mut state = room.state.write().await;
            let commit = state.commit_pending_consumer_setup(
                self.setup,
                route.consumer_transport_media_id(),
                transport_mid,
            );
            drop(state);
            commit
        };
        outcome.gauges.push(RoomGaugeDelta::media(before, after));
        match setup_outcome {
            ConsumerSetupOutcome::Committed {
                sender,
                track,
                transport_activity_update,
            } => {
                outcome.diagnostics.push(setup_diagnostics(
                    room.uuid(),
                    &target,
                    self.origin,
                    &route,
                ));
                if let Some(active) = transport_activity_update {
                    let mut routes = RoomRouteBatch::default();
                    routes.push_setup_activity(route, target.kind, active);
                    routes.execute(media_transport).await;
                }
                let _ = sender.send(UserOutbound::SetupRemoteTrack(Box::new(track)));
            }
            ConsumerSetupOutcome::Released(relays) => {
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
}

async fn declare_consumer(
    target: &ConsumerSetupTarget,
    rtp: &RouterRtpParameters,
    activity: ConsumerActivity,
    origin: ConsumerSetupOrigin,
    media_transport: &MediaTransport,
) -> Option<(TransportConsumerRoute, Option<String>)> {
    match media_transport
        .consume_media(
            &target.user_session,
            target.kind,
            &target.producer_session,
            target.media,
            rtp,
            activity,
        )
        .await
    {
        Ok(consumer_media_id) => {
            let transport_mid = media_transport
                .transport_media_mid(&target.user_session, consumer_media_id)
                .await;
            Some((
                target.transport_consumer_route(consumer_media_id),
                transport_mid,
            ))
        }
        Err(error) => {
            warn!(
                consumer_user_id = ?target.user,
                consumer_connection_id = ?target.connection,
                producer_user_id = ?target.producer_user,
                producer_connection_id = ?target.producer_connection,
                source_transport_media_id = ?target.media,
                error = ?error,
                consumer_mid = rtp.mid(),
                ?origin,
                "media transport rejected consume media declaration"
            );
            None
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
        let (before, after, relays) = state.release_pending_consumer_setup(setup);
        drop(state);
        (before, after, relays)
    };
    execute_relay_route_effects(room, media_transport, &relays).await;
    outcome.gauges.push(RoomGaugeDelta::media(before, after));
    outcome.source_policy.fanout_pressure_changed();
}
