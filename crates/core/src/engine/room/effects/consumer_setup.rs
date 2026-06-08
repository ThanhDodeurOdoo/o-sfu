use std::slice;

use o_sfu_router::{MediaKind, MediaStream as RouterRtpParameters};
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::{
    batch::RoomGaugeDelta, consumer_route::ConsumerRouteEffect, policy::RoomPolicyPlan,
    transport::execute_relay_route_effects,
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
    source_model::UserStreamId,
};

#[derive(Debug)]
pub(super) struct ConsumerSetupEffect {
    setup: PendingConsumerSetup,
    origin: ConsumerSetupOrigin,
}

impl ConsumerSetupEffect {
    pub(super) const fn new(setup: PendingConsumerSetup, origin: ConsumerSetupOrigin) -> Self {
        Self { setup, origin }
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        media_transport: &MediaTransport,
    ) -> ConsumerSetupEffectOutcome {
        let relays = &self.setup.relays;
        if !execute_relay_route_effects(room, media_transport, relays).await {
            return release_failed_setup(room, self.setup, media_transport).await;
        }
        let target = self.setup.target.clone();
        let activity =
            ConsumerActivity::from_active(self.setup.reservation.selection().delivery_active());
        let Some((route, mid)) = declare_consumer(
            &target,
            &self.setup.track.rtp,
            activity,
            self.origin,
            media_transport,
        )
        .await
        else {
            return release_failed_setup(room, self.setup, media_transport).await;
        };
        let (before, after, outcome) = {
            let mut state = room.state.write().await;
            let commit = state.commit_pending_consumer_setup(
                self.setup,
                route.consumer_transport_media_id(),
                mid,
            );
            drop(state);
            commit
        };
        finish_setup(
            room,
            media_transport,
            target,
            self.origin,
            route,
            RoomGaugeDelta::media(before, after),
            outcome,
        )
        .await
    }
}

#[derive(Debug)]
pub(super) struct ConsumerSetupEffectOutcome {
    pub(super) gauge: RoomGaugeDelta,
    pub(super) diagnostics: Option<DiagnosticsEventData>,
    pub(super) policy: RoomPolicyPlan,
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
        Ok(consumer_media) => {
            let mid = media_transport
                .transport_media_mid(&target.user_session, consumer_media)
                .await;
            Some((target.transport_consumer_route(consumer_media), mid))
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

async fn finish_setup(
    room: &Room,
    media_transport: &MediaTransport,
    target: ConsumerSetupTarget,
    origin: ConsumerSetupOrigin,
    route: TransportConsumerRoute,
    gauge: RoomGaugeDelta,
    outcome: ConsumerSetupOutcome,
) -> ConsumerSetupEffectOutcome {
    match outcome {
        ConsumerSetupOutcome::Committed {
            sender,
            track,
            transport_activity_update,
        } => {
            if let Some(active) = transport_activity_update {
                sync_activity(media_transport, &route, &target.stream, target.kind, active).await;
            }
            let diagnostics = setup_diagnostics(room.uuid(), &target, origin, &route);
            let _ = sender.send(UserOutbound::SetupRemoteTrack(Box::new(track)));
            ConsumerSetupEffectOutcome {
                gauge,
                diagnostics: Some(diagnostics),
                policy: RoomPolicyPlan::default(),
            }
        }
        ConsumerSetupOutcome::Released(relays) => {
            execute_relay_route_effects(room, media_transport, &relays).await;
            let cleanup = TransportCleanupOperation::RemoveMedia {
                session_key: route.consumer_session_key().clone(),
                connection_id: target.connection,
                transport_media_id: route.consumer_transport_media_id(),
            };
            room.execute_transport_cleanup_operations(media_transport, slice::from_ref(&cleanup))
                .await;
            ConsumerSetupEffectOutcome {
                gauge,
                diagnostics: None,
                policy: fanout_pressure_plan(),
            }
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

async fn sync_activity(
    media_transport: &MediaTransport,
    route: &TransportConsumerRoute,
    stream: &UserStreamId,
    kind: MediaKind,
    active: bool,
) {
    let outcome = ConsumerRouteEffect::new(route)
        .with_activity(active)
        .with_keyframe(active && kind == MediaKind::Video)
        .execute(media_transport)
        .await;
    if outcome.activity_failed {
        warn!(
            ?route,
            stream_id = %stream,
            active,
            "media transport failed to correct in-flight consumer setup activity"
        );
        return;
    }
    if outcome.keyframe_failed {
        warn!(
            ?route,
            stream_id = %stream,
            "media transport failed to request keyframe after consumer setup activity correction"
        );
    }
}

async fn release_failed_setup(
    room: &Room,
    setup: PendingConsumerSetup,
    media_transport: &MediaTransport,
) -> ConsumerSetupEffectOutcome {
    let (before, after, relays) = {
        let mut state = room.state.write().await;
        let (before, after, relays) = state.release_pending_consumer_setup(setup);
        drop(state);
        (before, after, relays)
    };
    execute_relay_route_effects(room, media_transport, &relays).await;
    ConsumerSetupEffectOutcome {
        gauge: RoomGaugeDelta::media(before, after),
        diagnostics: None,
        policy: fanout_pressure_plan(),
    }
}

fn fanout_pressure_plan() -> RoomPolicyPlan {
    let mut policy = RoomPolicyPlan::default();
    policy.fanout_pressure_changed();
    policy
}
