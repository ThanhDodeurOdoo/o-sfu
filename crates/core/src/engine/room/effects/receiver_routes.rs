use std::slice;

use o_sfu_router::MediaStream as RouterRtpParameters;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::{RoomRouteEffects, batch::RoomGaugeDelta, transport::execute_relay_route_effects};
use crate::engine::{
    diagnostics::DiagnosticsEventData,
    media_transport::{ConsumerActivity, MediaTransport, TransportConsumerRoute},
    room::{
        Room, UserOutbound,
        cleanup::TransportCleanupOperation,
        media_graph::{
            ConsumerRouteTarget, ConsumerSetupOrigin, ConsumerSetupOutcome, ConsumerSetupTarget,
            PendingConsumerSetup, ReceiverRouteActivity, ReceiverRouteWork,
            ResolvedRelayRouteEffect,
        },
        source_policy::SourcePolicyWakeups,
    },
};

#[derive(Debug, Default)]
pub(super) struct ReceiverRoutePlan {
    relays: Vec<ResolvedRelayRouteEffect>,
    routes: RoomRouteEffects,
    setups: Vec<ReceiverRouteSetup>,
}

impl ReceiverRoutePlan {
    pub(super) fn push_work(
        &mut self,
        work: ReceiverRouteWork,
        origin: ConsumerSetupOrigin,
        mut diagnostics: impl FnMut(&ReceiverRouteActivity) -> DiagnosticsEventData,
    ) {
        let (activities, setups, relays) = work.into_parts();
        self.relays.extend(relays);
        for activity in activities {
            let event = diagnostics(&activity);
            self.routes.push_activity(activity, event);
        }
        self.push_setups(setups, origin);
    }

    pub(super) fn push_keyframes(&mut self, targets: Vec<ConsumerRouteTarget>) {
        for target in targets {
            self.routes.push_keyframe(target);
        }
    }

    pub(super) fn push_setups(
        &mut self,
        setups: Vec<PendingConsumerSetup>,
        origin: ConsumerSetupOrigin,
    ) {
        self.setups.extend(
            setups
                .into_iter()
                .map(|setup| ReceiverRouteSetup { setup, origin }),
        );
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
    ) -> ReceiverRouteOutcome {
        let Some(media_transport) = media_transport else {
            return ReceiverRouteOutcome::default();
        };
        let mut outcome = ReceiverRouteOutcome::default();
        execute_relay_route_effects(room, media_transport, &self.relays).await;
        outcome
            .diagnostics
            .extend(self.routes.execute(media_transport).await.diagnostics);
        for setup in self.setups {
            let setup = setup.execute(room, media_transport).await;
            outcome.gauges.push(setup.gauge);
            if let Some(diagnostics) = setup.diagnostics {
                outcome.diagnostics.push(diagnostics);
            }
            outcome.source_policy.extend(setup.source_policy);
        }
        outcome
    }
}

#[derive(Debug, Default)]
pub(super) struct ReceiverRouteOutcome {
    pub(super) gauges: Vec<RoomGaugeDelta>,
    pub(super) diagnostics: Vec<DiagnosticsEventData>,
    pub(super) source_policy: SourcePolicyWakeups,
}

#[derive(Debug)]
struct ReceiverRouteSetup {
    setup: PendingConsumerSetup,
    origin: ConsumerSetupOrigin,
}

impl ReceiverRouteSetup {
    async fn execute(
        self,
        room: &Room,
        media_transport: &MediaTransport,
    ) -> ReceiverRouteSetupOutcome {
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
struct ReceiverRouteSetupOutcome {
    gauge: RoomGaugeDelta,
    diagnostics: Option<DiagnosticsEventData>,
    source_policy: SourcePolicyWakeups,
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
) -> ReceiverRouteSetupOutcome {
    match outcome {
        ConsumerSetupOutcome::Committed {
            sender,
            track,
            transport_activity_update,
        } => {
            if let Some(active) = transport_activity_update {
                let mut routes = RoomRouteEffects::default();
                routes.push_setup_activity(route.clone(), target.kind, active);
                routes.execute(media_transport).await;
            }
            let diagnostics = setup_diagnostics(room.uuid(), &target, origin, &route);
            let _ = sender.send(UserOutbound::SetupRemoteTrack(Box::new(track)));
            ReceiverRouteSetupOutcome {
                gauge,
                diagnostics: Some(diagnostics),
                source_policy: SourcePolicyWakeups::default(),
            }
        }
        ConsumerSetupOutcome::Released(relays) => {
            execute_relay_route_effects(room, media_transport, &relays).await;
            let cleanup = TransportCleanupOperation::RemoveMedia {
                session_key: route.consumer_session_key().clone(),
                transport_media_id: route.consumer_transport_media_id(),
            };
            room.execute_transport_cleanup_operations(media_transport, slice::from_ref(&cleanup))
                .await;
            ReceiverRouteSetupOutcome {
                gauge,
                diagnostics: None,
                source_policy: fanout_pressure_wakeups(),
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

async fn release_failed_setup(
    room: &Room,
    setup: PendingConsumerSetup,
    media_transport: &MediaTransport,
) -> ReceiverRouteSetupOutcome {
    let (before, after, relays) = {
        let mut state = room.state.write().await;
        let (before, after, relays) = state.release_pending_consumer_setup(setup);
        drop(state);
        (before, after, relays)
    };
    execute_relay_route_effects(room, media_transport, &relays).await;
    ReceiverRouteSetupOutcome {
        gauge: RoomGaugeDelta::media(before, after),
        diagnostics: None,
        source_policy: fanout_pressure_wakeups(),
    }
}

fn fanout_pressure_wakeups() -> SourcePolicyWakeups {
    let mut policy = SourcePolicyWakeups::default();
    policy.fanout_pressure_changed();
    policy
}
