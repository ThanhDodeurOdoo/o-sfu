use o_sfu_router::{MediaKind, MediaStream as RouterRtpParameters};
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::batch::{MediaCountDelta, execute_relay_route_effects};
use crate::engine::{
    diagnostics::DiagnosticsEventData,
    media_transport::{
        ConsumerActivity, MediaTransport, TransportConsumerRoute, TransportSourceKey,
    },
    room::{
        Room, SourcePolicyEvent, UserOutbound,
        cleanup::TransportCleanupOperation,
        media_graph::{
            ConsumerSetupCommit, ConsumerSetupOrigin, ConsumerSetupPlan, ConsumerSetupTarget,
        },
    },
};

#[derive(Debug)]
pub(super) struct ConsumerSetupEffect {
    setup: ConsumerSetupPlan,
    origin: ConsumerSetupOrigin,
}

impl ConsumerSetupEffect {
    pub(super) const fn new(setup: ConsumerSetupPlan, origin: ConsumerSetupOrigin) -> Self {
        Self { setup, origin }
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        media_transport: &MediaTransport,
    ) -> Option<DiagnosticsEventData> {
        let target = self.setup.target().clone();
        if !execute_relay_route_effects(room, media_transport, self.setup.relay_effects()).await {
            release_failed_setup(room, &target, media_transport).await;
            return None;
        }
        let activity = ConsumerActivity::from_active(self.setup.consumer_active());
        let (route, mid) = declare_consumer(
            &target,
            self.setup.rtp(),
            activity,
            self.origin,
            room,
            media_transport,
        )
        .await?;
        let (before, outbound, after) = {
            let mut state = room.state.write().await;
            let before = state.media_counts();
            let outbound =
                state.commit_consumer_setup(self.setup, route.consumer_transport_media_id(), mid);
            let after = state.media_counts();
            drop(state);
            (before, outbound, after)
        };
        finish(
            room,
            media_transport,
            &target,
            self.origin,
            route,
            MediaCountDelta::new(before, after),
            outbound,
        )
        .await
    }
}

async fn declare_consumer(
    target: &ConsumerSetupTarget,
    rtp: &RouterRtpParameters,
    activity: ConsumerActivity,
    origin: ConsumerSetupOrigin,
    room: &Room,
    media_transport: &MediaTransport,
) -> Option<(TransportConsumerRoute, Option<String>)> {
    let (consumer_session, producer_session) = {
        let state = room.state.read().await;
        (
            state.committed_transport_user_key(
                target.consumer_user_id(),
                target.consumer_connection_id(),
            ),
            state.committed_transport_user_key(
                target.producer_user_id(),
                target.producer_connection_id(),
            ),
        )
    };
    let Some((consumer_session, producer_session)) = consumer_session.zip(producer_session) else {
        release_failed_setup(room, target, media_transport).await;
        warn!(
            consumer_user_id = ?target.consumer_user_id(),
            consumer_connection_id = ?target.consumer_connection_id(),
            producer_user_id = ?target.producer_user_id(),
            producer_connection_id = ?target.producer_connection_id(),
            source_transport_media_id = ?target.transport_media_id(),
            consumer_mid = rtp.mid(),
            ?origin,
            "skipping consumer setup because routing no longer owns the target sessions"
        );
        return None;
    };
    match media_transport
        .consume_media(
            &consumer_session,
            target.media_kind(),
            &producer_session,
            target.transport_media_id(),
            rtp,
            activity,
        )
        .await
    {
        Ok(consumer_media) => {
            let mid = media_transport
                .transport_media_mid(&consumer_session, consumer_media)
                .await;
            Some((
                TransportConsumerRoute::new(
                    consumer_session,
                    consumer_media,
                    TransportSourceKey::new(producer_session, target.transport_media_id()),
                ),
                mid,
            ))
        }
        Err(error) => {
            release_failed_setup(room, target, media_transport).await;
            warn!(
                consumer_user_id = ?target.consumer_user_id(),
                consumer_connection_id = ?target.consumer_connection_id(),
                producer_user_id = ?target.producer_user_id(),
                producer_connection_id = ?target.producer_connection_id(),
                source_transport_media_id = ?target.transport_media_id(),
                error = ?error,
                consumer_mid = rtp.mid(),
                ?origin,
                "media transport rejected consume media declaration"
            );
            None
        }
    }
}

async fn finish(
    room: &Room,
    media_transport: &MediaTransport,
    target: &ConsumerSetupTarget,
    origin: ConsumerSetupOrigin,
    route: TransportConsumerRoute,
    delta: MediaCountDelta,
    commit: Option<ConsumerSetupCommit>,
) -> Option<DiagnosticsEventData> {
    let Some(commit) = commit else {
        delta.record(room);
        release_failed_setup(room, target, media_transport).await;
        let cleanup = [TransportCleanupOperation::RemoveMedia {
            session_key: route.consumer_session_key().clone(),
            connection_id: target.consumer_connection_id(),
            transport_media_id: route.consumer_transport_media_id(),
        }];
        room.execute_transport_cleanup_operations(media_transport, &cleanup)
            .await;
        return None;
    };
    delta.record(room);
    if let Some(active) = commit.transport_activity_update {
        sync_activity(media_transport, target, &route, active).await;
    }
    let _ = commit.sender.send(UserOutbound::Request(Box::new(
        commit.track.into_room_event_request(),
    )));
    Some(
        DiagnosticsEventData::for_user(
            room.uuid(),
            target.consumer_user_id(),
            telemetry_event::SUBSCRIBE_SUCCEEDED,
        )
        .with_connection_id(target.consumer_connection_id().as_u64())
        .with_media_worker_id(route.consumer_session_key().media_worker_id().as_usize())
        .with_transport_media_id(route.consumer_transport_media_id().as_u64())
        .insert_field(
            "producer_user_id",
            serde_json::to_value(target.producer_user_id()).unwrap_or(serde_json::Value::Null),
        )
        .insert_field(
            "source_transport_media_id",
            target.transport_media_id().as_u64(),
        )
        .insert_field("stream_id", target.stream_id().to_string())
        .insert_field("origin", origin.as_diagnostic_str()),
    )
}

async fn sync_activity(
    media_transport: &MediaTransport,
    target: &ConsumerSetupTarget,
    route: &TransportConsumerRoute,
    active: bool,
) {
    if media_transport
        .set_consumer_active(route, ConsumerActivity::from_active(active))
        .await
        .is_err()
    {
        warn!(
            ?route,
            stream_id = %target.stream_id(),
            active,
            "media transport failed to correct in-flight consumer setup activity"
        );
        return;
    }
    if active
        && target.media_kind() == MediaKind::Video
        && media_transport
            .request_consumer_keyframe(route)
            .await
            .is_err()
    {
        warn!(
            ?route,
            stream_id = %target.stream_id(),
            "media transport failed to request keyframe after consumer setup activity correction"
        );
    }
}

async fn release_failed_setup(
    room: &Room,
    target: &ConsumerSetupTarget,
    media_transport: &MediaTransport,
) {
    let (before, after, relays) = {
        let mut state = room.state.write().await;
        let before = state.media_counts();
        let relays = state.release_consumer_setup_plan(target);
        let relays = state.resolved_relay_route_effects(relays);
        let after = state.media_counts();
        drop(state);
        (before, after, relays)
    };
    MediaCountDelta::new(before, after).record(room);
    execute_relay_route_effects(room, media_transport, &relays).await;
    room.handle_source_policy_event(
        SourcePolicyEvent::FanoutPressureChanged,
        Some(media_transport),
    )
    .await;
}
