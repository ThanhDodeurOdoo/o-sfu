use tracing::warn;

use crate::{
    TransportEffectOutcome,
    engine::{
        diagnostics::DiagnosticsEventData,
        media_transport::{
            MediaTransport, ProducerActivity, TransportRelayRouteAction, TransportRelayRouteEffect,
            TransportSourceKey,
        },
        room::{
            Room,
            cleanup::TransportCleanupOperation,
            media_graph::{MediaTopologyEffects, ResolvedRelayRouteEffect},
        },
        source_model::UserStreamId,
    },
};

#[derive(Debug, Default)]
pub(super) struct RoomTransportPlan {
    topology: MediaTopologyEffects,
    producers: Vec<ProducerActivityEffect>,
}

impl RoomTransportPlan {
    pub(super) fn extend_topology(&mut self, effects: MediaTopologyEffects) {
        self.topology.extend(effects);
    }

    pub(super) fn extend_relays(&mut self, relays: Vec<ResolvedRelayRouteEffect>) {
        self.topology.extend_relays(relays);
    }

    pub(super) fn extend_cleanup(&mut self, cleanup: Vec<TransportCleanupOperation>) {
        self.topology.extend_cleanup(cleanup);
    }

    pub(super) fn push_cleanup(&mut self, operation: TransportCleanupOperation) {
        self.topology.push_cleanup(operation);
    }

    pub(super) fn push_producer(&mut self, producer: ProducerActivityEffect) {
        self.producers.push(producer);
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
        route_transport: Option<&MediaTransport>,
    ) -> Vec<DiagnosticsEventData> {
        let (relays, cleanup) = self.topology.into_parts();
        if let Some(media_transport) = route_transport {
            execute_relay_route_effects(room, media_transport, &relays).await;
        }
        let diagnostics = execute_producer_activity(media_transport, self.producers).await;
        execute_cleanup(room, route_transport, cleanup).await;
        diagnostics
    }
}

#[derive(Debug)]
pub(super) struct ProducerActivityEffect {
    source: TransportSourceKey,
    active: bool,
    stream: UserStreamId,
    diagnostics: DiagnosticsEventData,
}

impl ProducerActivityEffect {
    pub(super) const fn new(
        source: TransportSourceKey,
        active: bool,
        stream: UserStreamId,
        diagnostics: DiagnosticsEventData,
    ) -> Self {
        Self {
            source,
            active,
            stream,
            diagnostics,
        }
    }
}

async fn execute_cleanup(
    room: &Room,
    media_transport: Option<&MediaTransport>,
    cleanup: Vec<TransportCleanupOperation>,
) {
    if cleanup.is_empty() {
        return;
    }
    let Some(media_transport) = media_transport else {
        return;
    };
    room.execute_transport_cleanup_operations(media_transport, &cleanup)
        .await;
}

async fn execute_producer_activity(
    media_transport: Option<&MediaTransport>,
    producers: Vec<ProducerActivityEffect>,
) -> Vec<DiagnosticsEventData> {
    let mut diagnostics = Vec::with_capacity(producers.len());
    for op in producers {
        if let Some(media_transport) = media_transport
            && media_transport
                .set_producer_active(&op.source, ProducerActivity::from_active(op.active))
                .await
                .is_err()
        {
            warn!(
                source = ?op.source,
                stream_id = %op.stream,
                active = op.active,
                "media transport failed to update producer route activity"
            );
        }
        diagnostics.push(op.diagnostics);
    }
    diagnostics
}

pub(super) async fn execute_relay_route_effects(
    room: &Room,
    media_transport: &MediaTransport,
    effects: &[ResolvedRelayRouteEffect],
) -> bool {
    let mut applied = true;
    for effect in effects {
        if effect.action == TransportRelayRouteAction::Release {
            let operation = [TransportCleanupOperation::ReleaseRelayRoute {
                source_session_key: effect.source_session_key.clone(),
                route: effect.route.clone(),
            }];
            if room
                .execute_transport_cleanup_operations(media_transport, &operation)
                .await
                == TransportEffectOutcome::Failed
            {
                applied = false;
            }
            continue;
        }
        let transport_effect = TransportRelayRouteEffect {
            source: TransportSourceKey::new(
                effect.source_session_key.clone(),
                effect.route.source_media,
            ),
            target_media_worker_id: effect.route.target_worker,
            action: effect.action,
        };
        if let Err(error) = media_transport
            .apply_relay_route_effect(&transport_effect)
            .await
        {
            applied = false;
            warn!(
                ?effect,
                ?error,
                "media transport failed to apply relay route effect"
            );
        }
    }
    applied
}
