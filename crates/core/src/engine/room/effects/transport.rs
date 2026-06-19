use tracing::warn;

use crate::{
    TransportEffectOutcome,
    engine::{
        diagnostics::DiagnosticsEventData,
        media_transport::{
            MediaTransport, TransportRelayRouteAction, TransportRelayRouteEffect,
            TransportSourceKey,
        },
        room::{
            Room,
            cleanup::TransportCleanupOperation,
            effects::RoomRouteEffects,
            media_graph::{MediaTopologyEffects, ResolvedRelayRouteEffect},
        },
    },
};

#[derive(Debug, Default)]
pub(super) struct RoomTransportPlan {
    topology: MediaTopologyEffects,
    routes: RoomRouteEffects,
}

impl RoomTransportPlan {
    pub(super) fn extend_topology(&mut self, effects: MediaTopologyEffects) {
        self.topology.extend(effects);
    }

    pub(super) fn extend_cleanup(&mut self, cleanup: Vec<TransportCleanupOperation>) {
        self.topology.extend_cleanup(cleanup);
    }

    pub(super) fn push_cleanup(&mut self, operation: TransportCleanupOperation) {
        self.topology.push_cleanup(operation);
    }

    pub(super) fn push_producer(
        &mut self,
        source: TransportSourceKey,
        active: bool,
        diagnostics: DiagnosticsEventData,
    ) {
        self.routes.push_producer(source, active, diagnostics);
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        route_transport: Option<&MediaTransport>,
    ) -> Vec<DiagnosticsEventData> {
        let (relays, cleanup) = self.topology.into_parts();
        let Some(media_transport) = route_transport else {
            return Vec::new();
        };
        execute_relay_route_effects(room, media_transport, &relays).await;
        let diagnostics = self.routes.execute(media_transport).await.diagnostics;
        if !cleanup.is_empty() {
            room.execute_transport_cleanup_operations(media_transport, &cleanup)
                .await;
        }
        diagnostics
    }
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
