use tracing::warn;

use super::{
    batch::RoomGaugeDelta,
    consumer_route::ConsumerRouteEffect,
    consumer_setup::{ConsumerSetupEffect, ConsumerSetupEffectOutcome},
    policy::RoomPolicyPlan,
};
use crate::engine::{
    diagnostics::DiagnosticsEventData,
    media_transport::MediaTransport,
    room::{
        Room,
        media_graph::{ConsumerRouteTarget, ConsumerSetupOrigin, PendingConsumerSetup},
    },
};

#[derive(Debug, Default)]
pub(super) struct ConsumerTransportPlan {
    routes: Vec<ConsumerRouteTransportEffect>,
    setups: Vec<ConsumerSetupEffect>,
}

impl ConsumerTransportPlan {
    pub(super) fn push_activity(
        &mut self,
        target: ConsumerRouteTarget,
        active: bool,
        diagnostics: DiagnosticsEventData,
    ) {
        self.routes.push(ConsumerRouteTransportEffect::Activity {
            target,
            active,
            diagnostics,
        });
    }

    pub(super) fn extend_keyframe_refresh(&mut self, targets: Vec<ConsumerRouteTarget>) {
        self.routes.extend(
            targets
                .into_iter()
                .map(ConsumerRouteTransportEffect::Keyframe),
        );
    }

    pub(super) fn push_setups(
        &mut self,
        setups: Vec<PendingConsumerSetup>,
        origin: ConsumerSetupOrigin,
    ) {
        self.setups.extend(
            setups
                .into_iter()
                .map(|setup| ConsumerSetupEffect::new(setup, origin)),
        );
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
    ) -> ConsumerTransportOutcome {
        let Some(media_transport) = media_transport else {
            return ConsumerTransportOutcome::default();
        };
        let mut outcome = ConsumerTransportOutcome::default();
        for effect in self.routes {
            if let Some(diagnostics) = effect.execute(media_transport).await {
                outcome.diagnostics.push(diagnostics);
            }
        }
        for setup in self.setups {
            outcome.extend_setup(setup.execute(room, media_transport).await);
        }
        outcome
    }
}

#[derive(Debug, Default)]
pub(super) struct ConsumerTransportOutcome {
    pub(super) gauges: Vec<RoomGaugeDelta>,
    pub(super) diagnostics: Vec<DiagnosticsEventData>,
    pub(super) policy: RoomPolicyPlan,
}

impl ConsumerTransportOutcome {
    fn extend_setup(&mut self, outcome: ConsumerSetupEffectOutcome) {
        self.gauges.push(outcome.gauge);
        if let Some(diagnostics) = outcome.diagnostics {
            self.diagnostics.push(diagnostics);
        }
        self.policy.extend(outcome.policy);
    }
}

#[derive(Debug)]
enum ConsumerRouteTransportEffect {
    Activity {
        target: ConsumerRouteTarget,
        active: bool,
        diagnostics: DiagnosticsEventData,
    },
    Keyframe(ConsumerRouteTarget),
}

impl ConsumerRouteTransportEffect {
    async fn execute(self, media_transport: &MediaTransport) -> Option<DiagnosticsEventData> {
        match self {
            Self::Activity {
                target,
                active,
                diagnostics,
            } => Self::execute_activity(media_transport, target, active, diagnostics).await,
            Self::Keyframe(target) => {
                Self::execute_keyframe(media_transport, target).await;
                None
            }
        }
    }

    async fn execute_activity(
        media_transport: &MediaTransport,
        target: ConsumerRouteTarget,
        active: bool,
        diagnostics: DiagnosticsEventData,
    ) -> Option<DiagnosticsEventData> {
        let outcome = ConsumerRouteEffect::new(target.transport_route())
            .with_activity(active)
            .with_keyframe(target.request_keyframe_after_activity(active))
            .execute(media_transport)
            .await;
        if outcome.activity_failed {
            warn!(
                route = ?target.transport_route(),
                stream_id = %target.stream_id(),
                active,
                "media transport failed to update consumer route activity"
            );
        } else if outcome.keyframe_failed {
            warn!(
                route = ?target.transport_route(),
                stream_id = %target.stream_id(),
                "media transport failed to request a consumer keyframe refresh"
            );
        }
        Some(diagnostics)
    }

    async fn execute_keyframe(media_transport: &MediaTransport, target: ConsumerRouteTarget) {
        if ConsumerRouteEffect::new(target.transport_route())
            .with_keyframe(true)
            .execute(media_transport)
            .await
            .keyframe_failed
        {
            warn!(
                consumer_user_id = ?target.transport_route().consumer_session_key().user_id(),
                consumer_transport_media_id = ?target.transport_route().consumer_transport_media_id(),
                producer_user_id = ?target.producer_user_id(),
                source_transport_media_id = ?target.source_media_id(),
                "media transport failed to request a refreshed consumer keyframe"
            );
        }
    }
}
