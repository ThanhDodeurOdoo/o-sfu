//! shared post-lock room effect execution
//!
//! room transitions keep the `RoomState` lock only long enough to validate
//! intent, mutate authoritative state and capture the facts needed by external
//! systems
//!
//! [`RoomEffectBatch`] is the cold-path boundary that consumes those captured
//! facts after unlock
//! it gives membership, publish, unpublish and subscription
//! workflows one ordering point for metrics, transport cleanup, relay updates,
//! source policy events, lifecycle fan-out, diagnostics and outbound room
//! requests
//!
//! callers should build a batch only from state-owned results that have already
//! committed
//! the executor must not rediscover ownership from live room state
//! after async work starts, because a replacement session may have claimed the
//! same user-facing identity by then

use tracing::warn;

use crate::{
    TransportEffectOutcome,
    runtime::{
        ConnectionId, UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{
            MediaTransport, TransportRelayRouteAction, TransportRelayRouteEffect,
            TransportSourceKey,
        },
        room::{
            Room, RoomEventRequest, RoomMediaCounts, SourcePolicyEvent, UserOutbound,
            cleanup::TransportCleanupOperation,
            outbound::OutboundSender,
            state::{LifecycleEffects, RelayRouteEffect, TransportMediaRemoval},
        },
    },
};

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) struct RoomEffectContext<'a> {
    /// transport handle available for post-lock observations and policy work
    ///
    /// state-only tests may provide this while still disabling cleanup mutation
    /// so source policy can observe transport state without removing media
    media_transport: Option<&'a MediaTransport>,
    /// transport handle available for adapter mutation after room ownership ends
    ///
    /// relay updates, media removals and transport user closes use this handle
    /// only when the caller authorizes cleanup side effects
    cleanup_media_transport: Option<&'a MediaTransport>,
}

impl<'a> RoomEffectContext<'a> {
    /// build the production context for normal runtime room work
    pub(in crate::runtime::room) const fn runtime(media_transport: &'a MediaTransport) -> Self {
        Self {
            media_transport: Some(media_transport),
            cleanup_media_transport: Some(media_transport),
        }
    }

    /// build a context that preserves room state effects without mutating transport cleanup state
    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::room) const fn state_only(
        media_transport: Option<&'a MediaTransport>,
    ) -> Self {
        Self {
            media_transport,
            cleanup_media_transport: None,
        }
    }
}

/// media gauge delta captured while room state was authoritative
///
/// callers pass snapshots instead of recomputing counts after unlock, so metric
/// updates describe the committed transition that produced the batch
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) struct MediaCountDelta {
    before: RoomMediaCounts,
    after: RoomMediaCounts,
}

impl MediaCountDelta {
    pub(in crate::runtime::room) const fn new(
        before: RoomMediaCounts,
        after: RoomMediaCounts,
    ) -> Self {
        Self { before, after }
    }

    fn record(self, room: &Room) {
        room.record_media_count_delta(self.before, self.after);
    }
}

/// ordered side effects for one committed room transition
///
/// this type is private to `runtime::room` because it encodes room-local
/// lifecycle rules, transport cleanup policy and diagnostics ordering
/// each builder method records a fact that was already decided by room state or
/// by a transport boundary that just completed
///
/// execution is cold-path orchestration
/// the batch may allocate and move owned effect vectors, but it must not be
/// called from packet forwarding paths
#[derive(Debug, Default)]
pub(in crate::runtime::room) struct RoomEffectBatch {
    /// facts captured while room state was authoritative
    mutation: CommittedRoomMutation,
    /// relay route mutations derived from room topology before unlock
    relay_effects: Vec<RelayRouteEffect>,
    /// detached media ids that cleanup.rs may retry if the adapter is unavailable
    transport_removals: Vec<TransportMediaRemoval>,
    /// detached transport users that no longer have an owning room session
    transport_user_closes: Vec<TransportUserCleanup>,
    /// best-effort close requests and room fan-out captured by state
    lifecycle_effects: Vec<LifecycleEffects>,
}

#[derive(Debug, Clone, Copy)]
struct UserCountDelta {
    before: usize,
    after: usize,
}

/// committed room-state facts consumed by the post-lock executor
#[derive(Debug, Default)]
struct CommittedRoomMutation {
    /// active-user gauge delta from a committed membership transition
    user_count_delta: Option<UserCountDelta>,
    /// media gauge deltas from committed publish, unpublish or subscribe work
    media_count_deltas: Vec<MediaCountDelta>,
    /// source policy event requested after committed room work
    source_policy_event: Option<SourcePolicyEvent>,
    /// diagnostics store mutations that must preserve caller order
    diagnostics: Vec<DiagnosticsEffect>,
    /// room event requests enqueued only after state and transport effects run
    outbound_requests: Vec<OutboundRequestEffect>,
}

/// detached transport user that should be closed after room state moves on
///
/// the identity is captured before async cleanup starts, so replacement joins
/// cannot make cleanup target the new transport user by accident
#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct TransportUserCleanup {
    user_id: UserId,
    connection_id: ConnectionId,
}

impl TransportUserCleanup {
    pub(in crate::runtime::room) fn new(user_id: UserId, connection_id: ConnectionId) -> Self {
        Self {
            user_id,
            connection_id,
        }
    }
}

/// diagnostics mutations captured as ordered effects
///
/// register, record and forget operations stay in the batch so membership
/// finalization cannot drift into a different diagnostics order per path
#[derive(Debug)]
enum DiagnosticsEffect {
    RegisterUser(UserId),
    Record(DiagnosticsEventData),
    ForgetUser(UserId),
}

/// outbound request emitted after the room transition and transport work finish
///
/// this keeps request enqueueing behind the same post-lock ordering as the rest
/// of the transition, while send failure remains best-effort like room fan-out
#[derive(Debug)]
struct OutboundRequestEffect {
    sender: OutboundSender,
    request: RoomEventRequest,
}

/// result surface for follow-up decisions after batch execution
///
/// cleanup reports only retry-backed media removal outcome
/// relay success stays separate because consumer bootstrap must release pending
/// state when relay setup fails before the consumer can be created
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) struct RoomEffectExecution {
    cleanup: TransportEffectOutcome,
    relay_effects_applied: bool,
}

impl RoomEffectExecution {
    pub(in crate::runtime::room) const fn cleanup(self) -> TransportEffectOutcome {
        self.cleanup
    }

    pub(in crate::runtime::room) const fn relay_effects_applied(self) -> bool {
        self.relay_effects_applied
    }
}

impl RoomEffectBatch {
    /// start an empty batch for one committed transition
    pub(in crate::runtime::room) fn new() -> Self {
        Self::default()
    }

    /// record the active-user gauge delta that belongs to this transition
    pub(in crate::runtime::room) fn with_user_count_delta(
        mut self,
        before: usize,
        after: usize,
    ) -> Self {
        self.mutation.user_count_delta = Some(UserCountDelta { before, after });
        self
    }

    /// record a media-count gauge delta from state snapshots
    pub(in crate::runtime::room) fn with_media_count_delta(
        self,
        before: RoomMediaCounts,
        after: RoomMediaCounts,
    ) -> Self {
        self.with_media_count_delta_value(MediaCountDelta::new(before, after))
    }

    /// record a media-count delta only when the caller planned one
    pub(in crate::runtime::room) fn with_optional_media_count_delta(
        mut self,
        delta: Option<MediaCountDelta>,
    ) -> Self {
        if let Some(delta) = delta {
            self.mutation.media_count_deltas.push(delta);
        }
        self
    }

    /// record a prebuilt media-count delta
    pub(in crate::runtime::room) fn with_media_count_delta_value(
        mut self,
        delta: MediaCountDelta,
    ) -> Self {
        self.mutation.media_count_deltas.push(delta);
        self
    }

    /// queue relay route effects captured from room topology
    pub(in crate::runtime::room) fn with_relay_effects(
        mut self,
        effects: impl IntoIterator<Item = RelayRouteEffect>,
    ) -> Self {
        self.relay_effects.extend(effects);
        self
    }

    /// queue detached media removals under cleanup retry ownership
    pub(in crate::runtime::room) fn with_transport_removals(
        mut self,
        removals: impl IntoIterator<Item = TransportMediaRemoval>,
    ) -> Self {
        self.transport_removals.extend(removals);
        self
    }

    /// queue a transport user close after room state released ownership
    pub(in crate::runtime::room) fn with_transport_user_close(
        mut self,
        cleanup: TransportUserCleanup,
    ) -> Self {
        self.transport_user_closes.push(cleanup);
        self
    }

    /// record the source-policy consequence of the committed transition
    pub(in crate::runtime::room) fn with_source_policy_event(
        mut self,
        event: SourcePolicyEvent,
    ) -> Self {
        self.mutation.source_policy_event = Some(event);
        self
    }

    /// queue lifecycle notifications captured by room state
    pub(in crate::runtime::room) fn with_lifecycle_effects(
        mut self,
        effects: LifecycleEffects,
    ) -> Self {
        self.lifecycle_effects.push(effects);
        self
    }

    /// register a user in diagnostics after the join transition commits
    pub(in crate::runtime::room) fn register_diagnostics_user(mut self, user_id: UserId) -> Self {
        self.mutation
            .diagnostics
            .push(DiagnosticsEffect::RegisterUser(user_id));
        self
    }

    /// record a diagnostics event in the caller-selected position
    pub(in crate::runtime::room) fn record_diagnostics(
        mut self,
        diagnostics: DiagnosticsEventData,
    ) -> Self {
        self.mutation
            .diagnostics
            .push(DiagnosticsEffect::Record(diagnostics));
        self
    }

    /// forget a user in diagnostics after the room session is gone
    pub(in crate::runtime::room) fn forget_diagnostics_user(mut self, user_id: UserId) -> Self {
        self.mutation
            .diagnostics
            .push(DiagnosticsEffect::ForgetUser(user_id));
        self
    }

    /// enqueue a room event request after transport-facing effects have run
    pub(in crate::runtime::room) fn send_outbound_request(
        mut self,
        sender: OutboundSender,
        request: RoomEventRequest,
    ) -> Self {
        self.mutation
            .outbound_requests
            .push(OutboundRequestEffect { sender, request });
        self
    }

    /// execute one committed post-lock batch in the room-wide effect order
    ///
    /// the order is fixed so future room workflows do not interleave async
    /// cleanup and outward notifications differently
    ///
    /// 1. record room gauges from state-owned snapshots
    /// 2. apply relay route effects that were captured before unlock
    /// 3. run retry-backed transport media cleanup
    /// 4. close detached transport users
    /// 5. handle the source-policy event from transport observations
    /// 6. emit lifecycle fan-out and user close messages
    /// 7. write diagnostics effects in captured order
    /// 8. enqueue outbound room-event requests
    ///
    /// relay failures are reported through [`RoomEffectExecution`] because
    /// bootstrap callers may still need to release pending consumer state
    /// cleanup failures stay owned by cleanup.rs retry state and are surfaced
    /// through the returned cleanup outcome
    pub(in crate::runtime::room) async fn execute(
        self,
        room: &Room,
        context: RoomEffectContext<'_>,
    ) -> RoomEffectExecution {
        let Self {
            mutation,
            relay_effects,
            transport_removals,
            transport_user_closes,
            lifecycle_effects,
        } = self;
        let CommittedRoomMutation {
            user_count_delta,
            media_count_deltas,
            source_policy_event,
            diagnostics,
            outbound_requests,
        } = mutation;
        Self::record_gauge_deltas(room, user_count_delta, &media_count_deltas);
        let relay_effects_applied =
            Self::execute_relay_effects(room, context, &relay_effects).await;
        let cleanup = Self::execute_transport_cleanup(
            room,
            context,
            &transport_removals,
            &transport_user_closes,
        )
        .await;
        Self::execute_source_policy_event(room, context, source_policy_event).await;
        Self::emit_lifecycle_effects(lifecycle_effects);
        Self::record_diagnostics_effects(room, diagnostics);
        Self::send_outbound_requests(outbound_requests);
        RoomEffectExecution {
            cleanup,
            relay_effects_applied,
        }
    }

    fn record_gauge_deltas(
        room: &Room,
        user_count_delta: Option<UserCountDelta>,
        media_count_deltas: &[MediaCountDelta],
    ) {
        if let Some(delta) = user_count_delta {
            let before = i64::try_from(delta.before).unwrap_or(i64::MAX);
            let after = i64::try_from(delta.after).unwrap_or(i64::MAX);
            room.metrics.add_active_users(after.saturating_sub(before));
        }
        for delta in media_count_deltas {
            (*delta).record(room);
        }
    }

    async fn execute_relay_effects(
        room: &Room,
        context: RoomEffectContext<'_>,
        relay_effects: &[RelayRouteEffect],
    ) -> bool {
        let Some(media_transport) = context.cleanup_media_transport else {
            return true;
        };
        execute_relay_route_effects(room, media_transport, relay_effects).await
    }

    async fn execute_transport_cleanup(
        room: &Room,
        context: RoomEffectContext<'_>,
        transport_removals: &[TransportMediaRemoval],
        transport_user_closes: &[TransportUserCleanup],
    ) -> TransportEffectOutcome {
        let Some(media_transport) = context.cleanup_media_transport else {
            return TransportEffectOutcome::Applied;
        };
        let removals = transport_removals
            .iter()
            .map(|removal| {
                let connection_id = removal.connection();
                TransportCleanupOperation::RemoveMedia {
                    session_key: room.transport_user_key(removal.user(), connection_id),
                    connection_id,
                    transport_media_id: removal.transport_media(),
                }
            })
            .collect::<Vec<_>>();
        let cleanup = room
            .execute_transport_cleanup_operations(media_transport, &removals)
            .await;
        if !transport_user_closes.is_empty() {
            let closes = transport_user_closes
                .iter()
                .map(|cleanup| TransportCleanupOperation::CloseUser {
                    session_key: room.transport_user_key(&cleanup.user_id, cleanup.connection_id),
                    connection_id: cleanup.connection_id,
                })
                .collect::<Vec<_>>();
            let _ = room
                .execute_transport_cleanup_operations(media_transport, &closes)
                .await;
        }
        cleanup
    }

    async fn execute_source_policy_event(
        room: &Room,
        context: RoomEffectContext<'_>,
        source_policy_event: Option<SourcePolicyEvent>,
    ) {
        if let Some(event) = source_policy_event {
            room.handle_source_policy_event(event, context.media_transport)
                .await;
        }
    }

    fn emit_lifecycle_effects(lifecycle_effects: Vec<LifecycleEffects>) {
        for effects in lifecycle_effects {
            for close_request in effects.close_requests {
                let _ = close_request
                    .sender
                    .send(UserOutbound::Close(close_request.reason));
            }
            for fanout in effects.fanouts {
                fanout.emit();
            }
        }
    }

    fn record_diagnostics_effects(room: &Room, diagnostics: Vec<DiagnosticsEffect>) {
        for effect in diagnostics {
            match effect {
                DiagnosticsEffect::RegisterUser(user_id) => {
                    room.diagnostics.register_user(room.uuid(), &user_id);
                }
                DiagnosticsEffect::Record(diagnostics) => {
                    room.diagnostics.record(diagnostics);
                }
                DiagnosticsEffect::ForgetUser(user_id) => {
                    room.diagnostics.forget_user(room.uuid(), &user_id);
                }
            }
        }
    }

    fn send_outbound_requests(outbound_requests: Vec<OutboundRequestEffect>) {
        for outbound_request in outbound_requests {
            let _ = outbound_request
                .sender
                .send(UserOutbound::Request(Box::new(outbound_request.request)));
        }
    }
}

pub(super) async fn execute_relay_route_effects(
    room: &Room,
    media_port: &MediaTransport,
    effects: &[RelayRouteEffect],
) -> bool {
    let mut applied = true;
    for effect in effects {
        if effect.action == TransportRelayRouteAction::Release {
            let operation = [TransportCleanupOperation::ReleaseRelayRoute {
                source_session_key: room
                    .transport_user_key(&effect.route.source_user, effect.route.source_connection),
                route: effect.route.clone(),
            }];
            if room
                .execute_transport_cleanup_operations(media_port, &operation)
                .await
                == TransportEffectOutcome::Failed
            {
                applied = false;
            }
            continue;
        }
        let transport_effect = TransportRelayRouteEffect {
            source: TransportSourceKey::new(
                room.transport_user_key(&effect.route.source_user, effect.route.source_connection),
                effect.route.source_media,
            ),
            target_media_worker_id: effect.route.target_worker,
            action: effect.action,
        };
        let result = media_port.apply_relay_route_effect(&transport_effect).await;
        if let Err(error) = result {
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
