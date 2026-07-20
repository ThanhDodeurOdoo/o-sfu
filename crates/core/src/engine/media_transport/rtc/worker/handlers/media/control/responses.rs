//! command adapters for worker-local media control

use std::time::Instant;

use super::{
    super::{
        super::bwe,
        keyframe::{worker_request_consumer_kf, worker_request_remote_kf},
    },
    routes,
};
use crate::{
    Bitrate,
    engine::{
        media_transport::{
            ConsumerRouteControlFailure, ConsumerRouteControlOutcome,
            rtc::{
                commands::{
                    RouteControlRequest, RtcWorkerResponse, WorkerMediaControlBatch,
                    WorkerMediaControlBatchOutcome,
                },
                state::PacketLoopState,
            },
        },
        metrics::RtcMetricsRecorder,
    },
};

fn map_updates<T, R>(updates: Vec<(usize, T)>, mut apply: impl FnMut(T) -> R) -> Vec<R> {
    updates.into_iter().map(|(_, value)| apply(value)).collect()
}

pub fn apply_route_control_request(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    request: RouteControlRequest,
    response: Option<RtcWorkerResponse<()>>,
) {
    let result = match request {
        RouteControlRequest::AddRelayTarget {
            source,
            target_id,
            target,
        } => routes::worker_add_relay_target(state, &source, target_id, target),
        RouteControlRequest::RemoveRelayTarget { source, target_id } => {
            routes::worker_remove_relay_target(state, &source, target_id)
        }
        RouteControlRequest::SetRelayTargetActive {
            source,
            target_id,
            active,
        } => routes::worker_set_relay_target_active(state, &source, target_id, active),
        RouteControlRequest::RequestRemoteKeyframe {
            source,
            target_id,
            rid,
            kind,
        } => {
            worker_request_remote_kf(state, metrics, &source, target_id, rid, kind);
            Ok(())
        }
        RouteControlRequest::SetRemoteSourcePacketGate {
            source,
            target_id,
            packet_gate,
        } => {
            routes::set_remote_src_pkt_gate(state, &source, target_id, packet_gate);
            Ok(())
        }
    };
    if let Some(response) = response {
        let _ = response.send(result);
    }
}

pub fn apply_media_control_batch(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    max_bitrate_out: Bitrate,
    now: Instant,
    batch: WorkerMediaControlBatch,
) -> WorkerMediaControlBatchOutcome {
    use WorkerMediaControlBatch::{ConsumerFollowUp, ConsumerGates, ProducerActivity, ReceiverBwe};
    use WorkerMediaControlBatchOutcome::{Applied, Consumers};

    match batch {
        ReceiverBwe(updates) => Applied(map_updates(updates, |update| {
            bwe::apply_receiver_bwe_target(state, max_bitrate_out, &update)
        })),
        ProducerActivity(updates) => Applied(map_updates(updates, |control| {
            routes::worker_set_producer_active(state, &control.source, control.activity.is_active())
        })),
        ConsumerGates { source, updates } => Applied(routes::worker_set_consumer_pkt_gates(
            state, &source, updates, now,
        )),
        ConsumerFollowUp(updates) => Consumers(map_updates(updates, |control| {
            if let Some(activity) = control.activity
                && let Err(error) =
                    routes::worker_set_consumer_active(state, &control.route, activity.is_active())
            {
                return ConsumerRouteControlOutcome(Some(ConsumerRouteControlFailure::Activity(
                    error,
                )));
            }
            if control.request_keyframe
                && let Err(error) = worker_request_consumer_kf(state, metrics, &control.route)
            {
                return ConsumerRouteControlOutcome(Some(ConsumerRouteControlFailure::Keyframe(
                    error,
                )));
            }
            ConsumerRouteControlOutcome::default()
        })),
    }
}
