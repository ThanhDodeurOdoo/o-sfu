//! Defines the process-local metric catalog for runtime observability
//!
//! This module is the single place where `o-sfu` declares the metric families
//! it exports and the typed recording helpers that runtime edges are allowed to
//! call. having catalog centralized makes the `/metrics` schema explicit
//! and prevents HTTP, websocket, transport, and media code from inventing
//! adoc counters, gauges, etc...

use std::{sync::Arc, time::Duration};

use o_sfu_model::WebSocketCloseCode;

use super::{
    counter::{
        Counter, CounterFamily, Histogram, HistogramFamily, UpDownCounter, UpDownCounterFamily,
    },
    labels::{
        BudgetSolverOutcome, ControlPlaneDurationBucket, HttpDisconnectResponseStatus,
        HttpRoomResponseStatus, HttpRoute, MediaQualityLossDirection, MediaQualityRttBucket,
        MediaQualitySample, RecordingActionOutcome, RtcDatagramDropReason, RtcDatagramRoutePath,
        RtcKeyframeRequestOutcome, RtcRelayEnqueueResult, RtcRemoteControlDropKind,
        RtcRemotePacketGateConvergence, RtcRouteControlOutcome, RtpDecoderRefreshScope,
        RtpForwardDestinationKind, RtpRelayDropKind, SourceSelectionKind,
        TransportCleanupFailureKind, TransportHealthState, TransportHealthTransition,
        TransportIceState, TransportUserLifetimeBucket, WsBusClientFrameKind, WsBusDirection,
        WsBusFailureKind, WsConnectionStage, WsSessionLoopExitReason, WsStartupFailureKind,
    },
    rtc::{RtcMetrics, RtcMetricsRecorder, RtcRouteControlMetrics},
    rtp::{RtpMetrics, RtpMetricsRecorder},
};

#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    pub(super) http_requests: CounterFamily<HttpRoute>,
    pub(super) http_room_responses: CounterFamily<HttpRoomResponseStatus>,
    pub(super) http_disconnect_responses: CounterFamily<HttpDisconnectResponseStatus>,
    pub(super) http_inflight_requests: UpDownCounterFamily<HttpRoute>,
    pub(super) http_request_duration: HistogramFamily<HttpRoute, ControlPlaneDurationBucket>,
    pub(super) ws_connections: CounterFamily<WsConnectionStage>,
    pub(super) ws_handshake_rejections: CounterFamily<WebSocketCloseCode>,
    pub(super) ws_handshake_rejections_other: Counter,
    pub(super) ws_startup_failures: CounterFamily<WsStartupFailureKind>,
    pub(super) ws_user_loops_started: Counter,
    pub(super) ws_user_loop_exits: CounterFamily<WsSessionLoopExitReason>,
    pub(super) ws_bus_batches: CounterFamily<WsBusDirection>,
    pub(super) ws_bus_envelopes: CounterFamily<WsBusDirection>,
    pub(super) ws_bus_parse_failures: Counter,
    pub(super) ws_bus_failures: CounterFamily<WsBusFailureKind>,
    pub(super) ws_bus_client_frames: CounterFamily<WsBusClientFrameKind>,
    pub(super) ws_outbound_queued_messages: UpDownCounter,
    pub(super) ws_outbound_queue_overflows: Counter,
    pub(super) ws_handshake_duration: Histogram<ControlPlaneDurationBucket>,
    pub(super) ws_auth_duration: Histogram<ControlPlaneDurationBucket>,
    pub(super) ws_user_initialize_duration: Histogram<ControlPlaneDurationBucket>,
    pub(super) active_rooms: UpDownCounter,
    pub(super) active_users: UpDownCounter,
    pub(super) active_publications: UpDownCounter,
    pub(super) active_subscriptions: UpDownCounter,
    pub(super) active_recording_rooms: UpDownCounter,
    pub(super) active_transport_users: UpDownCounter,
    pub(super) transport_health_users: UpDownCounterFamily<TransportHealthState>,
    pub(super) recording_actions: CounterFamily<RecordingActionOutcome>,
    pub(super) recording_captured_packets: Counter,
    pub(super) recording_captured_streams: Counter,
    pub(super) rtp_metrics: RtpMetrics,
    pub(super) rtc_metrics: RtcMetrics,
    pub(super) rtp_relay_overload_drops: CounterFamily<RtpRelayDropKind>,
    pub(super) transport_health_transitions: CounterFamily<TransportHealthTransition>,
    pub(super) transport_ice_state_changes: CounterFamily<TransportIceState>,
    pub(super) transport_dtls_connected: Counter,
    pub(super) transport_user_lifetime_buckets: CounterFamily<TransportUserLifetimeBucket>,
    pub(super) transport_user_lifetime_count: Counter,
    pub(super) transport_user_lifetime_sum_micros: Counter,
    pub(super) media_quality_samples: CounterFamily<MediaQualitySample>,
    pub(super) media_quality_rtt: HistogramFamily<MediaQualitySample, MediaQualityRttBucket>,
    pub(super) media_quality_loss_ppm_observed: CounterFamily<MediaQualityLossDirection>,
    pub(super) media_quality_loss_observations: CounterFamily<MediaQualityLossDirection>,
    pub(super) media_quality_bwe_bps_observed: Counter,
    pub(super) media_quality_bwe_observations: Counter,
    pub(super) media_quality_jitter_rtp_timestamp_units_observed: Counter,
    pub(super) media_quality_jitter_observations: Counter,
    pub(super) transport_cleanup_retries: Counter,
    pub(super) transport_cleanup_retry_successes: Counter,
    pub(super) transport_cleanup_failures: CounterFamily<TransportCleanupFailureKind>,
    pub(super) source_selection_updates: CounterFamily<SourceSelectionKind>,
    pub(super) budget_solver_outcomes: CounterFamily<BudgetSolverOutcome>,
}

impl RuntimeMetrics {
    pub fn record_http_noop_request(&self) {
        self.http_requests.increment(HttpRoute::Noop);
    }

    pub fn record_http_stats_request(&self) {
        self.http_requests.increment(HttpRoute::Stats);
    }

    pub fn record_http_metrics_request(&self) {
        self.http_requests.increment(HttpRoute::Metrics);
    }

    pub fn record_http_room_request(&self) {
        self.http_requests.increment(HttpRoute::Room);
    }

    pub fn record_http_room_success(&self) {
        self.http_room_responses
            .increment(HttpRoomResponseStatus::Success);
    }

    pub fn record_http_room_unauthorized(&self) {
        self.http_room_responses
            .increment(HttpRoomResponseStatus::Unauthorized);
    }

    pub fn record_http_room_forbidden(&self) {
        self.http_room_responses
            .increment(HttpRoomResponseStatus::Forbidden);
    }

    pub fn record_http_room_bad_request(&self) {
        self.http_room_responses
            .increment(HttpRoomResponseStatus::BadRequest);
    }

    pub fn record_http_disconnect_request(&self) {
        self.http_requests.increment(HttpRoute::Disconnect);
    }

    pub fn record_http_disconnect_success(&self) {
        self.http_disconnect_responses
            .increment(HttpDisconnectResponseStatus::Success);
    }

    pub fn record_http_disconnect_bad_request(&self) {
        self.http_disconnect_responses
            .increment(HttpDisconnectResponseStatus::BadRequest);
    }

    pub fn record_http_disconnect_unprocessable_entity(&self) {
        self.http_disconnect_responses
            .increment(HttpDisconnectResponseStatus::UnprocessableEntity);
    }

    pub fn add_http_inflight_requests(&self, route: HttpRoute, delta: i64) {
        self.http_inflight_requests.add(route, delta);
    }

    pub fn record_http_request_duration(&self, route: HttpRoute, duration: Duration) {
        self.http_request_duration.observe(route, duration);
    }

    pub fn record_ws_connection_accepted(&self) {
        self.ws_connections.increment(WsConnectionStage::Accepted);
    }

    pub fn record_ws_handshake_credentials_received(&self) {
        self.ws_connections
            .increment(WsConnectionStage::CredentialsReceived);
    }

    pub fn record_ws_handshake_rejection(&self, close_code: Option<WebSocketCloseCode>) {
        match close_code {
            Some(
                close_code @ (WebSocketCloseCode::AuthTimeout
                | WebSocketCloseCode::AuthFailed
                | WebSocketCloseCode::ProtocolError
                | WebSocketCloseCode::RoomFull),
            ) => self.ws_handshake_rejections.increment(close_code),
            Some(
                WebSocketCloseCode::Error
                | WebSocketCloseCode::Clean
                | WebSocketCloseCode::Leaving
                | WebSocketCloseCode::Kicked,
            )
            | None => self.ws_handshake_rejections_other.increment(),
        }
    }

    pub fn record_ws_user_joined(&self) {
        self.ws_connections.increment(WsConnectionStage::Joined);
    }

    pub fn record_ws_startup_send_failure(&self) {
        self.ws_startup_failures
            .increment(WsStartupFailureKind::StartupSend);
    }

    pub fn record_ws_user_initialize_failure(&self) {
        self.ws_startup_failures
            .increment(WsStartupFailureKind::SessionInitialize);
    }

    pub fn record_ws_user_loop_started(&self) {
        self.ws_user_loops_started.increment();
    }

    pub fn record_ws_user_loop_exit(&self, reason: WsSessionLoopExitReason) {
        self.ws_user_loop_exits.increment(reason);
    }

    pub fn record_ws_bus_batch_received(&self, envelope_count: usize) {
        self.ws_bus_batches.increment(WsBusDirection::Received);
        self.ws_bus_envelopes
            .add(WsBusDirection::Received, envelope_count);
    }

    pub fn record_ws_bus_invalid_input_failure(&self) {
        self.ws_bus_parse_failures.increment();
        self.ws_bus_failures
            .increment(WsBusFailureKind::InvalidInput);
    }

    pub fn record_ws_bus_unsupported_feature_failure(&self) {
        self.ws_bus_parse_failures.increment();
        self.ws_bus_failures
            .increment(WsBusFailureKind::UnsupportedFeature);
    }

    pub fn record_ws_bus_client_request(&self) {
        self.ws_bus_client_frames
            .increment(WsBusClientFrameKind::Request);
    }

    pub fn record_ws_bus_client_message(&self) {
        self.ws_bus_client_frames
            .increment(WsBusClientFrameKind::Message);
    }

    pub fn record_ws_bus_batch_sent(&self, envelope_count: usize) {
        self.record_ws_bus_batches_sent(1, envelope_count);
    }

    pub fn record_ws_bus_batches_sent(&self, batch_count: usize, envelope_count: usize) {
        if batch_count == 0 {
            return;
        }
        self.ws_bus_batches.add(WsBusDirection::Sent, batch_count);
        self.ws_bus_envelopes
            .add(WsBusDirection::Sent, envelope_count);
    }

    pub fn record_ws_bus_send_failure(&self) {
        self.ws_bus_failures.increment(WsBusFailureKind::Send);
    }

    pub fn add_ws_outbound_queued_messages(&self, delta: i64) {
        self.ws_outbound_queued_messages.add(delta);
    }

    pub fn record_ws_outbound_queue_overflow(&self) {
        self.ws_outbound_queue_overflows.increment();
    }

    pub fn record_ws_handshake_duration(&self, duration: Duration) {
        self.ws_handshake_duration.observe(duration);
    }

    pub fn record_ws_auth_duration(&self, duration: Duration) {
        self.ws_auth_duration.observe(duration);
    }

    pub fn record_ws_user_initialize_duration(&self, duration: Duration) {
        self.ws_user_initialize_duration.observe(duration);
    }

    pub fn add_active_rooms(&self, delta: i64) {
        self.active_rooms.add(delta);
    }

    pub fn add_active_users(&self, delta: i64) {
        self.active_users.add(delta);
    }

    pub fn add_active_publications(&self, delta: i64) {
        self.active_publications.add(delta);
    }

    pub fn add_active_subscriptions(&self, delta: i64) {
        self.active_subscriptions.add(delta);
    }

    pub fn add_active_recording_rooms(&self, delta: i64) {
        self.active_recording_rooms.add(delta);
    }

    pub fn add_active_transport_users(&self, delta: i64) {
        self.active_transport_users.add(delta);
    }

    pub fn record_transport_health_transition(
        &self,
        previous: Option<TransportHealthState>,
        next: Option<TransportHealthState>,
    ) {
        if previous == next {
            return;
        }
        match (previous, next) {
            (None, Some(TransportHealthState::Connected)) => self
                .transport_health_transitions
                .increment(TransportHealthTransition::UnsetToConnected),
            (None, Some(TransportHealthState::Disconnected)) => self
                .transport_health_transitions
                .increment(TransportHealthTransition::UnsetToDisconnected),
            (Some(TransportHealthState::Connected), Some(TransportHealthState::Disconnected)) => {
                self.transport_health_transitions
                    .increment(TransportHealthTransition::ConnectedToDisconnected);
            }
            (Some(TransportHealthState::Disconnected), Some(TransportHealthState::Connected)) => {
                self.transport_health_transitions
                    .increment(TransportHealthTransition::DisconnectedToConnected);
            }
            (Some(TransportHealthState::Connected), None) => self
                .transport_health_transitions
                .increment(TransportHealthTransition::ConnectedToUnset),
            (Some(TransportHealthState::Disconnected), None) => self
                .transport_health_transitions
                .increment(TransportHealthTransition::DisconnectedToUnset),
            (None, None)
            | (Some(TransportHealthState::Connected), Some(TransportHealthState::Connected))
            | (
                Some(TransportHealthState::Disconnected),
                Some(TransportHealthState::Disconnected),
            ) => {}
        }
        if let Some(health) = previous {
            self.transport_health_users.add(health, -1);
        }
        if let Some(health) = next {
            self.transport_health_users.add(health, 1);
        }
    }

    pub fn record_recording_start_accepted(&self) {
        self.recording_actions
            .increment(RecordingActionOutcome::StartAccepted);
    }

    pub fn record_recording_start_rejected(&self) {
        self.recording_actions
            .increment(RecordingActionOutcome::StartRejected);
    }

    pub fn record_recording_stop_accepted(&self) {
        self.recording_actions
            .increment(RecordingActionOutcome::StopAccepted);
    }

    pub fn record_recording_stop_rejected(&self) {
        self.recording_actions
            .increment(RecordingActionOutcome::StopRejected);
    }

    pub fn record_recording_captured_packet(&self) {
        self.recording_captured_packets.increment();
    }

    pub fn record_recording_captured_stream(&self) {
        self.recording_captured_streams.increment();
    }

    /// Registers one worker-local recorder for hot RTP packet metrics.
    ///
    /// The caller should keep the returned handle beside the packet loop and
    /// reuse it for every RTP packet observation owned by that worker.
    pub fn register_rtp_worker(&self) -> Arc<RtpMetricsRecorder> {
        self.rtp_metrics.register_worker(None)
    }

    /// Registers one worker-local recorder for a known media worker.
    ///
    /// The media-worker id is exported only as a bounded worker label. Runtime
    /// code must not pass room or user identity here.
    pub fn register_rtp_worker_for_media_worker(
        &self,
        media_worker_id: usize,
    ) -> Arc<RtpMetricsRecorder> {
        self.rtp_metrics.register_worker(Some(media_worker_id))
    }

    /// Registers one worker-local recorder for RTC packet-loop metrics.
    ///
    /// The caller should keep the returned handle beside the packet loop and
    /// reuse it for UDP datagram and route-control observations owned by that
    /// worker.
    pub fn register_rtc_worker(&self) -> Arc<RtcMetricsRecorder> {
        self.rtc_metrics.register_worker()
    }

    pub fn record_rtp_ingress(&self, payload_bytes: usize) {
        self.rtp_metrics.record_ingress(payload_bytes);
    }

    pub fn record_rtp_egress(&self, payload_bytes: usize) {
        self.rtp_metrics.record_egress(payload_bytes);
    }

    pub fn record_rtp_forwarded(
        &self,
        destination: RtpForwardDestinationKind,
        payload_bytes: usize,
    ) {
        self.rtp_metrics
            .record_forwarded(destination, payload_bytes);
    }

    pub fn record_rtp_decoder_refresh(&self, scope: RtpDecoderRefreshScope) {
        self.rtp_metrics.record_decoder_refresh(scope);
    }

    pub fn record_rtp_relay_overload_drop(&self, destination: RtpRelayDropKind) {
        self.rtp_relay_overload_drops.increment(destination);
    }

    pub fn record_transport_ice_state_change(&self, state: TransportIceState) {
        self.transport_ice_state_changes.increment(state);
    }

    pub fn record_transport_dtls_connected(&self) {
        self.transport_dtls_connected.increment();
    }

    pub fn record_transport_user_lifetime(&self, duration: Duration) {
        self.transport_user_lifetime_count.increment();
        self.transport_user_lifetime_sum_micros
            .add_u64(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX));
        if duration <= Duration::from_secs(1) {
            self.transport_user_lifetime_buckets
                .increment(TransportUserLifetimeBucket::Le1Second);
        }
        if duration <= Duration::from_secs(10) {
            self.transport_user_lifetime_buckets
                .increment(TransportUserLifetimeBucket::Le10Seconds);
        }
        if duration <= Duration::from_mins(1) {
            self.transport_user_lifetime_buckets
                .increment(TransportUserLifetimeBucket::Le60Seconds);
        }
        if duration <= Duration::from_mins(5) {
            self.transport_user_lifetime_buckets
                .increment(TransportUserLifetimeBucket::Le300Seconds);
        }
    }

    pub fn record_media_quality_sample(&self, sample: MediaQualitySample) {
        self.media_quality_samples.increment(sample);
    }

    pub fn record_media_quality_rtt(&self, sample: MediaQualitySample, duration: Duration) {
        self.media_quality_rtt.observe(sample, duration);
    }

    pub fn record_media_quality_loss_ppm(
        &self,
        direction: MediaQualityLossDirection,
        loss_ppm: u64,
    ) {
        self.media_quality_loss_ppm_observed
            .add_u64(direction, loss_ppm);
        self.media_quality_loss_observations.increment(direction);
    }

    pub fn record_media_quality_bwe_bps(&self, bwe_bps: u64) {
        self.media_quality_bwe_bps_observed.add_u64(bwe_bps);
        self.media_quality_bwe_observations.increment();
    }

    pub fn record_media_quality_jitter_rtp_timestamp_units(&self, jitter: u64) {
        self.media_quality_jitter_rtp_timestamp_units_observed
            .add_u64(jitter);
        self.media_quality_jitter_observations.increment();
    }

    pub fn record_transport_cleanup_retry_scheduled(&self) {
        self.transport_cleanup_retries.increment();
    }

    pub fn record_transport_cleanup_retry_succeeded(&self) {
        self.transport_cleanup_retry_successes.increment();
    }

    pub fn record_transport_cleanup_failure(&self, kind: TransportCleanupFailureKind) {
        self.transport_cleanup_failures.increment(kind);
    }

    pub fn record_rtc_datagram_route(&self, path: RtcDatagramRoutePath) {
        self.rtc_metrics.record_datagram_route(path);
    }

    pub fn record_rtc_datagram_drop(&self, reason: RtcDatagramDropReason) {
        self.rtc_metrics.record_datagram_drop(reason);
    }

    pub fn record_rtc_datagram_fallback_scan(&self, examined_sessions: usize) {
        self.rtc_metrics
            .record_datagram_fallback_scan(examined_sessions);
    }

    pub fn record_rtc_route_control(&self, outcome: RtcRouteControlOutcome) {
        self.rtc_metrics.record_route_control(outcome);
    }

    pub fn record_rtc_keyframe_request(&self, outcome: RtcKeyframeRequestOutcome) {
        self.rtc_metrics.record_keyframe_request(outcome);
    }

    pub fn record_rtc_relay_enqueue(&self, result: RtcRelayEnqueueResult) {
        self.rtc_metrics.record_relay_enqueue(result);
    }

    pub fn record_rtc_relay_mailbox_depth(&self, depth: usize) {
        self.rtc_metrics.record_relay_mailbox_depth(depth);
    }

    pub fn record_rtc_relay_drain_batch(&self, drained_packets: usize, cap_hit: bool) {
        self.rtc_metrics
            .record_relay_drain_batch(drained_packets, cap_hit);
    }

    pub fn record_rtc_remote_control_drop(&self, kind: RtcRemoteControlDropKind) {
        self.rtc_metrics.record_remote_control_drop(kind);
    }

    pub fn record_rtc_remote_packet_gate_convergence(
        &self,
        outcome: RtcRemotePacketGateConvergence,
    ) {
        self.rtc_metrics
            .record_remote_packet_gate_convergence(outcome);
    }

    pub fn record_source_selection_update(&self, selector: SourceSelectionKind) {
        self.source_selection_updates.increment(selector);
    }

    pub fn record_budget_solver_outcome(&self, outcome: BudgetSolverOutcome) {
        self.budget_solver_outcomes.increment(outcome);
    }
}

impl RtcRouteControlMetrics for RuntimeMetrics {
    fn record_rtc_route_control(&self, outcome: RtcRouteControlOutcome) {
        self.rtc_metrics.record_route_control(outcome);
    }

    fn record_rtc_keyframe_request(&self, outcome: RtcKeyframeRequestOutcome) {
        self.rtc_metrics.record_keyframe_request(outcome);
    }
}
