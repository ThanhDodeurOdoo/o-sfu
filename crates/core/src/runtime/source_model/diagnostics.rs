/// Reason selected video is allowed to exceed the receiver bandwidth estimate.
///
/// # Example situation
///
/// After thumbnails and hidden routes have been degraded or paused, a pinned or
/// readable-detail route can still exceed BWE. Diagnostics use this reason to
/// show that the over-budget state came from protected room policy rather than
/// from a missing pause decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverBudgetExceptionReason {
    /// The remaining over-budget routes are protected by room policy.
    ///
    /// This means the planner already degraded or paused every non-protected
    /// route it could, but protected routes still exceed the latest BWE.
    ProtectedRoute,
}

/// Latest receiver-level budget facts attached to a source selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiverVideoBudgetDiagnostics {
    latest_receiver_bandwidth_bps: Option<u64>,
    selected_video_budget_bps: Option<u64>,
    active_video_route_count: usize,
    selected_video_bitrate_bps: u64,
    over_budget_exception_reason: Option<OverBudgetExceptionReason>,
}

impl ReceiverVideoBudgetDiagnostics {
    #[must_use]
    pub const fn new(
        latest_receiver_bandwidth_bps: Option<u64>,
        selected_video_budget_bps: Option<u64>,
        active_video_route_count: usize,
        selected_video_bitrate_bps: u64,
        over_budget_exception_reason: Option<OverBudgetExceptionReason>,
    ) -> Self {
        Self {
            latest_receiver_bandwidth_bps,
            selected_video_budget_bps,
            active_video_route_count,
            selected_video_bitrate_bps,
            over_budget_exception_reason,
        }
    }

    #[must_use]
    pub const fn latest_receiver_bandwidth_bps(self) -> Option<u64> {
        self.latest_receiver_bandwidth_bps
    }

    #[must_use]
    pub const fn selected_video_budget_bps(self) -> Option<u64> {
        self.selected_video_budget_bps
    }

    #[must_use]
    pub const fn active_video_route_count(self) -> usize {
        self.active_video_route_count
    }

    #[must_use]
    pub const fn selected_video_bitrate_bps(self) -> u64 {
        self.selected_video_bitrate_bps
    }

    #[must_use]
    pub const fn over_budget_exception_reason(self) -> Option<OverBudgetExceptionReason> {
        self.over_budget_exception_reason
    }
}
