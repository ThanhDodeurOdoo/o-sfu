use crate::Bitrate;

/// Latest receiver-level budget facts attached to a source selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiverVideoBudgetDiagnostics {
    latest_receiver_bandwidth: Option<Bitrate>,
    selected_video_budget: Option<Bitrate>,
    active_video_route_count: usize,
    selected_video_bitrate: Bitrate,
}

impl ReceiverVideoBudgetDiagnostics {
    #[must_use]
    pub const fn new(
        latest_receiver_bandwidth: Option<Bitrate>,
        selected_video_budget: Option<Bitrate>,
        active_video_route_count: usize,
        selected_video_bitrate: Bitrate,
    ) -> Self {
        Self {
            latest_receiver_bandwidth,
            selected_video_budget,
            active_video_route_count,
            selected_video_bitrate,
        }
    }

    #[must_use]
    pub const fn latest_receiver_bandwidth(self) -> Option<Bitrate> {
        self.latest_receiver_bandwidth
    }

    #[must_use]
    pub const fn selected_video_budget(self) -> Option<Bitrate> {
        self.selected_video_budget
    }

    #[must_use]
    pub const fn active_video_route_count(self) -> usize {
        self.active_video_route_count
    }

    #[must_use]
    pub const fn selected_video_bitrate(self) -> Bitrate {
        self.selected_video_bitrate
    }
}
