use super::{
    PolicyPauseReason, ReceiverVideoBudgetDiagnostics, SourceEncodingId, SourceOperatingPoint,
};
use crate::Bitrate;

/// Resolved packet-selection command for one consumer/source route.
///
/// The budget planner writes selectors into room state. A later projection step
/// turns them into transport packet gates such as "open" or "forward this RID".
/// # Example situations
///
/// [`Self::Open`] means the route has no source-level packet gate.
/// [`Self::Encoding`] means "forward the negotiated RID for this encoding".
/// [`Self::OperatingPoint`] means "forward this encoding up to this temporal
/// layer".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceSelector {
    /// Forward the source without a source-level packet gate.
    ///
    /// This is the default for sources that are not controlled by receiver-video
    /// adaptation or when the planner has not selected a narrower gate.
    #[default]
    Open,
    /// Forward only one advertised source encoding.
    ///
    /// Projection maps the encoding id to its negotiated RID. If the encoding
    /// has no RID, projection fails rather than guessing at packet identity.
    Encoding(SourceEncodingId),
    /// Forward one encoding up to a codec-native temporal layer ceiling.
    ///
    /// Projection requires advertised temporal metadata and rejects a selector
    /// whose temporal ceiling is higher than the source declared.
    #[allow(
        dead_code,
        reason = "operating-point selectors stay internal until RFC 9626 metadata negotiation is implemented"
    )]
    OperatingPoint(SourceOperatingPoint),
}

impl SourceSelector {
    #[must_use]
    pub const fn selected_encoding(self) -> Option<SourceEncodingId> {
        match self {
            Self::Encoding(encoding_id) => Some(encoding_id),
            Self::OperatingPoint(operating_point) => Some(operating_point.encoding_id()),
            Self::Open => None,
        }
    }

    #[must_use]
    pub const fn selected_operating_point(self) -> Option<SourceOperatingPoint> {
        match self {
            Self::OperatingPoint(operating_point) => Some(operating_point),
            Self::Open | Self::Encoding(_) => None,
        }
    }
}

/// Consumer-side desired state for one published source.
///
/// The active flag is the compatibility-level subscription decision. The
/// selector is the source-level quality intent that later adaptation and
/// layout policy can resolve into a transport-native gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerSourceSelection {
    active: bool,
    selector: SourceSelector,
    policy_pause_reason: Option<PolicyPauseReason>,
    budget: ReceiverVideoBudgetDiagnostics,
    pressure_observations: u8,
    upgrade_observations: u8,
}

impl ConsumerSourceSelection {
    #[must_use]
    pub const fn open(active: bool) -> Self {
        Self {
            active,
            selector: SourceSelector::Open,
            policy_pause_reason: None,
            budget: ReceiverVideoBudgetDiagnostics::new(None, None, 0, Bitrate::zero(), None),
            pressure_observations: 0,
            upgrade_observations: 0,
        }
    }

    #[must_use]
    pub const fn active(self) -> bool {
        self.active
    }

    #[must_use]
    pub const fn selector(self) -> SourceSelector {
        self.selector
    }

    #[must_use]
    pub const fn policy_pause_reason(self) -> Option<PolicyPauseReason> {
        self.policy_pause_reason
    }

    #[must_use]
    pub const fn policy_allows_delivery(self) -> bool {
        self.policy_pause_reason.is_none()
    }

    /// Returns whether this receiver selection currently permits packet delivery.
    ///
    /// Use this for route-state projections, load accounting and keyframe
    /// targeting. Source-policy planners should read [`Self::active`] so
    /// policy-paused routes can be resumed.
    #[must_use]
    pub const fn delivery_active(self) -> bool {
        self.active && self.policy_allows_delivery()
    }

    #[must_use]
    pub const fn budget(self) -> ReceiverVideoBudgetDiagnostics {
        self.budget
    }

    #[must_use]
    pub const fn pressure_observations(self) -> u8 {
        self.pressure_observations
    }

    #[must_use]
    pub const fn upgrade_observations(self) -> u8 {
        self.upgrade_observations
    }

    pub const fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    pub const fn set_selector(&mut self, selector: SourceSelector) {
        self.selector = selector;
    }

    pub const fn set_policy_pause_reason(&mut self, reason: Option<PolicyPauseReason>) {
        self.policy_pause_reason = reason;
    }

    pub const fn set_budget(&mut self, budget: ReceiverVideoBudgetDiagnostics) {
        self.budget = budget;
    }

    pub const fn set_adaptation_observations(
        &mut self,
        pressure_observations: u8,
        upgrade_observations: u8,
    ) {
        self.pressure_observations = pressure_observations;
        self.upgrade_observations = upgrade_observations;
    }
}
