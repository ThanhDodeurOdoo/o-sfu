use crate::engine::media_transport::{ConsumerActivity, MediaTransport, TransportConsumerRoute};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerRouteEffectOutcome {
    pub activity_failed: bool,
    pub keyframe_failed: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ConsumerRouteEffect<'a> {
    route: &'a TransportConsumerRoute,
    activity: Option<bool>,
    keyframe: bool,
}

impl<'a> ConsumerRouteEffect<'a> {
    pub const fn new(route: &'a TransportConsumerRoute) -> Self {
        Self {
            route,
            activity: None,
            keyframe: false,
        }
    }

    pub const fn with_activity(mut self, active: bool) -> Self {
        self.activity = Some(active);
        self
    }

    pub const fn with_activity_if(mut self, apply: bool, active: bool) -> Self {
        if apply {
            self.activity = Some(active);
        }
        self
    }

    pub const fn with_keyframe(mut self, keyframe: bool) -> Self {
        self.keyframe = keyframe;
        self
    }

    pub async fn execute(self, media_transport: &MediaTransport) -> ConsumerRouteEffectOutcome {
        let activity_failed = match self.activity {
            Some(active) => media_transport
                .set_consumer_active(self.route, ConsumerActivity::from_active(active))
                .await
                .is_err(),
            None => false,
        };
        let keyframe_failed = !activity_failed
            && self.keyframe
            && media_transport
                .request_consumer_keyframe(self.route)
                .await
                .is_err();
        ConsumerRouteEffectOutcome {
            activity_failed,
            keyframe_failed,
        }
    }
}
