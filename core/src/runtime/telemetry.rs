pub mod schema {
    pub mod event {
        pub const ROOM_CREATED: &str = "room.created";
        pub const USER_JOINED: &str = "user.joined";
        pub const USER_CLOSED: &str = "user.closed";
        pub const USER_DISCONNECTED: &str = "user.disconnected";
        pub const NEGOTIATION_STARTED: &str = "negotiation.started";
        pub const NEGOTIATION_SUCCEEDED: &str = "negotiation.succeeded";
        pub const NEGOTIATION_FAILED: &str = "negotiation.failed";
        pub const PUBLISH_PREPARED: &str = "publish.prepared";
        pub const PUBLISH_COMMITTED: &str = "publish.committed";
        pub const PUBLISH_ABORTED: &str = "publish.aborted";
        pub const SUBSCRIBE_PREPARED: &str = "subscribe.prepared";
        pub const SUBSCRIBE_SUCCEEDED: &str = "subscribe.succeeded";
        pub const SUBSCRIPTION_ACTIVITY_CHANGED: &str = "subscription.activity_changed";
        pub const PUBLICATION_ACTIVITY_CHANGED: &str = "publication.activity_changed";
        pub const TRANSPORT_HEALTH_CHANGED: &str = "transport.health.changed";
        pub const RECORDING_STARTED: &str = "recording.started";
        pub const RECORDING_STOPPED: &str = "recording.stopped";
    }
}
