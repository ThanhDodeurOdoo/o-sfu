#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Audio,
    Video,
}

/// Application-level stream role.
/// This is an "odoo" convention
///
/// This is intentionally narrower than raw RTP metadata. The router uses it to
/// keep camera, screen, and audio flows distinct without importing protocol
/// shapes into the core state machine.
///
/// I think that later I will try to make something more generic because it's a bit "leaky"
/// since we have functional "odoo discuss" concepts deep in the router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    Audio,
    Camera,
    Screen,
}
