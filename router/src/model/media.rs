#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    Audio,
    Camera,
    Screen,
}
