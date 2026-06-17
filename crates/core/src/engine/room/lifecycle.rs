use crate::engine::UserPermissions;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoomUserPermissions;

impl From<UserPermissions> for RoomUserPermissions {
    fn from(_value: UserPermissions) -> Self {
        Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserCloseReason {
    Replaced,
    RemovedByRuntime,
}
