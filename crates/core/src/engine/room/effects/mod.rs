//! shared post-lock effect batch executor for room transitions

pub mod batch;
mod observability;
mod output;
mod receiver_route;
pub(in crate::engine::room) mod transport;

pub(in crate::engine::room) use observability::RoomGaugeDelta;
