//! shared post-lock effect batch executor for room transitions

pub mod batch;
mod observability;
mod output;
mod receiver_route;
mod transport;

pub(super) use transport::RoomRouteBatch;
