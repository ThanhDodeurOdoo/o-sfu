//! shared post-lock effect batch executor for room transitions

pub mod batch;
mod observability;
mod output;
mod receiver_routes;
mod route;
mod transport;

pub(super) use route::RoomRouteEffects;
