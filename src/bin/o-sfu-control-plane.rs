use std::{env, net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use o_sfu_cluster::{
    ClusterControlPlane, ClusterRoomId, ControlPlaneError, DeclareFailoverRequest,
    RegisterNodeRequest, RenewLeaseRequest, ReportRoomHealthRequest, ResolveRoomRequest,
    RoomAssignmentSource, RoomDirectoryError, RoomResolution, TopologyVersion, TopologyWatchUpdate,
    node::NodeRegistryError,
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::Mutex};

const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8071";

#[derive(Debug, Clone)]
struct ControlPlaneState {
    control_plane: Arc<Mutex<ClusterControlPlane>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ResolveRoomResponse {
    resolution: RoomResolution,
    source: RoomAssignmentSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct WatchRoomTopologyRequest {
    room_id: ClusterRoomId,
    after: Option<TopologyVersion>,
}

#[derive(Debug)]
struct ControlPlaneHttpError(ControlPlaneError);

impl IntoResponse for ControlPlaneHttpError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            ControlPlaneError::NodeRegistry(NodeRegistryError::UnknownNode)
            | ControlPlaneError::RoomDirectory(RoomDirectoryError::UnknownOwner)
            | ControlPlaneError::MissingTopology => StatusCode::NOT_FOUND,
            ControlPlaneError::NodeRegistry(NodeRegistryError::StaleLease)
            | ControlPlaneError::RoomDirectory(
                RoomDirectoryError::StaleOwner
                | RoomDirectoryError::StaleEpoch
                | RoomDirectoryError::StaleLease,
            )
            | ControlPlaneError::StaleHealthReporter => StatusCode::CONFLICT,
            ControlPlaneError::RoomDirectory(RoomDirectoryError::NoSchedulableOwner) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            ControlPlaneError::RoomDirectory(
                RoomDirectoryError::EpochOverflow | RoomDirectoryError::TopologyVersionOverflow,
            ) => StatusCode::INSUFFICIENT_STORAGE,
        };
        (
            status,
            Json(ErrorResponse {
                error: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

impl From<ControlPlaneError> for ControlPlaneHttpError {
    fn from(value: ControlPlaneError) -> Self {
        Self(value)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let bind_address = control_plane_bind_address()?;
    let listener = TcpListener::bind(bind_address).await?;
    axum::serve(listener, app(ControlPlaneState::default())).await?;
    Ok(())
}

impl Default for ControlPlaneState {
    fn default() -> Self {
        Self {
            control_plane: Arc::new(Mutex::new(ClusterControlPlane::default())),
        }
    }
}

fn app(state: ControlPlaneState) -> Router {
    Router::new()
        .route("/v1/cluster/nodes/register", post(register_node))
        .route("/v1/cluster/nodes/renew-lease", post(renew_lease))
        .route("/v1/cluster/rooms/resolve", post(resolve_room))
        .route("/v1/cluster/rooms/failover", post(declare_failover))
        .route("/v1/cluster/rooms/health", post(report_room_health))
        .route("/v1/cluster/rooms/topology", post(watch_room_topology))
        .with_state(state)
}

async fn register_node(
    State(state): State<ControlPlaneState>,
    Json(request): Json<RegisterNodeRequest>,
) -> Result<StatusCode, ControlPlaneHttpError> {
    {
        let mut control_plane = state.control_plane.lock().await;
        control_plane.register_node(request)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn renew_lease(
    State(state): State<ControlPlaneState>,
    Json(request): Json<RenewLeaseRequest>,
) -> Result<Json<o_sfu_cluster::NodeLease>, ControlPlaneHttpError> {
    let lease = {
        let mut control_plane = state.control_plane.lock().await;
        control_plane.renew_lease(&request)?
    };
    Ok(Json(lease))
}

async fn resolve_room(
    State(state): State<ControlPlaneState>,
    Json(request): Json<ResolveRoomRequest>,
) -> Result<Json<ResolveRoomResponse>, ControlPlaneHttpError> {
    let (resolution, source) = {
        let mut control_plane = state.control_plane.lock().await;
        control_plane.resolve_room(request)?
    };
    Ok(Json(ResolveRoomResponse { resolution, source }))
}

async fn declare_failover(
    State(state): State<ControlPlaneState>,
    Json(request): Json<DeclareFailoverRequest>,
) -> Result<Json<RoomResolution>, ControlPlaneHttpError> {
    let resolution = {
        let mut control_plane = state.control_plane.lock().await;
        control_plane.declare_failover(&request)?
    };
    Ok(Json(resolution))
}

async fn report_room_health(
    State(state): State<ControlPlaneState>,
    Json(request): Json<ReportRoomHealthRequest>,
) -> Result<StatusCode, ControlPlaneHttpError> {
    {
        let mut control_plane = state.control_plane.lock().await;
        control_plane.report_room_health(request)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn watch_room_topology(
    State(state): State<ControlPlaneState>,
    Json(request): Json<WatchRoomTopologyRequest>,
) -> Result<Json<TopologyWatchUpdate>, ControlPlaneHttpError> {
    let update = {
        let control_plane = state.control_plane.lock().await;
        control_plane.watch_room_topology(&request.room_id, request.after)?
    };
    Ok(Json(update))
}

fn control_plane_bind_address() -> Result<SocketAddr> {
    env::var("CONTROL_PLANE_BIND_ADDRESS")
        .unwrap_or_else(|_error| DEFAULT_BIND_ADDRESS.to_owned())
        .parse()
        .context("CONTROL_PLANE_BIND_ADDRESS must be a valid socket address")
}
