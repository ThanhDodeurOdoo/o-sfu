use std::{net::SocketAddr, str};

use axum::{
    body::Bytes,
    http::{HeaderMap, header},
};

use crate::{
    application::rooms::{CreateRoomRequest, Room},
    config::Config,
    runtime::{
        RuntimeState,
        auth::{self, HttpDisconnectClaims, HttpRoomClaims},
        http_server::contract::CreateRoomQuery,
        request_origin::{resolve_remote_address, trusted_forwarded_header},
    },
};

pub(super) struct CreateRoomContext<'a> {
    pub(super) headers: &'a HeaderMap,
    pub(super) connect_address: Option<SocketAddr>,
    pub(super) query: &'a CreateRoomQuery,
}

pub(super) struct CreatedRoom {
    pub(super) uuid: String,
    pub(super) base_url: String,
}

pub(super) enum CreateRoomError {
    Unauthorized,
    Forbidden,
    BadRequest,
}

pub(super) async fn verify_and_get_room(
    state: &RuntimeState,
    context: CreateRoomContext<'_>,
) -> Result<CreatedRoom, CreateRoomError> {
    let Some(token) = authorization_token(context.headers) else {
        return Err(CreateRoomError::Unauthorized);
    };
    let Ok(claims) = auth::verify::<HttpRoomClaims>(token, &state.config.auth_key) else {
        return Err(CreateRoomError::Unauthorized);
    };
    let Some(issuer) = claims.registered.iss.as_deref() else {
        return Err(CreateRoomError::Forbidden);
    };
    if context.query.recording_address.is_some() && claims.key.is_none() {
        return Err(CreateRoomError::BadRequest);
    }
    let remote_address =
        resolve_remote_address(context.headers, &state.config, context.connect_address);
    let room = state
        .rooms
        .create_or_get(CreateRoomRequest {
            issuer,
            key: claims.key.as_deref(),
            web_rtc_enabled: context.query.web_rtc_enabled(),
            recording_address: context.query.recording_address.clone(),
            remote_address: Some(remote_address),
        })
        .await;
    Ok(CreatedRoom {
        uuid: room.uuid,
        base_url: request_base_url(context.headers, &state.config),
    })
}

pub(super) enum DisconnectError {
    BadRequest,
    UnprocessableEntity,
}

pub(super) async fn disconnect_users(
    rooms: &Room,
    config: &Config,
    body: &Bytes,
) -> Result<(), DisconnectError> {
    let Ok(token) = str::from_utf8(body) else {
        return Err(DisconnectError::BadRequest);
    };
    let Ok(claims) = auth::verify::<HttpDisconnectClaims>(token, &config.auth_key) else {
        return Err(DisconnectError::UnprocessableEntity);
    };
    rooms.disconnect_users(&claims.user_ids_by_room).await;
    Ok(())
}

pub(super) fn authorization_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' ').map(|(_, token)| token))
}

pub(crate) fn request_base_url(headers: &HeaderMap, config: &Config) -> String {
    let scheme = trusted_forwarded_header(headers, config, "x-forwarded-proto").unwrap_or("http");
    let host = trusted_forwarded_header(headers, config, "x-forwarded-host")
        .map(str::to_owned)
        .or_else(|| {
            headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| config.bind_address.to_string());
    format!("{scheme}://{host}")
}
