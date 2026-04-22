use std::{net::SocketAddr, str};

use axum::{
    body::Bytes,
    http::{HeaderMap, header},
};

use crate::{
    config::Config,
    runtime::{
        RuntimeState,
        auth::{self, HttpChannelClaims, HttpDisconnectClaims},
        channel::ChannelConfig,
        http_server::contract::CreateChannelQuery,
        request_origin::{resolve_remote_address, trusted_forwarded_header},
    },
};

pub(super) struct CreateChannelContext<'a> {
    pub(super) headers: &'a HeaderMap,
    pub(super) connect_address: Option<SocketAddr>,
    pub(super) query: &'a CreateChannelQuery,
}

pub(super) struct CreatedChannel {
    pub(super) uuid: String,
    pub(super) base_url: String,
}

pub(super) enum CreateChannelError {
    Unauthorized,
    Forbidden,
    BadRequest,
}

pub(super) async fn verify_and_get_channel(
    state: &RuntimeState,
    context: CreateChannelContext<'_>,
) -> Result<CreatedChannel, CreateChannelError> {
    let Some(token) = authorization_token(context.headers) else {
        return Err(CreateChannelError::Unauthorized);
    };
    let Ok(claims) = auth::verify::<HttpChannelClaims>(token, &state.config.auth_key) else {
        return Err(CreateChannelError::Unauthorized);
    };
    let Some(issuer) = claims.registered.iss.as_deref() else {
        return Err(CreateChannelError::Forbidden);
    };
    if context.query.recording_address.is_some() && claims.key.is_none() {
        return Err(CreateChannelError::BadRequest);
    }
    let remote_address =
        resolve_remote_address(context.headers, &state.config, context.connect_address);
    let channel = state
        .channel_manager
        .serve_channel(
            issuer,
            claims.key.as_deref(),
            &ChannelConfig {
                web_rtc_enabled: context.query.web_rtc_enabled(),
                recording_address: context.query.recording_address.clone(),
            },
            Some(&remote_address),
        )
        .await;
    Ok(CreatedChannel {
        uuid: channel.uuid().to_owned(),
        base_url: request_base_url(context.headers, &state.config),
    })
}

pub(super) enum DisconnectError {
    BadRequest,
    UnprocessableEntity,
}

pub(super) async fn disconnect_sessions(
    state: &RuntimeState,
    body: &Bytes,
) -> Result<(), DisconnectError> {
    let Ok(token) = str::from_utf8(body) else {
        return Err(DisconnectError::BadRequest);
    };
    let Ok(claims) = auth::verify::<HttpDisconnectClaims>(token, &state.config.auth_key) else {
        return Err(DisconnectError::UnprocessableEntity);
    };
    for (channel_uuid, session_ids) in &claims.session_ids_by_channel {
        state
            .channel_manager
            .disconnect_sessions(channel_uuid, session_ids, &state.transport_adapter)
            .await;
    }
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
