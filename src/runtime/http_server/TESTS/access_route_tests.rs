use std::net::SocketAddr;

use super::fixtures::*;

const OPERATOR_ROUTES: [&str; 2] = [route::v1::STATS, route::METRICS];

#[tokio::test]
async fn stats_and_metrics_preserve_method_not_allowed() -> TestResult {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));

    for path in OPERATOR_ROUTES {
        route_status(
            &state,
            Request::post(path),
            Body::empty(),
            StatusCode::METHOD_NOT_ALLOWED,
            path,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn stats_and_metrics_require_configured_token_on_loopback_listener() -> TestResult {
    let mut state = test_state();
    state.config.diagnostics.auth_token = Some(String::from("operator-secret"));

    for path in OPERATOR_ROUTES {
        route_status(
            &state,
            Request::get(path),
            Body::empty(),
            StatusCode::UNAUTHORIZED,
            path,
        )
        .await?;
        route_status(
            &state,
            Request::get(path).header(header::AUTHORIZATION, "Bearer wrong-secret"),
            Body::empty(),
            StatusCode::UNAUTHORIZED,
            path,
        )
        .await?;
        route_status(
            &state,
            Request::get(path).header(header::AUTHORIZATION, "Bearer operator-secret"),
            Body::empty(),
            StatusCode::OK,
            path,
        )
        .await?;
    }
    Ok(())
}
