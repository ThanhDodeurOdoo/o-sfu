//! User-centered Grafana node graph formatting.
//!
//! The graph is intentionally centered on one selected user while still showing
//! the packet path through local media-worker ownership:
//!
//! ```text
//! Outbound from selected user:
//!
//!   selected user
//!        |
//!        v
//!   selected user's media worker
//!        |
//!        v
//!   published source
//!        |
//!        v
//!   receiver media worker
//!        |
//!        v
//!   peer user
//!
//! Inbound to selected user:
//!
//!   peer user
//!        |
//!        v
//!   peer media worker
//!        |
//!        v
//!   published source
//!        |
//!        v
//!   selected user's media worker
//!        |
//!        v
//!   selected user
//! ```

use std::collections::HashSet;

use serde_json::{Value, json};

use super::common::{
    download_main_stat, push_unique_edge, push_unique_node, route_state_color, route_state_label,
    source_by_id, stream_type_color, stream_type_label, transport_health_label, user_by_id,
    user_id_matches, user_id_to_string,
};
use crate::runtime::diagnostics::types::{
    DiagnosticsRoomDetail, DiagnosticsSource, DiagnosticsSubscription, DiagnosticsUserView,
};

fn path_user_node(room_uuid: &str, user: &DiagnosticsUserView, selected: bool) -> Value {
    let session_id = user_id_to_string(&user.user_id);
    let title = if selected {
        format!("{session_id} selected")
    } else {
        session_id
    };
    json!({
        "id": format!("user:{}:{}", room_uuid, user_id_to_string(&user.user_id)),
        "title": title,
        "subtitle": transport_health_label(user.transport.health.as_ref()),
        "mainStat": format!("{} bps", user.transport.quality_summary.current_incoming_bitrate.total),
        "secondaryStat": format!("{} pub / {} sub", user.publications.len(), user.subscriptions.len()),
        "detail__connection": user.transport.connection_id.to_string(),
        "detail__worker": user.transport.media_worker_id,
        "color": if selected { "blue" } else { "gray" },
    })
}

fn path_worker_node(detail: &DiagnosticsRoomDetail, media_worker_id: usize) -> Value {
    let users_on_worker = detail
        .users
        .iter()
        .filter(|user| user.transport.media_worker_id == media_worker_id);
    let mut user_count = 0_usize;
    let mut publication_count = 0_usize;
    let mut subscription_count = 0_usize;
    for user in users_on_worker {
        user_count = user_count.saturating_add(1);
        publication_count = publication_count.saturating_add(user.publications.len());
        subscription_count = subscription_count.saturating_add(user.subscriptions.len());
    }

    json!({
        "id": format!("worker:{}", media_worker_id),
        "title": format!("worker {}", media_worker_id),
        "subtitle": "media worker",
        "mainStat": format!("{} users", user_count),
        "secondaryStat": format!("{} pub / {} sub", publication_count, subscription_count),
    })
}

fn path_source_node(room_uuid: &str, source: &DiagnosticsSource) -> Value {
    let stream_type = stream_type_label(source.stream_type);
    json!({
        "id": format!("source:{}:{}", room_uuid, source.source_id),
        "title": format!("{} #{}", stream_type, source.source_id),
        "subtitle": format!("{:?}", source.media_kind).to_lowercase(),
        "mainStat": format!("{} bps", source.current_incoming_bitrate_bps),
        "secondaryStat": format!("{} encodings", source.encodings.len()),
        "detail__owner_user": user_id_to_string(&source.owner_user_id),
        "detail__transport_media_id": source.transport_media_id,
    })
}

fn ensure_path_user(
    nodes: &mut Vec<Value>,
    seen_nodes: &mut HashSet<String>,
    detail: &DiagnosticsRoomDetail,
    user: &DiagnosticsUserView,
    selected: bool,
) {
    let room_uuid = detail.summary.uuid.as_str();
    let user_id = user_id_to_string(&user.user_id);
    push_unique_node(
        nodes,
        seen_nodes,
        format!("user:{room_uuid}:{user_id}"),
        path_user_node(room_uuid, user, selected),
    );
    push_unique_node(
        nodes,
        seen_nodes,
        format!("worker:{}", user.transport.media_worker_id),
        path_worker_node(detail, user.transport.media_worker_id),
    );
}

fn push_user_transport_edge(
    edges: &mut Vec<Value>,
    seen_edges: &mut HashSet<String>,
    detail: &DiagnosticsRoomDetail,
    user: &DiagnosticsUserView,
    direction: &str,
) {
    let room_uuid = detail.summary.uuid.as_str();
    let user_id = user_id_to_string(&user.user_id);
    let edge_id = format!(
        "transport:{room_uuid}:{user_id}:{}",
        user.transport.media_worker_id
    );
    push_unique_edge(
        edges,
        seen_edges,
        edge_id.clone(),
        json!({
            "id": edge_id,
            "source": format!("user:{}:{}", room_uuid, user_id),
            "target": format!("worker:{}", user.transport.media_worker_id),
            "mainStat": "transport",
            "secondaryStat": transport_health_label(user.transport.health.as_ref()),
            "detail__direction": direction,
            "detail__connection": user.transport.connection_id,
        }),
    );
}

fn push_publish_path(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    seen_nodes: &mut HashSet<String>,
    seen_edges: &mut HashSet<String>,
    detail: &DiagnosticsRoomDetail,
    owner: &DiagnosticsUserView,
    source: &DiagnosticsSource,
) {
    let room_uuid = detail.summary.uuid.as_str();
    push_unique_node(
        nodes,
        seen_nodes,
        format!("source:{room_uuid}:{}", source.source_id),
        path_source_node(room_uuid, source),
    );
    let edge_id = format!("publish:{room_uuid}:{}", source.source_id);
    push_unique_edge(
        edges,
        seen_edges,
        edge_id.clone(),
        json!({
            "id": edge_id,
            "source": format!("worker:{}", owner.transport.media_worker_id),
            "target": format!("source:{}:{}", room_uuid, source.source_id),
            "mainStat": format!("{} upload", stream_type_label(source.stream_type)),
            "secondaryStat": format!("{} bps", source.current_incoming_bitrate_bps),
            "color": stream_type_color(source.stream_type),
            "detail__owner_user": user_id_to_string(&owner.user_id),
            "detail__transport_media_id": source.transport_media_id,
        }),
    );
}

fn push_subscription_delivery_path(
    edges: &mut Vec<Value>,
    seen_edges: &mut HashSet<String>,
    detail: &DiagnosticsRoomDetail,
    receiver: &DiagnosticsUserView,
    sub: &DiagnosticsSubscription,
    direction: &str,
) {
    let room_uuid = detail.summary.uuid.as_str();
    let receiver_id = user_id_to_string(&receiver.user_id);
    let deliver_edge_id = format!("deliver:{room_uuid}:{}:{receiver_id}", sub.source_id);
    push_unique_edge(
        edges,
        seen_edges,
        deliver_edge_id.clone(),
        json!({
            "id": deliver_edge_id,
            "source": format!("source:{}:{}", room_uuid, sub.source_id),
            "target": format!("worker:{}", receiver.transport.media_worker_id),
            "mainStat": download_main_stat(sub),
            "secondaryStat": route_state_label(&sub.state),
            "color": route_state_color(&sub.state),
            "detail__direction": direction,
            "detail__receiver_user": receiver_id,
            "detail__selector": format!("{:?}", sub.selection.selector),
            "detail__source_transport_media_id": sub.source_transport_media_id,
            "detail__consumer_transport_media_id": sub.consumer_transport_media_id,
        }),
    );

    let consume_edge_id = format!(
        "consume:{room_uuid}:{}:{}",
        sub.source_id,
        user_id_to_string(&receiver.user_id)
    );
    push_unique_edge(
        edges,
        seen_edges,
        consume_edge_id.clone(),
        json!({
            "id": consume_edge_id,
            "source": format!("worker:{}", receiver.transport.media_worker_id),
            "target": format!("user:{}:{}", room_uuid, user_id_to_string(&receiver.user_id)),
            "mainStat": "consume",
            "secondaryStat": direction,
            "color": route_state_color(&sub.state),
        }),
    );
}

fn push_inbound_paths(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    seen_nodes: &mut HashSet<String>,
    seen_edges: &mut HashSet<String>,
    detail: &DiagnosticsRoomDetail,
    selected_user: &DiagnosticsUserView,
) {
    for sub in &selected_user.subscriptions {
        let Some(source) = source_by_id(detail, sub.source_id) else {
            continue;
        };
        let Some(owner) = user_by_id(detail, &source.owner_user_id) else {
            continue;
        };
        ensure_path_user(nodes, seen_nodes, detail, owner, false);
        push_user_transport_edge(edges, seen_edges, detail, owner, "inbound-source");
        push_publish_path(nodes, edges, seen_nodes, seen_edges, detail, owner, source);
        push_subscription_delivery_path(edges, seen_edges, detail, selected_user, sub, "inbound");
    }
}

fn push_outbound_paths(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    seen_nodes: &mut HashSet<String>,
    seen_edges: &mut HashSet<String>,
    detail: &DiagnosticsRoomDetail,
    selected_user: &DiagnosticsUserView,
) {
    let selected_sources = detail
        .sources
        .iter()
        .filter(|source| source.owner_user_id == selected_user.user_id);
    for source in selected_sources {
        push_publish_path(
            nodes,
            edges,
            seen_nodes,
            seen_edges,
            detail,
            selected_user,
            source,
        );
        for receiver in &detail.users {
            for sub in receiver
                .subscriptions
                .iter()
                .filter(|sub| sub.source_id == source.source_id)
            {
                ensure_path_user(nodes, seen_nodes, detail, receiver, false);
                push_user_transport_edge(edges, seen_edges, detail, receiver, "outbound-target");
                push_subscription_delivery_path(
                    edges, seen_edges, detail, receiver, sub, "outbound",
                );
            }
        }
    }
}

/// Build the Grafana node graph payload for one user's media paths in a room.
///
/// The graph shows the selected user's outbound publications through their
/// source media worker to every receiver, and their inbound subscriptions from
/// the producer user through the producer and receiver workers. It is a
/// diagnostics projection of the current routing snapshot, not a transport
/// control API.
pub(crate) fn build_user_graph(
    detail: &DiagnosticsRoomDetail,
    requested_user_id: &str,
) -> Option<Value> {
    let selected_user = detail
        .users
        .iter()
        .find(|user| user_id_matches(&user.user_id, requested_user_id))?;
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut seen_nodes = HashSet::new();
    let mut seen_edges = HashSet::new();

    ensure_path_user(&mut nodes, &mut seen_nodes, detail, selected_user, true);
    push_user_transport_edge(
        &mut edges,
        &mut seen_edges,
        detail,
        selected_user,
        "selected",
    );
    push_inbound_paths(
        &mut nodes,
        &mut edges,
        &mut seen_nodes,
        &mut seen_edges,
        detail,
        selected_user,
    );
    push_outbound_paths(
        &mut nodes,
        &mut edges,
        &mut seen_nodes,
        &mut seen_edges,
        detail,
        selected_user,
    );

    Some(json!({
        "nodes": nodes,
        "edges": edges,
    }))
}
