//! Grafana node graph formatting for one diagnostics room detail snapshot.
//!
//! This module is the last formatting step before
//! `/internal/diagnostics/node-graph/channels/{uuid}` returns JSON to the
//! `o-sfu-telemetry` Grafana dashboard. The input is an already assembled
//! [`DiagnosticsRoomDetail`], so this file does not query live runtime state,
//! hold locks or decide whether a route is valid. It only projects the
//! diagnostics snapshot into the field names understood by Grafana's node graph
//! panel through the Infinity datasource.
//!
//! # Graph model
//!
//! The channel is the root node. Each user is rendered as a session node linked
//! from the channel. Each publication source is rendered as a source node linked
//! from its owning session. Each subscription is rendered as an edge from the
//! source node to the receiving session.
//!
//! A missing source node means diagnostics saw a subscription whose producer
//! source was absent from the source snapshot. The formatter keeps that edge
//! visible instead of hiding it because it is useful evidence when debugging
//! stale routing state or a snapshot assembly bug.
//!
//! # Grafana field conventions
//!
//! `id`, `title`, `subtitle`, `mainStat` and `secondaryStat` are the visible
//! node graph fields. Edge rows use `source` and `target` to point at node ids.
//! `detail__*` fields become expandable metadata in Grafana. `arc__*` fields
//! render the session bitrate split as colored arcs. `color`, `thickness` and
//! `strokeDasharray` are presentation hints consumed by the node graph panel.
//!
//! # Performance model
//!
//! Graph formatting is a cold diagnostics path. Allocating strings and JSON
//! values is acceptable here because the endpoint runs on demand after the room
//! snapshot has already been collected. The formatter still builds source and
//! download indexes once so the JSON projection stays linear in the number of
//! users, sources and subscriptions.

use std::collections::{HashMap, HashSet};

use o_sfu_core::server::session::{StreamType, UserId};
use serde_json::{Value, json};

use super::types::{
    DiagnosticsIncomingBitrate, DiagnosticsRoomDetail, DiagnosticsRouteState, DiagnosticsSource,
    DiagnosticsSubscription, DiagnosticsTransportHealth, DiagnosticsUserView,
};

const ARC_RATIO_SCALE: u32 = 1_000_000;

fn user_id_to_string(user_id: &UserId) -> String {
    match user_id {
        UserId::Integer(i) => i.to_string(),
        UserId::String(s) => s.clone(),
    }
}

fn stream_type_label(stream_type: StreamType) -> &'static str {
    match stream_type {
        StreamType::Audio => "audio",
        StreamType::Camera => "camera",
        StreamType::Screen => "screen",
    }
}

fn stream_type_color(stream_type: StreamType) -> &'static str {
    match stream_type {
        StreamType::Audio => "blue",
        StreamType::Camera => "orange",
        StreamType::Screen => "purple",
    }
}

fn transport_health_label(health: Option<&DiagnosticsTransportHealth>) -> &'static str {
    match health {
        Some(DiagnosticsTransportHealth::Connected) => "connected",
        Some(DiagnosticsTransportHealth::Disconnected) => "disconnected",
        None => "unknown",
    }
}

fn channel_node(detail: &DiagnosticsRoomDetail) -> Value {
    let channel_uuid = detail.summary.uuid.as_str();
    let short_uuid = if channel_uuid.len() > 8 {
        &channel_uuid[..8]
    } else {
        channel_uuid
    };

    json!({
        "id": format!("channel:{}", channel_uuid),
        "title": short_uuid,
        "subtitle": "channel",
        "mainStat": format!("{} sessions", detail.summary.user_count),
        "secondaryStat": format!("{} pub / {} sub", detail.summary.publication_count, detail.summary.subscription_count),
        "detail__recording": format!("{:?}", detail.summary.recording_state.recording),
        "detail__worker": detail.summary.media_worker_id,
        "detail__transport": format!("{} conn / {} disc / {} unk", detail.summary.transport.connected, detail.summary.transport.disconnected, detail.summary.transport.unknown),
    })
}

fn source_ids(detail: &DiagnosticsRoomDetail) -> HashSet<u64> {
    detail
        .sources
        .iter()
        .map(|source| source.source_id)
        .collect()
}

fn download_counts(detail: &DiagnosticsRoomDetail) -> HashMap<u64, usize> {
    let mut download_counts: HashMap<u64, usize> = HashMap::new();
    for user in &detail.users {
        for sub in &user.subscriptions {
            *download_counts.entry(sub.source_id).or_insert(0) += 1;
        }
    }
    download_counts
}

fn bitrate_share(part: u64, total: u64) -> Option<f64> {
    if total == 0 {
        return None;
    }

    let scaled = (u128::from(part.min(total)) * u128::from(ARC_RATIO_SCALE)) / u128::from(total);
    let scaled = u32::try_from(scaled).map_or(ARC_RATIO_SCALE, |value| value);
    Some(f64::from(scaled) / f64::from(ARC_RATIO_SCALE))
}

fn add_bitrate_arcs(node: &mut Value, bitrate: &DiagnosticsIncomingBitrate) {
    if let Some(obj) = node.as_object_mut() {
        if let Some(share) = bitrate_share(bitrate.audio, bitrate.total) {
            obj.insert("arc__audio".to_string(), json!(share));
        }
        if let Some(share) = bitrate_share(bitrate.camera, bitrate.total) {
            obj.insert("arc__camera".to_string(), json!(share));
        }
        if let Some(share) = bitrate_share(bitrate.screen, bitrate.total) {
            obj.insert("arc__screen".to_string(), json!(share));
        }
    }
}

fn session_node(channel_uuid: &str, user: &DiagnosticsUserView) -> Value {
    let session_id = user_id_to_string(&user.user_id);
    let health = transport_health_label(user.transport.health.as_ref());
    let bitrate = &user.transport.quality_summary.current_incoming_bitrate;
    let mut node = json!({
        "id": format!("session:{}:{}", channel_uuid, session_id),
        "title": session_id,
        "subtitle": health,
        "mainStat": format!("{} bps", bitrate.total),
        "secondaryStat": format!("{} pub / {} sub", user.publications.len(), user.subscriptions.len()),
        "detail__connection": user.transport.connection_id.to_string(),
        "detail__worker": user.transport.media_worker_id,
    });

    add_bitrate_arcs(&mut node, bitrate);
    node
}

fn push_session_entries(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    detail: &DiagnosticsRoomDetail,
) {
    let channel_uuid = detail.summary.uuid.as_str();
    for user in &detail.users {
        let session_id = user_id_to_string(&user.user_id);
        let health = transport_health_label(user.transport.health.as_ref());
        let edge_color = match health {
            "connected" => "green",
            "disconnected" => "red",
            _ => "gray",
        };
        nodes.push(session_node(channel_uuid, user));
        edges.push(json!({
            "id": format!("member:{}:{}", channel_uuid, session_id),
            "source": format!("channel:{}", channel_uuid),
            "target": format!("session:{}:{}", channel_uuid, session_id),
            "mainStat": health,
            "color": edge_color,
        }));
    }
}

fn encoding_detail_lists(source: &DiagnosticsSource) -> (String, String, String, String, String) {
    let enc_ids: Vec<String> = source
        .encodings
        .iter()
        .map(|e| e.encoding_id.to_string())
        .collect();
    let rids: Vec<String> = source
        .encodings
        .iter()
        .filter_map(|e| e.rid.clone())
        .collect();
    let max_bitrates: Vec<String> = source
        .encodings
        .iter()
        .filter_map(|e| e.max_bitrate_bps.map(|b| b.to_string()))
        .collect();
    let primary_ssrcs: Vec<String> = source
        .encodings
        .iter()
        .filter_map(|e| e.primary_ssrc.map(|s| s.to_string()))
        .collect();
    let repair_ssrcs: Vec<String> = source
        .encodings
        .iter()
        .filter_map(|e| e.repair_ssrc.map(|s| s.to_string()))
        .collect();

    (
        enc_ids.join(", "),
        rids.join(", "),
        max_bitrates.join(", "),
        primary_ssrcs.join(", "),
        repair_ssrcs.join(", "),
    )
}

fn push_source_entries(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    detail: &DiagnosticsRoomDetail,
    download_counts: &HashMap<u64, usize>,
) {
    let channel_uuid = detail.summary.uuid.as_str();
    for source in &detail.sources {
        let stream_type = stream_type_label(source.stream_type);
        let media_kind_str = format!("{:?}", source.media_kind).to_lowercase();
        let active_str = if source.active { "active" } else { "inactive" };
        let downloads = download_counts.get(&source.source_id).copied().unwrap_or(0);
        let (enc_ids, rids, max_bitrates, primary_ssrcs, repair_ssrcs) =
            encoding_detail_lists(source);

        nodes.push(json!({
            "id": format!("source:{}:{}", channel_uuid, source.source_id),
            "title": format!("{} #{}", stream_type, source.source_id),
            "subtitle": format!("{} {}", active_str, media_kind_str),
            "mainStat": format!("{} bps", source.current_incoming_bitrate_bps),
            "secondaryStat": format!("{} encodings / {} downloads", source.encodings.len(), downloads),
            "detail__owner_session_id": user_id_to_string(&source.owner_user_id),
            "detail__stream_type": stream_type,
            "detail__media_kind": media_kind_str,
            "detail__transport_media_id": source.transport_media_id,
            "detail__mid": source.mid,
            "detail__encoding_ids": enc_ids,
            "detail__rids": rids,
            "detail__max_bitrates_bps": max_bitrates,
            "detail__primary_ssrcs": primary_ssrcs,
            "detail__repair_ssrcs": repair_ssrcs,
        }));

        let thickness = if source.current_incoming_bitrate_bps > 0 {
            2.0
        } else {
            1.0
        };
        let color = stream_type_color(source.stream_type);

        edges.push(json!({
            "id": format!("publish:{}:{}", channel_uuid, source.source_id),
            "source": format!("session:{}:{}", channel_uuid, user_id_to_string(&source.owner_user_id)),
            "target": format!("source:{}:{}", channel_uuid, source.source_id),
            "mainStat": format!("{} upload", stream_type),
            "secondaryStat": format!("{} bps", source.current_incoming_bitrate_bps),
            "thickness": thickness,
            "color": color,
            "detail__source_id": source.source_id,
            "detail__encoding_ids": enc_ids,
            "detail__rids": rids,
            "detail__transport_media_id": source.transport_media_id,
        }));
    }
}

fn route_state_label(state: &DiagnosticsRouteState) -> &'static str {
    match state {
        DiagnosticsRouteState::Active => "active",
        DiagnosticsRouteState::Inactive => "inactive",
        DiagnosticsRouteState::Pending => "pending",
    }
}

fn route_state_color(state: &DiagnosticsRouteState) -> &'static str {
    match state {
        DiagnosticsRouteState::Active => "green",
        DiagnosticsRouteState::Inactive => "gray",
        DiagnosticsRouteState::Pending => "yellow",
    }
}

fn download_main_stat(sub: &DiagnosticsSubscription) -> String {
    let stream_type = stream_type_label(sub.stream_type);
    match sub.selection.selected_rid.as_deref() {
        Some(rid) if !rid.is_empty() => format!("{stream_type} {rid}"),
        _ => stream_type.to_string(),
    }
}

fn download_edge(
    channel_uuid: &str,
    user: &DiagnosticsUserView,
    sub: &DiagnosticsSubscription,
) -> Value {
    let session_id = user_id_to_string(&user.user_id);
    let mut edge = json!({
        "id": format!("download:{}:{}:{}", channel_uuid, sub.source_id, session_id),
        "source": format!("source:{}:{}", channel_uuid, sub.source_id),
        "target": format!("session:{}:{}", channel_uuid, session_id),
        "mainStat": download_main_stat(sub),
        "secondaryStat": route_state_label(&sub.state),
        "color": route_state_color(&sub.state),
        "detail__source_id": sub.source_id,
        "detail__producer_session_id": user_id_to_string(&sub.producer_user_id),
        "detail__selector": format!("{:?}", sub.selection.selector),
        "detail__selection_reason": format!("{:?}", sub.selection.selection_reason),
        "detail__selection_active": sub.selection.active,
        "detail__pressure_observations": sub.selection.pressure_observations,
        "detail__upgrade_observations": sub.selection.upgrade_observations,
        "detail__source_transport_media_id": sub.source_transport_media_id,
        "detail__consumer_transport_media_id": sub.consumer_transport_media_id,
    });

    if let Some(obj) = edge.as_object_mut() {
        if sub.state != DiagnosticsRouteState::Active {
            obj.insert("strokeDasharray".to_string(), json!("5, 5"));
        }
        if let Some(enc_id) = sub.selection.selected_encoding_id {
            obj.insert("detail__selected_encoding_id".to_string(), json!(enc_id));
        }
        if let Some(rid) = &sub.selection.selected_rid {
            obj.insert("detail__selected_rid".to_string(), json!(rid));
        }
    }

    edge
}

fn push_download_entries(
    nodes: &mut Vec<Value>,
    edges: &mut Vec<Value>,
    detail: &DiagnosticsRoomDetail,
    source_ids: &HashSet<u64>,
) {
    let channel_uuid = detail.summary.uuid.as_str();
    for user in &detail.users {
        for sub in &user.subscriptions {
            if !source_ids.contains(&sub.source_id) {
                nodes.push(json!({
                    "id": format!("source:{}:{}", channel_uuid, sub.source_id),
                    "title": format!("missing #{}", sub.source_id),
                    "subtitle": "not found",
                }));
            }
            edges.push(download_edge(channel_uuid, user, sub));
        }
    }
}

/// Build the Grafana node graph payload for a single room detail response.
///
/// The returned JSON has two top-level arrays:
///
/// * `nodes` contains the channel, live sessions, publication sources and any
///   missing source placeholders needed by subscription edges.
/// * `edges` contains membership edges, publication upload edges and download
///   subscription edges.
///
/// This function is intentionally compatibility-shaped around Grafana's node
/// graph field names. Domain code should keep using [`DiagnosticsRoomDetail`]
/// and should not treat this JSON as the authoritative runtime model.
pub(crate) fn build_graph(detail: &DiagnosticsRoomDetail) -> Value {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let source_ids = source_ids(detail);
    let download_counts = download_counts(detail);

    nodes.push(channel_node(detail));
    push_session_entries(&mut nodes, &mut edges, detail);
    push_source_entries(&mut nodes, &mut edges, detail, &download_counts);
    push_download_entries(&mut nodes, &mut edges, detail, &source_ids);

    json!({
        "nodes": nodes,
        "edges": edges,
    })
}
