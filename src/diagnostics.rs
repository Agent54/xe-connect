//! Read-only diagnostics for the embedding gateway. Never serialize Config itself:
//! it contains the local control token and redeemable invitation tokens.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use futures_util::{StreamExt, stream};
use iroh::endpoint::{
    ConnectionStats, FrameStats, LocalTransportAddr, PathStats, Side, TransportAddrUsage, UdpStats,
};
use iroh_metrics::MetricsGroupSet;
use serde_json::{Value, json};

use super::*;

const REMOTE_INFO_BUDGET: Duration = Duration::from_secs(1);
const REMOTE_INFO_CONCURRENCY: usize = 8;

impl Agent {
    /// A best-effort, secret-free snapshot of agent and iroh state. Does not dial,
    /// probe services, run a net report, or access configuration files.
    ///
    /// Also served by GET /_xe/diagnostics with the normal local control token.
    /// Remote-info actor queries share a one-second deadline; missing or timed-out
    /// information is reported explicitly, rather than interpreted as offline.
    pub async fn diagnostics(&self) -> Value {
        let config = self.state.get();
        let sampled_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let live = self.live_connections.lock().await.clone();
        let mut connections: Vec<_> = live.iter().map(connection_info).collect();
        connections.sort_by_key(|entry| entry["connection_id"].as_u64());
        let mut ids = BTreeSet::new();
        ids.extend(config.peers.values().map(|peer| peer.address.id));
        ids.extend(live.iter().map(|live| live.connection.remote_id()));

        let mut dials = Vec::new();
        {
            let cached = self.connections.lock().await;
            for (id, entry) in cached.iter() {
                ids.insert(*id);
                let (state, error, connection_id) = match entry.attempt.peek() {
                    None => ("pending_or_unobserved", None, None),
                    Some(Ok(connection)) => (
                        if connection.close_reason().is_some() {
                            "closed"
                        } else {
                            "connected"
                        },
                        connection.close_reason().map(|error| error.to_string()),
                        Some(connection.stable_id()),
                    ),
                    Some(Err(error)) => ("failed", Some(error.to_string()), None),
                };
                dials.push(json!({
                    "peer_id": id.to_string(), "state": state,
                    "age_ms": millis(entry.started.elapsed()),
                    "connection_id": connection_id, "error": error,
                }));
            }
        }
        dials.sort_by_key(|entry| entry["peer_id"].as_str().unwrap_or_default().to_string());

        let mut in_flight = Vec::new();
        {
            let lookups = self.lookups.lock().await;
            for (hostname, gate) in lookups.iter() {
                if gate.strong_count() > 0 {
                    in_flight.push(hostname.clone());
                }
            }
        }
        in_flight.sort();
        let mut backoff = BTreeMap::new();
        {
            let misses = self.misses.lock().await;
            for (hostname, time) in misses.iter() {
                let remaining = DISCOVERY_MISS_BACKOFF.saturating_sub(time.elapsed());
                if !remaining.is_zero() {
                    backoff.insert(hostname.clone(), millis(remaining));
                }
            }
        }
        let mut recent_attempts = BTreeMap::<String, usize>::new();
        {
            let rate = self.pairing_rate.lock().await;
            for (id, time) in rate.attempts.iter() {
                if time.elapsed() < PAIRING_WINDOW {
                    *recent_attempts.entry(id.to_string()).or_default() += 1;
                }
            }
        }
        let attempts_total: usize = recent_attempts.values().sum();

        // Query only iroh's existing remote-address state, with bounded concurrency
        // and one shared deadline. No mutex guard is held across actor queries.
        let deadline = Deadline::now() + REMOTE_INFO_BUDGET;
        let mut remotes: Vec<_> = stream::iter(ids.into_iter().map(|id| {
            let config = &config;
            let live = &live;
            async move {
                let (status, addresses) =
                    match timeout_at(deadline, self.endpoint.remote_info(id)).await {
                        Ok(Some(info)) => (
                            "available",
                            info.addrs()
                                .map(|address| {
                                    json!({
                                        "address": address.addr(),
                                        "usage": match address.usage() {
                                            TransportAddrUsage::Active => "active",
                                            TransportAddrUsage::Inactive => "inactive",
                                            _ => "unknown",
                                        },
                                    })
                                })
                                .collect::<Vec<_>>(),
                        ),
                        Ok(None) => ("not_known", Vec::new()),
                        Err(_) => ("timed_out", Vec::new()),
                    };
                let id_string = id.to_string();
                let peer = config.peers.get(&id_string);
                let connection_ids: Vec<_> = live
                    .iter()
                    .filter(|live| {
                        live.connection.remote_id() == id
                            && live.connection.close_reason().is_none()
                    })
                    .map(|live| live.connection.stable_id())
                    .collect();
                json!({
                    "peer_id": id_string, "paired": peer.is_some(),
                    "profile": peer.map(|peer| &peer.profile),
                    "saved_address": peer.map(|peer| &peer.address),
                    "active_connection_ids": connection_ids,
                    "iroh_remote_info": { "status": status, "addresses": addresses },
                })
            }
        }))
        .buffer_unordered(REMOTE_INFO_CONCURRENCY)
        .collect()
        .await;
        remotes.sort_by_key(|entry| entry["peer_id"].as_str().unwrap_or_default().to_string());

        let relays: Vec<_> = self
            .endpoint
            .home_relay_status()
            .get()
            .iter()
            .map(|relay| {
                json!({
                    "url": relay.url(), "connected": relay.is_connected(),
                    "last_error": relay.last_error().map(|error| error.to_string()),
                    "auth_denied_reason": relay.auth_denied_reason(),
                })
            })
            .collect();
        let report = self.endpoint.net_report().get().map(|report| {
            let latencies: Vec<_> = report.relay_latency.iter().map(|(probe, relay, duration)| json!({
                "relay_url": relay, "probe": probe.to_string(), "rtt_ms": milliseconds(duration),
            })).collect();
            json!({
                "udp_v4": report.udp_v4, "udp_v6": report.udp_v6,
                "mapping_varies_by_dest_ipv4": report.mapping_varies_by_dest_ipv4,
                "mapping_varies_by_dest_ipv6": report.mapping_varies_by_dest_ipv6,
                "global_v4": report.global_v4, "global_v6": report.global_v6,
                "preferred_relay": report.preferred_relay,
                "relay_latencies": latencies, "captive_portal": report.captive_portal,
            })
        });
        let metrics = self.endpoint.metrics();
        let mut descriptions = BTreeMap::new();
        for group in metrics.groups() {
            let mut fields = BTreeMap::new();
            for metric in group.iter() {
                fields.insert(metric.name(), metric.help());
            }
            for index in 0..group.family_field_count() {
                if let Some(family) = group.family_field_ref(index) {
                    fields.insert(family.name(), family.help());
                }
            }
            descriptions.insert(group.name(), fields);
        }
        let unexpired = config
            .invitations
            .values()
            .filter(|invitation| invitation.expires_at >= now());
        let unredeemed = unexpired
            .clone()
            .filter(|invitation| invitation.redeemed_by.is_none())
            .count();
        let redeemed = unexpired
            .filter(|invitation| invitation.redeemed_by.is_some())
            .count();

        json!({
            "schema_version": 1,
            "sampled_at_unix_ms": millis(sampled_at),
            "agent": {
                "version": env!("CARGO_PKG_VERSION"), "uptime_ms": millis(self.started.elapsed()),
                "closing": self.tasks.cancel.is_cancelled(), "identity_lock_held": self.agent_lock.lock().unwrap().is_some(),
                "endpoint_id": self.endpoint.id().to_string(), "profile": config.profile,
                "state_directory": self.store.directory(), "saved_address": config.last_address,
                "configured_listen": config.listen, "upstream": config.upstream,
                "probe_timeout_ms": config.probe_timeout_ms,
                "persistence": { "write_busy": self.state.write_busy(), "closed": self.state.is_closed() },
            },
            "iroh": {
                "version": "1.3.0", "alpn": String::from_utf8_lossy(protocol::ALPN),
                "closed": self.endpoint.is_closed(), "address": self.endpoint.addr(),
                "bound_sockets": self.endpoint.bound_sockets(),
                "home_relays": relays, "net_report": report,
                "metrics": metrics, "metric_descriptions": descriptions,
            },
            "peers": remotes, "connections": connections, "dial_cache": dials,
            "discovery": {
                "routes": config.routes, "in_flight_hostnames": in_flight,
                "miss_backoff_remaining_ms": backoff,
            },
            "resources": {
                "trusted_connections": capacity(&self.peer_slots, MAX_PEER_CONNECTIONS),
                "incoming_handshakes": capacity(&self.handshake_slots, MAX_INCOMING_HANDSHAKES),
                "pending_pairings": capacity(&self.pending_slots, MAX_PENDING_PAIRINGS),
                "http_connections": capacity(&self.http_slots, MAX_HTTP_CONNECTIONS),
                "request_handlers": capacity(&self.requests, MAX_REQUESTS),
                "tracked_tasks": self.tasks.len(),
                "upstream_pool": { "max_idle_per_host": UPSTREAM_POOL_MAX_IDLE,
                    "idle_timeout_ms": millis(UPSTREAM_POOL_IDLE_TIMEOUT),
                    "current_idle_connections": Value::Null },
                "timeouts_ms": {
                    "handshake": millis(CONNECT_TIMEOUT), "authentication": millis(AUTHENTICATION_TIMEOUT),
                    "peer_operation": millis(OPERATION_TIMEOUT), "address_hint": millis(HINT_TIMEOUT),
                    "cached_provider": millis(CACHED_TIMEOUT), "response_headers": millis(HEADER_TIMEOUT),
                    "diagnostics_remote_info": millis(REMOTE_INFO_BUDGET),
                    "discovery_miss_backoff": millis(DISCOVERY_MISS_BACKOFF),
                },
            },
            "pairing": {
                "window_ms": millis(PAIRING_WINDOW), "global_limit": MAX_PAIRING_ATTEMPTS_GLOBAL,
                "per_identity_limit": MAX_PAIRING_ATTEMPTS_PER_IDENTITY,
                "attempts_in_window": attempts_total, "attempts_by_identity": recent_attempts,
                "remaining_global_attempts": MAX_PAIRING_ATTEMPTS_GLOBAL.saturating_sub(attempts_total),
                "invitations": { "unredeemed": unredeemed, "redeemed": redeemed,
                    "expired": config.invitations.len() - unredeemed - redeemed },
            },
        })
    }
}

fn capacity(semaphore: &Semaphore, limit: usize) -> Value {
    let available = semaphore.available_permits();
    json!({ "limit": limit, "used": limit.saturating_sub(available), "available": available })
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn connection_info(live: &LiveConnection) -> Value {
    let connection = &live.connection;
    let trusted = live.trusted.load(Ordering::Acquire);
    let paths: Vec<_> = connection.paths().iter().map(|path| {
        let local_address = match path.local_addr() {
            LocalTransportAddr::Ip(ip) => json!({ "kind": "ip", "address": ip }),
            LocalTransportAddr::Relay(url) => json!({ "kind": "relay", "address": url }),
            LocalTransportAddr::Custom(address) => json!({ "kind": "custom", "address": address }),
            _ => Value::Null,
        };
        let congestion = connection.congestion_state(path.id()).map(|controller| {
            let metrics = controller.metrics();
            json!({
                "window_bytes": metrics.congestion_window, "slow_start_threshold_bytes": metrics.ssthresh,
                "pacing_bytes_per_second": metrics.pacing_rate, "send_quantum_bytes": metrics.send_quantum,
            })
        });
        json!({
            "path_id": path.id().to_string(), "selected": path.is_selected(),
            "kind": if path.is_ip() { "direct_ip" } else if path.is_relay() { "relay" } else { "custom" },
            "local_address": local_address, "remote_address": path.remote_addr(),
            "stats": path_stats(path.stats()), "congestion": congestion,
        })
    }).collect();
    json!({
        "connection_id": connection.stable_id(), "peer_id": connection.remote_id().to_string(),
        "direction": match connection.side() { Side::Client => "outbound", Side::Server => "inbound" },
        "admission": if trusted { "trusted" } else { "pending_pairing" },
        "age_ms": millis(live.started.elapsed()), "closed": connection.close_reason().is_some(),
        "close_reason": connection.close_reason().map(|reason| reason.to_string()),
        "authentication_remaining_ms": (!trusted && connection.close_reason().is_none())
            .then(|| millis(live.auth_deadline.saturating_duration_since(Deadline::now()))),
        "alpn": String::from_utf8_lossy(connection.alpn()),
        "max_datagram_size_bytes": connection.max_datagram_size(),
        "datagram_send_buffer_space_bytes": connection.datagram_send_buffer_space(),
        "stats": connection_stats(connection.stats()), "paths": paths,
    })
}

fn traffic(stats: UdpStats) -> Value {
    json!({ "bytes": stats.bytes, "datagrams": stats.datagrams })
}

fn connection_stats(stats: ConnectionStats) -> Value {
    json!({
        "tx": traffic(stats.udp_tx), "rx": traffic(stats.udp_rx),
        "frames_tx": frames(stats.frame_tx), "frames_rx": frames(stats.frame_rx),
        "lost_packets": stats.lost_packets, "lost_bytes": stats.lost_bytes,
    })
}

fn path_stats(stats: PathStats) -> Value {
    json!({
        "rtt_ms": milliseconds(stats.rtt), "congestion_window_bytes": stats.cwnd,
        "tx": traffic(stats.udp_tx), "rx": traffic(stats.udp_rx),
        "frames_tx": frames(stats.frame_tx), "frames_rx": frames(stats.frame_rx),
        "congestion_events": stats.congestion_events, "spurious_congestion_events": stats.spurious_congestion_events,
        "lost_packets": stats.lost_packets, "lost_bytes": stats.lost_bytes,
        "mtu_bytes": stats.current_mtu, "mtu_probes_sent": stats.sent_plpmtud_probes,
        "mtu_probes_lost": stats.lost_plpmtud_probes, "black_holes_detected": stats.black_holes_detected,
    })
}

fn frames(stats: FrameStats) -> Value {
    // Preserve every public frame counter in the locked QUIC version. Frame
    // counts are protocol activity, not counts of HTTP requests or open streams.
    macro_rules! fields {
        ($($field:ident),* $(,)?) => {{
            let mut fields = serde_json::Map::new();
            $(fields.insert(stringify!($field).into(), json!(stats.$field));)*
            Value::Object(fields)
        }};
    }
    fields!(
        acks,
        path_acks,
        ack_frequency,
        crypto,
        connection_close,
        data_blocked,
        datagram,
        handshake_done,
        immediate_ack,
        max_data,
        max_stream_data,
        max_streams_bidi,
        max_streams_uni,
        new_connection_id,
        path_new_connection_id,
        new_token,
        path_challenge,
        path_response,
        ping,
        reset_stream,
        retire_connection_id,
        path_retire_connection_id,
        stream_data_blocked,
        streams_blocked_bidi,
        streams_blocked_uni,
        stop_sending,
        stream,
        observed_addr,
        path_abandon,
        path_status_available,
        path_status_backup,
        max_path_id,
        paths_blocked,
        path_cids_blocked,
        add_address,
        reach_out,
        remove_address
    )
}
