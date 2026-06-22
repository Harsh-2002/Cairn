//! Server-Sent-Events live-update channel (ARCH 26): a single multiplexed stream that pushes
//! periodic JSON snapshots of selected topics to the console, plus a short-lived single-use ticket
//! mint.
//!
//! `EventSource` cannot send an `Authorization` header, so the browser first `POST`s
//! `/api/v1/events/ticket` with its Bearer token to receive a ~60 s single-use ticket, then opens
//! `/api/v1/events/stream?ticket=<t>&topics=<csv>`. The ticket is a URL-borne capability that is
//! strictly weaker than the Bearer it was minted from: single-use, sub-minute, and read-only (it
//! only grants the SSE stream). Each topic is rendered by reusing the existing control-plane GET
//! handler, so the SSE payload is byte-identical to the view's normal fetch.

use crate::adapter::{ResponseBody, full_body};
use crate::stack::AppStack;
use bytes::Bytes;
use cairn_auth::hash_session_token;
use cairn_crypto::SystemClock;
use cairn_types::auth::{Principal, Role};
use cairn_types::error::BodyError;
use cairn_types::traits::Clock;
use http::{Method, Response, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// `hash(ticket) -> (expiry_ms, minting principal)`. In-process, node-local, single-use.
pub type SseTicketStore = Mutex<HashMap<String, (i64, Principal)>>;

/// How long a freshly minted ticket is valid before the browser must open the stream.
const TICKET_TTL_MS: i64 = 60_000;
/// The snapshot cadence.
const TICK: Duration = Duration::from_secs(3);
/// Bound a single stream so it ends and the client reconnects — this also bounds how long an open
/// stream can hold a connection open during graceful shutdown (the connection drain timeout backs
/// this up). ~5 minutes at the tick cadence.
const MAX_TICKS: u32 = 100;

fn now_ms() -> i64 {
    SystemClock::new().now().as_millis()
}

fn json(status: StatusCode, body: Vec<u8>) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full_body(Bytes::from(body)))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// `POST /api/v1/events/ticket` — admin-gated. Mints a single-use, ~60 s ticket bound to the caller.
pub fn mint_ticket(stack: &AppStack, principal: Option<&Principal>) -> Response<ResponseBody> {
    let Some(p) = principal.filter(|p| p.role == Role::Administrator) else {
        return json(
            StatusCode::FORBIDDEN,
            br#"{"error":"administrator role required"}"#.to_vec(),
        );
    };
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let now = now_ms();
    {
        let mut store = stack.sse_tickets.lock().unwrap();
        store.retain(|_, (exp, _)| *exp > now); // opportunistic prune of expired tickets
        store.insert(hash_session_token(&token), (now + TICKET_TTL_MS, p.clone()));
    }
    json(
        StatusCode::OK,
        format!(r#"{{"ticket":"{token}"}}"#).into_bytes(),
    )
}

/// `GET /api/v1/events/stream?ticket=&topics=` — validates and CONSUMES the single-use ticket, then
/// streams periodic per-topic snapshots as SSE events plus a keepalive heartbeat.
pub fn events_stream(
    stack: Arc<AppStack>,
    ticket: Option<&str>,
    topics: &str,
) -> Response<ResponseBody> {
    let Some(t) = ticket else {
        return json(
            StatusCode::UNAUTHORIZED,
            br#"{"error":"ticket required"}"#.to_vec(),
        );
    };
    let principal = {
        let mut store = stack.sse_tickets.lock().unwrap();
        match store.remove(&hash_session_token(t)) {
            Some((exp, p)) if exp > now_ms() => p,
            _ => {
                return json(
                    StatusCode::UNAUTHORIZED,
                    br#"{"error":"invalid or expired ticket"}"#.to_vec(),
                );
            }
        }
    };
    let topics: Vec<String> = topics
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();

    // A lazy stream: each step renders the requested topics by reusing the control GET handlers and
    // emits one SSE frame. The first frame is immediate; subsequent ones are spaced by TICK. The
    // stream self-terminates after MAX_TICKS so it never holds a connection indefinitely.
    let stream = futures_util::stream::unfold(
        (0u32, stack, principal, topics),
        |(tick, stack, principal, topics)| async move {
            if tick >= MAX_TICKS {
                return None;
            }
            if tick > 0 {
                tokio::time::sleep(TICK).await;
            }
            let mut buf = String::new();
            for topic in &topics {
                if let Some((path, query)) = topic_route(topic) {
                    let resp = stack
                        .control
                        .handle(&Method::GET, path, &query, Some(&principal), Bytes::new())
                        .await;
                    if resp.status == StatusCode::OK {
                        let data = String::from_utf8_lossy(&resp.body);
                        buf.push_str(&format!("event: {topic}\ndata: {data}\n\n"));
                    }
                }
            }
            // A heartbeat comment so intermediaries never idle-close the stream.
            buf.push_str(": keepalive\n\n");
            let frame = Frame::data(Bytes::from(buf));
            Some((
                Ok::<_, BodyError>(frame),
                (tick + 1, stack, principal, topics),
            ))
        },
    );

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        // Defeat proxy/buffering so events arrive promptly.
        .header("x-accel-buffering", "no")
        .body(BodyExt::boxed_unsync(StreamBody::new(stream)))
        .unwrap_or_else(|_| Response::new(full_body(Bytes::new())))
}

/// Map an SSE topic to the control-plane GET it renders (reusing the existing handler so the SSE
/// payload is byte-identical to the view's normal fetch). Unknown topics are ignored.
fn topic_route(topic: &str) -> Option<(&'static str, Vec<(String, String)>)> {
    match topic {
        "overview" => Some(("/overview", vec![])),
        "buckets" => Some(("/overview/buckets", vec![])),
        "replication" => Some(("/replication/summary", vec![])),
        "activity" => Some(("/activity", vec![("limit".to_owned(), "20".to_owned())])),
        "metrics" => Some((
            "/metrics/requests",
            vec![("range".to_owned(), "1h".to_owned())],
        )),
        _ => None,
    }
}
