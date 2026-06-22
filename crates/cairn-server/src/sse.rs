//! Server-Sent-Events live-update channel (ARCH 26): a single multiplexed stream that pushes
//! periodic per-topic *pulses* to the console, plus a short-lived single-use ticket mint.
//!
//! `EventSource` cannot send an `Authorization` header, so the browser first `POST`s
//! `/api/v1/events/ticket` with its Bearer token to receive a ~60 s single-use ticket, then opens
//! `/api/v1/events/stream?ticket=<t>&topics=<csv>`. The ticket is a URL-borne capability strictly
//! weaker than the Bearer it was minted from: single-use, sub-minute, and read-only.
//!
//! The stream carries **no data** — each tick emits a tiny `event: <topic>\ndata: {}` *pulse* and
//! the client re-fetches that topic through the normal, per-request-authenticated `/api/v1` path.
//! This keeps the stream cheap (no per-tick DB reads on an unmetered path) and means a principal
//! revoked mid-stream cannot read stale data: the pulse reveals nothing and the re-fetch it triggers
//! is re-authorized like any other request.

use crate::adapter::{ResponseBody, full_body};
use crate::stack::AppStack;
use bytes::Bytes;
use cairn_auth::hash_session_token;
use cairn_crypto::SystemClock;
use cairn_types::auth::{Principal, Role};
use cairn_types::error::BodyError;
use cairn_types::traits::Clock;
use http::{Response, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

/// `hash(ticket) -> (expiry_ms, minting principal)`. In-process, node-local, single-use.
pub type SseTicketStore = Mutex<HashMap<String, (i64, Principal)>>;

/// How long a freshly minted ticket is valid before the browser must open the stream.
const TICKET_TTL_MS: i64 = 60_000;
/// The pulse cadence.
const TICK: Duration = Duration::from_secs(3);
/// Bound a single stream so the client periodically reconnects (re-minting a ticket, which re-checks
/// the live role). ~5 minutes at the tick cadence. Shutdown ends the stream promptly regardless.
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

/// Mint a single-use ticket into the store for an Administrator principal, pruning expired entries.
/// Returns the opaque ticket, or `None` if the principal is missing or not an admin. Pure over the
/// store + clock so it is unit-testable without a full server stack.
fn mint_into(store: &SseTicketStore, principal: Option<&Principal>, now: i64) -> Option<String> {
    let p = principal.filter(|p| p.role == Role::Administrator)?;
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let mut s = store.lock().unwrap();
    s.retain(|_, (exp, _)| *exp > now); // opportunistic prune of expired tickets
    s.insert(hash_session_token(&token), (now + TICKET_TTL_MS, p.clone()));
    Some(token)
}

/// Validate and CONSUME a ticket (single-use): returns the minting principal if the ticket exists
/// and has not expired, else `None`. Removing it on lookup makes a second use fail.
fn consume_ticket(store: &SseTicketStore, ticket: &str, now: i64) -> Option<Principal> {
    let mut s = store.lock().unwrap();
    match s.remove(&hash_session_token(ticket)) {
        Some((exp, p)) if exp > now => Some(p),
        _ => None,
    }
}

/// `POST /api/v1/events/ticket` — admin-gated. Mints a single-use, ~60 s ticket bound to the caller.
pub fn mint_ticket(stack: &AppStack, principal: Option<&Principal>) -> Response<ResponseBody> {
    match mint_into(&stack.sse_tickets, principal, now_ms()) {
        Some(token) => json(
            StatusCode::OK,
            format!(r#"{{"ticket":"{token}"}}"#).into_bytes(),
        ),
        None => json(
            StatusCode::FORBIDDEN,
            br#"{"error":"administrator role required"}"#.to_vec(),
        ),
    }
}

/// Whether a topic name is one the console subscribes to. Pulses are only emitted for known topics
/// so a typo'd subscription is silently ignored rather than echoed back.
fn is_known_topic(topic: &str) -> bool {
    matches!(
        topic,
        "overview" | "buckets" | "replication" | "activity" | "metrics"
    )
}

/// `GET /api/v1/events/stream?ticket=&topics=` — validates and CONSUMES the single-use ticket, then
/// streams periodic per-topic *pulses* (empty `data`) plus a keepalive heartbeat. The stream ends on
/// the shutdown signal so it never delays graceful shutdown, and self-terminates after `MAX_TICKS`.
pub fn events_stream(
    stack: Arc<AppStack>,
    ticket: Option<&str>,
    topics: &str,
    shutdown: watch::Receiver<bool>,
) -> Response<ResponseBody> {
    let Some(t) = ticket else {
        return json(
            StatusCode::UNAUTHORIZED,
            br#"{"error":"ticket required"}"#.to_vec(),
        );
    };
    // Consume the single-use ticket (admin-minted, sub-minute). The principal it carried is not
    // reused: the stream serves no data, only pulses, and the client's re-fetches re-authenticate.
    if consume_ticket(&stack.sse_tickets, t, now_ms()).is_none() {
        return json(
            StatusCode::UNAUTHORIZED,
            br#"{"error":"invalid or expired ticket"}"#.to_vec(),
        );
    }
    let topics: Vec<String> = topics
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| is_known_topic(s))
        .collect();

    // A lazy stream: each step emits one pulse per subscribed topic. The first frame is immediate;
    // subsequent ones are spaced by TICK but race the shutdown signal so the body ends promptly when
    // graceful shutdown begins (an infinite body would otherwise hold the connection to the drain
    // timeout). `stack` is carried only to keep the stream's lifetime tied to the app.
    let stream = futures_util::stream::unfold(
        (0u32, topics, shutdown, stack),
        |(tick, topics, mut shutdown, stack)| async move {
            if tick >= MAX_TICKS || *shutdown.borrow() {
                return None;
            }
            if tick > 0 {
                tokio::select! {
                    () = tokio::time::sleep(TICK) => {}
                    _ = shutdown.changed() => return None,
                }
            }
            let mut buf = String::new();
            for topic in &topics {
                // An empty pulse: the client treats the event as a "refresh this topic" trigger and
                // re-fetches the real (authenticated) payload itself.
                buf.push_str(&format!("event: {topic}\ndata: {{}}\n\n"));
            }
            // A heartbeat comment so intermediaries never idle-close the stream.
            buf.push_str(": keepalive\n\n");
            let frame = Frame::data(Bytes::from(buf));
            Some((
                Ok::<_, BodyError>(frame),
                (tick + 1, topics, shutdown, stack),
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

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::auth::{AuthMethod, Principal, Role};
    use cairn_types::id::UserId;

    fn store() -> SseTicketStore {
        Mutex::new(HashMap::new())
    }

    fn principal(role: Role) -> Principal {
        Principal {
            user_id: UserId("u".to_owned()),
            display_name: "u".to_owned(),
            access_key_id: "cairn_u".to_owned(),
            role,
            method: AuthMethod::Bearer,
            chunk_signing: None,
            user_policy: None,
            is_session: false,
        }
    }

    #[test]
    fn mint_requires_administrator() {
        let s = store();
        assert!(
            mint_into(&s, None, 0).is_none(),
            "no principal -> no ticket"
        );
        assert!(
            mint_into(&s, Some(&principal(Role::Member)), 0).is_none(),
            "a member cannot mint a ticket"
        );
        assert!(s.lock().unwrap().is_empty(), "no ticket was stored");
        assert!(mint_into(&s, Some(&principal(Role::Administrator)), 0).is_some());
    }

    #[test]
    fn ticket_is_single_use() {
        let s = store();
        let t = mint_into(&s, Some(&principal(Role::Administrator)), 0).unwrap();
        assert!(consume_ticket(&s, &t, 1).is_some(), "first use succeeds");
        assert!(
            consume_ticket(&s, &t, 1).is_none(),
            "a consumed ticket cannot be used again"
        );
    }

    #[test]
    fn ticket_expires() {
        let s = store();
        let t = mint_into(&s, Some(&principal(Role::Administrator)), 0).unwrap();
        // Open exactly at expiry (now == exp) is rejected (strict `>`), as is anything later.
        assert!(
            consume_ticket(&s, &t, TICKET_TTL_MS).is_none(),
            "an expired ticket is rejected"
        );
    }

    #[test]
    fn mint_prunes_expired_entries() {
        let s = store();
        // A ticket minted at t=0 expires at TICKET_TTL_MS. Minting again well past that prunes it.
        let _ = mint_into(&s, Some(&principal(Role::Administrator)), 0).unwrap();
        assert_eq!(s.lock().unwrap().len(), 1);
        let _ = mint_into(&s, Some(&principal(Role::Administrator)), TICKET_TTL_MS + 1).unwrap();
        assert_eq!(
            s.lock().unwrap().len(),
            1,
            "the first (expired) ticket was pruned, leaving only the fresh one"
        );
    }

    #[test]
    fn unknown_ticket_is_rejected() {
        let s = store();
        assert!(consume_ticket(&s, "nope", 0).is_none());
    }
}
