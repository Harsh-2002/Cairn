# Zero-copy GETs on pooled keep-alive connections: design options

Date: 2026-10-28 · Branch: `research/get-put-root-cause` · Read-only design note; no code changed.

**Question.** `crates/cairn-server/src/fast_get.rs` serves eligible GETs with `sendfile(2)` and keeps the
connection for further GETs, but `serve_plaintext` (`server.rs:767-798`) cedes the socket to hyper
forever the first time the loop meets a request it cannot serve, and `head_eligible`
(`fast_get.rs:194-209`) requires the method to be `GET`. Warp reuses pooled connections that already
carried `HEAD /<bucket>/`, `?versioning`, `?object-lock`, `?list-type=2` and a prepare `PUT`, so
sendfile engages on **0 %** of Warp GETs (`research_notes/GET PUT root cause/fast-io-engagement.md`).

The prize is measured and real: same binary, 1 MiB, one keep-alive connection, 300 iterations —
**1.100 ms CPU/op streamed vs 0.667 ms sendfile** (`get-pipeline.md` §4.1), i.e. **0.43 ms/op and
15 syscalls/MiB**, ≈40 % of per-request CPU on a warm connection and ≈9 % of the 4.8 ms/op
whole-server figure.

**Bottom line.** Only two of the four options are worth building. The smallest change that actually
puts Warp GETs on `sendfile` is the *close-after-cede* rule (option 2): ~15 lines plus a config knob,
but it is a **client-visible connection-lifetime policy** whose cost is one TCP handshake per ceded
request. The smallest change that is *safe by construction* is mmap + `Bytes::from_owner` (option 3):
no protocol or connection changes at all, but it recovers only about a third of sendfile's advantage
because the 1 MiB copy into the socket buffer remains. A Cairn-owned all-method HTTP/1.1 loop
(option 1) is a 1.5–2.5 kLOC security-critical rewrite of what hyper already does, and hyper exposes
no transport hook that would avoid it (option 4).

---

## 1. Where the connection is lost, exactly

| Step | Code | Behaviour |
|---|---|---|
| Accept | `server.rs:418-426` | TLS → `serve_tls`; plaintext → `serve_plaintext` |
| Fast offer | `server.rs:776-796` | `fast-io` + `role.is_api()` only; one call per connection |
| Method gate | `fast_get.rs:194-209` | `head.is_get` **and** no `content-length`/`transfer-encoding`/`if-*`/`upgrade`/`origin`/`service-worker` |
| Cede | `fast_get.rs:378-380` | `fallback(stream, buf, "ineligible")` → `Fast::Fallback` |
| Hand-off | `server.rs:790-793` | `serve_io(stream, …)` — hyper owns the socket for its lifetime |
| Re-arm | — | none; `try_sendfile_get` is never called again for that socket |

The keep-alive loop itself already works: after a successful `sendfile` the socket is switched back
to non-blocking and re-registered (`fast_get.rs:572-583`), with pipelined bytes carried in `carry`
(`fast_get.rs:344`, `479`, `577`). The loop is not what is missing; **re-entry after a cede** is.

Pre-existing divergence found while tracing this (worth fixing before *any* option widens fast-path
engagement): `fast_get.rs:386-399` routes with `route_path(&path)` (path-style only), while the hyper
adapter routes with `route_request(stack.s3_domain, &host, &path)` (`adapter.rs:616-627`,
`2091-2109`). With `CAIRN_S3_DOMAIN` set, a virtual-host request `GET /a/b.jpg` on
`Host: photos.example.com` is resolved by the fast path as bucket `a`, key `b.jpg` — authorization
and the served object both use the wrong bucket. It is reachable today only in `fast-io` builds, and
it means: **fix vhost routing (one call, plus a test) before making the fast path serve more
traffic.**

---

## 2. Option 1 — a Cairn-owned HTTP/1.1 loop that serves every method

### 2.1 What it must implement beyond today's loop

The existing loop only ever consumes a **complete, bodyless GET head**. Serving `PUT`/`POST`/
`DELETE`/multipart/`HEAD`/list/conditional GET in the loop requires, in order of risk:

1. **Request framing.** `Content-Length` (duplicate/zero/oversized handling) and
   `Transfer-Encoding: chunked` (chunk extensions, trailers, size limits). This is the request
   smuggling surface: RFC 9112 §6.3 rules for CL+TE conflicts, obs-fold rejection, header-count and
   header-size limits, absolute-form targets, HTTP/1.0 semantics. hyper's parser
   (`hyper-1.10.1/src/proto/h1/role.rs:229-358`) already does all of this; a hand-rolled loop must
   reproduce it or become a desync vector behind a proxy.
2. **A bounded body stream.** `S3Service::handle` takes `cairn_types::BodyStream`
   (`request.rs:68-93`) and the aws-chunked/SigV4 streaming decoder wraps *that* stream
   (`service.rs:6571-6607`, `chunked.rs`). Producing it from the socket is easy
   (`AsyncReadExt::take(n)` + `stream::unfold`); *bounding* it correctly, draining it on early
   error, and never letting leftover bytes become the next request is the hard part — today that
   policy lives in `adapter.rs:673-679` + `finish_body_hygiene` + `drain_or_close`, and would have
   to be re-created at the socket layer for every method.
3. **`Expect: 100-continue`.** boto3/aws-sdk send it for uploads. hyper answers it when the body is
   first polled (`role.rs:229-310, 358`). A Cairn loop must emit `HTTP/1.1 100 Continue` itself or
   clients stall ~1 s per PUT, and must answer `417` for other expectations.
4. **Response framing.** `Empty`/`Bytes` bodies are trivial. `S3Body::Stream` is not: compressed or
   encrypted objects, multi-range responses and XML can be arbitrarily large, so the loop needs
   chunked transfer-encoding with per-chunk write timeouts and disconnect handling — a second
   streaming writer with its own backpressure, alongside the existing blocking `sendfile` writer.
5. **The adapter's policy tail.** The fast path currently defers every error to hyper precisely so
   it does not have to reproduce `render_negotiated` (HTML error pages, `adapter.rs:2358-2400`),
   `constrain_service_worker_scope`, console-origin CORS (`adapter.rs:660-672`), `x-amz-request-id`,
   the request-metrics middleware (`server.rs:883-990`) and STS/POST/OPTIONS interception
   (`adapter.rs:294-388`). Serving PUTs and their 4xx/5xx in the loop means all of that moves, or is
   refactored out of the adapter into a transport-neutral layer first.
6. **Keep-alive bookkeeping.** `Connection: close`, HTTP/1.0 default-close, idle and header
   timeouts, `max requests per connection`, and the `100-continue`/drain interaction with `close`.

### 2.2 Can it reuse `S3Service`?

Yes — `S3Service::handle(S3Request, BodyStream)` is transport-neutral: method, bucket, key, parsed
query, headers, `principal`, `source`, secure flag, request id. Authorization, policy/ACL/BPA,
routing and body selection would not be duplicated. That is the *only* cheap part. Everything in
§2.1 is new code that replaces hyper, and none of it is reusable from hyper.

### 2.3 Estimate and risk

- **Size:** ~1.5–2.5 kLOC + tests (framing/CL/TE matrix, 100-continue, chunked+trace, error drain,
  pipelining, limits). The existing loop is 979 lines for the bodyless-GET subset.
- **Risk:** high. The failure modes are request smuggling (two parsers disagreeing), connection
  desync (a body byte consumed as the next head), slowloris on a second code path, and a
  `100-continue` deadlock. Each is a security or availability incident, not a perf regression.
- **TLS:** unchanged — the loop is plaintext-only; `serve_tls` keeps hyper (and kTLS still does not
  provide file→socket transfers, `docs/data-plane.md` 7.6).
- **Verdict:** **reject.** It is the maximum-risk way to buy 0.43 ms/op, and it re-implements the one
  component (hyper's parser) that has had the most adversarial review in the stack.

---

## 3. Option 2 — force `Connection: close` after a ceded request

### 3.1 Mechanism (smaller than expected)

hyper 1.10 has exactly the switch needed: `http1::Builder::keep_alive(false)`
(`hyper-1.10.1/src/server/conn/http1.rs:272`). With keep-alive disabled, hyper inserts
`connection: close` on each HTTP/1.1 response and ends the connection after it
(`proto/h1/conn.rs:686-694`). No header injection, no service wrapper, no `Body` changes:

```rust
// server.rs:800-825, serve_io gains one flag
builder.http1().timer(…).header_read_timeout(…).keep_alive(!close_after_response);
```

`serve_plaintext` passes `close_after_response = true` only in the `Fast::Fallback` arm, so:
Fast GETs keep the loop; the **first ceded request is served and then the socket closes**; the
client's next request starts a fresh connection — whose first request is a GET, so it lands in the
loop and stays there. HTTP/2 is unaffected (`keep_alive` is an h1 builder option, and the fast path
cannot parse the h2 preface, so h2 connections never arrive with the flag).

### 3.2 Does Warp reconnect?

Yes. Warp is a Go client; Go's `net/http` sets `resp.Close` from `Connection: close`, removes the
connection from the idle pool, and dials a new one for the next request. The captured shape
(`fast-io-engagement.md` §2) becomes:

```
conn=1  HEAD /bucket/            -> ceded, served, closed
        (new conn) GET ?versioning   -> not_object fallback, no close
        ... ?object-lock, ?list-type=2 ...
        PUT /bucket/<key>        -> ceded, served, closed
        (new conn) GET /bucket/<key>  <- timed phase: FIRST request, eligible -> sendfile
        GET, GET, GET …               <- same connection, all sendfile
conn=2  PUT (first)              -> ceded, served, closed
        (new conn) GET …              <- sendfile
```

The handshake is paid **once per ceded connection, not once per GET** (the loop keeps the fresh
connection for every later GET). Warp pays ~16 reconnects per run against ~14.6 k GETs.

### 3.3 Effect by workload

| Workload | With close-after-cede | Cost |
|---|---|---|
| GET-only (curl, GET-only driver) | unchanged: first request eligible, loop keeps the socket | none |
| Warp `get` (HEAD/PUT then GETs) | **≈100 % of timed GETs sendfile** after one reconnect per pooled conn | 1 handshake per ceded request, amortised |
| Warp `put` (1 MiB, c=16) | every PUT served by hyper with `Connection: close` → one connection per PUT | +1 handshake/PUT; measured **54 µs** CPU per connect+accept+close (both ends, loopback) vs **19.6 ms p50** PUT latency — ≈0.3 % |
| Mixed PUT/GET alternation | ping-pong: handshake between each method change | worst case, one handshake per request |
| Compressed/encrypted buckets (GET-only) | **must not close**: `not_zerocopy` cedes are object-specific, not method-specific | none, if scoped (below) |
| Management `:7373` API, STS, presigned PUT | `not_object` / non-GET cedes; closing would pin one request per connection | avoid by scoping to the S3 object route + non-GET method |

### 3.4 Scoping — and the part that cannot be scoped away

Three guards make this safe without losing the benefit:

1. **Only a non-GET method closes.** Split the cede reason in `head_eligible`: `ineligible_method`
   (`!head.is_get`) closes; `ineligible_get` (conditional, `Origin`, `Content-Encoding`,
   service-worker, body-bearing GET) and `not_zerocopy` / `below_floor` do **not**. Otherwise a
   compressed-object or conditional-GET workload would reconnect on every request.
2. **Only when nothing is pipelined.** Force close only if `buf.len() == head_len` (no carried
   leftover). If the client pipelined a second request, today's behaviour (cede, no forced close)
   is preserved, so a pipelined non-idempotent request is never dropped.
3. **Only on the plaintext data-plane listener** (`fast-io`, `role.is_api()`) — which is already the
   only place `try_sendfile_get` runs.

What **cannot** be scoped away: the PUT-first case *is* the Warp case. "Close only after HEAD" or
"close only after bodyless requests" costs nothing on PUT, and helps nothing, because the `PUT` is
what cedes the connection on both captured Warp connection shapes. The trade is inherent: the
server cannot distinguish a benchmark that only PUTs from a client that will GET next.

The earlier note (`get-pipeline.md` §6.4) rejected this option as "would regress PUT far more than
sendfile could win". That claim was not quantified and is wrong on this host: a loopback handshake
is **54 µs of CPU for both ends** (measured, 2000 iterations, 3 runs: 54.6/53.2/55.2 µs) while the
PUT it would be attached to has a **19.6 ms p50** (`fr38-results/put.json`) and the GET win is
**430 µs**. Even a pure-PUT workload pays <1 % CPU. The real cost is not CPU but **one extra RTT of
latency per ceded request on a high-latency link** (≈1 RTT per PUT for a WAN client doing sequential
PUTs, which is a 2× latency regression at 20 ms RTT for a small object). That is deployment-specific
and unmeasurable here, which is why this must ship as a knob, not as unconditional behaviour.

### 3.5 Implementation sketch

- `fast_get.rs`: `pub enum Fast { Handled, Fallback { stream: Rewind, close_after: bool } }`;
  `fallback(stream, buf, reason, close_after)`; at the `!head_eligible` site pass
  `!head.is_get && buf.len() == head_len`; add `cairn_sendfile_ceded_close_total` so the policy is
  observable.
- `server.rs`: `serve_io(stream, state, peer, secure, role, conn_shutdown, close_after_response)`
  (four call sites: `618`, `633`, `792`, `797`); `.keep_alive(!close_after_response)` at `821-824`;
  `serve_plaintext` forwards `Fast::Fallback { close_after, .. }`.
- `config.rs`: `CAIRN_FASTIO_CLOSE_CEDED` (bool, default **on** in `fast-io` builds; `off` restores
  today's behaviour) with validation + `docs/configuration.md` §28, and update
  `docs/data-plane.md` 7.6 / `docs/benchmarks.md`.
- Guard first: the vhost routing fix from §1.

---

## 4. Option 3 — `mmap` + `Bytes::from_owner` through hyper

### 4.1 Shape

`S3Body::ZeroCopy` already carries `Arc<File>` + `offset` + `len` (`types/blob.rs:269-276`, produced
at `blob/lib.rs:1078-1082`). In `adapter.rs::render` (`2333-2348`) the `ZeroCopy` arm currently
throws that away and streams. Instead: map exactly the requested range and hand hyper one `Bytes`:

```rust
// adapter.rs, ZeroCopy arm, cfg(feature = "fast-io")
let mmap = MmapOptions::new().offset(zc.offset).len(zc.len as usize).map(&zc.file)?; // unsafe fn
full_body(Bytes::from_owner(mmap))   // Mmap: AsRef<[u8]> + Send + 'static
```

`bytes-1.11.1` has `Bytes::from_owner<T: AsRef<[u8]> + Send + 'static>` (`bytes.rs:250-252`);
`memmap2::Mmap` implements `AsRef<[u8]>` and Derefs to the exact requested range (memmap2 aligns
internally; `MmapOptions::offset(30)` is a documented example), so no wrapper struct and no slice
bookkeeping. `Bytes` keeps the mapping alive for as long as hyper holds the frame.

Two constraints that are easy to miss:

- **`memmap2::MmapOptions::map` is `unsafe`.** `cairn-server` is `forbid(unsafe_code)` in default
  builds and `deny` only under `fast-io` (`main.rs:11-12`), so this arm must be
  `#[cfg(feature = "fast-io")]` with an `#[allow(unsafe_code)]` + SAFETY note, exactly like
  `sendfile.rs`. No new `unsafe` in `cairn-blob` (which is `forbid(unsafe_code)`,
  `blob/lib.rs:8`), and the mapping is built in the server from the fd the blob layer already opened.
- **New dependency:** `memmap2` (not in `Cargo.lock` today). It is a safe wrapper over `mmap`,
  used by rustls' ecosystem and many storage crates; still a new dependency to audit and pin.

### 4.2 Copies, syscalls, measured CPU

Isolated microbenchmark on this host (`/tmp/opencode/zerocopy-probe.c`): page-cached 1 MiB file →
loopback TCP, peer draining, 400 iterations, process CPU only. Two runs:

| path | kernel copies of the 1 MiB | syscalls per MiB | measured CPU / 1 MiB |
|---|---|---:|---:|
| current: `pread` 64 KiB + `write` ×16 | 2 (page cache→user, user→socket); the server adds 1 more (`Bytes::copy_from_slice`) | 16 pread + ~12.6 write | **725–735 µs** |
| `mmap` + one `write` | 1 (mapped page→socket) + 256 minor faults | 1 mmap + 1 write + 1 munmap | **583–593 µs** |
| `sendfile(2)` | 1 (page cache→socket; may be avoided with `sendpage`) | 1 | **291–311 µs** |

So mmap removes the read syscalls, the per-chunk allocations and the reader-thread/channel hops, but
it keeps the 1 MiB copy into the socket buffer and adds page-table work. In the full request path
those extra streamed-path costs (16 allocs, 16 channel sends, 3 blocking-pool hops; see
`get-pipeline.md` §2) are worth more than in this tight loop, so the expected server figure is
**≈0.75–0.85 ms/op vs 1.100 streamed and 0.667 sendfile** — i.e. it recovers roughly **one third** of
the gap. That is an estimate, not a measurement; it must be A/B'd before it is quoted.

### 4.3 Risks

- **SIGBUS on a short/truncated file.** The streamed path deliberately tolerates a blob shorter than
  its recorded length (`blob/lib.rs:1032-1039`: "must NOT panic the read path"); `sendfile` reports
  it as a `WriteZero` error. Touching a mapped page past EOF raises **SIGBUS, which kills the
  process**. Mitigation: `fstat` the fd and map only `min(len, file_len - offset)` (fall back to the
  stream when short), and never map a blob whose length check failed. Residual race (external
  truncation after the fstat) still terminates the process — the streamed path degrades instead.
- **RSS / memory accounting.** File-backed clean pages are reclaimable, but they count in RSS and
  cgroup memory; concurrent large GETs create one mapping each (VMA count, page-table memory).
  No per-request buffer is held, so it is not the `read_memory_bound` budget — that is fine, but the
  relationship to the blob read permits changes (mmap drops the stream, hence the permit).
- **Large objects.** A 5 GiB object becomes a single 5 GiB `Bytes` frame. hyper handles partial
  writes, but `graceful_shutdown` and idle timeouts only get control between frames; either cap the
  mmap path (e.g. ≤64 MiB) or emit page-aligned window frames. This is real added complexity.
- **No behaviour change:** works for pooled connections, any first method, TLS, h2, conditional
  GETs — none of the option-2 blast radius.

---

## 5. Option 4 — hyper 1.x transport hooks

**There are none.** Verified against `hyper-1.10.1`:

- No `sendfile`/`splice`/`zerocopy` anywhere in the crate (`grep` over `src/` returns nothing).
- `hyper::body::Body` yields `Frame`s of `Bytes` only; `http-body-util` adds `Full`/`StreamBody`,
  no file-backed body. The response head is written before the body, so a `Body` impl cannot
  influence *how* the body reaches the socket.
- `hyper::rt::{Read, Write, ReadBufCursor}` is the IO abstraction. A custom `Write` wrapper sees
  only opaque byte slices; nothing in the stream says "these bytes are a file range", so a
  transport-level `sendfile` cannot be triggered from inside a response body.
- `http1::Builder` options are `keep_alive`, `half_close`, `writev`, `max_buf_size`,
  `pipeline_flush`, `max_headers`, `header_read_timeout`, `timer` (`http1.rs:264-410`). `writev(true)`
  is worth enabling regardless (head+first frame in one `writev`), but it does not remove the copy.
- The only way to get the IO back is `Connection::without_shutdown()` / `into_parts()`
  (`http1.rs:150-190`) — **after** the connection completes — or `with_upgrades()` +
  `hyper::upgrade::on()`, which requires the service to return `101 Switching Protocols`. Neither
  can take a live connection back between requests.
- `hyper::ext` carries h1/h2 header extensions (`ReasonPhrase`), not transport hooks.

So "patch or fork hyper" is the only path to sendfile-through-hyper, and that is out of scope by
construction.

---

## 6. Recommendation

**Primary (do this): option 2, scoped and gated** — it is the only small change that actually puts
Warp's GETs on `sendfile`, at 15 lines plus a knob and a conformance test.

Order of work:

1. **Fix the vhost routing divergence** (`fast_get.rs:394` → `route_request(stack.s3_domain, &host,
   &path)`) + test. Prerequisite: it is a correctness/authorization bug that wide engagement would
   make routine.
2. **Split the cede reason** and add `Fast::Fallback { close_after }` with the two guards
   (non-GET only, no pipelined leftover).
3. **`keep_alive(!close_after_response)`** in `serve_io`; new `CAIRN_FASTIO_CLOSE_CEDED` (default on
   under `fast-io`, `off` restores today).
4. **New counters**: `cairn_sendfile_ceded_close_total` and reuse
   `cairn_sendfile_fallback_total{reason="ineligible_method"}`; document in `docs/benchmarks.md`
   and `docs/data-plane.md` 7.6 that engagement for pooled clients now depends on this policy.
5. **Measure**: `warp get` engagement must go from 0 % to ~100 %, `warp put` CPU/op and p50 must stay
   within noise, and `conformance/sendfile_keepalive.sh` must stay 20/20.

**If the WAN handshake cost is unacceptable** (put-heavy remote clients), make the knob default
`off` for those deployments and take option 3 instead: it costs no protocol risk and works for every
client, but expect ~⅓ of the sendfile gain and accept the SIGBUS/new-dependency caveats. Option 3 is
also the right answer for **TLS** GETs, where option 2 does nothing at all (the fast path is
plaintext-only), making it the better long-term investment if both are eventually wanted.

**Reject option 1** (full all-method loop) and **do not look for option 4** (there is nothing to
find).

---

## 7. Tests

Unit (`crates/cairn-server/src/fast_get.rs` `#[cfg(test)]`):

- `cede_reason_classification`: GET/HEAD/PUT/POST/DELETE, conditional GET, `Origin` GET,
  body-bearing GET, `?versioning` GET → asserts `close_after` is true **only** for non-GET methods.
- `pipelined_leftover_suppresses_close`: two heads in one write → `close_after == false`, and the
  second request is still reachable through `Rewind`.
- `fallback_when_vhost_domain_configured`: `Host: photos.s3.example.com`, `GET /a/b.jpg`, with
  `s3_domain = Some("s3.example.com")` → bucket `photos`, key `a/b.jpg` (regression for §1).

Connection-level (new `conformance/sendfile_pooled.sh`, modelled on `sendfile_keepalive.sh`; raw
Python client so the request shape is explicit):

- `get_only_keepalive_all_sendfile`: 20 × GET on one connection → `cairn_sendfile_get_total{result="ok"}`
  +20, bodies byte-identical (existing gate).
- `head_then_get_reconnects`: `HEAD /b/`, then `GET /b/k` **on a new connection** → HEAD response has
  `connection: close`; GET is sendfile'd. Asserts the policy, not just the counter.
- `put_then_get_reconnects`: `PUT /b/k` (1 MiB, `Content-Length`), read the response, observe close,
  reconnect, `GET /b/k` → sendfile; body byte-identical; `cairn_sendfile_ceded_close_total` +1.
- `put_only_no_extra_requests_lost`: 5 sequential PUTs, each on its own connection → all 200, all
  bodies readable, no truncated responses (guards the "close before the response drains" failure).
- `pipelined_gets_are_not_split`: two GET heads in one `send()` on a fast connection → two complete
  responses, correct framing (existing `carry` path).
- `range_and_multi_range`: single range → 206 + exact `content-range`, sendfile; multi-range and
  unsatisfiable → fallback, byte-correct, **no** forced close.
- `slow_client`: 8 MiB object, client stops reading → server must not hang past
  `FAST_WRITE_TIMEOUT`, connection torn down, no task leak.
- `tls_fallback`: same sequence over `https://` → correct bodies, `cairn_sendfile_get_total` flat, no
  `connection: close` injected, kTLS counter unchanged.
- `http2_prior_knowledge`: h2c client on the same listener still works and is never force-closed.

Byte-identical parity: for a ceded `PUT`/`HEAD`/list response, diff status line, headers (excluding
`connection`) and body against a non-`fast-io` build — the existing conformance suites
(`objects.sh`, `listing.sh`, `multipart.sh`) cover the semantics; add one explicit assertion that the
only added header is `connection: close`.

## 8. Residual risks

- **Latency, not CPU:** one RTT per ceded request on high-latency links (WAN sequential PUTs ≈2×
  latency). Mitigated only by the knob; must be documented in the config reference.
- **Pipelined close race:** hyper may have read a second head ahead; with `keep_alive(false)` it is
  dropped and the client must retry (idempotent requests only). Acceptable (pipelining is extinct),
  but state it.
- **Connection-count observability:** dashboards/LB metrics will see connection churn rise on
  PUT-heavy traffic. Add the ceded-close counter so it is explainable.
- **Interaction with `drain_or_close`:** a ceded request that already declares `Connection: close`
  is unaffected; `set_connection_close` (`adapter.rs:2320-2325`) and `keep_alive(false)` agree.
- **Option 3 only:** SIGBUS on externally truncated blobs, new `memmap2` dependency, `unsafe` block
  gated behind `fast-io`, and large-object frame sizing.

## 9. Artefacts

- `/tmp/opencode/zerocopy-probe.c` — 1 MiB transfer microbenchmark (pread/`sendfile`/mmap, table
  in §4.2).
- `/tmp/opencode/connect-probe.c` — loopback connect+accept+close CPU (54 µs, §3.4).
- Measurements reused from `get-pipeline.md` §4.1 (`fr38-results/get-fastio.json`, probe_ab.py) and
  `fr38-results/put.json`.
