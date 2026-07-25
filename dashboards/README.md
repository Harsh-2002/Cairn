# Cairn observability dashboards

A ready-to-import [OpenObserve](https://openobserve.ai) dashboard for a Cairn node, plus the
generator that produces it.

| File | What it is |
|---|---|
| `Cairn.dashboard.json` | The dashboard, in OpenObserve's community v5 schema — import this. |
| `generate.py` | Regenerates the JSON. Edit this, not the JSON. |

## What it shows

Five tabs, 35 panels, shaped around Cairn's actual architecture rather than a generic
object-store template:

| Tab | Answers |
|---|---|
| **Overview** | Is it healthy? Buckets, objects, bytes on disk, compression saved; request rate by route, errors by status, throughput, S3 latency quantiles. |
| **Requests** | Where is traffic going and what is failing? Rate, error ratio, 5xx, worst p99; breakdowns by method and status, mean latency per route, share-link responses. |
| **Storage** | What is stored and how efficiently? Noncurrent versions, average object size, compression ratio, logical bytes and version growth. |
| **Metadata engine** | Cairn's distinctive core: the single group-committing writer. Queue depth, commits/sec, group-commit duration and batch-size quantiles, cache hit ratio, WAL size and checkpoints. |
| **Replication & integrity** | Asynchronous redundancy and fail-closed crypto: outbox depth, replication lag, failed/unreplicated entries, and refused key-less encrypted reads. |

Two invariants are enforced by `generate.py` at build time rather than by eye:

- **No duplicated information.** No query string may appear twice anywhere in the dashboard;
  `assert_unique()` fails the build otherwise. A scalar whose *current value* matters is a stat
  tile; a series whose *trend* matters is a chart — never both for the same thing.
- **Correct units.** Only units in OpenObserve's vocabulary are accepted, and `percent` is
  rejected unless the query already scales to 0–100 (a raw 0–1 fraction needs `percent-1`).

## Import

Console → **Dashboards → Import**, and upload `Cairn.dashboard.json`. Or via the API:

```sh
curl -u "$O2_USER:$O2_PASS" -X POST \
  "$O2_URL/api/default/dashboards" \
  -H 'Content-Type: application/json' \
  --data-binary @Cairn.dashboard.json
```

## Getting Cairn's metrics into OpenObserve

Cairn exposes Prometheus metrics at `/metrics` on the S3 port (`:7373`), unauthenticated and ahead
of the concurrency limiter. OpenObserve does not scrape; point any Prometheus-compatible collector
at it. An OpenTelemetry Collector is the smallest thing that works:

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: cairn
          scrape_interval: 15s
          metrics_path: /metrics
          scheme: https                    # http:// if the node serves plaintext
          tls_config:
            insecure_skip_verify: true     # only for a self-signed dev cert
          static_configs:
            - targets: ['127.0.0.1:7373']
              labels:
                node: cairn-1

processors:
  batch: { send_batch_size: 8192, timeout: 10s }

exporters:
  otlphttp/openobserve:
    endpoint: http://127.0.0.1:5080/api/default
    headers:
      Authorization: "Basic <base64 of email:password>"
      stream-name: cairn

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [batch]
      exporters: [otlphttp/openobserve]
```

Prometheus itself with `remote_write` to
`$O2_URL/api/default/prometheus/api/v1/write` works equally well.

## Regenerating

```sh
python3 generate.py     # rewrites Cairn.dashboard.json, failing on duplicate data or bad units
```

The metric names come from `crates/cairn-server/src/observability.rs`; see `docs/observability.md`
(ARCH 26) for what each one means.
