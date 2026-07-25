#!/usr/bin/env python3
"""Generate the Cairn dashboard for OpenObserve.

Schema: the community format (github.com/openobserve/dashboards), version 5 —
dashboard -> tabs[] -> panels[], each panel a PromQL query on a 48-column grid.

Two rules are enforced mechanically, not by eye:

1. NO DUPLICATED INFORMATION. Every panel must answer a question no other panel answers, so no
   query string may appear twice anywhere in the dashboard. `assert_unique()` fails the build
   otherwise. The split is: a *stat tile* for a scalar whose current value is what matters, a
   *chart* for a series whose trend is what matters — never both for the same thing.

2. UNITS MUST BE REAL AND CORRECT. Only units in OpenObserve's vocabulary are allowed
   (`UNITS` below, taken from the community dashboards), and `percent` is reserved for values a
   query has already scaled to 0-100 — a raw 0-1 fraction would need `percent-1`.
"""
import json
import os

GRID = 48

# OpenObserve's unit vocabulary, as used across the community dashboards.
UNITS = {
    "numbers", "short", "bytes", "percent", "percent-1", "ops", "bps", "Bps",
    "seconds", "ms", "milliseconds", "µs", "microseconds", "nanoseconds", "s",
    "megabytes", "dtdurations", "currency-dollar", "custom", "default",
}

_seen_queries: dict[str, str] = {}
panel_seq = 0


def assert_unique(query: str, where: str) -> str:
    """No question may be asked twice anywhere in the dashboard."""
    q = " ".join(query.split())
    if q in _seen_queries:
        raise SystemExit(
            f"DUPLICATE PANEL DATA:\n  query: {q}\n  first: {_seen_queries[q]}\n  again: {where}"
        )
    _seen_queries[q] = where
    return query


def panel(kind, title, queries, x, y, w, h, unit, decimals, desc, tab, legend=True):
    global panel_seq
    if unit not in UNITS:
        raise SystemExit(f"INVALID UNIT {unit!r} on panel {title!r}; valid: {sorted(UNITS)}")
    # A `percent` unit must be fed an already-scaled 0-100 value.
    for q, _ in queries:
        if unit == "percent" and "100 *" not in q and "100*" not in q:
            raise SystemExit(f"UNIT percent on {title!r} but the query does not scale to 0-100: {q}")
        assert_unique(q, f"{tab}/{title}")
    panel_seq += 1
    return {
        "id": f"Panel_{panel_seq:03d}",
        "type": kind,
        "title": title,
        "description": desc,
        "config": {
            "show_legends": legend,
            "legends_position": None,
            "unit": unit,
            "decimals": decimals,
            "line_thickness": 1.5,
            "top_results_others": False,
            "axis_border_show": False,
            "label_option": {"rotate": 0},
            "show_symbol": False,
            "line_interpolation": "smooth",
            "legend_width": {"unit": "px"},
            "base_map": {"type": "osm"},
            "map_type": {"type": "world"},
            "map_view": {"zoom": 1, "lat": 0, "lng": 0},
            "map_symbol_style": {"size": "by Value", "size_by_value": {"min": 1, "max": 100},
                                 "size_fixed": 2},
            "drilldown": [],
            "mark_line": [],
            "connect_nulls": False,
            "no_value_replacement": "",
            "wrap_table_cells": False,
            "table_transpose": False,
            "table_dynamic_columns": False,
        },
        "queryType": "promql",
        "queries": [
            {
                "query": q,
                "vrlFunctionQuery": "",
                "customQuery": True,
                "fields": {
                    "stream": "cairn_requests_total",
                    "stream_type": "metrics",
                    "x": [], "y": [], "z": [], "breakdown": [],
                    "filter": {"filterType": "group", "logicalOperator": "AND", "conditions": []},
                },
                "config": {"promql_legend": leg},
            }
            for q, leg in queries
        ],
        "layout": {"x": x, "y": y, "w": w, "h": h, "i": panel_seq},
        "htmlContent": "",
        "markdownContent": "",
    }


class Tab:
    """Lays panels left-to-right, wrapping at the 48-column grid."""

    def __init__(self, tab_id, name):
        self.tab_id, self.name = tab_id, name
        self.x = self.y = self.row_h = 0
        self.panels = []

    def _add(self, kind, title, queries, w, h, unit, decimals, desc, legend=True):
        if self.x + w > GRID:
            self.x, self.row_h = 0, 0
            self.y += self.row_h or h
        self.panels.append(
            panel(kind, title, queries, self.x, self.y, w, h, unit, decimals, desc, self.name,
                  legend)
        )
        self.x += w
        self.row_h = max(self.row_h, h)
        return self

    def stat(self, title, q, unit="short", decimals=0, desc=""):
        """A scalar whose *current value* is the point. Never also charted."""
        return self._add("metric", title, [(q, "")], 12, 4, unit, decimals, desc, legend=False)

    def line(self, title, queries, unit="short", decimals=2, desc=""):
        """A series whose *trend* is the point. Never also a stat."""
        return self._add("line", title, queries, 24, 9, unit, decimals, desc)

    def area(self, title, queries, unit="short", decimals=2, desc=""):
        return self._add("area", title, queries, 24, 9, unit, decimals, desc)

    def row(self):
        self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        return self

    def json(self):
        return {"tabId": self.tab_id, "name": self.name, "panels": self.panels}


# ── Overview — the health snapshot. Stored state now, plus the four top-level trends. ────────
ov = Tab("overview", "Overview")
ov.stat("Buckets", "cairn_buckets", desc="Buckets on this node.")
ov.stat("Objects", "cairn_objects", desc="Current objects, excluding noncurrent versions.")
ov.stat("Stored on disk", "cairn_physical_bytes", unit="bytes",
        desc="Physical bytes after compression — what actually occupies the filesystem.")
ov.stat("Compression saved", "clamp_min(cairn_logical_bytes - cairn_physical_bytes, 0)",
        unit="bytes",
        desc="Logical minus physical. Reads 0 for incompressible data, where the block container "
             "costs marginally more than it saves.")
ov.row()
ov.line("Request rate by route",
        [("sum by (route) (rate(cairn_requests_total[5m]))", "{route}")], unit="ops",
        desc="Requests/sec by surface: s3, api, web, share, and the health probes.")
ov.line("Errors by status",
        [('sum by (status) (rate(cairn_requests_total{status=~"4..|5.."}[5m]))', "{status}")],
        unit="ops", desc="Only 4xx and 5xx. A rising 5xx line is the thing to alert on.")
ov.line("Throughput",
        [("rate(cairn_bytes_sent_total[5m])", "sent"),
         ("rate(cairn_bytes_received_total[5m])", "received")], unit="Bps",
        desc="Object bytes leaving and entering the node.")
ov.line("S3 latency",
        # The summary is labelled by method+route, so aggregate — otherwise each quantile draws a
        # dozen identically-named series.
        [('max(cairn_request_duration_seconds{route="s3",quantile="0.5"})', "p50"),
         ('max(cairn_request_duration_seconds{route="s3",quantile="0.9"})', "p90"),
         ('max(cairn_request_duration_seconds{route="s3",quantile="0.99"})', "p99")],
        unit="seconds", decimals=4,
        desc="Worst-case data-plane handling latency across methods, by quantile.")

# ── Requests — the traffic breakdowns Overview does not show. ────────────────────────────────
rq = Tab("requests", "Requests")
rq.stat("Requests/sec", "sum(rate(cairn_requests_total[5m]))", unit="ops", decimals=2,
        desc="All surfaces combined.")
rq.stat("Error ratio",
        '100 * sum(rate(cairn_requests_total{status=~"4..|5.."}[5m])) '
        "/ clamp_min(sum(rate(cairn_requests_total[5m])), 0.0001)",
        unit="percent", decimals=2, desc="Share of requests that failed.")
rq.stat("5xx/sec", 'sum(rate(cairn_requests_total{status=~"5.."}[5m]))', unit="ops", decimals=3,
        desc="Server faults only — should sit at zero.")
rq.stat("Slowest route p99", 'max(cairn_request_duration_seconds{quantile="0.99"})',
        unit="seconds", decimals=4, desc="The worst p99 across every surface.")
rq.row()
rq.line("By method", [("sum by (method) (rate(cairn_requests_total[5m]))", "{method}")],
        unit="ops", desc="The GET/PUT/HEAD/DELETE/POST mix.")
rq.line("All responses by status",
        [("sum by (status) (rate(cairn_requests_total[5m]))", "{status}")], unit="ops",
        desc="Every status, not just failures — the success baseline Overview omits.")
rq.line("Mean latency by route",
        [("sum by (route) (rate(cairn_request_duration_seconds_sum[5m])) "
          "/ clamp_min(sum by (route) (rate(cairn_request_duration_seconds_count[5m])), 0.0001)",
          "{route}")], unit="seconds", decimals=4,
        desc="Average handling time per surface (the quantiles live on Overview).")
rq.line("Share-link responses",
        [('sum by (status) (rate(cairn_requests_total{route="share"}[5m]))', "{status}")],
        unit="ops",
        desc="Public /share/{token} traffic. 410 means expired or revoked, 404 an unknown token.")

# ── Storage — shape of what is stored. No counters repeated from Overview. ───────────────────
st = Tab("storage", "Storage")
st.stat("Noncurrent versions", "clamp_min(cairn_versions - cairn_objects, 0)",
        desc="Superseded versions still retained. Lifecycle rules expire these.")
st.stat("Average object size", "cairn_logical_bytes / clamp_min(cairn_objects, 1)", unit="bytes",
        desc="Logical bytes per current object — small-object workloads stress metadata, not disk.")
st.stat("Compression ratio", "cairn_compression_ratio", decimals=4,
        desc="Physical / logical. Below 1.0 means compression is winning.")
st.row()
st.area("Logical bytes stored",
        [("cairn_logical_bytes", "logical")], unit="bytes",
        desc="Bytes as clients see them, before compression. What this costs on disk is the "
             "'Stored on disk' tile on Overview; the ratio between them is the tile to the left.")
st.line("Versions over time", [("cairn_versions", "versions")],
        desc="Total versions, current plus noncurrent. A rising line with a flat object count "
             "means superseded versions are accumulating — what lifecycle rules expire.")

# ── Metadata engine — the single group-committing writer, the WAL, the cache. ────────────────
md = Tab("metadata", "Metadata engine")
md.stat("Writer queue depth", "cairn_writer_queue_depth",
        desc="Mutations submitted but not yet committed. Sustained growth is write saturation.")
md.stat("Commits/sec", "rate(cairn_writer_commit_seconds_count[5m])", unit="ops", decimals=2,
        desc="Group commits per second — each one is a durability barrier (fsync).")
md.stat("WAL size", "cairn_wal_bytes", unit="bytes",
        desc="Unbounded growth means a long-lived reader is starving the checkpointer.")
md.stat("Cache hit ratio",
        "100 * rate(cairn_meta_cache_hits_total[5m]) "
        "/ clamp_min(rate(cairn_meta_cache_hits_total[5m]) "
        "+ rate(cairn_meta_cache_misses_total[5m]), 0.0001)",
        unit="percent", decimals=1, desc="Read-through config cache in front of SQLite.")
md.row()
md.line("Group-commit duration",
        [('cairn_writer_commit_seconds{quantile="0.5"}', "p50"),
         ('cairn_writer_commit_seconds{quantile="0.9"}', "p90"),
         ('cairn_writer_commit_seconds{quantile="0.99"}', "p99")],
        unit="seconds", decimals=5, desc="Wall time of one metadata durability barrier.")
md.line("Group-commit batch size",
        [('cairn_writer_batch_size{quantile="0.5"}', "p50"),
         ('cairn_writer_batch_size{quantile="0.99"}', "p99")], decimals=1,
        desc="Mutations coalesced into each fsync — higher amortises the barrier better.")
md.line("Metadata cache traffic",
        [("rate(cairn_meta_cache_hits_total[5m])", "hits/sec"),
         ("rate(cairn_meta_cache_misses_total[5m])", "misses/sec")], unit="ops",
        desc="Raw hit and miss rates behind the ratio above.")
md.line("WAL checkpoints",
        [("rate(cairn_wal_checkpoints_total[5m])", "checkpoints/sec"),
         ("rate(cairn_wal_checkpointed_frames_total[5m])", "frames/sec")], unit="ops", decimals=3,
        desc="Truncating checkpoints are what keep the WAL bounded.")

# ── Replication & integrity ──────────────────────────────────────────────────────────────────
rp = Tab("replication", "Replication & integrity")
rp.stat("Failed", "cairn_replication_failed",
        desc="Terminally failed outbox entries, after the retry budget. Should be 0.")
rp.stat("Unreplicated", "cairn_replication_unreplicated",
        desc="Versions with no replica yet.")
rp.stat("Claimed", "cairn_replication_claimed", desc="Entries currently leased by a worker.")
rp.stat("Key-less blob reads", "cairn_blob_encrypted_without_key_total",
        desc="Encrypted reads refused for want of a key. Crypto fails closed: an error, never "
             "plaintext or zeros. Non-zero warrants investigation.")
rp.row()
rp.line("Replication lag", [("cairn_replication_lag_seconds", "oldest due entry")], unit="seconds",
        decimals=1,
        desc="Age of the oldest entry still owed. Cross-host redundancy is asynchronous, so this "
             "is the observable window of exposure.")
rp.line("Outbox depth", [("cairn_replication_queue_depth", "due")],
        desc="Entries due for shipping. Draining to zero is healthy; a rising floor is not.")

tabs = [ov, rq, st, md, rp]
dashboard = {
    "version": 5,
    "dashboardId": "cairn-object-storage",
    "title": "Cairn — Object Storage",
    "description": (
        "Cairn single-node S3-compatible object storage: traffic, stored bytes and compression, "
        "the group-committing metadata writer, and asynchronous replication. Scraped from Cairn's "
        "Prometheus endpoint on :7373/metrics."
    ),
    "role": "",
    "owner": "",
    "created": "2026-07-25T00:00:00.000Z",
    "tabs": [t.json() for t in tabs],
    "variables": {"list": [], "showDynamicFilters": True},
}

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Cairn.dashboard.json")
with open(out, "w") as f:
    json.dump(dashboard, f, indent=2)

total = sum(len(t.panels) for t in tabs)
print(f"wrote {out}")
print(f"tabs {len(tabs)}  panels {total}  distinct queries {len(_seen_queries)}")
for t in tabs:
    print(f"  {t.name:<26} {len(t.panels):>2} panels")
# every query is unique by construction (assert_unique), so this must hold:
assert len(_seen_queries) == sum(len(p['queries']) for t in tabs for p in t.panels)
print("✓ no duplicated panel data")
units = {p["config"]["unit"] for t in tabs for p in t.panels}
print("✓ units used, all valid:", ", ".join(sorted(units)))
