// Replication regressions run in real Chrome alongside the console performance fixtures.
import { act } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter, Route, Routes } from "react-router";
import { api } from "../../src/lib/api";
import {
  parseReplication,
  replicationXml,
} from "../../src/lib/replication-config";
import {
  readBucketReplication,
  readReplicationBuckets,
  replicationRows,
  replicationHealth,
} from "../../src/lib/replication-status";
import {
  getReplication,
  putReplication,
  deleteReplication,
} from "../../src/lib/s3";
import { BucketSettings } from "../../src/views/bucket-settings";
import { Replication } from "../../src/views/replication";
import type { ReplicationStatusResp } from "../../src/lib/types";

function assert(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}
async function rejects(run: () => unknown, message: string) {
  let failed = false;
  try {
    await run();
  } catch {
    failed = true;
  }
  assert(failed, message);
}
const rule = (id: string, target: string, enabled = true) =>
  `<Rule><ID>${id}</ID><Status>${enabled ? "Enabled" : "Disabled"}</Status><Priority>7</Priority><Filter><Prefix>old/</Prefix></Filter><Destination><Bucket>${target}</Bucket></Destination></Rule>`;
const configuration = (...rules: string[]) =>
  `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>retained-role</Role>${rules.join("")}</ReplicationConfiguration>`;
const simple = configuration(rule("retained-id", "target-a"));
const multi = configuration(
  rule("first", "target-a", false),
  rule("second", "target-b"),
);

export async function runReplicationTests() {
  const original = { ...api };
  const originalFetch = window.fetch;
  let xml = simple;
  let responseStatus = 200;
  let writes = 0;
  let written = "";
  let targetError = false;
  let statusError = false;
  let failureError = false;
  let listError = false;
  let bucketCount = 1;
  let status: ReplicationStatusResp = {
    bucket: "source",
    pending: 0,
    claimed: 1,
    failed: 0,
    lag_seconds: 0,
    by_target: [{ target_arn: "target-a", pending: 0, claimed: 1, failed: 0 }],
    recent_errors: [],
  };
  api.presignShare = async (bucket) => ({
    url: `https://s3.example.test/${bucket}`,
    expires_at_ms: Date.now() + 300000,
    absolute: true,
    session: {
      access_key_id: "fixture",
      session_token: "fixture",
      expires_at_ms: Date.now() + 3600000,
    },
  });
  window.fetch = async (_input, init) => {
    if (init?.method && init.method !== "GET") {
      writes++;
      written = String(init.body ?? "");
      return new Response(null, { status: 204 });
    }
    return new Response(xml, { status: responseStatus });
  };
  api.listBuckets = async () => {
    if (listError) throw new Error("bucket list unavailable");
    return {
      buckets: Array.from({ length: bucketCount }, (_, i) => ({
        name: `source-${String(i).padStart(4, "0")}`,
        owner_id: "fixture",
        created_at_ms: 0,
        versioning: "Enabled" as const,
      })),
    };
  };
  api.listReplicationTargets = async () => {
    if (targetError) throw new Error("target service unavailable");
    return {
      targets: ["a", "b"].map((id) => ({
        arn: `target-${id}`,
        endpoint: `https://destination-${id}.example.test`,
        dest_bucket: `destination-${id}`,
        region: "us-east-1",
        access_key_id: "public-id",
        insecure_skip_verify: false,
        has_ca_cert: false,
      })),
    };
  };
  api.replicationStatus = async () => {
    if (statusError) throw new Error("status service unavailable");
    return status;
  };
  api.failedReplication = async () => {
    if (failureError) throw new Error("failure service unavailable");
    return {
      entries: [
        {
          id: "attempt-1",
          bucket: "source-0000",
          key: "same-object",
          version_id: "same-version",
          target_arn: "target-a",
          attempts: 4,
          error: "<Error><Message>Access denied</Message></Error>",
          next_attempt_at_ms: 0,
        },
        {
          id: "attempt-2",
          bucket: "source-0000",
          key: "same-object",
          version_id: "same-version",
          target_arn: "target-b",
          attempts: 4,
          error: "destination unavailable",
          next_attempt_at_ms: 0,
        },
      ],
    };
  };
  api.eventsTicket = async () => {
    throw new Error("fixture SSE disabled");
  };
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  try {
    const parsed = parseReplication(simple);
    assert(parsed.editable, "one enabled supported rule is editable");
    const updated = parseReplication(
      replicationXml(parsed, "new&destination", " prefix<& / ", true, true),
    );
    assert(
      updated.rules[0].id === "retained-id" &&
        updated.rules[0].prefix === " prefix<& / " &&
        updated.rules[0].dest_bucket === "new&destination",
      "edits retain ID and exact escaped prefix/destination",
    );
    assert(
      updated.xml.includes("retained-role") &&
        updated.xml.includes("<Priority>7</Priority>"),
      "edits preserve Role and priority",
    );
    const namespaced = simple
      .replaceAll("<", "<s:")
      .replaceAll("<s:/", "</s:")
      .replace("xmlns=", "xmlns:s=");
    assert(
      parseReplication(namespaced).editable,
      "prefixed S3 namespaces are supported",
    );
    assert(
      parseReplication(multi).rules.length === 2 &&
        !parseReplication(multi).editable,
      "every rule is read and multi-rule edits are protected",
    );
    assert(
      !parseReplication(configuration(rule("disabled", "target-a", false)))
        .editable,
      "disabled rule is protected",
    );
    assert(
      !parseReplication(
        simple.replace(
          "</Destination>",
          "<StorageClass>STANDARD</StorageClass></Destination>",
        ),
      ).editable,
      "unknown options are protected",
    );
    await rejects(
      () => parseReplication("<ReplicationConfiguration>"),
      "malformed XML fails",
    );
    await rejects(() => parseReplication("<Other/>"), "unexpected XML fails");
    xml = multi;
    await rejects(
      () =>
        putReplication("protected", "replacement", "", { expected: parsed }),
      "fresh multi-rule configuration blocks save",
    );
    await rejects(
      () => deleteReplication("protected", parsed),
      "fresh multi-rule configuration blocks remove",
    );
    xml = simple.replace("retained-id", "changed-id");
    await rejects(
      () => putReplication("changed", "replacement", "", { expected: parsed }),
      "changed configuration blocks save",
    );
    assert(writes === 0, "protected edits send no mutations");
    xml = simple;
    await putReplication("editable", "new-target", " exact prefix ", {
      expected: parsed,
    });
    assert(
      Number(writes) === 1 &&
        parseReplication(written).rules[0].prefix === " exact prefix ",
      "supported save uses fresh configuration and preserves whitespace",
    );
    for (const [code, http] of [
      ["AccessDenied", 403],
      ["NoSuchBucket", 404],
      ["ServiceUnavailable", 503],
    ] as const) {
      xml = `<Error><Code>${code}</Code></Error>`;
      responseStatus = http;
      await rejects(
        () => getReplication("missing"),
        `${code} is not treated as absent configuration`,
      );
    }
    xml = "<Error><Code>ReplicationConfigurationNotFoundError</Code></Error>";
    responseStatus = 404;
    assert(
      (await getReplication("missing")) === null,
      "only explicit missing configuration is absence",
    );
    responseStatus = 200;
    xml = simple;
    let rows = replicationRows(await readBucketReplication("source"));
    assert(
      replicationHealth(rows[0]).label === "Replicating",
      "claimed-only queue stays active",
    );
    xml = multi;
    status = {
      ...status,
      claimed: 0,
      failed: 2,
      by_target: [
        { target_arn: "target-b", pending: 0, claimed: 0, failed: 2 },
        { target_arn: null, pending: 0, claimed: 1, failed: 0 },
      ],
    };
    rows = replicationRows(await readBucketReplication("source"));
    assert(
      rows.length === 3 &&
        replicationHealth(rows[0]).label === "Disabled" &&
        replicationHealth(rows[1]).label === "Failing" &&
        replicationHealth(rows[2]).label === "Replicating",
      "destinations and legacy claims have independent health",
    );
    for (const kind of ["configuration", "target", "status"] as const) {
      targetError = kind === "target";
      statusError = kind === "status";
      xml = kind === "configuration" ? "broken XML" : multi;
      rows = replicationRows(await readBucketReplication("source"));
      assert(
        rows.length > 0 &&
          rows.every((r) => replicationHealth(r).label === "Unavailable"),
        `${kind} failure never becomes healthy or absent`,
      );
    }
    targetError = false;
    statusError = false;
    xml = simple;
    const savedStatus = api.replicationStatus;
    let active = 0;
    let maximum = 0;
    api.replicationStatus = async () => {
      active++;
      maximum = Math.max(maximum, active);
      await new Promise((resolve) => setTimeout(resolve, 0));
      active--;
      return status;
    };
    const bounded = await readReplicationBuckets(
      Array.from({ length: 25 }, (_, i) => ({ name: `bounded-${i}` })),
    );
    assert(
      maximum === 4 && bounded[24].bucket === "bounded-24",
      "bucket loading is bounded to four and retains order",
    );
    api.replicationStatus = savedStatus;
    status = { ...status, pending: 0, claimed: 0, failed: 0, by_target: [] };
    bucketCount = 2000;
    await act(async () =>
      root.render(
        <MemoryRouter>
          <Replication />
        </MemoryRouter>,
      ),
    );
    for (
      let i = 0;
      i < 100 && !host.textContent?.includes("Page 1 of 40");
      i++
    ) {
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
      });
    }
    const destinations = () =>
      host.querySelector(
        'section[aria-labelledby="replication-destinations"]',
      )!;
    const failures = () =>
      host.querySelector('section[aria-labelledby="replication-failures"]')!;
    const button = (label: string) => {
      const result = [...host.querySelectorAll("button")].find(
        (b) => b.textContent?.trim() === label,
      );
      assert(result, `replication button ${label}`);
      return result;
    };
    assert(
      destinations().querySelectorAll("tbody tr").length === 50 &&
        host.textContent?.includes("Page 1 of 40"),
      "2000 replication destinations render 50 rows",
    );
    assert(
      failures().querySelectorAll("tbody tr").length === 2 &&
        failures().textContent?.includes("Manual retry required"),
      "same-version failures retain distinct target entries and explain retry",
    );
    await act(async () => button("Next").click());
    assert(
      host.textContent?.includes("Page 2 of 40") &&
        destinations().textContent?.includes("source-0050"),
      "replication pagination advances",
    );
    listError = true;
    failureError = true;
    await act(async () => button("Refresh").click());
    assert(
      destinations().textContent?.includes("Stale") &&
        !destinations().textContent?.includes("Healthy"),
      "failed refresh neutralizes stale health",
    );
    assert(
      failures().textContent?.includes("Failure list unavailable") &&
        failures().querySelectorAll("tbody tr").length === 2,
      "failure refresh error preserves diagnostics with a stale warning",
    );
    assert(
      document.documentElement.scrollWidth <= window.innerWidth,
      "replication does not overflow the viewport",
    );
    api.getBucketConfig = async () => ({
      versioning: "Enabled",
      ownership_mode: "BucketOwnerEnforced",
      quota_bytes: null,
      policy: null,
      cors: null,
      tagging: null,
      lifecycle: null,
      acl: null,
      public_access_block: null,
      encryption: null,
    });
    api.getBucket = async () => {
      throw new Error("fixture compression unavailable");
    };
    api.getNotifications = async () => ({ endpoints: [] });
    xml = multi;
    await act(async () =>
      root.render(
        <MemoryRouter
          key="settings"
          initialEntries={["/buckets/protected/settings"]}
        >
          <Routes>
            <Route
              path="/buckets/:name/settings"
              element={<BucketSettings />}
            />
          </Routes>
        </MemoryRouter>,
      ),
    );
    for (let i = 0; i < 100 && !host.querySelector('[aria-label="Settings sections"] button'); i++) {
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
      });
    }
    const integrations = [
      ...host.querySelectorAll<HTMLButtonElement>('[aria-label="Settings sections"] button'),
    ].find((b) => b.textContent?.includes("Integrations"));
    assert(integrations, "settings integrations section exists");
    await act(async () => {
      integrations.dispatchEvent(
        new MouseEvent("mousedown", { bubbles: true, button: 0 }),
      );
      integrations.click();
    });
    for (
      let i = 0;
      i < 100 && !host.textContent?.includes("Rule configuration is read-only");
      i++
    ) {
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
      });
    }
    const title = [...host.querySelectorAll("h3")].find((h) =>
      h.textContent?.startsWith("Replication"),
    );
    const card = title?.closest('[data-slot="card"]');
    assert(
      card && card.textContent?.includes("Rule configuration is read-only"),
      "multi-rule settings visibly explain protection",
    );
    const save = [...card.querySelectorAll("button")].find(
      (b) => b.textContent === "Save",
    );
    assert(
      save?.disabled &&
        ![...card.querySelectorAll("button")].some(
          (b) => b.textContent === "Remove rule" && !b.disabled,
        ),
      "multi-rule Settings cannot save or remove rules",
    );
    const before = writes;
    await act(async () => save.click());
    assert(writes === before, "protected Settings sends no mutation");
  } finally {
    await act(async () => root.unmount());
    host.remove();
    Object.assign(api, original);
    window.fetch = originalFetch;
  }
}
