import { api, errorMessage } from "./api";
import { getReplication } from "./s3";
import type {
  ReplicationConfiguration,
  ReplicationRule,
  ReplicationStatusResp,
  ReplicationTarget,
  ReplicationTargetCount,
} from "./types";

export interface BucketReplication {
  bucket: string;
  configuration: ReplicationConfiguration | null;
  targets: ReplicationTarget[];
  status: ReplicationStatusResp | null;
  errors: string[];
  configurationError: string | null;
}

async function read<T>(
  load: () => Promise<T>,
  context: string,
): Promise<{ value: T | null; error: string | null }> {
  try {
    return { value: await load(), error: null };
  } catch (e) {
    return {
      value: null,
      error: `${context}: ${errorMessage(e, "Refresh to try again.")}`,
    };
  }
}

export async function readBucketReplication(
  bucket: string,
): Promise<BucketReplication> {
  const [configuration, targets, status] = await Promise.all([
    read(() => getReplication(bucket), "Configuration unavailable"),
    read(
      async () => (await api.listReplicationTargets(bucket)).targets,
      "Targets unavailable",
    ),
    read(async () => {
      const result = await api.replicationStatus(bucket);
      const valid = (
        r: Pick<ReplicationTargetCount, "pending" | "claimed" | "failed">,
      ) =>
        [r.pending, r.claimed, r.failed].every(
          (n) => Number.isSafeInteger(n) && n >= 0,
        );
      if (
        !valid(result) ||
        !Array.isArray(result.by_target) ||
        !result.by_target.every(valid)
      ) {
        throw new Error(
          "Incomplete replication counters. Refresh after updating the node.",
        );
      }
      return result;
    }, "Status unavailable"),
  ]);
  return {
    bucket,
    configuration: configuration.value,
    targets: targets.value ?? [],
    status: status.value,
    configurationError: configuration.error,
    errors: [configuration.error, targets.error, status.error].filter(
      (e): e is string => e != null,
    ),
  };
}

/** Bound the bucket fan-out, including its presign and configuration requests. */
export async function readReplicationBuckets(
  buckets: { name: string }[],
): Promise<BucketReplication[]> {
  const results: BucketReplication[] = new Array(buckets.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(4, buckets.length) }, async () => {
      while (next < buckets.length) {
        const index = next++;
        results[index] = await readBucketReplication(buckets[index]!.name);
      }
    }),
  );
  return results;
}

export interface ReplicationRow {
  bucket: string;
  target: string | null;
  destination: string;
  rules: ReplicationRule[];
  counts: ReplicationTargetCount | null;
  errors: string[];
}

export function replicationRows(snapshot: BucketReplication): ReplicationRow[] {
  const groups = new Map<string | null, ReplicationRule[]>();
  for (const rule of snapshot.configuration?.rules ?? []) {
    const rules = groups.get(rule.dest_bucket) ?? [];
    rules.push(rule);
    groups.set(rule.dest_bucket, rules);
  }
  for (const counts of snapshot.status?.by_target ?? []) {
    if (!groups.has(counts.target_arn)) groups.set(counts.target_arn, []);
  }
  if (!groups.size && snapshot.errors.length) groups.set(null, []);
  return [...groups].map(([target, rules]) => {
    const match = snapshot.targets.find((t) => t.arn === target);
    return {
      bucket: snapshot.bucket,
      target,
      rules,
      errors: snapshot.errors,
      destination: match
        ? `${match.dest_bucket} @ ${match.endpoint}`
        : (target ?? "Legacy / unassigned target"),
      counts: snapshot.status
        ? (snapshot.status.by_target.find((t) => t.target_arn === target) ?? {
            target_arn: target,
            pending: 0,
            claimed: 0,
            failed: 0,
          })
        : null,
    };
  });
}

export function replicationHealth(row: ReplicationRow): {
  tone: "negative" | "warning" | "neutral" | "positive";
  label: string;
} {
  if (row.errors.length || !row.counts)
    return { tone: "neutral", label: "Unavailable" };
  if (row.counts.failed > 0) return { tone: "negative", label: "Failing" };
  if (row.counts.pending > 0 || row.counts.claimed > 0)
    return { tone: "warning", label: "Replicating" };
  if (!row.rules.some((r) => r.enabled))
    return { tone: "neutral", label: "Disabled" };
  return { tone: "positive", label: "Healthy" };
}
