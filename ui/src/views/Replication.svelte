<script>
  import { api } from "../lib/api.js";
  import { whenMs } from "../lib/format.js";
  import Skeleton from "../components/Skeleton.svelte";

  let entries = $state([]);
  let error = $state("");
  let loading = $state(true);
  let refreshing = $state(false);

  // Show the full skeleton only on first load; later refreshes keep the table visible and just
  // mark the Refresh button busy.
  async function load() {
    if (!loading) refreshing = true;
    error = "";
    try {
      const res = await api.failedReplication(100);
      entries = (res && res.entries) || [];
    } catch (err) {
      error = err.message || "Could not load replication status.";
    } finally {
      loading = false;
      refreshing = false;
    }
  }

  load();

  // A stable key per row: the object identity plus attempt count. Falling back to the index keeps
  // keys unique if two entries ever share the same fields.
  const rowKey = (e, i) =>
    `${e.bucket}:${e.key}:${e.version_id || ""}:${e.attempts}:${i}`;
</script>

<h1>Replication</h1>
<p class="subtitle">
  Objects that could not be copied to your replication target after repeated tries.
</p>

{#if error}
  <div class="notice danger" role="alert">{error}</div>
{/if}

<div class="toolbar">
  <button onclick={load} disabled={refreshing || loading} aria-busy={refreshing}>
    {refreshing ? "Refreshing…" : "Refresh"}
  </button>
</div>

{#if loading}
  <div class="table-wrap" aria-busy="true">
    <Skeleton lines={6} gap="0.9em" />
  </div>
{:else if entries.length === 0}
  <div class="empty">
    <p class="empty-title">No failed replications.</p>
    <p class="empty-body">
      Objects mirror to your target automatically. Anything that cannot be copied after
      several tries will be listed here so you can look into it.
    </p>
  </div>
{:else}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Bucket</th>
          <th>Key</th>
          <th>Version</th>
          <th class="num">Attempts</th>
          <th>Next attempt</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody>
        {#each entries as e, i (rowKey(e, i))}
          <tr>
            <td class="mono">{e.bucket}</td>
            <td class="mono">{e.key}</td>
            <td class="mono">{e.version_id || "—"}</td>
            <td class="num">{e.attempts}</td>
            <td>{whenMs(e.next_attempt_at_ms)}</td>
            <td class="mono err">{e.error || "—"}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .err {
    color: var(--danger-ink);
    max-width: 28rem;
    word-break: break-word;
    white-space: normal;
  }
  .empty-title {
    margin: 0 0 4px;
    font-weight: 600;
    color: var(--text);
  }
  .empty-body {
    margin: 0 auto;
    max-width: 34rem;
    font-size: 0.92rem;
    line-height: 1.55;
  }
</style>
