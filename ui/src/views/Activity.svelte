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
      const res = await api.activity(50);
      entries = (res && res.entries) || [];
    } catch (err) {
      error = err.message || "Could not load activity.";
    } finally {
      loading = false;
      refreshing = false;
    }
  }

  load();

  // A stable key per row: the timestamp plus what changed. Falling back to the index keeps keys
  // unique if two entries ever share the same fields.
  const rowKey = (e, i) =>
    `${e.at_ms}:${e.action}:${e.bucket || ""}:${e.key || ""}:${i}`;
</script>

<h1>Activity</h1>
<p class="subtitle">A log of recent changes admins have made to buckets, users, and settings.</p>

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
    <Skeleton lines={8} gap="0.9em" />
  </div>
{:else if entries.length === 0}
  <div class="empty">
    <p class="empty-title">No changes recorded yet.</p>
    <p class="empty-body">
      Actions like creating a bucket, adding a user, or editing a policy will appear here.
    </p>
  </div>
{:else}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>When</th>
          <th>Action</th>
          <th>Bucket</th>
          <th>Key</th>
        </tr>
      </thead>
      <tbody>
        {#each entries as e, i (rowKey(e, i))}
          <tr>
            <td>{whenMs(e.at_ms)}</td>
            <td class="mono">{e.action}</td>
            <td class="mono">{e.bucket || "—"}</td>
            <td class="mono">{e.key || "—"}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .empty-title {
    margin: 0 0 4px;
    font-weight: 600;
    color: var(--text);
  }
  .empty-body {
    margin: 0;
    font-size: 0.92rem;
  }
</style>
