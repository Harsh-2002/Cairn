<script>
  import { api } from "../lib/api.js";
  import { whenMs } from "../lib/format.js";

  let entries = $state([]);
  let error = $state("");
  let loading = $state(true);

  async function load() {
    loading = true;
    error = "";
    try {
      const res = await api.failedReplication(100);
      entries = (res && res.entries) || [];
    } catch (err) {
      error = err.message || "Failed to load replication failures.";
    } finally {
      loading = false;
    }
  }

  load();
</script>

<h1>Replication</h1>
<p class="subtitle">Outbox entries the replication engine has given up on.</p>

{#if error}
  <div class="notice error">{error}</div>
{/if}

<div class="toolbar">
  <button onclick={load}>Refresh</button>
</div>

{#if loading}
  <p class="muted">Loading…</p>
{:else if entries.length === 0}
  <div class="empty">No failed replication entries.</div>
{:else}
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
      {#each entries as e, i (i)}
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
{/if}

<style>
  .err {
    color: var(--danger);
    max-width: 28rem;
    word-break: break-word;
    white-space: normal;
  }
</style>
