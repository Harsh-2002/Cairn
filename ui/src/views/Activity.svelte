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
      const res = await api.activity(50);
      entries = (res && res.entries) || [];
    } catch (err) {
      error = err.message || "Failed to load activity.";
    } finally {
      loading = false;
    }
  }

  load();
</script>

<h1>Activity</h1>
<p class="subtitle">Recent mutating actions across the control plane.</p>

{#if error}
  <div class="notice error">{error}</div>
{/if}

<div class="toolbar">
  <button onclick={load}>Refresh</button>
</div>

{#if loading}
  <p class="muted">Loading…</p>
{:else if entries.length === 0}
  <div class="empty">No recent activity.</div>
{:else}
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
      {#each entries as e, i (i)}
        <tr>
          <td>{whenMs(e.at_ms)}</td>
          <td class="mono">{e.action}</td>
          <td class="mono">{e.bucket || "—"}</td>
          <td class="mono">{e.key || "—"}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}
