<script>
  import { api } from "../lib/api.js";
  import { bytes, count, ratio } from "../lib/format.js";

  let data = $state(null);
  let error = $state("");
  let loading = $state(true);

  async function load() {
    loading = true;
    error = "";
    try {
      data = await api.overview();
    } catch (err) {
      error = err.message || "Failed to load overview.";
    } finally {
      loading = false;
    }
  }

  load();
</script>

<h1>Overview</h1>
<p class="subtitle">Store-wide storage and compression figures.</p>

{#if error}
  <div class="notice error">{error}</div>
{/if}

{#if loading}
  <p class="muted">Loading…</p>
{:else if data}
  <div class="cards">
    <div class="card">
      <div class="label">Buckets</div>
      <div class="value">{count(data.buckets)}</div>
    </div>
    <div class="card">
      <div class="label">Objects</div>
      <div class="value">{count(data.objects)}</div>
    </div>
    <div class="card">
      <div class="label">Versions</div>
      <div class="value">{count(data.versions)}</div>
    </div>
    <div class="card">
      <div class="label">Logical bytes</div>
      <div class="value mono">{bytes(data.logical_bytes)}</div>
    </div>
    <div class="card">
      <div class="label">Physical bytes</div>
      <div class="value mono">{bytes(data.physical_bytes)}</div>
    </div>
    <div class="card">
      <div class="label">Compression ratio</div>
      <div class="value">{ratio(data.compression_ratio)}</div>
    </div>
  </div>

  <div class="toolbar" style="margin-top:1.4rem;">
    <button onclick={load}>Refresh</button>
  </div>
{/if}
