<script>
  import { api } from "../lib/api.js";
  import { bytes, count, whenMs } from "../lib/format.js";
  import { navigate } from "../lib/router.js";

  let { name } = $props();

  let detail = $state(null);
  let objects = $state([]);
  let next = $state(null);
  let error = $state("");
  let loading = $state(true);
  let prefix = $state("");

  async function loadDetail() {
    try {
      detail = await api.getBucket(name);
    } catch (err) {
      // A detail failure should not hide the object listing.
      detail = null;
      if (!error) error = err.message || "Failed to load bucket detail.";
    }
  }

  async function loadObjects() {
    loading = true;
    error = "";
    try {
      const res = await api.listObjects(name, { prefix, limit: 100 });
      objects = (res && res.objects) || [];
      next = (res && res.next) || null;
    } catch (err) {
      error = err.message || "Failed to load objects.";
      objects = [];
    } finally {
      loading = false;
    }
  }

  function applyPrefix(e) {
    e.preventDefault();
    loadObjects();
  }

  // Re-run whenever the bucket name from the route changes.
  $effect(() => {
    // touch `name` so the effect tracks it
    void name;
    detail = null;
    objects = [];
    next = null;
    prefix = "";
    loadDetail();
    loadObjects();
  });
</script>

<div class="crumbs">
  <a
    href="#/buckets"
    onclick={(e) => {
      e.preventDefault();
      navigate("/buckets");
    }}>Buckets</a
  >
  <span> / </span>
  <span class="mono">{name}</span>
</div>

<h1 class="mono">{name}</h1>

{#if error}
  <div class="notice error">{error}</div>
{/if}

{#if detail}
  <div class="cards" style="margin-bottom:1.4rem;">
    <div class="card">
      <div class="label">Objects</div>
      <div class="value">{count(detail.object_count)}</div>
    </div>
    <div class="card">
      <div class="label">Logical bytes</div>
      <div class="value mono">{bytes(detail.logical_bytes)}</div>
    </div>
    <div class="card">
      <div class="label">Versioning</div>
      <div class="value" style="font-size:1.1rem;">{detail.versioning}</div>
    </div>
    <div class="card">
      <div class="label">Ownership</div>
      <div class="value" style="font-size:1.1rem;">
        {detail.ownership_mode}
      </div>
    </div>
    <div class="card">
      <div class="label">Region</div>
      <div class="value" style="font-size:1.1rem;">{detail.region}</div>
    </div>
  </div>
{/if}

<h2>Objects</h2>
<form class="toolbar" onsubmit={applyPrefix}>
  <input placeholder="prefix filter" bind:value={prefix} autocomplete="off" />
  <button type="submit">Filter</button>
  <button
    type="button"
    onclick={() => {
      prefix = "";
      loadObjects();
    }}>Clear</button
  >
</form>

{#if loading}
  <p class="muted">Loading…</p>
{:else if objects.length === 0}
  <div class="empty">No objects{prefix ? ` under "${prefix}"` : ""}.</div>
{:else}
  <table>
    <thead>
      <tr>
        <th>Key</th>
        <th class="num">Size</th>
        <th>ETag</th>
        <th>Last modified</th>
      </tr>
    </thead>
    <tbody>
      {#each objects as o (o.key)}
        <tr>
          <td class="mono">{o.key}</td>
          <td class="num">{bytes(o.size)}</td>
          <td class="mono">{o.etag}</td>
          <td>{whenMs(o.last_modified_ms)}</td>
        </tr>
      {/each}
    </tbody>
  </table>
  {#if next}
    <p class="muted" style="margin-top:0.8rem;">
      More results available (next token: <span class="mono">{next}</span>).
    </p>
  {/if}
{/if}
