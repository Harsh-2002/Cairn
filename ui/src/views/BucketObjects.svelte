<script>
  import { api, s3 } from "../lib/api.js";
  import { bytes, count, whenMs } from "../lib/format.js";
  import { navigate } from "../lib/router.js";
  import BucketConfig from "./BucketConfig.svelte";

  let { name } = $props();

  let detail = $state(null);
  let objects = $state([]);
  let next = $state(null);
  let error = $state("");
  let loading = $state(true);
  let prefix = $state("");
  let busy = $state(false);
  let fileInput;
  let shared = $state(null); // { key, url } of the most recently minted share link
  let copied = $state(false);

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

  async function refresh() {
    await loadObjects();
    await loadDetail();
  }

  function applyPrefix(e) {
    e.preventDefault();
    loadObjects();
  }

  async function upload(e) {
    const files = e.target.files;
    if (!files || !files.length) return;
    busy = true;
    error = "";
    try {
      for (const f of files) {
        await s3.putObject(name, (prefix || "") + f.name, f);
      }
      await refresh();
    } catch (err) {
      error = err.message || "Upload failed.";
    } finally {
      busy = false;
      if (fileInput) fileInput.value = "";
    }
  }

  async function download(key) {
    error = "";
    try {
      const blob = await s3.getObjectBlob(name, key);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = key.split("/").pop() || key;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      error = err.message || "Download failed.";
    }
  }

  async function remove(key) {
    if (!confirm(`Delete "${key}"? This cannot be undone.`)) return;
    busy = true;
    error = "";
    try {
      await s3.deleteObject(name, key);
      await refresh();
    } catch (err) {
      error = err.message || "Delete failed.";
    } finally {
      busy = false;
    }
  }

  async function share(key) {
    error = "";
    copied = false;
    try {
      const res = await api.shareObject(name, key, 3600);
      shared = { key, url: window.location.origin + res.url };
    } catch (err) {
      error = err.message || "Could not create share link.";
    }
  }

  async function copyShare() {
    try {
      await navigator.clipboard.writeText(shared.url);
      copied = true;
    } catch {
      copied = false;
    }
  }

  // Re-run whenever the bucket name from the route changes. The loads are kicked off in a
  // microtask so their synchronous prop reads don't run inside the effect's tracking scope (which
  // otherwise re-ran the effect and clobbered the freshly-loaded list back to empty).
  $effect(() => {
    void name;
    detail = null;
    objects = [];
    next = null;
    prefix = "";
    queueMicrotask(() => {
      loadDetail();
      loadObjects();
    });
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

{#key name}
  <BucketConfig {name} />
{/key}

<h2>Objects</h2>
<div class="toolbar">
  <form class="row" onsubmit={applyPrefix} style="gap:8px;">
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
  <span class="spacer"></span>
  <input
    type="file"
    multiple
    bind:this={fileInput}
    onchange={upload}
    style="display:none"
  />
  <button
    class="primary"
    disabled={busy}
    onclick={() => fileInput && fileInput.click()}
  >
    {busy ? "Working…" : "Upload"}
  </button>
</div>
{#if prefix}
  <p class="muted" style="margin:-8px 0 12px;font-size:0.85rem;">
    Uploads will be prefixed with <span class="mono">{prefix}</span>.
  </p>
{/if}

{#if shared}
  <div class="panel" style="margin-bottom:14px;">
    <div class="label-sm">
      Public share link for <span class="mono">{shared.key}</span> — valid for 1 hour,
      no sign-in required
    </div>
    <div class="secret-box">{shared.url}</div>
    <div class="actions">
      <button class="sm primary" onclick={copyShare}>
        {copied ? "Copied!" : "Copy link"}
      </button>
      <a class="btn sm" href={shared.url} target="_blank" rel="noopener">Open</a>
      <button class="sm" onclick={() => (shared = null)}>Dismiss</button>
    </div>
  </div>
{/if}

{#if loading}
  <p class="muted">Loading…</p>
{:else if objects.length === 0}
  <div class="empty">No objects{prefix ? ` under "${prefix}"` : ""}.</div>
{:else}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Key</th>
          <th class="num">Size</th>
          <th>ETag</th>
          <th>Last modified</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {#each objects as o (o.key)}
          <tr>
            <td class="mono">{o.key}</td>
            <td class="num">{bytes(o.size)}</td>
            <td class="mono">{o.etag}</td>
            <td>{whenMs(o.last_modified_ms)}</td>
            <td>
              <div class="actions">
                <button class="sm" onclick={() => download(o.key)}>
                  Download
                </button>
                <button class="sm" onclick={() => share(o.key)}>Share</button>
                <button class="sm danger" disabled={busy} onclick={() => remove(o.key)}>
                  Delete
                </button>
              </div>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
  {#if next}
    <p class="muted" style="margin-top:0.8rem;">
      More results available (next token: <span class="mono">{next}</span>).
    </p>
  {/if}
{/if}
