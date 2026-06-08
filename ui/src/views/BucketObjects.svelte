<script>
  import { api, s3 } from "../lib/api.js";
  import { bytes, count, whenMs } from "../lib/format.js";
  import { navigate } from "../lib/router.js";
  import { ok, err } from "../lib/toast.js";
  import ConfirmDialog from "../components/ConfirmDialog.svelte";
  import Skeleton from "../components/Skeleton.svelte";

  let { name } = $props();

  let detail = $state(null);
  let objects = $state([]);
  let next = $state(null); // continuation cursor for the next page, or null
  let error = $state("");
  let loading = $state(true);
  let loadingMore = $state(false);
  let prefix = $state("");
  let busy = $state(false);
  let fileInput;
  let shared = $state(null); // { key, url, expiresAtMs } of the most recently minted share link
  let previewing = $state(null); // { key, kind, url, text }
  let encryptUploads = $state(false);

  let pendingDelete = $state(null); // object key awaiting delete confirmation
  let deleting = $state(false);

  // Focus management for the preview modal.
  let previewCloseEl = $state(null);
  let previewReturnTo = null;

  const SHARE_TTL_SECS = 3600;

  async function loadDetail() {
    try {
      detail = await api.getBucket(name);
    } catch (e) {
      // A detail failure should not hide the object listing.
      detail = null;
      if (!error) error = e.message || "Failed to load bucket detail.";
    }
  }

  async function loadObjects() {
    loading = true;
    error = "";
    try {
      const res = await api.listObjects(name, { prefix, limit: 100 });
      objects = (res && res.objects) || [];
      next = (res && res.next) || null;
    } catch (e) {
      error = e.message || "Failed to load objects.";
      objects = [];
      next = null;
    } finally {
      loading = false;
    }
  }

  // Append the next page using the continuation cursor returned by the prior page.
  async function loadMore() {
    if (!next || loadingMore) return;
    loadingMore = true;
    error = "";
    try {
      const res = await api.listObjects(name, { prefix, limit: 100, cursor: next });
      objects = [...objects, ...((res && res.objects) || [])];
      next = (res && res.next) || null;
    } catch (e) {
      error = e.message || "Failed to load more objects.";
    } finally {
      loadingMore = false;
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
        await s3.putObject(name, (prefix || "") + f.name, f, {
          encrypt: encryptUploads,
        });
      }
      ok(files.length === 1 ? "File uploaded." : `${files.length} files uploaded.`);
      await refresh();
    } catch (e) {
      error = e.message || "Upload failed.";
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
    } catch (e) {
      error = e.message || "Download failed.";
    }
  }

  function askDelete(key) {
    pendingDelete = key;
  }

  async function confirmDelete() {
    const key = pendingDelete;
    if (!key) return;
    deleting = true;
    error = "";
    try {
      await s3.deleteObject(name, key);
      ok("Object deleted.");
      pendingDelete = null;
      await refresh();
    } catch (e) {
      err(e.message || "Delete failed.");
    } finally {
      deleting = false;
    }
  }

  async function share(key) {
    error = "";
    try {
      const res = await api.shareObject(name, key, SHARE_TTL_SECS);
      shared = {
        key,
        url: window.location.origin + res.url,
        expiresAtMs: res.expires_at_ms ?? Date.now() + SHARE_TTL_SECS * 1000,
      };
    } catch (e) {
      err(e.message || "Could not create share link.");
    }
  }

  async function copyShare() {
    try {
      await navigator.clipboard.writeText(shared.url);
      ok("Share link copied.");
    } catch {
      err("Copy was blocked. Select the link and copy it manually.");
    }
  }

  // ---- preview modal: focus trap + return focus -------------------------------------------------
  function closePreview() {
    if (previewing?.url) URL.revokeObjectURL(previewing.url);
    previewing = null;
    if (previewReturnTo && document.contains(previewReturnTo)) previewReturnTo.focus();
    previewReturnTo = null;
  }

  async function preview(key) {
    error = "";
    if (previewing?.url) URL.revokeObjectURL(previewing.url);
    previewReturnTo =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    try {
      const blob = await s3.getObjectBlob(name, key);
      const type = blob.type || "";
      if (type.startsWith("image/")) {
        previewing = { key, kind: "image", url: URL.createObjectURL(blob) };
      } else if (
        type.startsWith("text/") ||
        type.includes("json") ||
        type.includes("xml") ||
        type.includes("javascript") ||
        blob.size < 256 * 1024
      ) {
        const text = await blob.text();
        previewing = { key, kind: "text", text };
      } else {
        previewing = { key, kind: "none" };
      }
      queueMicrotask(() => previewCloseEl?.focus());
    } catch (e) {
      err(e.message || "Preview failed.");
    }
  }

  // Keep focus inside the open preview dialog (simple two-edge trap).
  function trapPreviewFocus(e) {
    if (e.key === "Escape") {
      e.preventDefault();
      closePreview();
      return;
    }
    if (e.key !== "Tab") return;
    const root = e.currentTarget;
    const focusable = root.querySelectorAll(
      'button, a[href], input, [tabindex]:not([tabindex="-1"])',
    );
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault();
      last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault();
      first.focus();
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
    shared = null;
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
  <div class="notice danger" role="alert">{error}</div>
{/if}

{#if loading && !detail}
  <div class="cards" style="margin-bottom:1.4rem;">
    {#each Array(4) as _, i (i)}
      <div class="card"><Skeleton lines={2} /></div>
    {/each}
  </div>
{:else if detail}
  <div class="cards" style="margin-bottom:1.4rem;">
    <div class="card">
      <div class="label">Objects</div>
      <div class="value">{count(detail.object_count)}</div>
    </div>
    <div class="card">
      <div class="label">Stored size</div>
      <div class="value mono">{bytes(detail.logical_bytes)}</div>
    </div>
    <div class="card">
      <div class="label">Versioning</div>
      <div class="value sm-value">{detail.versioning}</div>
    </div>
    <div class="card">
      <div class="label">Region</div>
      <div class="value sm-value">{detail.region}</div>
    </div>
  </div>
{/if}

<h2>Objects</h2>
<div class="toolbar">
  <form class="row prefix-form" onsubmit={applyPrefix}>
    <input placeholder="Filter by prefix" bind:value={prefix} autocomplete="off" />
    <button type="submit">Filter</button>
    {#if prefix}
      <button
        type="button"
        onclick={() => {
          prefix = "";
          loadObjects();
        }}>Clear</button
      >
    {/if}
  </form>
  <span class="spacer"></span>
  <label class="encrypt-toggle" for="encrypt-uploads">
    <input id="encrypt-uploads" type="checkbox" bind:checked={encryptUploads} />
    <span class="encrypt-text">
      <span class="encrypt-title">Encrypt new uploads</span>
      <span class="encrypt-hint">Files are encrypted at rest with a server-managed key (SSE-S3).</span>
    </span>
  </label>
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
    {busy ? "Uploading…" : "Upload files"}
  </button>
</div>
{#if prefix}
  <p class="muted prefix-note">
    Uploads will be saved under <span class="mono">{prefix}</span>.
  </p>
{/if}

{#if shared}
  <div class="share-card" role="group" aria-label="Public share link">
    <div class="share-head">
      <span class="badge warn">Public link</span>
      <span class="share-expiry">Expires {whenMs(shared.expiresAtMs)}</span>
    </div>
    <p class="share-explain">
      Anyone with this link can download
      <span class="mono">{shared.key}</span>
      with no sign-in until it expires. Share it only with people who should see this file.
    </p>
    <div class="share-link mono">{shared.url}</div>
    <div class="actions">
      <button class="sm primary" onclick={copyShare}>Copy link</button>
      <a class="btn sm" href={shared.url} target="_blank" rel="noopener">Open in new tab</a>
      <button class="sm" onclick={() => (shared = null)}>Done</button>
    </div>
  </div>
{/if}

{#if loading}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Key</th>
          <th class="num">Size</th>
          <th>ETag</th>
          <th>Last modified</th>
          <th><span class="visually-hidden">Actions</span></th>
        </tr>
      </thead>
      <tbody>
        {#each Array(4) as _, i (i)}
          <tr><td colspan="5"><Skeleton lines={1} /></td></tr>
        {/each}
      </tbody>
    </table>
  </div>
{:else if objects.length === 0}
  <div class="empty">
    {#if prefix}
      <p class="empty-title">No objects under "{prefix}"</p>
      <p class="empty-body">
        Nothing matches that prefix. Clear the filter to see everything, or upload a file
        to this prefix.
      </p>
    {:else}
      <p class="empty-title">This bucket is empty</p>
      <p class="empty-body">
        Use Upload files to add objects. They will appear here with their size and last
        modified time.
      </p>
    {/if}
  </div>
{:else}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Key</th>
          <th class="num">Size</th>
          <th>ETag</th>
          <th>Last modified</th>
          <th><span class="visually-hidden">Actions</span></th>
        </tr>
      </thead>
      <tbody>
        {#each objects as o (o.key)}
          <tr>
            <td class="mono key-cell">{o.key}</td>
            <td class="num">{bytes(o.size)}</td>
            <td class="mono etag-cell">{o.etag}</td>
            <td class="nowrap">{whenMs(o.last_modified_ms)}</td>
            <td>
              <div class="actions">
                <button class="sm" onclick={() => preview(o.key)}>Preview</button>
                <button class="sm" onclick={() => download(o.key)}>Download</button>
                <button class="sm" onclick={() => share(o.key)}>Share</button>
                <button class="sm danger" disabled={busy} onclick={() => askDelete(o.key)}>
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
    <div class="load-more">
      <button onclick={loadMore} disabled={loadingMore}>
        {loadingMore ? "Loading…" : "Load more objects"}
      </button>
    </div>
  {/if}
{/if}

{#if previewing}
  <div
    class="modal"
    role="presentation"
    onclick={closePreview}
  >
    <div
      class="modal-card"
      role="dialog"
      aria-modal="true"
      aria-label={`Preview of ${previewing.key}`}
      tabindex="-1"
      onclick={(e) => e.stopPropagation()}
      onkeydown={trapPreviewFocus}
    >
      <div class="modal-head">
        <span class="mono">{previewing.key}</span>
        <span class="spacer"></span>
        <button class="sm" onclick={() => download(previewing.key)}>Download</button>
        <button class="sm" bind:this={previewCloseEl} onclick={closePreview}>Close</button>
      </div>
      <div class="modal-body">
        {#if previewing.kind === "image"}
          <img src={previewing.url} alt={previewing.key} />
        {:else if previewing.kind === "text"}
          <pre>{previewing.text}</pre>
        {:else}
          <p class="muted">
            This file type has no inline preview. Use Download to open it.
          </p>
        {/if}
      </div>
    </div>
  </div>
{/if}

<ConfirmDialog
  open={pendingDelete !== null}
  danger
  title="Delete object"
  body={pendingDelete
    ? `This permanently deletes "${pendingDelete}". This cannot be undone.`
    : ""}
  confirmLabel={deleting ? "Deleting…" : "Delete object"}
  cancelLabel="Keep object"
  onconfirm={confirmDelete}
  oncancel={() => (pendingDelete = null)}
/>

<style>
  .sm-value {
    font-size: 1.1rem;
  }
  .prefix-form {
    gap: 8px;
    flex-wrap: wrap;
  }
  .prefix-note {
    margin: -6px 0 12px;
    font-size: 0.85rem;
  }
  .nowrap {
    white-space: nowrap;
  }
  .key-cell {
    word-break: break-all;
    min-width: 12ch;
  }
  .etag-cell {
    max-width: 16ch;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* Encrypt-uploads toggle: a real associated label with a one-line explainer. */
  .encrypt-toggle {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    cursor: pointer;
    max-width: 260px;
  }
  .encrypt-toggle input {
    width: auto;
    margin-top: 3px;
    flex-shrink: 0;
  }
  .encrypt-text {
    display: flex;
    flex-direction: column;
    gap: 1px;
  }
  .encrypt-title {
    font-size: 0.9rem;
    font-weight: 550;
    color: var(--text);
  }
  .encrypt-hint {
    font-size: 0.8rem;
    color: var(--text-muted);
    line-height: 1.4;
  }

  /* Public share link: treated with gravity (warning frame, prominent expiry). */
  .share-card {
    border: 2px solid var(--warning);
    background: var(--warning-tint);
    border-radius: var(--r);
    padding: 16px 18px;
    margin-bottom: 16px;
  }
  .share-head {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
    margin-bottom: 8px;
  }
  .share-expiry {
    font-size: 0.85rem;
    font-weight: 550;
    color: var(--warning-ink);
  }
  .share-explain {
    margin: 0 0 10px;
    font-size: 0.9rem;
    line-height: 1.5;
    color: var(--text);
  }
  .share-link {
    font-size: 0.85rem;
    background: var(--surface);
    border: 1px solid var(--border-strong);
    border-radius: var(--r-sm);
    padding: 9px 11px;
    word-break: break-all;
    margin-bottom: 12px;
  }

  .load-more {
    display: flex;
    justify-content: center;
    margin-top: 14px;
  }

  .empty-title {
    font-size: 1.05rem;
    font-weight: 600;
    color: var(--text);
    margin: 0 0 6px;
  }
  .empty-body {
    margin: 0 auto;
    max-width: 42ch;
    line-height: 1.55;
  }

  @media (max-width: 560px) {
    .encrypt-toggle {
      max-width: none;
      width: 100%;
    }
  }
</style>
