<script>
  import { api, ApiError } from "../lib/api.js";
  import { whenMs } from "../lib/format.js";
  import { navigate } from "../lib/router.js";
  import { ok, err } from "../lib/toast.js";
  import ConfirmDialog from "../components/ConfirmDialog.svelte";
  import Skeleton from "../components/Skeleton.svelte";

  let buckets = $state([]);
  let error = $state("");
  let loading = $state(true);

  let newName = $state("");
  let nameError = $state("");
  let creating = $state(false);

  // Delete confirmation is driven by a typed-name gate, not a native confirm().
  let pendingDelete = $state(null); // bucket name awaiting confirmation, or null
  let deleting = $state(false);

  async function load() {
    loading = true;
    error = "";
    try {
      const res = await api.listBuckets();
      buckets = (res && res.buckets) || [];
    } catch (e) {
      error = e.message || "Failed to load buckets.";
    } finally {
      loading = false;
    }
  }

  async function create(e) {
    e.preventDefault();
    nameError = "";
    const name = newName.trim();
    if (!name) {
      nameError = "Enter a name for the new bucket.";
      return;
    }
    creating = true;
    try {
      await api.createBucket(name);
      ok(`Bucket "${name}" created.`);
      newName = "";
      await load();
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        nameError = `A bucket named "${name}" already exists.`;
      } else {
        nameError = e.message || "Failed to create bucket.";
      }
    } finally {
      creating = false;
    }
  }

  function askDelete(name) {
    pendingDelete = name;
  }

  async function confirmDelete() {
    const name = pendingDelete;
    if (!name) return;
    deleting = true;
    try {
      await api.deleteBucket(name);
      ok(`Bucket "${name}" deleted.`);
      pendingDelete = null;
      await load();
    } catch (e) {
      err(e.message || "Failed to delete bucket.");
    } finally {
      deleting = false;
    }
  }

  load();
</script>

<h1>Buckets</h1>
<p class="subtitle">List, create, inspect, and remove buckets.</p>

{#if error}
  <div class="notice danger" role="alert">{error}</div>
{/if}

<div class="panel">
  <form class="create-row" onsubmit={create}>
    <div class="field create-field">
      <label class="label" for="new-bucket-name">New bucket name</label>
      <input
        id="new-bucket-name"
        placeholder="photos"
        bind:value={newName}
        oninput={() => (nameError = "")}
        aria-invalid={nameError ? "true" : undefined}
        aria-describedby={nameError ? "new-bucket-error" : undefined}
        autocomplete="off"
      />
      {#if nameError}
        <span id="new-bucket-error" class="field-error" role="alert">{nameError}</span>
      {/if}
    </div>
    <button class="primary" type="submit" disabled={creating}>
      {creating ? "Creating…" : "Create bucket"}
    </button>
  </form>
</div>

{#if loading}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Owner</th>
          <th>Versioning</th>
          <th>Created</th>
          <th><span class="visually-hidden">Actions</span></th>
        </tr>
      </thead>
      <tbody>
        {#each Array(3) as _, i (i)}
          <tr>
            <td colspan="5"><Skeleton lines={1} /></td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{:else if buckets.length === 0}
  <div class="empty">
    <p class="empty-title">No buckets yet</p>
    <p class="empty-body">
      A bucket is a top-level container for your files. Name one above to get started,
      then open it to upload objects.
    </p>
  </div>
{:else}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Owner</th>
          <th>Versioning</th>
          <th>Created</th>
          <th><span class="visually-hidden">Actions</span></th>
        </tr>
      </thead>
      <tbody>
        {#each buckets as b (b.name)}
          <tr>
            <td class="mono">
              <a
                href={`#/buckets/${encodeURIComponent(b.name)}/browser`}
                onclick={(e) => {
                  e.preventDefault();
                  navigate(`/buckets/${encodeURIComponent(b.name)}/browser`);
                }}>{b.name}</a
              >
            </td>
            <td class="mono">{b.owner_id || "—"}</td>
            <td><span class="badge">{b.versioning}</span></td>
            <td>{whenMs(b.created_at_ms)}</td>
            <td class="row-actions">
              <button class="danger sm" onclick={() => askDelete(b.name)}>
                Delete
              </button>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<ConfirmDialog
  open={pendingDelete !== null}
  danger
  title="Delete bucket"
  body="This permanently deletes the bucket and everything in it. Type the name to confirm."
  confirmLabel={deleting ? "Deleting…" : "Delete bucket"}
  cancelLabel="Keep bucket"
  requireText={pendingDelete}
  onconfirm={confirmDelete}
  oncancel={() => (pendingDelete = null)}
/>

<style>
  .create-row {
    display: flex;
    align-items: flex-end;
    gap: 12px;
    flex-wrap: wrap;
  }
  .create-field {
    flex: 1 1 220px;
    margin-bottom: 0;
    min-width: 0;
  }
  .create-row > button {
    flex-shrink: 0;
  }
  .row-actions {
    text-align: right;
    white-space: nowrap;
  }
  .empty-title {
    font-size: 1.05rem;
    font-weight: 600;
    color: var(--text);
    margin: 0 0 6px;
  }
  .empty-body {
    margin: 0 auto;
    max-width: 38ch;
    line-height: 1.55;
  }
</style>
