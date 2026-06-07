<script>
  import { api, ApiError } from "../lib/api.js";
  import { whenMs } from "../lib/format.js";
  import { navigate } from "../lib/router.js";

  let buckets = $state([]);
  let error = $state("");
  let notice = $state("");
  let loading = $state(true);

  let newName = $state("");
  let creating = $state(false);

  async function load() {
    loading = true;
    error = "";
    try {
      const res = await api.listBuckets();
      buckets = (res && res.buckets) || [];
    } catch (err) {
      error = err.message || "Failed to load buckets.";
    } finally {
      loading = false;
    }
  }

  async function create(e) {
    e.preventDefault();
    error = "";
    notice = "";
    const name = newName.trim();
    if (!name) {
      error = "Bucket name is required.";
      return;
    }
    creating = true;
    try {
      await api.createBucket(name);
      notice = `Bucket "${name}" created.`;
      newName = "";
      await load();
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        error = `A bucket named "${name}" already exists.`;
      } else {
        error = err.message || "Failed to create bucket.";
      }
    } finally {
      creating = false;
    }
  }

  async function remove(name) {
    if (
      !window.confirm(
        `Delete bucket "${name}"? This force-empties it and cannot be undone.`,
      )
    )
      return;
    error = "";
    notice = "";
    try {
      await api.deleteBucket(name);
      notice = `Bucket "${name}" deleted.`;
      await load();
    } catch (err) {
      error = err.message || "Failed to delete bucket.";
    }
  }

  load();
</script>

<h1>Buckets</h1>
<p class="subtitle">List, create, inspect, and remove buckets.</p>

{#if error}
  <div class="notice error">{error}</div>
{/if}
{#if notice}
  <div class="notice success">{notice}</div>
{/if}

<div class="panel">
  <form class="row" onsubmit={create}>
    <input
      placeholder="new-bucket-name"
      bind:value={newName}
      autocomplete="off"
    />
    <button class="primary" type="submit" disabled={creating}>
      {creating ? "Creating…" : "Create bucket"}
    </button>
  </form>
</div>

{#if loading}
  <p class="muted">Loading…</p>
{:else if buckets.length === 0}
  <div class="empty">No buckets yet. Create one above.</div>
{:else}
  <table>
    <thead>
      <tr>
        <th>Name</th>
        <th>Owner</th>
        <th>Versioning</th>
        <th>Created</th>
        <th></th>
      </tr>
    </thead>
    <tbody>
      {#each buckets as b (b.name)}
        <tr>
          <td class="mono">
            <a
              href={`#/buckets/${encodeURIComponent(b.name)}`}
              onclick={(e) => {
                e.preventDefault();
                navigate(`/buckets/${encodeURIComponent(b.name)}`);
              }}>{b.name}</a
            >
          </td>
          <td class="mono">{b.owner_id || "—"}</td>
          <td><span class="badge">{b.versioning}</span></td>
          <td>{whenMs(b.created_at_ms)}</td>
          <td style="text-align:right;">
            <button class="danger" onclick={() => remove(b.name)}>Delete</button>
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}
