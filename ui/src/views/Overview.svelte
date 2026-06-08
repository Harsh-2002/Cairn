<script>
  import { api } from "../lib/api.js";
  import { bytes, count, ratio } from "../lib/format.js";
  import { navigate } from "../lib/router.js";

  let data = $state(null);
  let buckets = $state([]);
  let error = $state("");
  let loading = $state(true);

  async function load() {
    loading = true;
    error = "";
    try {
      data = await api.overview();
      const list = await api.listBuckets();
      const names = (list?.buckets || []).map((b) => b.name);
      const details = await Promise.all(
        names.map(async (n) => {
          try {
            const d = await api.getBucket(n);
            return { name: n, objects: d.object_count, size: d.logical_bytes };
          } catch {
            return { name: n, objects: 0, size: 0 };
          }
        }),
      );
      details.sort((a, b) => b.size - a.size);
      buckets = details;
    } catch (err) {
      error = err.message || "Failed to load overview.";
    } finally {
      loading = false;
    }
  }
  load();

  const maxSize = $derived(Math.max(1, ...buckets.map((b) => b.size)));
  const saved = $derived(
    data ? Math.max(0, data.logical_bytes - data.physical_bytes) : 0,
  );
</script>

<h1>Overview</h1>
<p class="subtitle">Storage, compression, and per-bucket usage across the node.</p>

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
    <div class="card">
      <div class="label">Space saved</div>
      <div class="value mono">{bytes(saved)}</div>
    </div>
  </div>

  <div class="toolbar">
    <h2 style="margin:0;">Storage by bucket</h2>
    <span class="spacer"></span>
    <button onclick={load}>Refresh</button>
  </div>

  {#if buckets.length === 0}
    <div class="empty">No buckets yet.</div>
  {:else}
    <div class="panel" style="padding:6px 0;">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Bucket</th>
              <th class="num">Objects</th>
              <th class="num">Size</th>
              <th style="width:34%;">Share</th>
            </tr>
          </thead>
          <tbody>
            {#each buckets as b (b.name)}
              <tr>
                <td>
                  <a
                    href={`#/buckets/${b.name}`}
                    class="mono"
                    onclick={(e) => {
                      e.preventDefault();
                      navigate(`/buckets/${b.name}`);
                    }}>{b.name}</a
                  >
                </td>
                <td class="num">{count(b.objects)}</td>
                <td class="num mono">{bytes(b.size)}</td>
                <td>
                  <div
                    style="height:8px;border-radius:999px;background:var(--surface-3);overflow:hidden;"
                  >
                    <div
                      style={`height:100%;border-radius:999px;background:var(--primary);width:${Math.round((b.size / maxSize) * 100)}%;`}
                    ></div>
                  </div>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </div>
  {/if}
{/if}
