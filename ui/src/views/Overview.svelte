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
  // Stored fraction of original (0..1), for the compression bar.
  const storedFrac = $derived(
    data && data.logical_bytes > 0
      ? Math.min(1, data.physical_bytes / data.logical_bytes)
      : 1,
  );
  const savedPct = $derived(
    data && data.logical_bytes > 0
      ? Math.round((saved / data.logical_bytes) * 100)
      : 0,
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

  <div class="card" style="margin-top:18px">
    <div class="row" style="justify-content:space-between; align-items:baseline;">
      <h2 style="margin:0;">Compression</h2>
      <span class="muted">{savedPct}% smaller · {ratio(data.compression_ratio)} ratio</span>
    </div>
    <div class="comp-bar" title={`Stored ${bytes(data.physical_bytes)} of ${bytes(data.logical_bytes)} original`}>
      <div class="comp-stored" style={`width:${Math.round(storedFrac * 100)}%`}></div>
    </div>
    <div class="row comp-legend">
      <span><i class="swatch stored"></i> Stored {bytes(data.physical_bytes)}</span>
      <span><i class="swatch saved"></i> Saved {bytes(saved)}</span>
      <span class="muted">Original {bytes(data.logical_bytes)}</span>
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
                    href={`#/buckets/${b.name}/browser`}
                    class="mono"
                    onclick={(e) => {
                      e.preventDefault();
                      navigate(`/buckets/${b.name}/browser`);
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

<style>
  .comp-bar {
    height: 14px;
    border-radius: 999px;
    background: var(--success-tint);
    overflow: hidden;
    margin: 12px 0 10px;
  }
  .comp-stored {
    height: 100%;
    background: var(--primary);
    border-radius: 999px;
  }
  .comp-legend {
    gap: 18px;
    font-size: 0.85rem;
    flex-wrap: wrap;
  }
  .swatch {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 3px;
    margin-right: 5px;
    vertical-align: middle;
  }
  .swatch.stored {
    background: var(--primary);
  }
  .swatch.saved {
    background: var(--success-tint);
    border: 1px solid var(--success);
  }
</style>

