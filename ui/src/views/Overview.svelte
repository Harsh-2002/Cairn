<script>
  import { api } from "../lib/api.js";
  import { bytes, count, ratio } from "../lib/format.js";
  import { navigate } from "../lib/router.js";
  import Skeleton from "../components/Skeleton.svelte";

  let data = $state(null);
  let buckets = $state([]);
  let error = $state("");
  // `loading` is the first paint (no data yet). `refreshing` is a re-fetch while
  // data is still on screen, so we never tear the page down on Refresh.
  let loading = $state(true);
  let refreshing = $state(false);

  async function load() {
    if (data) refreshing = true;
    else loading = true;
    error = "";
    try {
      const next = await api.overview();
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
      data = next;
      buckets = details;
    } catch (err) {
      error = err.message || "Failed to load overview.";
    } finally {
      loading = false;
      refreshing = false;
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
  const storedPct = $derived(Math.round(storedFrac * 100));
  const savedPct = $derived(
    data && data.logical_bytes > 0
      ? Math.round((saved / data.logical_bytes) * 100)
      : 0,
  );

  function bucketSharePct(size) {
    return Math.round((size / maxSize) * 100);
  }
</script>

<h1>Overview</h1>
<p class="subtitle">Storage, compression, and per-bucket usage across the node.</p>

{#if error}
  <div class="notice danger" role="alert">{error}</div>
{/if}

{#if loading}
  <!-- First paint: skeletons that mirror the real layout so nothing jumps. -->
  <div class="tier tier-primary" aria-hidden="true">
    <div class="card stat stat-lead"><Skeleton lines={2} /></div>
    <div class="card stat stat-lead"><Skeleton lines={2} /></div>
    <div class="card stat stat-lead"><Skeleton lines={2} /></div>
  </div>
  <div class="tier tier-storage" aria-hidden="true">
    <div class="card stat"><Skeleton lines={2} /></div>
    <div class="card stat"><Skeleton lines={2} /></div>
  </div>
  <div class="panel" aria-hidden="true">
    <Skeleton lines={1} width="40%" />
    <div class="skel-bar"><Skeleton block height="14px" /></div>
    <Skeleton lines={2} />
  </div>
  <span class="visually-hidden" role="status">Loading overview…</span>
{:else if data}
  <!-- Primary tier: the three counts the eye should land on first. -->
  <div class="tier tier-primary">
    <div class="card stat stat-lead">
      <div class="label">Buckets</div>
      <div class="value">{count(data.buckets)}</div>
    </div>
    <div class="card stat stat-lead">
      <div class="label">Objects</div>
      <div class="value">{count(data.objects)}</div>
    </div>
    <div class="card stat stat-lead">
      <div class="label">Versions</div>
      <div class="value">{count(data.versions)}</div>
    </div>
  </div>

  <!-- Storage tier: the two size figures, with a plain-language gloss. -->
  <div class="tier tier-storage">
    <div class="card stat">
      <div class="label">Original size</div>
      <div class="value mono">{bytes(data.logical_bytes)}</div>
      <p class="stat-gloss">What you uploaded.</p>
    </div>
    <div class="card stat">
      <div class="label">Stored size</div>
      <div class="value mono">{bytes(data.physical_bytes)}</div>
      <p class="stat-gloss">On disk after compression.</p>
    </div>
  </div>

  <!-- Compression panel. The ratio + space-saved figures live here now, next to
       the bar that explains them, instead of as duplicate cards above. -->
  <div class="panel comp-panel">
    <div class="comp-head">
      <h2 class="comp-title">Compression</h2>
      <span class="comp-summary"
        >{savedPct}% smaller · {ratio(data.compression_ratio)} ratio</span
      >
    </div>

    <div
      class="comp-bar"
      role="progressbar"
      aria-valuenow={storedPct}
      aria-valuemin="0"
      aria-valuemax="100"
      aria-label={`Stored size: ${bytes(data.physical_bytes)} of ${bytes(
        data.logical_bytes,
      )} original (${savedPct}% saved)`}
    >
      <div class="comp-stored" style:width={`${storedPct}%`}>
        {#if savedPct >= 14}
          <span class="comp-inline-label">Stored {storedPct}%</span>
        {/if}
      </div>
      {#if savedPct > 0}
        <span class="comp-saved-label">Saved {savedPct}%</span>
      {/if}
    </div>

    <dl class="comp-legend">
      <div class="comp-leg-item">
        <dt><i class="swatch stored"></i> Stored</dt>
        <dd class="mono">{bytes(data.physical_bytes)}</dd>
      </div>
      <div class="comp-leg-item">
        <dt><i class="swatch saved"></i> Saved</dt>
        <dd class="mono">{bytes(saved)}</dd>
      </div>
      <div class="comp-leg-item">
        <dt><i class="swatch original"></i> Original</dt>
        <dd class="mono">{bytes(data.logical_bytes)}</dd>
      </div>
    </dl>
  </div>

  <div class="toolbar">
    <h2 class="section-title">Storage by bucket</h2>
    <span class="spacer"></span>
    <button onclick={load} disabled={refreshing} aria-busy={refreshing}>
      {refreshing ? "Refreshing…" : "Refresh"}
    </button>
  </div>

  {#if buckets.length === 0}
    <div class="empty">No buckets yet.</div>
  {:else}
    <div class="panel table-panel">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Bucket</th>
              <th class="num">Objects</th>
              <th class="num">Original size</th>
              <th class="share-col">Share</th>
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
                    class="share-bar"
                    role="progressbar"
                    aria-valuenow={bucketSharePct(b.size)}
                    aria-valuemin="0"
                    aria-valuemax="100"
                    aria-label={`${b.name}: ${bytes(b.size)}, ${bucketSharePct(
                      b.size,
                    )}% of the largest bucket`}
                  >
                    <div
                      class="share-fill"
                      style:width={`${bucketSharePct(b.size)}%`}
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
  /* ---- tiered stat grid ---------------------------------------------------- */
  .tier {
    display: grid;
    gap: 14px;
    margin-bottom: 16px;
  }
  .tier-primary {
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  }
  .tier-storage {
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    margin-bottom: 22px;
  }
  .stat {
    display: flex;
    flex-direction: column;
  }
  /* Primary tier carries the visual weight so the eye lands there first. */
  .stat-lead {
    padding: 20px 22px;
    background: var(--surface);
    border-color: var(--border-strong);
    box-shadow: var(--shadow);
  }
  .stat-lead .value {
    font-size: 2.1rem;
  }
  .stat-gloss {
    margin: 6px 0 0;
    color: var(--text-faint);
    font-size: 0.82rem;
    line-height: 1.35;
  }

  /* ---- compression panel --------------------------------------------------- */
  .comp-panel {
    margin-bottom: 22px;
  }
  .comp-head {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 12px;
    flex-wrap: wrap;
  }
  .comp-title {
    margin: 0;
  }
  .comp-summary {
    color: var(--text-muted);
    font-size: 0.92rem;
    font-variant-numeric: tabular-nums;
  }

  .comp-bar {
    position: relative;
    height: 28px;
    border-radius: var(--r-sm);
    /* The whole track reads as the "saved" win; the fill is what is actually stored. */
    background: var(--success-tint);
    border: 1px solid var(--success);
    overflow: hidden;
    margin: 14px 0 12px;
  }
  .comp-stored {
    height: 100%;
    background: var(--primary-strong);
    display: flex;
    align-items: center;
    min-width: 2px;
  }
  .comp-inline-label {
    padding-left: 10px;
    color: var(--on-primary);
    font-size: 0.78rem;
    font-weight: 600;
    white-space: nowrap;
  }
  /* The saved portion sits to the right of the fill, labelling the empty track. */
  .comp-saved-label {
    position: absolute;
    top: 50%;
    right: 10px;
    transform: translateY(-50%);
    color: var(--success-ink);
    font-size: 0.78rem;
    font-weight: 600;
    white-space: nowrap;
  }

  .comp-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 10px 26px;
    margin: 0;
  }
  .comp-leg-item {
    display: flex;
    align-items: baseline;
    gap: 8px;
  }
  .comp-leg-item dt {
    color: var(--text-muted);
    font-size: 0.85rem;
  }
  .comp-leg-item dd {
    margin: 0;
    font-size: 0.85rem;
    font-variant-numeric: tabular-nums;
  }
  .swatch {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 3px;
    margin-right: 4px;
    vertical-align: middle;
  }
  .swatch.stored {
    background: var(--primary-strong);
  }
  .swatch.saved {
    background: var(--success-tint);
    border: 1px solid var(--success);
  }
  .swatch.original {
    background: var(--surface-3);
    border: 1px solid var(--border-strong);
  }

  /* ---- storage-by-bucket table -------------------------------------------- */
  .section-title {
    margin: 0;
  }
  .table-panel {
    padding: 6px 0;
  }
  .share-col {
    width: 34%;
  }
  .share-bar {
    height: 8px;
    border-radius: 999px;
    background: var(--surface-3);
    overflow: hidden;
  }
  .share-fill {
    height: 100%;
    border-radius: 999px;
    background: var(--primary);
  }

  /* ---- loading skeleton spacing ------------------------------------------- */
  .skel-bar {
    margin: 14px 0 12px;
  }

  @media (prefers-reduced-motion: reduce) {
    .comp-stored,
    .share-fill {
      transition: none;
    }
  }
</style>
