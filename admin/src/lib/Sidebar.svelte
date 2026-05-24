<script lang="ts">
  import type { PostSummary } from "./api";
  import CairnMark from "./CairnMark.svelte";

  let {
    posts,
    selected,
    onSelect,
    onNew,
    onDelete,
    workspace = "Cairn",
  }: {
    posts: PostSummary[] | null;
    selected: string | null;
    onSelect: (slug: string) => void;
    onNew: () => void;
    onDelete: (slug: string) => void;
    workspace?: string;
  } = $props();

  function fmtDate(iso: string): string {
    try {
      const d = new Date(iso);
      return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
    } catch {
      return "";
    }
  }

  function pad2(n: number): string {
    return n.toString().padStart(2, "0");
  }
</script>

<aside class="sidebar">
  <header class="sidebar-head">
    <div class="workspace">
      <CairnMark size={20} />
      <span class="workspace-name">{workspace}</span>
    </div>
    <div class="workspace-sub">Notes, dispatches, fragments</div>
  </header>

  <div class="section-label">
    <span>Index</span>
    {#if posts}<span class="count">{posts.length}</span>{/if}
  </div>

  <nav class="post-list" aria-label="Posts">
    {#if posts === null}
      <div class="loading">Loading…</div>
    {:else if posts.length === 0}
      <div class="empty">No entries yet.</div>
    {:else}
      {#each posts as p, i (p.slug)}
        <div
          class="post-row"
          class:active={selected === p.slug}
          style="--enter-delay: {i * 35}ms"
        >
          <button class="post-item" onclick={() => onSelect(p.slug)}>
            <span class="post-num">{pad2(i + 1)}</span>
            <span class="post-title-wrap">
              <span class="post-title">{p.title || p.slug}</span>
              <span class="post-meta">
                {#if p.draft}<span class="draft-tag">draft</span>{/if}
                <span class="post-date">{fmtDate(p.date)}</span>
              </span>
            </span>
          </button>
          <button
            class="post-delete"
            title="Delete entry"
            aria-label="Delete {p.title}"
            onclick={(e) => {
              e.stopPropagation();
              if (confirm(`Delete "${p.title}"? This commits the removal to main.`)) {
                onDelete(p.slug);
              }
            }}
          >
            <svg viewBox="0 0 12 12" width="12" height="12" fill="currentColor">
              <path
                d="M3 3l6 6M9 3l-6 6"
                stroke="currentColor"
                stroke-width="1.4"
                stroke-linecap="round"
                fill="none"
              />
            </svg>
          </button>
        </div>
      {/each}
    {/if}
  </nav>

  <button class="new-button" onclick={onNew}>
    <span class="plus" aria-hidden="true">+</span>
    <span class="new-label">Place a new stone</span>
  </button>
</aside>

<style>
  .sidebar {
    background: var(--paper-low);
    border-right: 1px solid var(--rule);
    display: flex;
    flex-direction: column;
    height: 100vh;
    width: var(--sidebar-w);
    flex-shrink: 0;
    animation: fade-in 400ms var(--ease-out);
  }
  .sidebar-head {
    padding: 1.4rem 1.2rem 1.1rem;
    border-bottom: 1px solid var(--rule);
    background:
      radial-gradient(circle at 0% 0%, var(--accent-soft), transparent 60%),
      var(--paper-low);
  }
  .workspace {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    font-family: var(--font-display);
    font-size: 1.25rem;
    font-weight: 500;
    color: var(--ink);
    letter-spacing: -0.015em;
    font-variation-settings: "opsz" 40;
  }
  .workspace-sub {
    margin-top: 0.35rem;
    font-family: var(--font-ui);
    font-size: 0.7rem;
    letter-spacing: 0.04em;
    color: var(--ink-faint);
    font-style: italic;
  }
  .section-label {
    padding: 1.2rem 1.2rem 0.5rem;
    font-family: var(--font-ui);
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.16em;
    font-weight: 500;
    color: var(--ink-faint);
    display: flex;
    justify-content: space-between;
    align-items: baseline;
  }
  .section-label .count {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    letter-spacing: 0;
    color: var(--ink-faint);
  }
  .post-list {
    flex: 1;
    overflow-y: auto;
    padding: 0 0.4rem;
  }
  .post-row {
    position: relative;
    display: flex;
    align-items: stretch;
    border-radius: var(--r-md);
    margin-bottom: 1px;
    transition: background-color 140ms var(--ease-out);
    opacity: 0;
    animation: rise 360ms var(--ease-out) forwards;
    animation-delay: var(--enter-delay, 0ms);
  }
  .post-row:hover {
    background: rgba(0, 0, 0, 0.035);
  }
  @media (prefers-color-scheme: dark) {
    .post-row:hover { background: rgba(255, 255, 255, 0.04); }
  }
  .post-row.active {
    background: var(--paper-shadow);
  }
  .post-row.active::before {
    content: "";
    position: absolute;
    left: -1px;
    top: 8px;
    bottom: 8px;
    width: 2px;
    background: var(--accent);
    border-radius: 0 2px 2px 0;
  }
  .post-item {
    flex: 1;
    min-width: 0;
    background: transparent;
    border: 0;
    text-align: left;
    padding: 0.65rem 0.65rem 0.65rem 0.55rem;
    display: grid;
    grid-template-columns: 1.4rem 1fr;
    gap: 0.5rem;
    color: var(--ink);
    cursor: pointer;
    align-items: start;
  }
  .post-num {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    color: var(--ink-faint);
    line-height: 1.4;
    font-feature-settings: "tnum";
    padding-top: 0.05rem;
  }
  .post-row.active .post-num {
    color: var(--accent);
  }
  .post-title-wrap {
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
  }
  .post-title {
    font-family: var(--font-display);
    font-size: 0.92rem;
    font-weight: 500;
    color: var(--ink);
    line-height: 1.3;
    overflow: hidden;
    text-overflow: ellipsis;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    font-variation-settings: "opsz" 16;
  }
  .post-meta {
    display: flex;
    align-items: center;
    gap: 0.45rem;
    font-family: var(--font-ui);
    font-size: 0.7rem;
    color: var(--ink-faint);
  }
  .draft-tag {
    color: var(--warning);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-size: 0.62rem;
    font-weight: 500;
  }
  .post-date {
    font-feature-settings: "tnum";
  }
  .post-delete {
    flex-shrink: 0;
    background: transparent;
    border: 0;
    color: var(--ink-faint);
    padding: 0 0.6rem;
    cursor: pointer;
    opacity: 0;
    display: flex;
    align-items: center;
    transition: opacity 120ms var(--ease-out), color 120ms var(--ease-out);
  }
  .post-row:hover .post-delete { opacity: 1; }
  .post-delete:hover { color: var(--error); }
  .loading,
  .empty {
    padding: 0.7rem 0.9rem;
    color: var(--ink-faint);
    font-size: 0.82rem;
    font-style: italic;
  }
  .new-button {
    margin: 0.5rem 0.6rem 1rem;
    padding: 0.7rem 0.85rem;
    background: transparent;
    border: 1px dashed var(--rule-strong);
    border-radius: var(--r-md);
    color: var(--ink-muted);
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.4rem;
    font-family: var(--font-ui);
    font-size: 0.78rem;
    letter-spacing: 0.04em;
    text-transform: lowercase;
    font-variant: small-caps;
    transition:
      all 160ms var(--ease-out),
      transform 80ms var(--ease-out);
  }
  .new-button:hover {
    border-color: var(--accent);
    color: var(--accent);
    background: var(--accent-soft);
    transform: translateY(-1px);
  }
  .new-button:active {
    transform: translateY(0);
  }
  .plus {
    font-size: 1.05rem;
    font-weight: 300;
    line-height: 1;
  }
  .new-label {
    letter-spacing: 0.06em;
  }
</style>
