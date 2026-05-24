<script lang="ts">
  type Status = "idle" | "loading" | "saving" | "saved" | "error";

  let {
    slug,
    status,
    lastSaved,
    onPublish,
  }: {
    slug: string | null;
    status: Status;
    lastSaved: string | null;
    onPublish: () => void;
  } = $props();

  const STATUS_LABEL: Record<Status, string> = {
    idle: "idle",
    loading: "loading",
    saving: "saving",
    saved: "saved",
    error: "error",
  };
</script>

<header class="topbar">
  <div class="left">
    <span class="crumb-label">Section</span>
    <span class="crumb-sep">·</span>
    <span class="crumb">Index</span>
    {#if slug}
      <span class="crumb-sep">·</span>
      <span class="crumb current">{slug}</span>
    {/if}
  </div>
  <div class="right">
    <span class="status" data-status={status}>
      <span class="status-dot"></span>
      <span class="status-text">{STATUS_LABEL[status]}</span>
    </span>
    {#if lastSaved}
      <span class="hash" title={lastSaved}>
        <span class="hash-prefix">@</span>{lastSaved.slice(0, 7)}
      </span>
    {/if}
    <button
      class="publish"
      disabled={!slug || status === "loading"}
      onclick={onPublish}
    >
      <span class="publish-label">Set in stone</span>
      <span class="publish-arrow" aria-hidden="true">
        <svg viewBox="0 0 12 12" width="11" height="11" fill="none">
          <path
            d="M6 2v8M2.5 5.5L6 2l3.5 3.5"
            stroke="currentColor"
            stroke-width="1.4"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
        </svg>
      </span>
    </button>
  </div>
</header>

<style>
  .topbar {
    height: var(--topbar-h);
    border-bottom: 1px solid var(--rule);
    background: var(--paper);
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 1.4rem;
    flex-shrink: 0;
    animation: fade-in 320ms var(--ease-out) 80ms backwards;
  }
  .left {
    display: flex;
    align-items: baseline;
    gap: 0.45rem;
    color: var(--ink-muted);
    font-family: var(--font-ui);
    font-size: 0.74rem;
  }
  .crumb-label {
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--ink-faint);
    font-weight: 500;
  }
  .crumb-sep {
    color: var(--ink-faint);
  }
  .crumb {
    color: var(--ink-muted);
  }
  .crumb.current {
    color: var(--ink);
    font-family: var(--font-mono);
    font-size: 0.74rem;
    font-weight: 500;
  }
  .right {
    display: flex;
    align-items: center;
    gap: 0.95rem;
  }
  .status {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    font-family: var(--font-ui);
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--ink-muted);
    padding: 0.3rem 0.65rem;
    border-radius: 999px;
    transition: all 200ms var(--ease-out);
    background: transparent;
  }
  .status-dot {
    width: 5px;
    height: 5px;
    border-radius: 50%;
    background: var(--ink-faint);
    transition: background-color 200ms var(--ease-out);
  }
  .status[data-status="loading"] .status-dot,
  .status[data-status="saving"] .status-dot {
    background: var(--warning);
    animation: pulse 1.1s ease-in-out infinite;
  }
  .status[data-status="saved"] {
    color: var(--success);
    background: linear-gradient(
      90deg,
      var(--success-soft),
      var(--success-soft) 50%,
      transparent 50%,
      transparent 100%
    );
    background-size: 200% 100%;
    animation: saved-sweep 600ms var(--ease-out) forwards;
  }
  .status[data-status="saved"] .status-dot {
    background: var(--success);
  }
  .status[data-status="error"] {
    color: var(--error);
    background: rgba(138, 54, 24, 0.07);
  }
  .status[data-status="error"] .status-dot {
    background: var(--error);
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.35; }
  }
  .hash {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    color: var(--ink-faint);
    letter-spacing: 0.02em;
  }
  .hash-prefix {
    color: var(--ink-faint);
    opacity: 0.6;
    margin-right: 0.05em;
  }
  .publish {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    background: var(--ink);
    color: var(--paper);
    border: 0;
    padding: 0.55rem 1.05rem;
    border-radius: var(--r-md);
    font-family: var(--font-ui);
    font-size: 0.78rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    font-weight: 500;
    transition:
      transform 140ms var(--ease-out),
      background-color 140ms var(--ease-out),
      box-shadow 140ms var(--ease-out);
    box-shadow: var(--shadow-sm);
    position: relative;
    overflow: hidden;
  }
  .publish:not(:disabled):hover {
    background: var(--accent);
    transform: translateY(-1px);
    box-shadow: var(--shadow-md);
  }
  .publish:not(:disabled):hover .publish-arrow {
    transform: translateY(-2px);
  }
  .publish:disabled {
    opacity: 0.32;
    cursor: not-allowed;
  }
  .publish-label {
    line-height: 1;
  }
  .publish-arrow {
    display: inline-flex;
    line-height: 0;
    transition: transform 180ms var(--ease-out);
  }
</style>
