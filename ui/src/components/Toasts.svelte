<script>
  // Renders the transient toast queue (top-right). Driven by lib/toast.js.
  // De-striped: a full subtle border plus a leading status dot with a NON-color glyph cue
  // (check / cross / dot) so kind is distinguishable without relying on colour. The whole toast is
  // still dismiss-on-click; a dedicated, keyboard-reachable dismiss button is also provided.
  import { toasts, dismiss } from "../lib/toast.js";

  // Non-color cue per kind: a distinct glyph in the status dot.
  const GLYPH = { ok: "✓", error: "✕", info: "•" };
  const KIND_LABEL = { ok: "Success", error: "Error", info: "Notice" };
</script>

<div class="toasts" role="status" aria-live="polite">
  {#each $toasts as t (t.id)}
    <div class="toast {t.kind}">
      <span class="dot" aria-hidden="true">{GLYPH[t.kind] ?? GLYPH.info}</span>
      <span class="msg">
        <span class="visually-hidden">{KIND_LABEL[t.kind] ?? KIND_LABEL.info}: </span>{t.message}
      </span>
      <button
        type="button"
        class="dismiss"
        aria-label="Dismiss notification"
        onclick={() => dismiss(t.id)}>
        <span aria-hidden="true">{"✕"}</span>
      </button>
    </div>
  {/each}
</div>

<style>
  .toasts {
    position: fixed;
    top: 16px;
    right: 16px;
    z-index: 200;
    display: flex;
    flex-direction: column;
    gap: 8px;
    max-width: min(360px, calc(100vw - 32px));
  }
  .toast {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    background: var(--surface);
    border: 1px solid var(--border-strong);
    border-radius: var(--r-sm);
    box-shadow: var(--shadow-lg);
    padding: 11px 12px 11px 14px;
    font-size: 0.88rem;
    color: var(--text);
    animation: slide-in 0.16s ease-out;
  }
  .dot {
    flex-shrink: 0;
    width: 18px;
    height: 18px;
    margin-top: 1px;
    border-radius: 50%;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 0.7rem;
    font-weight: 700;
    line-height: 1;
    background: var(--surface-3);
    color: var(--text-muted);
  }
  .toast.ok .dot {
    background: var(--success-tint);
    color: var(--success-ink);
  }
  .toast.error .dot {
    background: var(--danger-tint);
    color: var(--danger-ink);
  }
  .toast.info .dot {
    background: var(--primary-tint);
    color: var(--primary-ink);
  }
  .msg {
    flex: 1 1 auto;
    min-width: 0;
    line-height: 1.45;
  }
  .dismiss {
    flex-shrink: 0;
    min-height: 0;
    width: 24px;
    height: 24px;
    padding: 0;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border: none;
    background: transparent;
    color: var(--text-muted);
    border-radius: 4px;
    font-size: 0.8rem;
    line-height: 1;
  }
  .dismiss:hover {
    background: var(--surface-3);
    color: var(--text);
  }
  .dismiss:focus-visible {
    outline: 3px solid var(--ring-color);
    outline-offset: 2px;
  }
  @keyframes slide-in {
    from {
      opacity: 0;
      transform: translateX(12px);
    }
  }
</style>
