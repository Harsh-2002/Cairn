<script>
  // Renders the transient toast queue (top-right). Driven by lib/toast.js.
  import { toasts, dismiss } from "../lib/toast.js";
</script>

<div class="toasts">
  {#each $toasts as t (t.id)}
    <div class="toast {t.kind}" onclick={() => dismiss(t.id)} role="alert">
      {t.message}
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
    background: var(--surface);
    border: 1px solid var(--border-strong);
    border-left: 3px solid var(--text-muted);
    border-radius: var(--r-sm);
    box-shadow: var(--shadow-lg);
    padding: 11px 14px;
    font-size: 0.88rem;
    color: var(--text);
    cursor: pointer;
    animation: slide-in 0.16s ease-out;
  }
  .toast.ok {
    border-left-color: var(--success);
  }
  .toast.error {
    border-left-color: var(--danger);
  }
  .toast.info {
    border-left-color: var(--primary);
  }
  @keyframes slide-in {
    from {
      opacity: 0;
      transform: translateX(12px);
    }
  }
</style>
