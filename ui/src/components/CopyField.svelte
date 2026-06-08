<script>
  // A read-only credential field with a copy button. `secret` styles it for one-time secrets.
  import { ok } from "../lib/toast.js";
  let { label = "", value = "", secret = false } = $props();
  let copied = $state(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(value);
      copied = true;
      ok(`${label || "Value"} copied`);
      setTimeout(() => (copied = false), 1500);
    } catch {
      /* clipboard may be blocked; the value is selectable anyway */
    }
  }
</script>

<div class="copyfield">
  {#if label}<div class="label-sm">{label}</div>{/if}
  <div class="copyrow" class:secret>
    <code class="mono">{value}</code>
    <button class="btn" onclick={copy}>{copied ? "Copied" : "Copy"}</button>
  </div>
</div>

<style>
  .copyfield {
    margin-bottom: 12px;
  }
  .copyrow {
    display: flex;
    align-items: center;
    gap: 8px;
    background: var(--surface-2);
    border: 1px solid var(--border);
    border-radius: var(--r-sm);
    padding: 8px 10px;
  }
  .copyrow.secret {
    border-color: var(--warning);
    background: var(--warning-tint);
  }
  .copyrow code {
    flex: 1;
    overflow-x: auto;
    white-space: nowrap;
    font-size: 0.85rem;
  }
  .copyrow .btn {
    flex-shrink: 0;
  }
</style>
