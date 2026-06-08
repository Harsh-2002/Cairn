<script>
  // A read-only credential field with a copy button. `secret` gives one-time secrets more gravity
  // (stronger affordance + a standing reminder to store them now). When the Clipboard API is
  // unavailable or blocked, the value text is selected and an inline fallback hint is shown instead
  // of silently failing.
  import { ok } from "../lib/toast.js";
  let { label = "", value = "", secret = false } = $props();
  let copied = $state(false);
  let fallback = $state(false);
  let codeEl = $state(null);
  let resetTimer = null;

  function selectValue() {
    if (!codeEl) return;
    const sel = window.getSelection?.();
    if (!sel) return;
    const range = document.createRange();
    range.selectNodeContents(codeEl);
    sel.removeAllRanges();
    sel.addRange(range);
  }

  async function copy() {
    clearTimeout(resetTimer);
    fallback = false;
    try {
      if (!navigator.clipboard?.writeText) throw new Error("no clipboard");
      await navigator.clipboard.writeText(value);
      copied = true;
      ok(`${label || "Value"} copied`);
      resetTimer = setTimeout(() => (copied = false), 1500);
    } catch {
      // Clipboard blocked (insecure context / permissions). Select the text and tell the user how.
      selectValue();
      fallback = true;
      resetTimer = setTimeout(() => (fallback = false), 6000);
    }
  }
</script>

<div class="copyfield" class:secret>
  {#if label}<div class="label-sm">{label}</div>{/if}
  <div class="copyrow" class:secret>
    <code bind:this={codeEl} class="mono">{value}</code>
    <button class="btn" class:primary={secret} type="button" onclick={copy}>
      {copied ? "Copied" : "Copy"}
    </button>
  </div>
  {#if fallback}
    <p class="copy-fallback" role="status">
      Copy was blocked by your browser. The value is selected, press
      <kbd>Ctrl</kbd>/<kbd>Cmd</kbd> + <kbd>C</kbd> to copy it.
    </p>
  {:else if secret}
    <p class="secret-hint">Store this now. It is shown once and cannot be retrieved later.</p>
  {/if}
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
    border: 2px solid var(--warning);
    background: var(--warning-tint);
    padding: 9px 10px;
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
  .copy-fallback {
    margin: 6px 0 0;
    font-size: 0.85rem;
    color: var(--text-muted);
  }
  .copy-fallback kbd {
    font-family: var(--mono);
    font-size: 0.8em;
    background: var(--surface-3);
    border: 1px solid var(--border-strong);
    border-radius: 4px;
    padding: 1px 5px;
  }
  .secret-hint {
    margin: 6px 0 0;
    font-size: 0.85rem;
    font-weight: 500;
    color: var(--warning-ink);
  }
</style>
