<script>
  // A reassuring confirmation dialog built on the native <dialog> element. It traps focus, closes on
  // Escape, focuses the safe default (cancel, or the typed-confirmation input) on open, and returns
  // focus to the previously focused element on close. The `danger` variant styles the confirm button
  // with --danger and, when `requireText` is set, only enables confirm once the typed text matches.
  //
  // Props:
  //   open          boolean  — bound by the parent; show/hide the dialog
  //   title         string   — heading (labels the dialog)
  //   body          string   — plain explanation of what the action does (state it plainly)
  //   confirmLabel  string   — verb+object label for the confirm button (default "Confirm")
  //   cancelLabel   string   — label for the cancel button (default "Cancel")
  //   danger        boolean  — destructive styling + gravity (default false)
  //   requireText   string?  — if set, the user must type this exact text to enable confirm
  //   onconfirm()            — called when confirmed
  //   oncancel()             — called when cancelled (Escape, backdrop, or Cancel)
  let {
    open = false,
    title = "Are you sure?",
    body = "",
    confirmLabel = "Confirm",
    cancelLabel = "Cancel",
    danger = false,
    requireText = null,
    onconfirm = () => {},
    oncancel = () => {},
  } = $props();

  let dialogEl = $state(null);
  let cancelEl = $state(null);
  let inputEl = $state(null);
  let typed = $state("");
  let returnFocusTo = null;

  const titleId = `confirm-title-${Math.random().toString(36).slice(2, 8)}`;
  const bodyId = `confirm-body-${Math.random().toString(36).slice(2, 8)}`;

  let confirmReady = $derived(!requireText || typed === requireText);

  // Open/close the native dialog in step with the `open` prop.
  $effect(() => {
    const el = dialogEl;
    if (!el) return;
    if (open && !el.open) {
      returnFocusTo =
        document.activeElement instanceof HTMLElement ? document.activeElement : null;
      typed = "";
      el.showModal();
      // Focus the safe target after the dialog paints.
      queueMicrotask(() => {
        if (requireText && inputEl) inputEl.focus();
        else if (cancelEl) cancelEl.focus();
      });
    } else if (!open && el.open) {
      el.close();
    }
  });

  function cancel() {
    if (open) oncancel();
  }
  function confirm() {
    if (!confirmReady) return;
    onconfirm();
  }

  // The native dialog fires `cancel` on Escape and `close` when dismissed; route both to oncancel.
  function onDialogCancel(e) {
    e.preventDefault();
    cancel();
  }
  function onDialogClose() {
    if (returnFocusTo && document.contains(returnFocusTo)) returnFocusTo.focus();
    returnFocusTo = null;
  }
  // Clicking the backdrop (outside the inner panel) cancels.
  function onDialogClick(e) {
    if (e.target === dialogEl) cancel();
  }
  function onKeydownInput(e) {
    if (e.key === "Enter" && confirmReady) {
      e.preventDefault();
      confirm();
    }
  }
</script>

<dialog
  class="confirm-dialog"
  bind:this={dialogEl}
  aria-labelledby={titleId}
  aria-describedby={body ? bodyId : undefined}
  oncancel={onDialogCancel}
  onclose={onDialogClose}
  onclick={onDialogClick}>
  <div class="confirm-inner">
    <h2 id={titleId} class="confirm-title">{title}</h2>
    {#if body}<p id={bodyId} class="confirm-body">{body}</p>{/if}

    {#if requireText}
      <label class="confirm-typed">
        <span class="confirm-typed-label">
          Type <code class="mono">{requireText}</code> to confirm
        </span>
        <input
          bind:this={inputEl}
          bind:value={typed}
          type="text"
          autocomplete="off"
          autocapitalize="off"
          autocorrect="off"
          spellcheck="false"
          aria-invalid={typed.length > 0 && !confirmReady}
          onkeydown={onKeydownInput} />
      </label>
    {/if}

    <div class="confirm-actions">
      <button bind:this={cancelEl} type="button" onclick={cancel}>{cancelLabel}</button>
      <button
        type="button"
        class="primary"
        class:danger
        disabled={!confirmReady}
        onclick={confirm}>{confirmLabel}</button>
    </div>
  </div>
</dialog>

<style>
  .confirm-inner {
    padding: 22px 22px 18px;
  }
  .confirm-title {
    margin: 0 0 0.5em;
    font-size: 1.2rem;
  }
  .confirm-body {
    margin: 0 0 16px;
    color: var(--text-muted);
    font-size: 0.95rem;
    line-height: 1.55;
  }
  .confirm-typed {
    display: block;
    margin-bottom: 18px;
  }
  .confirm-typed-label {
    display: block;
    margin-bottom: 6px;
    font-size: 0.9rem;
    font-weight: 500;
  }
  .confirm-typed code {
    background: var(--surface-3);
    border-radius: 4px;
    padding: 1px 5px;
    font-size: 0.86em;
  }
  .confirm-actions {
    display: flex;
    justify-content: flex-end;
    gap: 10px;
  }
  /* Open/close animation, disabled under reduced-motion via the global media block. */
  dialog.confirm-dialog[open] {
    animation: confirm-in 0.16s ease-out;
  }
  @keyframes confirm-in {
    from {
      opacity: 0;
      transform: translateY(8px);
    }
  }
</style>
