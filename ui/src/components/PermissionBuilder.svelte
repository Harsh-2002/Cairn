<script>
  // A policy editor with three synced views: a visual Builder, a JSON Code editor, and Split
  // (side-by-side, the default) where editing either side updates the other. Emits the current
  // policy doc via `onchange(docOrNull)` — null means the JSON is invalid (parent disables save).
  import {
    LEVELS,
    LEVEL_ACTIONS,
    ACTION_GROUPS,
    presetToPolicy,
    advancedToPolicy,
    policyToPreset,
    validate,
    pretty,
  } from "../lib/policy.js";

  let { buckets = [], initial = null, onchange = () => {} } = $props();

  let mode = $state("split"); // "split" | "builder" | "code"
  let advanced = $state(false);
  let scope = $state("all"); // "all" | "specific"
  let pickedBuckets = $state([]);
  let level = $state("read");
  let pickedActions = $state([...LEVEL_ACTIONS.read]);
  let custom = $state(false); // JSON is an unrecognized (but valid) doc
  let rawText = $state(pretty(presetToPolicy({ scope: "all", level: "read" })));
  let error = $state("");

  // Seed from an existing policy (UserDetail edit).
  if (initial) {
    rawText = pretty(initial);
    const p = policyToPreset(initial);
    if (p.recognized) {
      scope = p.scope;
      pickedBuckets = p.buckets;
      level = p.level;
    } else {
      custom = true;
    }
  }
  onchange(initial || presetToPolicy({ scope, buckets: pickedBuckets, level }));

  // Builder edits → regenerate JSON, emit.
  function fromBuilder() {
    custom = false;
    error = "";
    const doc = advanced
      ? advancedToPolicy({ scope, buckets: pickedBuckets, actions: pickedActions })
      : presetToPolicy({ scope, buckets: pickedBuckets, level });
    rawText = pretty(doc);
    onchange(doc);
  }

  // Code edits → validate, re-derive the builder (best-effort), emit.
  function fromCode() {
    const v = validate(rawText);
    if (!v.ok) {
      error = v.error;
      onchange(null);
      return;
    }
    error = "";
    const p = policyToPreset(v.doc);
    if (p.recognized) {
      custom = false;
      advanced = false;
      scope = p.scope;
      pickedBuckets = p.buckets;
      level = p.level;
    } else {
      custom = true;
    }
    onchange(v.doc);
  }

  function setLevel(l) {
    level = l;
    if (advanced) pickedActions = [...LEVEL_ACTIONS[l]];
    fromBuilder();
  }
  function toggleBucket(b) {
    pickedBuckets = pickedBuckets.includes(b)
      ? pickedBuckets.filter((x) => x !== b)
      : [...pickedBuckets, b];
    fromBuilder();
  }
  function toggleAction(a) {
    pickedActions = pickedActions.includes(a)
      ? pickedActions.filter((x) => x !== a)
      : [...pickedActions, a];
    fromBuilder();
  }
  function setScope(s) {
    scope = s;
    fromBuilder();
  }
  function setAdvanced(on) {
    advanced = on;
    if (on && pickedActions.length === 0) pickedActions = [...LEVEL_ACTIONS[level]];
    fromBuilder();
  }
</script>

<div class="pb">
  <div class="segmented" role="tablist">
    {#each [["split", "Split"], ["builder", "Builder"], ["code", "Code"]] as [m, label]}
      <button class="seg" class:active={mode === m} onclick={() => (mode = m)}>{label}</button>
    {/each}
  </div>

  <div class="pb-body" class:split={mode === "split"}>
    {#if mode !== "code"}
      <div class="pb-builder">
        {#if custom}
          <p class="notice">
            This policy isn't one the visual builder recognizes — edit it as JSON. Switching a control
            below will replace it.
          </p>
        {/if}

        <div class="label-sm">Scope</div>
        <div class="seg-row">
          <button class="chip" class:on={scope === "all"} onclick={() => setScope("all")}
            >All buckets</button>
          <button class="chip" class:on={scope === "specific"} onclick={() => setScope("specific")}
            >Specific buckets</button>
        </div>

        {#if scope === "specific"}
          {#if buckets.length === 0}
            <p class="muted">No buckets yet.</p>
          {:else}
            <div class="bucket-pick">
              {#each buckets as b}
                <label class="check">
                  <input
                    type="checkbox"
                    checked={pickedBuckets.includes(b)}
                    onchange={() => toggleBucket(b)} />
                  <span class="mono">{b}</span>
                </label>
              {/each}
            </div>
          {/if}
        {/if}

        <div class="label-sm" style="margin-top:14px">Permission</div>
        <label class="check" style="margin-bottom:8px">
          <input type="checkbox" checked={advanced} onchange={(e) => setAdvanced(e.target.checked)} />
          <span>Advanced (pick individual actions)</span>
        </label>

        {#if !advanced}
          <div class="levels">
            {#each LEVELS as l}
              <button class="level" class:on={level === l.id} onclick={() => setLevel(l.id)}>
                <strong>{l.label}</strong>
                <span class="muted">{l.hint}</span>
              </button>
            {/each}
          </div>
        {:else}
          <div class="action-groups">
            {#each ACTION_GROUPS as g}
              <div class="action-group">
                <div class="label-sm">{g.label}</div>
                {#each g.actions as a}
                  <label class="check">
                    <input
                      type="checkbox"
                      checked={pickedActions.includes(a)}
                      onchange={() => toggleAction(a)} />
                    <span class="mono">{a}</span>
                  </label>
                {/each}
              </div>
            {/each}
          </div>
        {/if}
      </div>
    {/if}

    {#if mode !== "builder"}
      <div class="pb-code">
        <div class="label-sm">Policy JSON</div>
        <textarea
          class="policy-editor"
          spellcheck="false"
          bind:value={rawText}
          oninput={fromCode}></textarea>
        {#if error}<p class="err">{error}</p>{/if}
      </div>
    {/if}
  </div>
</div>

<style>
  .segmented {
    display: inline-flex;
    background: var(--surface-3);
    border-radius: var(--r-sm);
    padding: 3px;
    gap: 2px;
    margin-bottom: 12px;
  }
  .seg {
    border: none;
    background: transparent;
    color: var(--text-muted);
    padding: 5px 14px;
    border-radius: var(--r-sm);
    cursor: pointer;
    font-size: 0.85rem;
  }
  .seg.active {
    background: var(--surface);
    color: var(--text);
    box-shadow: var(--shadow-sm);
  }
  .pb-body.split {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 18px;
    align-items: start;
  }
  .pb-code textarea {
    width: 100%;
    min-height: 260px;
  }
  .seg-row {
    display: flex;
    gap: 8px;
    margin: 6px 0;
    flex-wrap: wrap;
  }
  .chip {
    border: 1px solid var(--border-strong);
    background: var(--surface);
    color: var(--text-muted);
    padding: 6px 12px;
    border-radius: 999px;
    cursor: pointer;
    font-size: 0.85rem;
  }
  .chip.on {
    border-color: var(--primary);
    background: var(--primary-tint);
    color: var(--primary-hover);
  }
  .bucket-pick {
    display: flex;
    flex-direction: column;
    gap: 4px;
    max-height: 150px;
    overflow: auto;
    border: 1px solid var(--border);
    border-radius: var(--r-sm);
    padding: 8px;
  }
  .check {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.88rem;
    cursor: pointer;
  }
  .levels {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .level {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 2px;
    text-align: left;
    border: 1px solid var(--border-strong);
    background: var(--surface);
    border-radius: var(--r);
    padding: 10px 14px;
    cursor: pointer;
  }
  .level.on {
    border-color: var(--primary);
    background: var(--primary-tint);
  }
  .level .muted {
    font-size: 0.8rem;
  }
  .action-groups {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
  }
  .notice {
    background: var(--warning-tint);
    color: var(--warning);
    border-radius: var(--r-sm);
    padding: 8px 12px;
    font-size: 0.85rem;
    margin-bottom: 10px;
  }
  @media (max-width: 720px) {
    .pb-body.split {
      grid-template-columns: 1fr;
    }
    .action-groups {
      grid-template-columns: 1fr;
    }
  }
</style>
