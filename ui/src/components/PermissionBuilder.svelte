<script>
  // A policy editor with two synced views: a visual Builder (the default, where most choices are
  // plain language) and a JSON Code editor, plus Split (side-by-side) where editing either side
  // updates the other. Emits the current policy doc via `onchange(docOrNull)` — null means the JSON
  // is invalid OR the selection grants nothing actionable, so the parent disables save and shows why.
  import {
    LEVELS,
    LEVEL_ACTIONS,
    ACTION_GROUPS,
    ACTION_GLOSS,
    actionSummary,
    presetToPolicy,
    advancedToPolicy,
    policyToPreset,
    grantsAccess,
    validate,
    pretty,
  } from "../lib/policy.js";

  let {
    buckets = [],
    initial = null,
    bucketsLoading = false,
    onchange = () => {},
  } = $props();

  // `initial` is a one-time seed read at mount; the parent remounts via {#key user.id} when it
  // changes, so capturing it once here is intentional. Read it through a closure so the compiler
  // doesn't treat the prop as a live reactive dependency of the initial $state values.
  const seed = (() => initial)();

  // Default to the Builder for new users (no initial policy). When editing an existing policy we
  // start in Split so the JSON they may already understand stays visible.
  let mode = $state(seed ? "split" : "builder"); // "builder" | "split" | "code"
  let advanced = $state(false);
  let scope = $state("all"); // "all" | "specific"
  let pickedBuckets = $state([]);
  let level = $state("read");
  let pickedActions = $state([...LEVEL_ACTIONS.read]);
  let custom = $state(false); // JSON is an unrecognized (but valid) doc
  let rawText = $state(pretty(presetToPolicy({ scope: "all", level: "read" })));
  let error = $state(""); // JSON parse/validation error (Code side)
  let bucketFilter = $state("");

  const uid = Math.random().toString(36).slice(2, 8);

  // Seed from an existing policy (UserDetail edit).
  if (seed) {
    rawText = pretty(seed);
    const p = policyToPreset(seed);
    if (p.recognized) {
      scope = p.scope;
      pickedBuckets = p.buckets;
      level = p.level;
    } else {
      custom = true;
    }
  }

  // Current doc derived from the visual model (used by the Builder side and the summary).
  let builderDoc = $derived(
    advanced
      ? advancedToPolicy({ scope, buckets: pickedBuckets, actions: pickedActions })
      : presetToPolicy({ scope, buckets: pickedBuckets, level }),
  );

  // True when the visual selection cannot grant anything (specific scope, nothing picked, or no
  // actions in advanced mode). Drives the inline block and the emitted-null guard.
  let noBuckets = $derived(scope === "specific" && pickedBuckets.length === 0);
  let noActions = $derived(advanced && pickedActions.length === 0);
  let builderGrantsNothing = $derived(!custom && (noBuckets || noActions));

  // Plain-language "what this allows" summary for the Builder.
  let allowPhrases = $derived(
    advanced ? actionSummary(pickedActions) : actionSummary(LEVEL_ACTIONS[level]),
  );
  let scopePhrase = $derived(
    scope === "all"
      ? "every bucket on this server"
      : pickedBuckets.length === 0
        ? "no buckets yet"
        : pickedBuckets.length === 1
          ? `the bucket ${pickedBuckets[0]}`
          : `${pickedBuckets.length} buckets`,
  );

  let filteredBuckets = $derived(
    bucketFilter.trim()
      ? buckets.filter((b) => b.toLowerCase().includes(bucketFilter.trim().toLowerCase()))
      : buckets,
  );
  let allFilteredPicked = $derived(
    filteredBuckets.length > 0 && filteredBuckets.every((b) => pickedBuckets.includes(b)),
  );

  // Emit the initial doc once on mount (or null if the seed grants nothing).
  emit();

  // Compute + emit the current doc. When the JSON is a custom (unrecognized) doc it stays
  // authoritative; otherwise the visual model is. A selection that grants nothing emits null so the
  // parent blocks Create with a clear reason.
  function emit() {
    if (custom) {
      // Custom JSON authored in the Code/Split view is authoritative — re-validate and emit it.
      const v = validate(rawText);
      onchange(v.ok && grantsAccess(v.doc) ? v.doc : null);
      return;
    }
    if (builderGrantsNothing || !grantsAccess(builderDoc)) {
      onchange(null);
      return;
    }
    onchange(builderDoc);
  }

  // Builder edits → regenerate JSON, emit.
  function fromBuilder() {
    custom = false;
    error = "";
    rawText = pretty(builderDoc);
    emit();
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
    // Even a valid, parseable doc can grant nothing (empty resources). Guard the same way.
    onchange(grantsAccess(v.doc) ? v.doc : null);
  }

  function setMode(m) {
    mode = m;
    // Re-emit from whichever side is now authoritative.
    if (m === "code") fromCode();
    else emit();
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
  function toggleSelectAll() {
    if (allFilteredPicked) {
      const drop = new Set(filteredBuckets);
      pickedBuckets = pickedBuckets.filter((b) => !drop.has(b));
    } else {
      const add = new Set([...pickedBuckets, ...filteredBuckets]);
      pickedBuckets = [...add];
    }
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
  <!-- View switch: plain buttons with aria-pressed (a toggle group, not a tab panel set). -->
  <div class="segmented" role="group" aria-label="Policy editor view">
    {#each [["builder", "Builder"], ["split", "Split"], ["code", "Code"]] as [m, label]}
      <button
        type="button"
        class="seg"
        class:active={mode === m}
        aria-pressed={mode === m}
        onclick={() => setMode(m)}>{label}</button>
    {/each}
  </div>

  <div class="pb-body" class:split={mode === "split"}>
    {#if mode !== "code"}
      <div class="pb-builder">
        {#if custom}
          <p class="notice warn" role="status">
            This policy isn't one the visual builder recognizes, so edit it as JSON in the Code view.
            Changing a control below will replace it.
          </p>
        {/if}

        <fieldset class="group">
          <legend class="label-sm">Which buckets</legend>
          <div class="seg-row" role="radiogroup" aria-label="Which buckets">
            <button
              type="button"
              class="chip"
              class:on={scope === "all"}
              role="radio"
              aria-checked={scope === "all"}
              onclick={() => setScope("all")}>All buckets</button>
            <button
              type="button"
              class="chip"
              class:on={scope === "specific"}
              role="radio"
              aria-checked={scope === "specific"}
              onclick={() => setScope("specific")}>Specific buckets</button>
          </div>

          {#if scope === "specific"}
            {#if bucketsLoading}
              <div class="bucket-pick is-status">
                <p class="muted" role="status">Loading buckets…</p>
              </div>
            {:else if buckets.length === 0}
              <p class="notice" role="status">
                There are no buckets yet. Create a bucket first, then come back to scope this user to
                it.
              </p>
            {:else}
              {#if buckets.length > 6}
                <div class="bucket-toolbar">
                  <input
                    type="search"
                    class="bucket-search"
                    placeholder="Filter buckets"
                    aria-label="Filter buckets"
                    bind:value={bucketFilter} />
                  <button
                    type="button"
                    class="btn small"
                    onclick={toggleSelectAll}
                    disabled={filteredBuckets.length === 0}>
                    {allFilteredPicked ? "Clear shown" : "Select shown"}
                  </button>
                </div>
              {/if}

              <div class="bucket-pick" role="group" aria-label="Buckets this user may use">
                {#if filteredBuckets.length === 0}
                  <p class="muted" style="margin:4px">No buckets match “{bucketFilter}”.</p>
                {:else}
                  {#each filteredBuckets as b (b)}
                    <label class="check">
                      <input
                        type="checkbox"
                        checked={pickedBuckets.includes(b)}
                        onchange={() => toggleBucket(b)} />
                      <span class="mono">{b}</span>
                    </label>
                  {/each}
                {/if}
              </div>

              {#if noBuckets}
                <p class="field-error" role="alert">
                  Pick at least one bucket, or switch to All buckets. With none selected this user
                  gets no access.
                </p>
              {:else}
                <p class="picked-count muted">
                  {pickedBuckets.length} selected.
                </p>
              {/if}
            {/if}
          {/if}
        </fieldset>

        <fieldset class="group">
          <legend class="label-sm">What they can do</legend>
          <label class="check toggle-adv">
            <input
              type="checkbox"
              checked={advanced}
              onchange={(e) => setAdvanced(e.target.checked)} />
            <span>Advanced: pick individual actions</span>
          </label>

          {#if !advanced}
            <div class="levels" role="radiogroup" aria-label="Permission level">
              {#each LEVELS as l (l.id)}
                <button
                  type="button"
                  class="level"
                  class:on={level === l.id}
                  role="radio"
                  aria-checked={level === l.id}
                  onclick={() => setLevel(l.id)}>
                  <strong>{l.label}</strong>
                  <span class="muted">{l.hint}</span>
                </button>
              {/each}
            </div>
          {:else}
            <div class="action-groups">
              {#each ACTION_GROUPS as g (g.label)}
                <div class="action-group">
                  <div class="label-sm">{g.label}</div>
                  {#each g.actions as a (a)}
                    <label class="check action-check">
                      <input
                        type="checkbox"
                        checked={pickedActions.includes(a)}
                        onchange={() => toggleAction(a)} />
                      <span class="action-text">
                        <span class="action-gloss">{ACTION_GLOSS[a] || a}</span>
                        <span class="mono action-verb">{a}</span>
                      </span>
                    </label>
                  {/each}
                </div>
              {/each}
            </div>
            {#if noActions}
              <p class="field-error" role="alert">
                Pick at least one action. With none selected this user gets no access.
              </p>
            {/if}
          {/if}
        </fieldset>

        <!-- Running, plain-language summary so the JSON is never the only thing explaining intent. -->
        {#if !custom}
          <div class="allow-summary" class:empty={builderGrantsNothing}>
            <div class="label-sm">This lets the user</div>
            {#if builderGrantsNothing}
              <p class="muted" style="margin:4px 0 0">
                Nothing yet. {noBuckets ? "Pick at least one bucket" : "Pick at least one action"} to
                grant access.
              </p>
            {:else}
              <ul>
                {#each allowPhrases as phrase (phrase)}
                  <li>{phrase}</li>
                {/each}
              </ul>
              <p class="scope-line muted">on {scopePhrase}.</p>
            {/if}
          </div>
        {/if}
      </div>
    {/if}

    {#if mode !== "builder"}
      <div class="pb-code">
        <label class="label-sm" for={`pb-json-${uid}`}>Policy JSON</label>
        <textarea
          id={`pb-json-${uid}`}
          class="policy-editor"
          class:invalid={!!error}
          aria-invalid={!!error}
          aria-describedby={error ? `pb-json-err-${uid}` : undefined}
          spellcheck="false"
          bind:value={rawText}
          oninput={fromCode}></textarea>
        {#if error}
          <p id={`pb-json-err-${uid}`} class="field-error" role="alert">{error}</p>
        {/if}
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
    padding: 6px 14px;
    border-radius: var(--r-sm);
    cursor: pointer;
    font-size: 0.85rem;
    font-weight: 500;
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
  .group {
    border: none;
    padding: 0;
    margin: 0 0 16px;
    min-width: 0;
  }
  .group > legend {
    padding: 0;
    margin-bottom: 2px;
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
    color: var(--primary-ink);
  }
  .bucket-toolbar {
    display: flex;
    gap: 8px;
    margin: 6px 0;
    align-items: center;
  }
  .bucket-search {
    flex: 1;
    min-width: 0;
  }
  .btn.small {
    padding: 5px 10px;
    font-size: 0.82rem;
    white-space: nowrap;
  }
  .bucket-pick {
    display: flex;
    flex-direction: column;
    gap: 4px;
    max-height: 170px;
    overflow: auto;
    border: 1px solid var(--border);
    border-radius: var(--r-sm);
    padding: 8px;
  }
  .bucket-pick.is-status {
    display: block;
  }
  .check {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.88rem;
    cursor: pointer;
    padding: 3px 2px;
  }
  .check input {
    flex-shrink: 0;
    width: 16px;
    height: 16px;
  }
  .toggle-adv {
    margin: 4px 0 10px;
  }
  .picked-count {
    margin: 6px 0 0;
    font-size: 0.82rem;
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
    gap: 12px 18px;
  }
  .action-check {
    align-items: flex-start;
    padding: 4px 2px;
  }
  .action-text {
    display: flex;
    flex-direction: column;
    gap: 1px;
    line-height: 1.3;
  }
  .action-gloss {
    font-size: 0.86rem;
    color: var(--text);
  }
  .action-verb {
    font-size: 0.74rem;
    color: var(--text-faint);
  }
  .allow-summary {
    margin-top: 4px;
    background: var(--primary-tint);
    border-radius: var(--r);
    padding: 12px 14px;
  }
  .allow-summary.empty {
    background: var(--surface-2);
    border: 1px solid var(--border);
  }
  .allow-summary ul {
    margin: 6px 0 0;
    padding-left: 18px;
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .allow-summary li {
    font-size: 0.88rem;
    color: var(--text);
  }
  .allow-summary .scope-line {
    margin: 7px 0 0;
    font-size: 0.84rem;
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
