<script>
  // A horizontal tab bar. `tabs` = [{ id, label }]; `active` is the current id; `onselect(id)` fires
  // on click. Collapses to a labelled <select> on narrow screens. `label` names the tab group for
  // assistive tech and the mobile select (default "Section").
  let { tabs = [], active = "", onselect = () => {}, label = "Section" } = $props();
  const selectId = `tabselect-${Math.random().toString(36).slice(2, 8)}`;
</script>

<div class="tabs">
  <div class="tabbar" role="tablist" aria-label={label}>
    {#each tabs as t}
      <button
        class="tab"
        class:active={t.id === active}
        role="tab"
        aria-selected={t.id === active}
        onclick={() => onselect(t.id)}>
        {t.label}
      </button>
    {/each}
  </div>
  <div class="tabselect-wrap">
    <label class="label-sm" for={selectId}>{label}</label>
    <select
      id={selectId}
      class="tabselect"
      value={active}
      onchange={(e) => onselect(e.target.value)}>
      {#each tabs as t}<option value={t.id}>{t.label}</option>{/each}
    </select>
  </div>
</div>

<style>
  .tabbar {
    display: flex;
    gap: 4px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 18px;
  }
  .tab {
    border: none;
    background: transparent;
    color: var(--text-muted);
    padding: 9px 16px;
    cursor: pointer;
    font-size: 0.9rem;
    border-bottom: 2px solid transparent;
    margin-bottom: -1px;
  }
  .tab:hover {
    color: var(--text);
  }
  .tab.active {
    color: var(--primary-hover);
    border-bottom-color: var(--primary);
  }
  .tabselect-wrap {
    display: none;
    margin-bottom: 18px;
  }
  .tabselect-wrap .label-sm {
    display: block;
    margin-bottom: 5px;
  }
  .tabselect {
    width: 100%;
  }
  @media (max-width: 560px) {
    .tabbar {
      display: none;
    }
    .tabselect-wrap {
      display: block;
    }
  }
</style>
