<script>
  // A horizontal tab bar. `tabs` = [{ id, label }]; `active` is the current id; `onselect(id)` fires
  // on click. Collapses to a <select> on narrow screens.
  let { tabs = [], active = "", onselect = () => {} } = $props();
</script>

<div class="tabs">
  <div class="tabbar">
    {#each tabs as t}
      <button class="tab" class:active={t.id === active} onclick={() => onselect(t.id)}>
        {t.label}
      </button>
    {/each}
  </div>
  <select class="tabselect" value={active} onchange={(e) => onselect(e.target.value)}>
    {#each tabs as t}<option value={t.id}>{t.label}</option>{/each}
  </select>
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
  .tabselect {
    display: none;
    width: 100%;
    margin-bottom: 18px;
  }
  @media (max-width: 560px) {
    .tabbar {
      display: none;
    }
    .tabselect {
      display: block;
    }
  }
</style>
