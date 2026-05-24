<script lang="ts">
  import CairnMark from "./CairnMark.svelte";
  let { onNew }: { onNew: () => void } = $props();
</script>

<div class="empty">
  <div class="topo" aria-hidden="true"></div>
  <div class="empty-inner">
    <div class="mark">
      <CairnMark size={72} />
    </div>
    <p class="kicker">A path is marked</p>
    <h1>
      with stones,<br />
      <em>one at a time.</em>
    </h1>
    <p class="lede">
      Pick an entry from the index, or place a new stone. Your work lives in git;
      the published path runs on no compute behind it.
    </p>
    <button onclick={onNew}>
      <span>Place a new stone</span>
      <svg viewBox="0 0 12 12" width="12" height="12" fill="none">
        <path
          d="M2 6h8M6.5 2.5L10 6l-3.5 3.5"
          stroke="currentColor"
          stroke-width="1.4"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
    </button>
  </div>
</div>

<style>
  .empty {
    flex: 1;
    display: grid;
    place-items: center;
    padding: 2rem;
    position: relative;
    background:
      radial-gradient(circle at 32% 28%, var(--accent-soft), transparent 55%),
      radial-gradient(circle at 70% 75%, rgba(63, 91, 58, 0.06), transparent 55%),
      var(--paper);
    overflow: hidden;
  }
  /* Faint topographic contours overlay — small SVG inlined as data URI. */
  .topo {
    position: absolute;
    inset: 0;
    pointer-events: none;
    opacity: 0.16;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 360 360' width='360' height='360'><g fill='none' stroke='%231c1a18' stroke-width='0.5'><ellipse cx='180' cy='180' rx='140' ry='52'/><ellipse cx='180' cy='180' rx='110' ry='40'/><ellipse cx='180' cy='180' rx='80' ry='28'/><ellipse cx='180' cy='180' rx='50' ry='18'/></g></svg>");
    background-size: 480px 480px;
    background-repeat: repeat;
  }
  .empty-inner {
    position: relative;
    max-width: 34rem;
    text-align: center;
    animation: rise 700ms var(--ease-out);
  }
  .mark {
    color: var(--accent);
    display: flex;
    justify-content: center;
    margin-bottom: 1.6rem;
    animation: rise 800ms var(--ease-out) 80ms backwards;
  }
  .kicker {
    font-family: var(--font-ui);
    font-size: 0.74rem;
    text-transform: uppercase;
    letter-spacing: 0.2em;
    color: var(--ink-faint);
    margin: 0 0 1rem;
    font-weight: 500;
  }
  h1 {
    font-family: var(--font-display);
    font-weight: 400;
    font-size: 2.8rem;
    line-height: 1.1;
    letter-spacing: -0.025em;
    color: var(--ink);
    margin: 0 0 1.4rem;
    font-variation-settings: "opsz" 72;
  }
  h1 em {
    font-style: italic;
    color: var(--accent);
    font-weight: 400;
  }
  .lede {
    font-family: var(--font-display);
    color: var(--ink-muted);
    line-height: 1.6;
    margin: 0 0 1.8rem;
    font-size: 1.02rem;
    font-variation-settings: "opsz" 16;
  }
  button {
    background: var(--ink);
    color: var(--paper);
    border: 0;
    padding: 0.7rem 1.3rem;
    border-radius: var(--r-md);
    font-family: var(--font-ui);
    font-size: 0.8rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-weight: 500;
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    transition:
      background-color 160ms var(--ease-out),
      transform 100ms var(--ease-out),
      box-shadow 160ms var(--ease-out);
    box-shadow: var(--shadow-sm);
  }
  button:hover {
    background: var(--accent);
    transform: translateY(-1px);
    box-shadow: var(--shadow-md);
  }
  button svg {
    transition: transform 200ms var(--ease-out);
  }
  button:hover svg {
    transform: translateX(3px);
  }
</style>
