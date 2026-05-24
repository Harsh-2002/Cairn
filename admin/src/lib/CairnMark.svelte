<script lang="ts">
  // The signature mark — three stones ascending in size, evoking a cairn.
  // Used in the sidebar header at 18px and in the empty state at 64px.
  let { size = 18 }: { size?: number } = $props();
</script>

<svg
  width={size}
  height={(size * 24) / 20}
  viewBox="0 0 20 24"
  fill="currentColor"
  aria-hidden="true"
  class="cairn-mark"
>
  <ellipse cx="10" cy="4" rx="2.6" ry="1.6" />
  <ellipse cx="10" cy="10" rx="4.5" ry="2.1" />
  <ellipse cx="10" cy="17.5" rx="6.5" ry="3.2" />
</svg>

<style>
  .cairn-mark {
    color: var(--accent);
    flex-shrink: 0;
    display: block;
  }
</style>
