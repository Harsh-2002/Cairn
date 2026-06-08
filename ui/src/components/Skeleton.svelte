<script>
  // A layout-preserving loading placeholder using the shared .skeleton class (shimmer disabled under
  // prefers-reduced-motion). Render either a stack of lines or a single block.
  //
  // Props:
  //   lines   number  — number of skeleton lines to render (default 3). Ignored when `block` is true.
  //   block   boolean — render one solid block instead of lines (default false)
  //   height  string  — block height, any CSS length (default "1em" for lines as line-height proxy)
  //   width   string  — width of the block / last line (default "100%")
  //   gap     string  — gap between lines (default "0.55em")
  let {
    lines = 3,
    block = false,
    height = null,
    width = "100%",
    gap = "0.55em",
  } = $props();

  let count = $derived(Math.max(1, lines | 0));
</script>

{#if block}
  <div
    class="skeleton"
    style:height={height || "120px"}
    style:width
    aria-hidden="true"></div>
{:else}
  <div class="skeleton-stack" style:gap aria-hidden="true">
    {#each Array(count) as _, i (i)}
      <div
        class="skeleton skeleton-line"
        style:height={height || undefined}
        style:width={i === count - 1 ? width : undefined}></div>
    {/each}
  </div>
{/if}

<style>
  .skeleton-stack {
    display: flex;
    flex-direction: column;
  }
  .skeleton-stack .skeleton-line {
    margin: 0;
  }
</style>
