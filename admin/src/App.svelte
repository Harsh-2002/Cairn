<script lang="ts">
  import "./styles.css";
  import Sidebar from "./lib/Sidebar.svelte";
  import TopBar from "./lib/TopBar.svelte";
  import Editor from "./lib/Editor.svelte";
  import EmptyState from "./lib/EmptyState.svelte";
  import {
    listPosts,
    createPost,
    deletePost,
    type PostSummary,
  } from "./lib/api";

  type Status = "idle" | "loading" | "saving" | "saved" | "error";

  let posts = $state<PostSummary[] | null>(null);
  let selected = $state<string | null>(null);
  let status = $state<Status>("idle");
  let lastSaved = $state<string | null>(null);
  let publishSignal = $state<{ count: number }>({ count: 0 });
  let topError = $state<string | null>(null);

  async function refresh() {
    try {
      posts = await listPosts();
      if (!selected && posts.length > 0) selected = posts[0].slug;
    } catch (e) {
      topError = String((e as Error).message ?? e);
    }
  }

  async function handleNew() {
    try {
      const r = await createPost("Untitled");
      await refresh();
      selected = r.slug;
    } catch (e) {
      console.error("createPost:", e);
      topError = String((e as Error).message ?? e);
    }
  }

  async function handleDelete(slug: string) {
    try {
      await deletePost(slug);
      if (selected === slug) selected = null;
      await refresh();
    } catch (e) {
      console.error("deletePost:", e);
      topError = String((e as Error).message ?? e);
    }
  }

  function handlePublish() {
    publishSignal = { count: publishSignal.count + 1 };
  }

  $effect(() => {
    refresh();
  });
</script>

<div class="app">
  <Sidebar
    {posts}
    {selected}
    onSelect={(s) => (selected = s)}
    onNew={handleNew}
    onDelete={handleDelete}
  />
  <main class="main">
    <TopBar slug={selected} {status} {lastSaved} onPublish={handlePublish} />
    {#if topError}
      <div class="banner">{topError}</div>
    {/if}
    {#if selected}
      <Editor
        slug={selected}
        onStatus={(s) => (status = s)}
        onLastSaved={(id) => (lastSaved = id)}
        {publishSignal}
      />
    {:else}
      <EmptyState onNew={handleNew} />
    {/if}
  </main>
</div>

<style>
  .app {
    display: flex;
    height: 100vh;
    overflow: hidden;
    background: var(--paper);
  }
  .main {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-width: 0;
  }
  .banner {
    background: rgba(138, 54, 24, 0.06);
    color: var(--error);
    border-bottom: 1px solid rgba(138, 54, 24, 0.18);
    padding: 0.55rem 1.2rem;
    font-size: 0.78rem;
    font-family: var(--font-mono);
    letter-spacing: 0.02em;
    animation: rise 280ms var(--ease-out);
  }
</style>
