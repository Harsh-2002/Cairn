<script lang="ts">
  import { onDestroy } from "svelte";
  import { Crepe } from "@milkdown/crepe";
  import { listenerCtx } from "@milkdown/kit/plugin/listener";
  import "@milkdown/crepe/theme/common/style.css";
  import "@milkdown/crepe/theme/frame.css";

  import { getPost, autosavePost, publishPost, presignAsset } from "./api";

  type Status = "idle" | "loading" | "saving" | "saved" | "error";

  let {
    slug,
    onStatus,
    onLastSaved,
    publishSignal,
  }: {
    slug: string;
    onStatus: (s: Status) => void;
    onLastSaved: (id: string) => void;
    publishSignal: { count: number };
  } = $props();

  let container: HTMLDivElement | undefined = $state(undefined);
  let crepe: Crepe | null = null;

  let title = $state("");
  let originalTitle = $state("");
  let originalBody = $state("");
  let currentBody = $state("");
  let date = $state("");
  let activeSlug = $state<string | null>(null);
  let lastPublishCount = $state(0);
  const session = crypto.randomUUID();
  let autosaveTimer: ReturnType<typeof setTimeout> | null = null;

  async function load(targetSlug: string) {
    if (!container) return;
    onStatus("loading");

    if (crepe) {
      try {
        await crepe.destroy();
      } catch (e) {
        console.warn("destroy previous Crepe:", e);
      }
      crepe = null;
    }

    let initial = "";
    let initialTitle = "";
    try {
      const p = await getPost(targetSlug);
      initial = p.body;
      initialTitle = p.title || "";
      date = p.date;
      originalBody = p.body;
      originalTitle = initialTitle;
      currentBody = p.body;
      title = initialTitle;
    } catch (e) {
      console.error("load post:", e);
      onStatus("error");
      return;
    }

    try {
      crepe = new Crepe({ root: container, defaultValue: initial });
      crepe.editor.config((ctx) => {
        ctx.get(listenerCtx).markdownUpdated((_ctx, markdown, prev) => {
          if (markdown === prev) return;
          currentBody = markdown;
          scheduleAutosave();
        });
      });
      await crepe.create();
      try {
        const normalized = crepe.getMarkdown();
        originalBody = normalized;
        currentBody = normalized;
      } catch (e) {
        console.warn("getMarkdown after create failed:", e);
      }
      activeSlug = targetSlug;
      onStatus("idle");
    } catch (e) {
      console.error("init Crepe:", e);
      onStatus("error");
    }
  }

  $effect(() => {
    if (container && slug && slug !== activeSlug) {
      void load(slug);
    }
  });

  $effect(() => {
    if (publishSignal.count > lastPublishCount) {
      lastPublishCount = publishSignal.count;
      void doPublish();
    }
  });

  onDestroy(() => {
    if (autosaveTimer) clearTimeout(autosaveTimer);
    if (crepe) void crepe.destroy();
  });

  function scheduleAutosave() {
    if (autosaveTimer) clearTimeout(autosaveTimer);
    autosaveTimer = setTimeout(() => void doAutosave(), 1500);
  }

  async function doAutosave() {
    if (currentBody === originalBody && title === originalTitle) return;
    onStatus("saving");
    try {
      // Always send the current title — the server stacks autosaves on the
      // draft branch, so a missing title would otherwise revert to whatever
      // main has on the next save.
      const r = await autosavePost(slug, currentBody, session, title);
      onLastSaved(r.branch);
      originalBody = currentBody;
      originalTitle = title;
      onStatus("saved");
    } catch (e) {
      console.error("autosave:", e);
      onStatus("error");
    }
  }

  async function doPublish() {
    await doAutosave();
    try {
      const r = await publishPost(slug, session);
      onLastSaved(r.commit);
      onStatus("saved");
    } catch (e) {
      console.error("publish:", e);
      onStatus("error");
    }
  }

  function onTitleInput(e: Event) {
    const t = e.target as HTMLInputElement;
    title = t.value;
    scheduleAutosave();
  }

  function onTitleKey(e: KeyboardEvent) {
    if (e.key === "Enter") {
      e.preventDefault();
      const pm = container?.querySelector(".ProseMirror") as HTMLElement | null;
      pm?.focus();
    }
  }

  async function uploadImage(file: File) {
    if (!crepe) return;
    try {
      onStatus("saving");
      const buf = await file.arrayBuffer();
      const digest = await crypto.subtle.digest("SHA-256", buf);
      const sha256 = Array.from(new Uint8Array(digest))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
      const ext =
        (file.name.split(".").pop() || file.type.split("/")[1] || "bin").toLowerCase();
      const { url, key } = await presignAsset(sha256, ext);
      await fetch(url, { method: "PUT", body: file });
      const updated = `${currentBody}\n\n![](${key})\n`;
      currentBody = updated;
      const c = crepe as unknown as { setMarkdown?: (s: string) => void };
      if (typeof c.setMarkdown === "function") c.setMarkdown(updated);
      scheduleAutosave();
    } catch (e) {
      console.error("uploadImage:", e);
      onStatus("error");
    }
  }

  function onFileInput(e: Event) {
    const t = e.target as HTMLInputElement;
    const file = t.files?.[0];
    if (file) void uploadImage(file);
    t.value = "";
  }

  function onPaste(e: ClipboardEvent) {
    const items = e.clipboardData?.items;
    if (!items) return;
    for (const item of items) {
      if (item.kind === "file" && item.type.startsWith("image/")) {
        e.preventDefault();
        e.stopPropagation();
        const file = item.getAsFile();
        if (file) void uploadImage(file);
        return;
      }
    }
  }

  function onDrop(e: DragEvent) {
    const files = e.dataTransfer?.files;
    if (!files || files.length === 0) return;
    let intercepted = false;
    for (const file of Array.from(files)) {
      if (file.type.startsWith("image/")) {
        if (!intercepted) {
          e.preventDefault();
          e.stopPropagation();
          intercepted = true;
        }
        void uploadImage(file);
      }
    }
  }

  $effect(() => {
    if (!container) return;
    const el = container;
    el.addEventListener("paste", onPaste, true);
    el.addEventListener("drop", onDrop, true);
    return () => {
      el.removeEventListener("paste", onPaste, true);
      el.removeEventListener("drop", onDrop, true);
    };
  });

  function fmtDateLong(iso: string): string {
    try {
      const d = new Date(iso);
      return d.toLocaleDateString(undefined, {
        year: "numeric",
        month: "long",
        day: "numeric",
      });
    } catch {
      return "";
    }
  }
</script>

<div class="page">
  <div class="paper-bg" aria-hidden="true"></div>
  <div class="page-inner">
    <div class="folio">
      <span class="folio-section">Index</span>
      <span class="folio-sep">·</span>
      <span class="folio-slug">{slug}</span>
    </div>

    <input
      class="title"
      placeholder="Untitled"
      value={title}
      oninput={onTitleInput}
      onkeydown={onTitleKey}
      aria-label="Post title"
    />

    <div class="meta">
      {#if date}
        <span class="meta-item">
          <span class="meta-key">filed</span>
          <span class="meta-val">{fmtDateLong(date)}</span>
        </span>
        <span class="meta-divider" aria-hidden="true"></span>
      {/if}
      <label class="meta-upload">
        <svg viewBox="0 0 12 12" width="11" height="11" fill="none" aria-hidden="true">
          <path
            d="M2.5 9.5h7M6 2v6M3.5 4.5L6 2l2.5 2.5"
            stroke="currentColor"
            stroke-width="1.3"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
        </svg>
        <span class="meta-upload-text">attach image</span>
        <input type="file" accept="image/*" onchange={onFileInput} hidden />
      </label>
    </div>

    <hr class="title-rule" />

    <div class="editor-host" bind:this={container}></div>
  </div>
</div>

<style>
  .page {
    flex: 1;
    overflow-y: auto;
    background: var(--paper);
    position: relative;
  }
  /* Ultra-faint ruled-line texture — the notebook page. */
  .paper-bg {
    position: absolute;
    inset: 0;
    pointer-events: none;
    background-image: linear-gradient(
      to bottom,
      transparent 0,
      transparent calc(2rem - 1px),
      rgba(28, 26, 24, 0.03) calc(2rem - 1px),
      rgba(28, 26, 24, 0.03) 2rem
    );
    background-size: 100% 2rem;
    opacity: 0.7;
  }
  .page-inner {
    position: relative;
    max-width: var(--editor-max-w);
    margin: 0 auto;
    padding: 3.5rem 1.5rem 6rem;
    animation: rise 500ms var(--ease-out);
  }
  .folio {
    display: flex;
    align-items: baseline;
    gap: 0.4rem;
    font-family: var(--font-ui);
    font-size: 0.7rem;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--ink-faint);
    margin-bottom: 1.4rem;
  }
  .folio-sep { color: var(--ink-faint); }
  .folio-slug {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    letter-spacing: 0.04em;
    text-transform: none;
    color: var(--ink-muted);
  }
  .title {
    width: 100%;
    border: 0;
    background: transparent;
    color: var(--ink);
    font-family: var(--font-display);
    font-size: 3rem;
    font-weight: 500;
    letter-spacing: -0.028em;
    padding: 0;
    margin: 0;
    outline: none;
    line-height: 1.05;
    font-variation-settings: "opsz" 72;
  }
  .title::placeholder {
    color: var(--ink-faint);
    font-style: italic;
    font-weight: 400;
  }
  .meta {
    display: flex;
    gap: 0.85rem;
    align-items: center;
    margin: 1.4rem 0 0;
    font-family: var(--font-ui);
    font-size: 0.74rem;
    color: var(--ink-muted);
    flex-wrap: wrap;
  }
  .meta-item,
  .meta-upload {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
  }
  .meta-key {
    color: var(--ink-faint);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-size: 0.65rem;
    font-weight: 500;
  }
  .meta-val {
    color: var(--ink-muted);
    font-family: var(--font-display);
    font-size: 0.8rem;
    font-style: italic;
    font-variation-settings: "opsz" 16;
  }
  .meta-divider {
    width: 1px;
    height: 0.7em;
    background: var(--rule-strong);
  }
  .meta-upload {
    cursor: pointer;
    padding: 0.25rem 0.55rem;
    border-radius: var(--r-sm);
    color: var(--ink-muted);
    transition:
      background-color 120ms var(--ease-out),
      color 120ms var(--ease-out);
  }
  .meta-upload:hover {
    background: var(--paper-deep);
    color: var(--accent);
  }
  .meta-upload-text {
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-size: 0.65rem;
    font-weight: 500;
  }
  .title-rule {
    border: 0;
    border-top: 1px solid var(--rule-strong);
    margin: 1.5rem 0 0.5rem;
    width: 3.5rem;
    opacity: 0.8;
  }
  .editor-host {
    min-height: 60vh;
  }
</style>
