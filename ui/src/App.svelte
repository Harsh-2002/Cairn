<script>
  import { hasToken, clearToken } from "./lib/api.js";
  import { route, navigate } from "./lib/router.js";
  import Login from "./views/Login.svelte";
  import Overview from "./views/Overview.svelte";
  import Buckets from "./views/Buckets.svelte";
  import BucketDetail from "./views/BucketDetail.svelte";
  import Users from "./views/Users.svelte";
  import UserDetail from "./views/UserDetail.svelte";
  import Replication from "./views/Replication.svelte";
  import Activity from "./views/Activity.svelte";
  import Toasts from "./components/Toasts.svelte";

  let authed = $state(hasToken());
  let current = $state($route);
  let menuOpen = $state(false);

  let sidebarEl = $state(null);
  let hamburgerEl = $state(null);

  $effect(() => route.subscribe((r) => (current = r)));

  // Drawer focus management: when the off-canvas menu opens, move focus into it, trap Tab inside,
  // close on Escape, and return focus to the hamburger when it closes.
  $effect(() => {
    if (!menuOpen) return;
    const el = sidebarEl;
    if (!el) return;
    const first = el.querySelector(
      'a, button, input, select, textarea, [tabindex]:not([tabindex="-1"])',
    );
    queueMicrotask(() => first?.focus());

    function onKeydown(e) {
      if (e.key === "Escape") {
        e.preventDefault();
        closeMenu();
        return;
      }
      if (e.key !== "Tab") return;
      const focusable = el.querySelectorAll(
        'a, button, input, select, textarea, [tabindex]:not([tabindex="-1"])',
      );
      if (focusable.length === 0) return;
      const firstEl = focusable[0];
      const lastEl = focusable[focusable.length - 1];
      if (e.shiftKey && document.activeElement === firstEl) {
        e.preventDefault();
        lastEl.focus();
      } else if (!e.shiftKey && document.activeElement === lastEl) {
        e.preventDefault();
        firstEl.focus();
      }
    }
    document.addEventListener("keydown", onKeydown);
    return () => document.removeEventListener("keydown", onKeydown);
  });

  function closeMenu() {
    if (!menuOpen) return;
    menuOpen = false;
    queueMicrotask(() => hamburgerEl?.focus());
  }

  function onauth() {
    authed = true;
    if (!window.location.hash || window.location.hash === "#") navigate("/overview");
  }
  function signOut() {
    clearToken();
    authed = false;
    menuOpen = false;
  }

  const nav = [
    { key: "overview", label: "Overview", path: "/overview" },
    { key: "buckets", label: "Buckets", path: "/buckets" },
    { key: "users", label: "Users", path: "/users" },
    { key: "activity", label: "Activity", path: "/activity" },
    { key: "replication", label: "Monitoring", path: "/replication" },
  ];

  // Map a route name to its top-level sidebar section.
  function section(name) {
    if (name.startsWith("bucket")) return "buckets";
    if (name === "user") return "users";
    return name;
  }

  let activeSection = $derived(section(current.name));

  function go(path) {
    navigate(path);
    menuOpen = false;
  }
</script>

<Toasts />

{#if !authed}
  <Login {onauth} />
{:else}
  <a class="skip-link" href="#main-content">Skip to main content</a>
  <div class="app" class:menu-open={menuOpen}>
    <aside class="sidebar" id="primary-sidebar" bind:this={sidebarEl} aria-label="Primary">
      <div class="brand"><span class="dot"></span> Cairn</div>
      {#each nav as item (item.key)}
        <a
          class="nav-link"
          class:active={activeSection === item.key}
          aria-current={activeSection === item.key ? "page" : undefined}
          href={`#${item.path}`}
          onclick={(e) => {
            e.preventDefault();
            go(item.path);
          }}>{item.label}</a>
      {/each}
      <div class="sidebar-footer">
        <button class="full" onclick={signOut}>Sign out</button>
      </div>
    </aside>

    <button class="scrim" aria-label="Close menu" onclick={closeMenu}></button>

    <div class="content">
      <header class="topbar">
        <button
          class="hamburger"
          bind:this={hamburgerEl}
          aria-label="Open menu"
          aria-expanded={menuOpen}
          aria-controls="primary-sidebar"
          onclick={() => (menuOpen = true)}>
          <span></span><span></span><span></span>
        </button>
        <div class="brand"><span class="dot"></span> Cairn</div>
      </header>

      <main class="main" id="main-content" tabindex="-1">
        {#if current.name === "overview"}
          <Overview />
        {:else if current.name === "buckets"}
          <Buckets />
        {:else if current.name === "bucket.browser" || current.name === "bucket.settings"}
          {#key current.params.name}
            <BucketDetail
              name={current.params.name}
              tab={current.name === "bucket.settings" ? "settings" : "browser"} />
          {/key}
        {:else if current.name === "users"}
          <Users />
        {:else if current.name === "user"}
          {#key current.params.id}
            <UserDetail id={current.params.id} />
          {/key}
        {:else if current.name === "activity"}
          <Activity />
        {:else if current.name === "replication"}
          <Replication />
        {:else}
          <Overview />
        {/if}
      </main>
    </div>
  </div>
{/if}
