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

  $effect(() => route.subscribe((r) => (current = r)));

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
  <div class="app" class:menu-open={menuOpen}>
    <aside class="sidebar">
      <div class="brand"><span class="dot"></span> Cairn</div>
      {#each nav as item (item.key)}
        <a
          class="nav-link"
          class:active={activeSection === item.key}
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

    <button class="scrim" aria-label="Close menu" onclick={() => (menuOpen = false)}></button>

    <div class="content">
      <header class="topbar">
        <button class="hamburger" aria-label="Open menu" onclick={() => (menuOpen = true)}>
          <span></span><span></span><span></span>
        </button>
        <div class="brand"><span class="dot"></span> Cairn</div>
      </header>

      <main class="main">
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
