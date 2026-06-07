<script>
  import { hasToken, clearToken } from "./lib/api.js";
  import { route, navigate } from "./lib/router.js";
  import Login from "./views/Login.svelte";
  import Overview from "./views/Overview.svelte";
  import Buckets from "./views/Buckets.svelte";
  import BucketObjects from "./views/BucketObjects.svelte";
  import Users from "./views/Users.svelte";
  import Activity from "./views/Activity.svelte";

  let authed = $state(hasToken());
  let current = $state($route);

  // Keep the local route snapshot in sync with the store.
  $effect(() => route.subscribe((r) => (current = r)));

  function onauth() {
    authed = true;
    if (!window.location.hash || window.location.hash === "#") {
      navigate("/overview");
    }
  }

  function signOut() {
    clearToken();
    authed = false;
  }

  const nav = [
    { view: "overview", label: "Overview" },
    { view: "buckets", label: "Buckets" },
    { view: "users", label: "Users" },
    { view: "activity", label: "Activity" },
  ];

  function go(view) {
    navigate(`/${view}`);
  }
</script>

{#if !authed}
  <Login {onauth} />
{:else}
  <div class="app">
    <aside class="sidebar">
      <div class="brand"><span class="dot"></span> Cairn</div>
      {#each nav as item (item.view)}
        <a
          class="nav-link"
          class:active={current.view === item.view ||
            (item.view === "buckets" && current.view === "buckets")}
          href={`#/${item.view}`}
          onclick={(e) => {
            e.preventDefault();
            go(item.view);
          }}>{item.label}</a
        >
      {/each}
      <div class="sidebar-footer">
        <button class="full" onclick={signOut}>Sign out</button>
      </div>
    </aside>

    <main class="main">
      {#if current.view === "overview"}
        <Overview />
      {:else if current.view === "buckets"}
        {#if current.params.length > 0}
          {#key current.params[0]}
            <BucketObjects name={current.params[0]} />
          {/key}
        {:else}
          <Buckets />
        {/if}
      {:else if current.view === "users"}
        <Users />
      {:else if current.view === "activity"}
        <Activity />
      {:else}
        <Overview />
      {/if}
    </main>
  </div>
{/if}
