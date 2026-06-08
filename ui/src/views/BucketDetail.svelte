<script>
  // A bucket's two-section detail: a Browser tab (objects) and a Settings tab (configuration). The
  // active tab is reflected in the URL (#/buckets/<name>/browser | /settings) so it deep-links.
  import { navigate } from "../lib/router.js";
  import Tabs from "../components/Tabs.svelte";
  import BucketObjects from "./BucketObjects.svelte";
  import BucketConfig from "./BucketConfig.svelte";

  let { name, tab = "browser" } = $props();

  const tabs = [
    { id: "browser", label: "Browser" },
    { id: "settings", label: "Settings" },
  ];
  const select = (id) => navigate(`/buckets/${encodeURIComponent(name)}/${id}`);
</script>

<div class="crumbs">
  <a
    href="#/buckets"
    onclick={(e) => {
      e.preventDefault();
      navigate("/buckets");
    }}>Buckets</a>
  <span>/</span>
  <span class="mono">{name}</span>
</div>

<Tabs {tabs} active={tab} onselect={select} label="Bucket sections" />

{#if tab === "settings"}
  <BucketConfig {name} />
{:else}
  <BucketObjects {name} />
{/if}
