<script>
  // A user's detail page: identity + S3 access key, active toggle, credential rotation, and the
  // attached identity policy edited via the PermissionBuilder. Created users are S3-API-only.
  import { api } from "../lib/api.js";
  import { navigate } from "../lib/router.js";
  import { ok, err } from "../lib/toast.js";
  import PermissionBuilder from "../components/PermissionBuilder.svelte";
  import CopyField from "../components/CopyField.svelte";

  let { id } = $props();
  let user = $state(null);
  let buckets = $state([]);
  let loading = $state(true);
  let error = $state("");
  let pendingPolicy = $state(undefined); // doc from the builder; null = invalid JSON
  let saving = $state(false);
  let rotated = $state(null);

  async function load() {
    loading = true;
    error = "";
    try {
      user = await api.getUser(id);
      const b = await api.listBuckets();
      buckets = (b.buckets || []).map((x) => x.name);
    } catch (e) {
      error = e.message;
    }
    loading = false;
  }
  load();

  const onPolicy = (doc) => (pendingPolicy = doc);

  async function savePolicy() {
    if (pendingPolicy === null) return err("Fix the policy JSON before saving");
    saving = true;
    try {
      await api.setUserPolicy(id, JSON.stringify(pendingPolicy));
      ok("Policy saved");
      await load();
    } catch (e) {
      err(e.message);
    }
    saving = false;
  }
  async function detachPolicy() {
    saving = true;
    try {
      await api.deleteUserPolicy(id);
      ok("Policy removed");
      await load();
    } catch (e) {
      err(e.message);
    }
    saving = false;
  }
  async function toggleActive() {
    try {
      await api.patchUser(id, { is_active: !user.is_active });
      ok(user.is_active ? "User deactivated" : "User activated");
      await load();
    } catch (e) {
      err(e.message);
    }
  }
  async function rotate() {
    try {
      rotated = await api.rotateCredentials(id);
      ok("New Bearer secret minted");
    } catch (e) {
      err(e.message);
    }
  }
</script>

<div class="crumbs">
  <a
    href="#/users"
    onclick={(e) => {
      e.preventDefault();
      navigate("/users");
    }}>Users</a>
  <span>/</span>
  <span class="mono">{user ? user.display_name : id}</span>
</div>

{#if error}<div class="error">{error}</div>{/if}
{#if loading}
  <p class="muted">Loading…</p>
{:else if user}
  <div class="card">
    <div class="row" style="justify-content:space-between; align-items:flex-start;">
      <div>
        <h2 style="margin:0">{user.display_name}</h2>
        <p class="muted" style="margin:4px 0 0">
          <span class="badge">{user.role}</span>
          <span class="badge" class:ok-badge={user.is_active}
            >{user.is_active ? "active" : "inactive"}</span>
          · S3-API access only
        </p>
      </div>
      <div class="row" style="gap:8px;">
        <button class="btn" onclick={toggleActive}>{user.is_active ? "Deactivate" : "Activate"}</button>
        <button class="btn" onclick={rotate}>Rotate Bearer secret</button>
      </div>
    </div>

    <div style="margin-top:14px">
      <CopyField label="S3 access key id" value={user.sigv4_access_key_id || "—"} />
      <CopyField label="Bearer access key id" value={user.access_key_id} />
    </div>

    {#if rotated}
      <div class="panel" style="margin-top:10px">
        <p class="label-sm">New Bearer secret (shown once)</p>
        <CopyField label="" value={rotated.bearer_secret} secret={true} />
      </div>
    {/if}
  </div>

  <div class="card" style="margin-top:18px">
    <h2 style="margin-top:0">Access policy</h2>
    <p class="muted" style="margin-top:-6px">
      Controls exactly which buckets and actions this user's S3 credentials may perform.
    </p>
    {#key user.id}
      <PermissionBuilder {buckets} initial={user.policy} onchange={onPolicy} />
    {/key}
    <div class="row" style="gap:8px; margin-top:14px;">
      <button class="btn primary" onclick={savePolicy} disabled={saving}>
        {saving ? "Saving…" : "Save policy"}
      </button>
      {#if user.policy}
        <button class="btn danger" onclick={detachPolicy} disabled={saving}>Remove policy</button>
      {/if}
    </div>
  </div>
{/if}

<style>
  .ok-badge {
    background: var(--success-tint);
    color: var(--success);
  }
</style>
