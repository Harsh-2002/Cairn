<script>
  // Users are S3-API-only credentials scoped by an access policy. The root admin is the sole admin,
  // so there is no role selector here. Create mints an S3 (SigV4) key/secret shown exactly once and
  // attaches the policy built with the PermissionBuilder.
  import { api } from "../lib/api.js";
  import { navigate } from "../lib/router.js";
  import { ok, err } from "../lib/toast.js";
  import PermissionBuilder from "../components/PermissionBuilder.svelte";
  import CopyField from "../components/CopyField.svelte";

  let users = $state([]);
  let buckets = $state([]);
  let loading = $state(true);

  let showCreate = $state(false);
  let displayName = $state("");
  let pendingPolicy = $state(undefined); // doc from the builder; null = invalid
  let creating = $state(false);
  let created = $state(null); // one-time credentials

  async function load() {
    loading = true;
    try {
      const res = await api.listUsers();
      users = (res && res.users) || [];
      const b = await api.listBuckets();
      buckets = (b.buckets || []).map((x) => x.name);
    } catch (e) {
      err(e.message);
    }
    loading = false;
  }
  load();

  const onPolicy = (doc) => (pendingPolicy = doc);

  async function create() {
    const dn = displayName.trim();
    if (!dn) return err("Display name is required");
    if (pendingPolicy === null) return err("Fix the policy JSON before creating");
    creating = true;
    try {
      const res = await api.createUser(dn); // member; S3-only
      if (pendingPolicy) await api.setUserPolicy(res.id, JSON.stringify(pendingPolicy));
      created = res;
      ok(`Created ${dn}`);
      showCreate = false;
      displayName = "";
      await load();
    } catch (e) {
      err(e.message);
    }
    creating = false;
  }
</script>

<div class="row" style="justify-content:space-between; align-items:center;">
  <div>
    <h1 style="margin-bottom:2px">Users</h1>
    <p class="subtitle" style="margin:0">S3-API access keys, each scoped by an access policy.</p>
  </div>
  <button class="btn primary" onclick={() => (showCreate = !showCreate)}>
    {showCreate ? "Cancel" : "New user"}
  </button>
</div>

{#if created}
  <div class="card" style="margin-top:16px; border-color: var(--warning);">
    <strong>Save these credentials now — the secret is shown only once.</strong>
    <p class="muted" style="margin:4px 0 12px">
      These are S3 credentials: use them in any S3 client (boto3, aws-cli, …) against this server.
    </p>
    <CopyField label="S3 access key id" value={created.s3_access_key_id} secret={true} />
    <CopyField label="S3 secret key" value={created.s3_secret_key} secret={true} />
    <details>
      <summary class="muted" style="cursor:pointer; font-size:0.85rem;">
        Bearer token (alternative auth)
      </summary>
      <div style="margin-top:8px">
        <CopyField
          label="Bearer token"
          value={`${created.bearer_access_key_id}.${created.bearer_secret}`}
          secret={true} />
      </div>
    </details>
    <button class="btn" style="margin-top:10px" onclick={() => (created = null)}>Dismiss</button>
  </div>
{/if}

{#if showCreate}
  <div class="card" style="margin-top:16px">
    <h2 style="margin-top:0">New user</h2>
    <div class="field">
      <label for="dn">Display name</label>
      <input id="dn" placeholder="e.g. backup-bot" bind:value={displayName} autocomplete="off" />
    </div>
    <div class="label-sm" style="margin-top:14px">Access policy</div>
    <p class="muted" style="margin-top:2px">
      Choose what this user's S3 credentials can do. Unfamiliar with policy JSON? Use the split view.
    </p>
    <PermissionBuilder {buckets} onchange={onPolicy} />
    <div class="row" style="gap:8px; margin-top:14px;">
      <button class="btn primary" onclick={create} disabled={creating}>
        {creating ? "Creating…" : "Create user"}
      </button>
      <button class="btn" onclick={() => (showCreate = false)}>Cancel</button>
    </div>
  </div>
{/if}

{#if loading}
  <p class="muted" style="margin-top:16px">Loading…</p>
{:else if users.length === 0}
  <div class="empty" style="margin-top:16px">No users yet.</div>
{:else}
  <div class="table-wrap" style="margin-top:16px">
    <table>
      <thead>
        <tr>
          <th>Display name</th>
          <th>S3 access key</th>
          <th>Status</th>
          <th style="text-align:right;"></th>
        </tr>
      </thead>
      <tbody>
        {#each users as u (u.id)}
          <tr>
            <td>
              <a
                href={`#/users/${encodeURIComponent(u.id)}`}
                onclick={(e) => {
                  e.preventDefault();
                  navigate(`/users/${encodeURIComponent(u.id)}`);
                }}>{u.display_name}</a>
            </td>
            <td class="mono">{u.access_key_id}</td>
            <td>
              <span class="badge" class:ok-badge={u.is_active}
                >{u.is_active ? "active" : "inactive"}</span>
            </td>
            <td style="text-align:right;">
              <button class="btn" onclick={() => navigate(`/users/${encodeURIComponent(u.id)}`)}>
                Manage
              </button>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .ok-badge {
    background: var(--success-tint);
    color: var(--success);
  }
</style>
