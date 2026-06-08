<script>
  // Users are S3-API-only credentials scoped by an access policy. The root admin is the sole admin,
  // so there is no role selector here. Create mints an S3 (SigV4) key/secret shown exactly once and
  // attaches the policy built with the PermissionBuilder.
  import { tick } from "svelte";
  import { api } from "../lib/api.js";
  import { navigate } from "../lib/router.js";
  import { ok, err } from "../lib/toast.js";
  import PermissionBuilder from "../components/PermissionBuilder.svelte";
  import CopyField from "../components/CopyField.svelte";
  import Skeleton from "../components/Skeleton.svelte";

  let users = $state([]);
  let buckets = $state([]);
  let loading = $state(true);
  let bucketsLoading = $state(true);

  let showCreate = $state(false);
  let displayName = $state("");
  let nameTouched = $state(false);
  let pendingPolicy = $state(undefined); // doc from the builder; null = invalid or grants nothing
  let creating = $state(false);
  let created = $state(null); // one-time credentials
  let savedAck = $state(false); // user confirmed they stored the one-time secret
  let createdPanel = $state(null); // element to focus when credentials appear

  // Inline, pre-submit validity so the button state explains itself instead of waiting for a toast.
  let nameError = $derived(nameTouched && !displayName.trim() ? "Enter a display name." : "");
  let policyInvalid = $derived(pendingPolicy === null);
  let canCreate = $derived(!!displayName.trim() && !policyInvalid && !creating);

  async function load() {
    loading = true;
    bucketsLoading = true;
    try {
      const res = await api.listUsers();
      users = (res && res.users) || [];
    } catch (e) {
      err(e.message);
    }
    loading = false;
    try {
      const b = await api.listBuckets();
      buckets = (b.buckets || []).map((x) => x.name);
    } catch (e) {
      err(e.message);
    }
    bucketsLoading = false;
  }
  load();

  const onPolicy = (doc) => (pendingPolicy = doc);

  async function create() {
    nameTouched = true;
    const dn = displayName.trim();
    if (!dn) return;
    if (policyInvalid) return; // button is disabled; guard anyway
    creating = true;
    try {
      const res = await api.createUser(dn); // member; S3-only
      if (pendingPolicy) await api.setUserPolicy(res.id, JSON.stringify(pendingPolicy));
      created = res;
      savedAck = false;
      ok(`Created ${dn}`);
      showCreate = false;
      displayName = "";
      nameTouched = false;
      await load();
      await tick();
      createdPanel?.focus();
    } catch (e) {
      err(e.message);
    }
    creating = false;
  }

  function dismissCreated() {
    created = null;
    savedAck = false;
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
  <!-- One-time credentials: a focused, high-gravity panel that cannot be dismissed by accident. It
       takes focus on appear, is labelled as an alert, and only the explicit "I have saved these"
       acknowledgement enables Done. -->
  <section
    class="cred-panel"
    bind:this={createdPanel}
    tabindex="-1"
    role="group"
    aria-live="assertive"
    aria-labelledby="cred-title"
    aria-describedby="cred-desc">
    <h2 id="cred-title" class="cred-title">Save these credentials now</h2>
    <p id="cred-desc" class="cred-desc">
      The secret key is shown only once and cannot be retrieved later. Store it somewhere safe before
      you close this. These are S3 credentials: use them in any S3 client (boto3, aws-cli, and
      others) against this server.
    </p>

    <CopyField label="S3 access key id" value={created.s3_access_key_id} secret={true} />
    <CopyField label="S3 secret key" value={created.s3_secret_key} secret={true} />

    <details class="cred-advanced">
      <summary>Other ways to authenticate</summary>
      <p class="cred-adv-note">
        Most clients use the S3 key and secret above. This token is an alternative used by Cairn's own
        API and tools that authenticate with a single bearer token instead of an S3 key pair.
      </p>
      <CopyField
        label="Bearer token"
        value={`${created.bearer_access_key_id}.${created.bearer_secret}`}
        secret={true} />
    </details>

    <label class="cred-ack check">
      <input type="checkbox" bind:checked={savedAck} />
      <span>I have saved these credentials.</span>
    </label>

    <div class="row" style="gap:8px; margin-top:4px;">
      <button class="btn primary" disabled={!savedAck} onclick={dismissCreated}>Done</button>
    </div>
  </section>
{/if}

{#if showCreate}
  <div class="card" style="margin-top:16px">
    <h2 style="margin-top:0">New user</h2>
    <div class="field">
      <label for="dn">Display name</label>
      <input
        id="dn"
        placeholder="e.g. backup-bot"
        bind:value={displayName}
        onblur={() => (nameTouched = true)}
        class:invalid={!!nameError}
        aria-invalid={!!nameError}
        aria-describedby={nameError ? "dn-err" : undefined}
        autocomplete="off" />
      {#if nameError}<span id="dn-err" class="field-error" role="alert">{nameError}</span>{/if}
    </div>

    <div class="label-sm" style="margin-top:14px">Access policy</div>
    <p class="muted" style="margin-top:2px">
      Choose what this user's S3 credentials can do. The Builder uses plain choices; switch to Split
      or Code if you prefer to edit the policy JSON directly.
    </p>
    <PermissionBuilder {buckets} {bucketsLoading} onchange={onPolicy} />

    {#if policyInvalid}
      <p class="field-error" role="alert" style="margin-top:10px">
        This policy grants nothing or the JSON is invalid. Fix it above before creating the user.
      </p>
    {/if}

    <div class="row" style="gap:8px; margin-top:14px;">
      <button class="btn primary" onclick={create} disabled={!canCreate}>
        {creating ? "Creating…" : "Create user"}
      </button>
      <button class="btn" onclick={() => (showCreate = false)}>Cancel</button>
    </div>
  </div>
{/if}

{#if loading}
  <div class="table-wrap" style="margin-top:16px" aria-busy="true">
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
        {#each Array(3) as _, i (i)}
          <tr>
            <td><Skeleton lines={1} width="60%" /></td>
            <td><Skeleton lines={1} width="80%" /></td>
            <td><Skeleton lines={1} width="40%" /></td>
            <td></td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{:else if users.length === 0}
  <div class="empty" style="margin-top:16px">
    No users yet. Create one to mint an S3 access key scoped to the buckets and actions you choose.
  </div>
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
              <span class="badge" class:success={u.is_active}
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
  .cred-panel {
    margin-top: 16px;
    background: var(--surface);
    border: 2px solid var(--warning);
    border-radius: var(--r-lg);
    padding: 20px 22px;
    box-shadow: var(--shadow);
  }
  .cred-panel:focus-visible {
    outline: 3px solid var(--ring-color);
    outline-offset: 2px;
  }
  .cred-title {
    margin: 0 0 6px;
    font-size: 1.15rem;
  }
  .cred-desc {
    margin: 0 0 16px;
    color: var(--text-muted);
    font-size: 0.92rem;
    line-height: 1.55;
  }
  .cred-advanced {
    margin: 6px 0 14px;
  }
  .cred-advanced summary {
    cursor: pointer;
    font-size: 0.88rem;
    color: var(--link);
    font-weight: 500;
    padding: 4px 0;
  }
  .cred-adv-note {
    margin: 8px 0 10px;
    font-size: 0.85rem;
    color: var(--text-muted);
    line-height: 1.5;
  }
  .cred-ack {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 4px 0 14px;
    font-size: 0.92rem;
    cursor: pointer;
    font-weight: 500;
  }
  .cred-ack input {
    width: 16px;
    height: 16px;
  }
</style>
