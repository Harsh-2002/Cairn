<script>
  // A user's detail page: identity + S3 access key, active toggle, credential rotation, and the
  // attached identity policy edited via the PermissionBuilder. Created users are S3-API-only.
  import { api } from "../lib/api.js";
  import { navigate } from "../lib/router.js";
  import { ok, err } from "../lib/toast.js";
  import PermissionBuilder from "../components/PermissionBuilder.svelte";
  import CopyField from "../components/CopyField.svelte";
  import ConfirmDialog from "../components/ConfirmDialog.svelte";
  import Skeleton from "../components/Skeleton.svelte";

  let { id } = $props();
  let user = $state(null);
  let buckets = $state([]);
  let loading = $state(true);
  let error = $state("");
  let pendingPolicy = $state(undefined); // doc from the builder; null = invalid JSON
  let saving = $state(false);
  let rotated = $state(null);

  // Which destructive confirmation is open, if any: "deactivate" | "rotate" | "remove-policy".
  let confirming = $state(null);

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
    if (pendingPolicy === null) return err("Fix the policy before saving");
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
    confirming = null;
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

  // Activating is non-destructive, so it runs immediately. Deactivating goes through confirmation.
  async function setActive(next) {
    confirming = null;
    try {
      await api.patchUser(id, { is_active: next });
      ok(next ? "User activated" : "User deactivated");
      await load();
    } catch (e) {
      err(e.message);
    }
  }
  function onToggleActive() {
    if (user.is_active) confirming = "deactivate";
    else setActive(true);
  }

  async function rotate() {
    confirming = null;
    try {
      rotated = await api.rotateCredentials(id);
      ok("New Bearer secret created");
    } catch (e) {
      err(e.message);
    }
  }

  function dismissRotated() {
    rotated = null;
  }

  // When a fresh one-time secret appears, bring it into view and move focus to it so it is not
  // scrolled off unnoticed. (Reduced-motion users get an instant jump, honored by the browser.)
  let secretEl = $state(null);
  $effect(() => {
    if (rotated && secretEl) {
      const reduce = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
      secretEl.scrollIntoView({ block: "center", behavior: reduce ? "auto" : "smooth" });
      secretEl.focus();
    }
  });
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

{#if error}<div class="notice danger" role="alert">{error}</div>{/if}

{#if loading}
  <div class="card">
    <Skeleton lines={2} width="40%" />
    <div style="margin-top:18px">
      <Skeleton lines={1} height="2.4em" />
      <div style="height:10px"></div>
      <Skeleton lines={1} height="2.4em" />
    </div>
  </div>
{:else if user}
  <div class="card">
    <div class="head">
      <div>
        <h2 class="name">{user.display_name}</h2>
        <p class="badges">
          <span class="badge">{user.role}</span>
          <span class="badge" class:success={user.is_active} class:off={!user.is_active}>
            {user.is_active ? "active" : "inactive"}
          </span>
          <span class="scope">Signs in to the S3 API only</span>
        </p>
      </div>
      <div class="actions">
        <button class="btn" onclick={onToggleActive}>
          {user.is_active ? "Deactivate user" : "Activate user"}
        </button>
        <button class="btn" onclick={() => (confirming = "rotate")}>
          Rotate Bearer secret
        </button>
      </div>
    </div>

    <div class="keys">
      <CopyField label="S3 access key id" value={user.sigv4_access_key_id || "—"} />
      <CopyField label="Bearer access key id" value={user.access_key_id} />
      <p class="key-note">
        The <strong>S3 access key</strong> signs requests from S3 tools and SDKs. The
        <strong>Bearer access key</strong> is for the management API and CLI. Both
        identify the same user.
      </p>
    </div>

    {#if rotated}
      <div
        class="secret-reveal"
        role="group"
        aria-label="New Bearer secret"
        tabindex="-1"
        bind:this={secretEl}>
        <div class="secret-head">
          <p class="secret-title">New Bearer secret created</p>
          <button class="btn" type="button" onclick={dismissRotated}>Dismiss</button>
        </div>
        <CopyField label="Bearer secret (shown once)" value={rotated.bearer_secret} secret={true} />
      </div>
    {/if}
  </div>

  <div class="card" style="margin-top:18px">
    <h2 style="margin-top:0">Access policy</h2>
    <p class="muted lead">
      Choose which buckets this user can reach and what they can do in them.
    </p>
    {#key user.id}
      <PermissionBuilder {buckets} initial={user.policy} onchange={onPolicy} />
    {/key}
    <div class="row" style="gap:8px; margin-top:14px;">
      <button class="btn primary" onclick={savePolicy} disabled={saving} aria-busy={saving}>
        {saving ? "Saving…" : "Save policy"}
      </button>
      {#if user.policy}
        <button
          class="btn danger"
          onclick={() => (confirming = "remove-policy")}
          disabled={saving}>Remove policy</button>
      {/if}
    </div>
  </div>
{/if}

<ConfirmDialog
  open={confirming === "deactivate"}
  title="Deactivate this user?"
  body="Deactivating blocks this user's S3 access immediately. You can reactivate them later."
  confirmLabel="Deactivate user"
  cancelLabel="Keep active"
  danger={true}
  onconfirm={() => setActive(false)}
  oncancel={() => (confirming = null)} />

<ConfirmDialog
  open={confirming === "rotate"}
  title="Rotate the Bearer secret?"
  body="Rotating stops the current key working immediately. Anything using the old secret will need the new one. The new secret is shown only once."
  confirmLabel="Rotate secret"
  cancelLabel="Keep current secret"
  danger={true}
  onconfirm={rotate}
  oncancel={() => (confirming = null)} />

<ConfirmDialog
  open={confirming === "remove-policy"}
  title="Remove this policy?"
  body="Removing the policy revokes this user's access to all buckets. They keep their keys but can do nothing until a new policy is attached."
  confirmLabel="Remove policy"
  cancelLabel="Keep policy"
  danger={true}
  onconfirm={detachPolicy}
  oncancel={() => (confirming = null)} />

<style>
  .head {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 16px;
    flex-wrap: wrap;
  }
  .name {
    margin: 0;
  }
  .badges {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px;
    margin: 8px 0 0;
  }
  .scope {
    color: var(--text-muted);
    font-size: 0.9rem;
  }
  .keys {
    margin-top: 16px;
  }
  .key-note {
    margin: 4px 0 0;
    color: var(--text-muted);
    font-size: 0.88rem;
    line-height: 1.55;
  }
  .key-note strong {
    color: var(--text);
    font-weight: 600;
  }
  .lead {
    margin-top: -4px;
  }
  /* The one-time secret gets a framed, self-contained block so it cannot be mistaken for the
     persistent key ids above and carries its own dismiss control. */
  .secret-reveal {
    margin-top: 16px;
    padding: 14px;
    border: 2px solid var(--warning);
    border-radius: var(--r);
    background: var(--warning-tint);
  }
  .secret-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 10px;
  }
  .secret-title {
    margin: 0;
    font-weight: 600;
    color: var(--warning-ink);
  }
</style>
