<script>
  import { api } from "../lib/api.js";

  let users = $state([]);
  let error = $state("");
  let loading = $state(true);

  let displayName = $state("");
  let role = $state("member");
  let creating = $state(false);
  let notice = $state("");

  // The id of the user a per-row action is currently in flight for (disables
  // that row's buttons without blocking the whole table).
  let busyId = $state(null);

  // The one-time bearer secret returned on creation or rotation. Shown once,
  // then the server only retains a hash (ARCH §23.4).
  let created = $state(null);

  async function load() {
    loading = true;
    error = "";
    try {
      const res = await api.listUsers();
      users = (res && res.users) || [];
    } catch (err) {
      error = err.message || "Failed to load users.";
    } finally {
      loading = false;
    }
  }

  async function create(e) {
    e.preventDefault();
    error = "";
    notice = "";
    const dn = displayName.trim();
    if (!dn) {
      error = "Display name is required.";
      return;
    }
    creating = true;
    try {
      const res = await api.createUser(dn, role);
      // Assemble the full Bearer token the user will sign in with.
      const token =
        res.bearer_access_key_id && res.bearer_secret
          ? `${res.bearer_access_key_id}.${res.bearer_secret}`
          : null;
      created = {
        id: res.id,
        access_key_id: res.bearer_access_key_id,
        secret: res.bearer_secret,
        token,
      };
      displayName = "";
      role = "member";
      await load();
    } catch (err) {
      error = err.message || "Failed to create user.";
    } finally {
      creating = false;
    }
  }

  async function setActive(u, active) {
    error = "";
    notice = "";
    busyId = u.id;
    try {
      await api.patchUser(u.id, { is_active: active });
      notice = `${u.display_name} ${active ? "reactivated" : "deactivated"}.`;
      await load();
    } catch (err) {
      error = err.message || "Failed to update user.";
    } finally {
      busyId = null;
    }
  }

  async function setRole(u, newRole) {
    if (newRole === u.role) return;
    error = "";
    notice = "";
    busyId = u.id;
    try {
      await api.patchUser(u.id, { role: newRole });
      notice = `${u.display_name} is now ${newRole}.`;
      await load();
    } catch (err) {
      error = err.message || "Failed to change role.";
    } finally {
      busyId = null;
    }
  }

  async function rotate(u) {
    if (
      !window.confirm(
        `Rotate credentials for "${u.display_name}"? The current secret stops working immediately.`,
      )
    )
      return;
    error = "";
    notice = "";
    busyId = u.id;
    try {
      const res = await api.rotateCredentials(u.id);
      const token =
        res.bearer_access_key_id && res.bearer_secret
          ? `${res.bearer_access_key_id}.${res.bearer_secret}`
          : null;
      created = {
        id: u.id,
        access_key_id: res.bearer_access_key_id,
        secret: res.bearer_secret,
        token,
      };
    } catch (err) {
      error = err.message || "Failed to rotate credentials.";
    } finally {
      busyId = null;
    }
  }

  load();
</script>

<h1>Users</h1>
<p class="subtitle">Manage administrators and members.</p>

{#if error}
  <div class="notice error">{error}</div>
{/if}
{#if notice}
  <div class="notice success">{notice}</div>
{/if}

{#if created}
  <div class="notice warn">
    <strong>Save these credentials now — the secret is shown only once.</strong>
    <div style="margin-top:0.5rem;">
      <div class="muted">User ID</div>
      <div class="secret-box">{created.id}</div>
      <div class="muted">Access key ID</div>
      <div class="secret-box">{created.access_key_id}</div>
      <div class="muted">Secret</div>
      <div class="secret-box">{created.secret}</div>
      {#if created.token}
        <div class="muted">Bearer token (use this to sign in)</div>
        <div class="secret-box">{created.token}</div>
      {/if}
    </div>
    <button
      style="margin-top:0.6rem;"
      onclick={() => {
        created = null;
      }}>Dismiss</button
    >
  </div>
{/if}

<div class="panel">
  <form class="row" onsubmit={create}>
    <input
      placeholder="Display name"
      bind:value={displayName}
      autocomplete="off"
    />
    <select bind:value={role}>
      <option value="member">member</option>
      <option value="administrator">administrator</option>
    </select>
    <button class="primary" type="submit" disabled={creating}>
      {creating ? "Creating…" : "Create user"}
    </button>
  </form>
</div>

{#if loading}
  <p class="muted">Loading…</p>
{:else if users.length === 0}
  <div class="empty">No users yet.</div>
{:else}
  <table>
    <thead>
      <tr>
        <th>Display name</th>
        <th>ID</th>
        <th>Access key</th>
        <th>Role</th>
        <th>Status</th>
        <th style="text-align:right;">Actions</th>
      </tr>
    </thead>
    <tbody>
      {#each users as u (u.id)}
        <tr>
          <td>{u.display_name}</td>
          <td class="mono">{u.id}</td>
          <td class="mono">{u.access_key_id}</td>
          <td>
            <span class="badge {u.role === 'administrator' ? 'admin' : ''}"
              >{u.role}</span
            >
          </td>
          <td>
            {#if u.is_active}
              <span class="badge ok">active</span>
            {:else}
              <span class="badge off">inactive</span>
            {/if}
          </td>
          <td class="actions">
            <select
              value={u.role}
              disabled={busyId === u.id}
              onchange={(e) => setRole(u, e.currentTarget.value)}
              aria-label="Change role"
            >
              <option value="member">member</option>
              <option value="administrator">administrator</option>
            </select>
            {#if u.is_active}
              <button
                disabled={busyId === u.id}
                onclick={() => setActive(u, false)}>Deactivate</button
              >
            {:else}
              <button
                disabled={busyId === u.id}
                onclick={() => setActive(u, true)}>Reactivate</button
              >
            {/if}
            <button disabled={busyId === u.id} onclick={() => rotate(u)}
              >Rotate</button
            >
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

<style>
  .actions {
    text-align: right;
    white-space: nowrap;
  }
  .actions select,
  .actions button {
    margin-left: 0.4rem;
  }
</style>
