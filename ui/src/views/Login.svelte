<script>
  import { api, setToken, ApiError } from "../lib/api.js";

  let { onauth } = $props();

  let value = $state("");
  let error = $state("");
  let busy = $state(false);

  async function submit(e) {
    e.preventDefault();
    error = "";
    const token = value.trim();
    if (!token) {
      error = "Enter an admin Bearer token (cairn_<id>.<secret>).";
      return;
    }
    busy = true;
    // Provisionally set the token, then verify it against an admin-gated
    // endpoint. /overview requires the administrator role, so a 403 here means
    // the token authenticated but is not an admin.
    setToken(token);
    try {
      await api.overview();
      onauth();
    } catch (err) {
      setToken("");
      if (err instanceof ApiError && err.status === 403) {
        error = "That credential is not an administrator.";
      } else if (err instanceof ApiError && err.status === 401) {
        error = "Invalid credential.";
      } else {
        error = err.message || "Could not sign in.";
      }
    } finally {
      busy = false;
    }
  }
</script>

<div class="login">
  <form class="login-card" onsubmit={submit}>
    <h1><span class="brand"><span class="dot"></span> Cairn</span></h1>
    <p class="subtitle">Management console</p>

    {#if error}
      <div class="notice error">{error}</div>
    {/if}

    <div class="field">
      <label for="token">Admin Bearer token</label>
      <input
        id="token"
        type="password"
        placeholder="cairn_&lt;id&gt;.&lt;secret&gt;"
        bind:value
        autocomplete="off"
      />
    </div>

    <button class="primary full" type="submit" disabled={busy}>
      {busy ? "Signing in…" : "Sign in"}
    </button>
    <p class="muted" style="margin-top:1rem;font-size:0.8rem;">
      The token is stored in your browser and sent as the
      <code>Authorization</code> header on every request.
    </p>
  </form>
</div>
