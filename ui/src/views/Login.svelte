<script>
  import { api, setToken, ApiError } from "../lib/api.js";

  let { onauth } = $props();

  let accessKey = $state("");
  let secretKey = $state("");
  let error = $state("");
  let busy = $state(false);

  async function submit(e) {
    e.preventDefault();
    error = "";
    const id = accessKey.trim();
    const secret = secretKey;
    if (!id || !secret) {
      error = "Enter your access key and secret key.";
      return;
    }
    busy = true;
    // The management API authenticates with a Bearer token of the form
    // `<access-key>.<secret>`. Build it from the two fields, set it
    // provisionally, then verify against an admin-gated endpoint (/overview
    // requires the administrator role, so a 403 means it authenticated but is
    // not an admin).
    setToken(`${id}.${secret}`);
    try {
      await api.overview();
      onauth();
    } catch (err) {
      setToken("");
      if (err instanceof ApiError && err.status === 403) {
        error = "That credential is not an administrator.";
      } else if (err instanceof ApiError && err.status === 401) {
        error = "Invalid access key or secret key.";
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
    <p class="subtitle">Sign in to the management console</p>

    {#if error}
      <div class="notice error">{error}</div>
    {/if}

    <div class="field">
      <label for="ak">Access key</label>
      <input
        id="ak"
        type="text"
        placeholder="access key"
        bind:value={accessKey}
        autocomplete="username"
      />
    </div>

    <div class="field">
      <label for="sk">Secret key</label>
      <input
        id="sk"
        type="password"
        placeholder="secret key"
        bind:value={secretKey}
        autocomplete="current-password"
      />
    </div>

    <button class="primary full" type="submit" disabled={busy}>
      {busy ? "Signing in…" : "Sign in"}
    </button>
    <p class="muted" style="margin-top:1rem;font-size:0.8rem;">
      Use your administrator access key and secret key — the same credentials
      work with the S3 API and the CLI. Credentials are stored in your browser
      only.
    </p>
  </form>
</div>
