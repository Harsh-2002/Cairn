<script>
  import { api, setToken, ApiError } from "../lib/api.js";

  let { onauth } = $props();

  let accessKey = $state("");
  let secretKey = $state("");
  let error = $state("");
  let busy = $state(false);
  let showSecret = $state(false);
  let capsOn = $state(false);

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
        error = "That credential works, but it is not an administrator.";
      } else if (err instanceof ApiError && err.status === 401) {
        error = "Access key or secret key is incorrect.";
      } else {
        error = err.message || "Could not sign in.";
      }
    } finally {
      busy = false;
    }
  }

  // Surface a caps-lock warning while typing the secret, since it is masked by default.
  function onSecretKey(e) {
    if (typeof e.getModifierState === "function") {
      capsOn = e.getModifierState("CapsLock");
    }
  }
</script>

<div class="login">
  <form class="login-card" onsubmit={submit}>
    <h1><span class="brand"><span class="dot"></span> Cairn</span></h1>
    <p class="subtitle">Sign in to the management console</p>

    {#if error}
      <div class="notice danger" role="alert">{error}</div>
    {/if}

    <div class="field">
      <label for="ak">Access key</label>
      <input
        id="ak"
        type="text"
        placeholder="Your admin access key"
        bind:value={accessKey}
        autocomplete="username"
        autocapitalize="off"
        autocorrect="off"
        spellcheck="false" />
    </div>

    <div class="field">
      <label for="sk">Secret key</label>
      <div class="secret-row">
        {#if showSecret}
          <input
            id="sk"
            type="text"
            placeholder="Your admin secret key"
            bind:value={secretKey}
            onkeyup={onSecretKey}
            autocomplete="off"
            autocapitalize="off"
            autocorrect="off"
            spellcheck="false" />
        {:else}
          <input
            id="sk"
            type="password"
            placeholder="Your admin secret key"
            bind:value={secretKey}
            onkeyup={onSecretKey}
            autocomplete="current-password" />
        {/if}
        <button
          type="button"
          class="btn reveal"
          aria-pressed={showSecret}
          onclick={() => (showSecret = !showSecret)}>
          {showSecret ? "Hide" : "Show"}
        </button>
      </div>
      {#if capsOn}
        <p class="caps-hint" role="status">Caps Lock is on.</p>
      {/if}
    </div>

    <button class="primary full" type="submit" disabled={busy} aria-busy={busy}>
      {busy ? "Signing in…" : "Sign in"}
    </button>
    <p class="hint">
      Enter your administrator access key and secret key. The same credentials work
      with the S3 API and the CLI. They are stored in this browser only.
    </p>
  </form>
</div>

<style>
  /* Quiet the page background: a flat surface reads as a sign-in screen, not an ad. The shared
     .login glow is overridden here. */
  .login {
    background: var(--bg);
  }
  .secret-row {
    display: flex;
    gap: 8px;
    align-items: stretch;
  }
  .secret-row input {
    flex: 1;
    min-width: 0;
  }
  .secret-row .reveal {
    flex-shrink: 0;
  }
  .caps-hint {
    margin: 6px 0 0;
    font-size: 0.85rem;
    font-weight: 500;
    color: var(--warning-ink);
  }
  .hint {
    margin-top: 1rem;
    font-size: 0.85rem;
    line-height: 1.55;
    color: var(--text-muted);
  }
</style>
