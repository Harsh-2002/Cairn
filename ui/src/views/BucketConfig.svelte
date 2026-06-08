<script>
  import { api, s3, ApiError } from "../lib/api.js";
  import { bytes } from "../lib/format.js";

  let { name } = $props();

  let config = $state(null);
  let error = $state("");
  let notice = $state("");
  let loading = $state(true);

  // Versioning control.
  let versioning = $state("Unversioned");
  let savingVersioning = $state(false);

  // Quota control. The text field holds a byte count; empty means "no limit".
  let quotaInput = $state("");
  let savingQuota = $state(false);

  // Policy editor. `policyText` is the textarea contents; empty means no policy.
  let policyText = $state("");
  let savingPolicy = $state(false);
  let deletingPolicy = $state(false);

  // Compression control ("zstd" | "lz4" | "none"); read from the bucket detail.
  let compression = $state("none");
  let savingCompression = $state(false);

  // Replication rule (via the S3 ?replication subresource).
  let replDest = $state("");
  let replPrefix = $state("");
  let replActive = $state(false);
  let savingRepl = $state(false);

  // Map the server's lowercase versioning string to the capitalized form the
  // PUT /versioning endpoint expects.
  function statusFromState(s) {
    if (s === "enabled") return "Enabled";
    if (s === "suspended") return "Suspended";
    return "Unversioned";
  }

  // Aspects that are surfaced only as a present/absent indicator.
  const aspectKeys = [
    ["cors", "CORS"],
    ["tagging", "Tagging"],
    ["lifecycle", "Lifecycle"],
    ["acl", "ACL"],
    ["public_access_block", "Public access block"],
  ];

  async function load() {
    loading = true;
    error = "";
    try {
      const res = await api.getBucketConfig(name);
      config = res;
      versioning = statusFromState(res.versioning);
      quotaInput =
        res.quota_bytes === null || res.quota_bytes === undefined
          ? ""
          : String(res.quota_bytes);
      policyText = res.policy ? JSON.stringify(res.policy, null, 2) : "";
      try {
        const detail = await api.getBucket(name);
        compression = detail.compression || "none";
      } catch {
        compression = "none";
      }
      try {
        const repl = await s3.getReplication(name);
        replActive = !!repl;
        replDest = repl ? repl.dest_bucket : "";
        replPrefix = repl ? repl.prefix : "";
      } catch {
        replActive = false;
      }
    } catch (err) {
      error = err.message || "Failed to load bucket configuration.";
      config = null;
    } finally {
      loading = false;
    }
  }

  async function saveVersioning() {
    error = "";
    notice = "";
    savingVersioning = true;
    try {
      await api.setVersioning(name, versioning);
      notice = `Versioning set to ${versioning}.`;
      await load();
    } catch (err) {
      error = err.message || "Failed to update versioning.";
    } finally {
      savingVersioning = false;
    }
  }

  async function saveQuota() {
    error = "";
    notice = "";
    const raw = quotaInput.trim();
    let quota = null;
    if (raw !== "") {
      if (!/^\d+$/.test(raw)) {
        error = "Quota must be a whole number of bytes, or empty to clear.";
        return;
      }
      quota = Number(raw);
      if (!Number.isSafeInteger(quota)) {
        error = "Quota is too large.";
        return;
      }
    }
    savingQuota = true;
    try {
      await api.setQuota(name, quota);
      notice = quota === null ? "Quota cleared." : `Quota set to ${quota} bytes.`;
      await load();
    } catch (err) {
      error = err.message || "Failed to update quota.";
    } finally {
      savingQuota = false;
    }
  }

  function clearQuota() {
    quotaInput = "";
    saveQuota();
  }

  async function saveCompression() {
    error = "";
    notice = "";
    savingCompression = true;
    try {
      await api.setCompression(name, compression);
      notice =
        compression === "none"
          ? "Compression disabled."
          : `Compression set to ${compression}.`;
      await load();
    } catch (err) {
      error = err.message || "Failed to update compression.";
    } finally {
      savingCompression = false;
    }
  }

  async function saveReplication() {
    error = "";
    notice = "";
    if (!replDest.trim()) {
      error = "Enter a destination bucket for replication.";
      return;
    }
    savingRepl = true;
    try {
      await s3.putReplication(name, replDest.trim(), replPrefix.trim());
      notice = `Replicating to "${replDest.trim()}".`;
      await load();
    } catch (err) {
      error =
        (err.message || "Failed to set replication.") +
        " (replication requires versioning enabled and a matching CAIRN_REPLICATION target).";
    } finally {
      savingRepl = false;
    }
  }

  async function clearReplication() {
    error = "";
    notice = "";
    savingRepl = true;
    try {
      await s3.deleteReplication(name);
      notice = "Replication rule removed.";
      replDest = "";
      replPrefix = "";
      await load();
    } catch (err) {
      error = err.message || "Failed to clear replication.";
    } finally {
      savingRepl = false;
    }
  }

  async function savePolicy() {
    error = "";
    notice = "";
    const raw = policyText.trim();
    if (raw === "") {
      error = "Policy is empty. Use Delete policy to remove it.";
      return;
    }
    // Validate JSON locally so an obvious typo never reaches the server.
    try {
      JSON.parse(raw);
    } catch (e) {
      error = `Invalid JSON: ${e.message || e}`;
      return;
    }
    savingPolicy = true;
    try {
      await api.setPolicy(name, raw);
      notice = "Policy saved.";
      await load();
    } catch (err) {
      if (err instanceof ApiError && err.status === 400) {
        error = err.message || "The policy was rejected as invalid.";
      } else {
        error = err.message || "Failed to save policy.";
      }
    } finally {
      savingPolicy = false;
    }
  }

  async function deletePolicy() {
    if (!window.confirm(`Delete the policy on "${name}"?`)) return;
    error = "";
    notice = "";
    deletingPolicy = true;
    try {
      await api.deletePolicy(name);
      notice = "Policy deleted.";
      policyText = "";
      await load();
    } catch (err) {
      error = err.message || "Failed to delete policy.";
    } finally {
      deletingPolicy = false;
    }
  }

  load();
</script>

<h2>Configuration</h2>

{#if error}
  <div class="notice error">{error}</div>
{/if}
{#if notice}
  <div class="notice success">{notice}</div>
{/if}

{#if loading}
  <p class="muted">Loading configuration…</p>
{:else if config}
  <div class="panel">
    <div class="config-grid">
      <div>
        <div class="muted label-sm">Ownership mode</div>
        <div>{config.ownership_mode}</div>
      </div>
      {#each aspectKeys as [key, label] (key)}
        <div>
          <div class="muted label-sm">{label}</div>
          <div>
            {#if config[key]}
              <span class="badge ok">present</span>
            {:else}
              <span class="badge off">none</span>
            {/if}
          </div>
        </div>
      {/each}
    </div>
  </div>

  <div class="panel">
    <div class="muted label-sm">Versioning</div>
    <form class="row" onsubmit={(e) => {
      e.preventDefault();
      saveVersioning();
    }}>
      <select bind:value={versioning} aria-label="Versioning state">
        <option value="Enabled">Enabled</option>
        <option value="Suspended">Suspended</option>
        <option value="Unversioned">Unversioned</option>
      </select>
      <button class="primary" type="submit" disabled={savingVersioning}>
        {savingVersioning ? "Saving…" : "Apply"}
      </button>
    </form>
  </div>

  <div class="panel">
    <div class="muted label-sm">
      Quota (bytes) — leave empty for no limit{config.quota_bytes != null
        ? ` · current: ${bytes(config.quota_bytes)}`
        : ""}
    </div>
    <form class="row" onsubmit={(e) => {
      e.preventDefault();
      saveQuota();
    }}>
      <input
        placeholder="no limit"
        bind:value={quotaInput}
        inputmode="numeric"
        autocomplete="off"
        aria-label="Quota bytes"
      />
      <button class="primary" type="submit" disabled={savingQuota}>
        {savingQuota ? "Saving…" : "Set quota"}
      </button>
      <button type="button" onclick={clearQuota} disabled={savingQuota}>
        Clear quota
      </button>
    </form>
  </div>

  <div class="panel">
    <div class="muted label-sm">Compression (applied to new uploads)</div>
    <form
      class="row"
      onsubmit={(e) => {
        e.preventDefault();
        saveCompression();
      }}
    >
      <select bind:value={compression} aria-label="Compression algorithm">
        <option value="zstd">Zstandard (zstd)</option>
        <option value="lz4">LZ4</option>
        <option value="none">Off</option>
      </select>
      <button class="primary" type="submit" disabled={savingCompression}>
        {savingCompression ? "Saving…" : "Apply"}
      </button>
    </form>
  </div>

  <div class="panel">
    <div class="muted label-sm">
      Replication {replActive ? "· active" : "· not configured"}
    </div>
    <form
      class="row"
      style="flex-wrap:wrap;"
      onsubmit={(e) => {
        e.preventDefault();
        saveReplication();
      }}
    >
      <input
        placeholder="destination bucket"
        bind:value={replDest}
        autocomplete="off"
        aria-label="Replication destination bucket"
        style="max-width:220px;"
      />
      <input
        placeholder="prefix (optional)"
        bind:value={replPrefix}
        autocomplete="off"
        aria-label="Replication prefix"
        style="max-width:180px;"
      />
      <button class="primary" type="submit" disabled={savingRepl}>
        {savingRepl ? "Saving…" : "Apply"}
      </button>
      {#if replActive}
        <button type="button" onclick={clearReplication} disabled={savingRepl}>
          Remove
        </button>
      {/if}
    </form>
  </div>

  <div class="panel">
    <div class="muted label-sm">Bucket policy (raw JSON)</div>
    <textarea
      class="policy-editor mono"
      bind:value={policyText}
      spellcheck="false"
      placeholder="No policy set. Paste a policy JSON document to attach one."
      aria-label="Bucket policy JSON"
    ></textarea>
    <div class="row" style="margin-top:0.6rem;">
      <button class="primary" onclick={savePolicy} disabled={savingPolicy}>
        {savingPolicy ? "Saving…" : "Save policy"}
      </button>
      <button
        class="danger"
        onclick={deletePolicy}
        disabled={deletingPolicy || !config.policy}
      >
        {deletingPolicy ? "Deleting…" : "Delete policy"}
      </button>
    </div>
  </div>
{/if}

<style>
  .config-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 1rem;
  }
  .label-sm {
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 0.3rem;
  }
  .policy-editor {
    width: 100%;
    min-height: 180px;
    resize: vertical;
    background: var(--bg);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.6rem 0.8rem;
    font-size: 0.85rem;
    line-height: 1.45;
  }
  .policy-editor:focus {
    outline: none;
    border-color: var(--accent);
  }
</style>
