<script>
  import { api, s3, ApiError } from "../lib/api.js";
  import { bytes } from "../lib/format.js";
  import { navigate } from "../lib/router.js";
  import { validate, pretty } from "../lib/policy.js";
  import { ok, err } from "../lib/toast.js";
  import ConfirmDialog from "../components/ConfirmDialog.svelte";
  import Skeleton from "../components/Skeleton.svelte";

  let { name } = $props();

  let config = $state(null);
  let error = $state("");
  let loading = $state(true);

  // Versioning control.
  let versioning = $state("Unversioned");
  let savingVersioning = $state(false);

  // Quota control. The text field holds a byte count; empty means "no limit".
  let quotaInput = $state("");
  let quotaError = $state("");
  let savingQuota = $state(false);

  // Policy editor. `policyText` is the textarea contents; empty means no policy.
  let policyText = $state("");
  let policyError = $state("");
  let savingPolicy = $state(false);
  let confirmDeletePolicy = $state(false);
  let deletingPolicy = $state(false);

  // Compression control ("zstd" | "lz4" | "none"); read from the bucket detail.
  let compression = $state("none");
  let savingCompression = $state(false);

  // Replication rule (via the S3 ?replication subresource).
  let replDest = $state("");
  let replPrefix = $state("");
  let replActive = $state(false);
  let replError = $state("");
  let savingRepl = $state(false);

  // Live JSON validity for the policy editor (inline, before save).
  let policyValid = $derived(
    policyText.trim() === "" ? null : validate(policyText).ok,
  );

  const EXAMPLE_POLICY = pretty({
    Version: "2012-10-17",
    Statement: [
      {
        Sid: "AllowPublicRead",
        Effect: "Allow",
        Principal: "*",
        Action: ["s3:GetObject"],
        Resource: ["arn:aws:s3:::BUCKET/*"],
      },
    ],
  });

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
    } catch (e) {
      error = e.message || "Failed to load bucket configuration.";
      config = null;
    } finally {
      loading = false;
    }
  }

  async function saveVersioning() {
    savingVersioning = true;
    try {
      await api.setVersioning(name, versioning);
      ok(`Versioning set to ${versioning.toLowerCase()}.`);
      await load();
    } catch (e) {
      err(e.message || "Failed to update versioning.");
    } finally {
      savingVersioning = false;
    }
  }

  async function saveQuota() {
    quotaError = "";
    const raw = quotaInput.trim();
    let quota = null;
    if (raw !== "") {
      if (!/^\d+$/.test(raw)) {
        quotaError = "Enter a whole number of bytes, or leave empty for no limit.";
        return;
      }
      quota = Number(raw);
      if (!Number.isSafeInteger(quota)) {
        quotaError = "That number is too large.";
        return;
      }
    }
    savingQuota = true;
    try {
      await api.setQuota(name, quota);
      ok(quota === null ? "Quota cleared." : `Quota set to ${bytes(quota)}.`);
      await load();
    } catch (e) {
      err(e.message || "Failed to update quota.");
    } finally {
      savingQuota = false;
    }
  }

  function clearQuota() {
    quotaInput = "";
    quotaError = "";
    saveQuota();
  }

  async function saveCompression() {
    savingCompression = true;
    try {
      await api.setCompression(name, compression);
      ok(
        compression === "none"
          ? "Compression turned off."
          : `Compression set to ${compression}.`,
      );
      await load();
    } catch (e) {
      err(e.message || "Failed to update compression.");
    } finally {
      savingCompression = false;
    }
  }

  async function saveReplication() {
    replError = "";
    if (!replDest.trim()) {
      replError = "Enter a destination bucket to replicate into.";
      return;
    }
    savingRepl = true;
    try {
      await s3.putReplication(name, replDest.trim(), replPrefix.trim());
      ok(`Replicating to "${replDest.trim()}".`);
      await load();
    } catch (e) {
      replError =
        (e.message || "Failed to set replication.") +
        " Replication needs versioning enabled and a matching destination configured on the server.";
    } finally {
      savingRepl = false;
    }
  }

  async function clearReplication() {
    replError = "";
    savingRepl = true;
    try {
      await s3.deleteReplication(name);
      ok("Replication rule removed.");
      replDest = "";
      replPrefix = "";
      await load();
    } catch (e) {
      err(e.message || "Failed to clear replication.");
    } finally {
      savingRepl = false;
    }
  }

  async function savePolicy() {
    policyError = "";
    const raw = policyText.trim();
    if (raw === "") {
      policyError = "The policy is empty. Use Delete policy to remove it.";
      return;
    }
    const v = validate(raw);
    if (!v.ok) {
      policyError = v.error;
      return;
    }
    savingPolicy = true;
    try {
      await api.setPolicy(name, raw);
      ok("Policy saved.");
      await load();
    } catch (e) {
      if (e instanceof ApiError && e.status === 400) {
        policyError = e.message || "The server rejected this policy as invalid.";
      } else {
        err(e.message || "Failed to save policy.");
      }
    } finally {
      savingPolicy = false;
    }
  }

  function insertExample() {
    policyText = EXAMPLE_POLICY.replace(/BUCKET/g, name);
    policyError = "";
  }

  async function doDeletePolicy() {
    deletingPolicy = true;
    try {
      await api.deletePolicy(name);
      ok("Policy deleted.");
      policyText = "";
      confirmDeletePolicy = false;
      await load();
    } catch (e) {
      err(e.message || "Failed to delete policy.");
    } finally {
      deletingPolicy = false;
    }
  }

  load();
</script>

<h2>Settings</h2>

{#if error}
  <div class="notice danger" role="alert">{error}</div>
{/if}

{#if loading}
  <div class="panel"><Skeleton lines={4} /></div>
  <div class="panel"><Skeleton lines={4} /></div>
  <div class="panel"><Skeleton lines={3} /></div>
{:else if config}
  <!-- ============================ DATA ============================ -->
  <section class="group" aria-labelledby="group-data">
    <h3 id="group-data" class="group-title">Data</h3>
    <p class="group-desc">How objects in this bucket are kept and stored.</p>

    <div class="panel">
      <div class="control">
        <div class="control-text">
          <div class="label">Versioning</div>
          <p class="control-desc">
            Keep previous versions of an object when it is overwritten or deleted, so you
            can recover them later.
          </p>
        </div>
        <form
          class="row control-input"
          onsubmit={(e) => {
            e.preventDefault();
            saveVersioning();
          }}
        >
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

      <div class="control">
        <div class="control-text">
          <div class="label">Storage quota</div>
          <p class="control-desc">
            Cap how much this bucket can store, in bytes. Leave empty for no limit.{config.quota_bytes !=
            null
              ? ` Current limit: ${bytes(config.quota_bytes)}.`
              : ""}
          </p>
        </div>
        <form
          class="control-input"
          onsubmit={(e) => {
            e.preventDefault();
            saveQuota();
          }}
        >
          <div class="row">
            <input
              placeholder="No limit"
              bind:value={quotaInput}
              oninput={() => (quotaError = "")}
              inputmode="numeric"
              autocomplete="off"
              aria-label="Quota in bytes"
              aria-invalid={quotaError ? "true" : undefined}
              aria-describedby={quotaError ? "quota-error" : undefined}
            />
            <button class="primary" type="submit" disabled={savingQuota}>
              {savingQuota ? "Saving…" : "Set quota"}
            </button>
            <button type="button" onclick={clearQuota} disabled={savingQuota}>
              Clear
            </button>
          </div>
          {#if quotaError}
            <span id="quota-error" class="field-error" role="alert">{quotaError}</span>
          {/if}
        </form>
      </div>

      <div class="control no-divider">
        <div class="control-text">
          <div class="label">Compression</div>
          <p class="control-desc">
            Compress new uploads at rest to save space. Existing objects are not changed.
          </p>
        </div>
        <form
          class="row control-input"
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
    </div>
  </section>

  <!-- ========================= PROTECTION ========================= -->
  <section class="group" aria-labelledby="group-protection">
    <h3 id="group-protection" class="group-title">Protection</h3>
    <p class="group-desc">Copies and encryption that guard against data loss.</p>

    <div class="panel">
      <div class="control">
        <div class="control-text">
          <div class="label">
            Replication
            {#if replActive}<span class="badge ok">active</span>{:else}<span class="badge off">off</span>{/if}
          </div>
          <p class="control-desc">
            Continuously copy new objects to another bucket. Needs versioning enabled and
            a matching destination configured on the server.
          </p>
        </div>
        <form
          class="control-input"
          onsubmit={(e) => {
            e.preventDefault();
            saveReplication();
          }}
        >
          <div class="row repl-row">
            <input
              placeholder="Destination bucket"
              bind:value={replDest}
              oninput={() => (replError = "")}
              autocomplete="off"
              aria-label="Replication destination bucket"
              aria-invalid={replError ? "true" : undefined}
              class="repl-dest"
            />
            <input
              placeholder="Prefix (optional)"
              bind:value={replPrefix}
              autocomplete="off"
              aria-label="Replication prefix"
              class="repl-prefix"
            />
            <button class="primary" type="submit" disabled={savingRepl}>
              {savingRepl ? "Saving…" : "Apply"}
            </button>
            {#if replActive}
              <button type="button" onclick={clearReplication} disabled={savingRepl}>
                Remove
              </button>
            {/if}
          </div>
          {#if replError}
            <span class="field-error" role="alert">{replError}</span>
          {/if}
        </form>
      </div>

      <div class="control no-divider">
        <div class="control-text">
          <div class="label">Default encryption</div>
          <p class="control-desc">
            New uploads are encrypted at rest with a server-managed key (SSE-S3). Set this
            per file from the Browser tab using the Encrypt new uploads toggle.
          </p>
        </div>
        <div class="control-input">
          <span class="badge off">Per upload</span>
        </div>
      </div>
    </div>
  </section>

  <!-- =========================== ACCESS =========================== -->
  <section class="group" aria-labelledby="group-access">
    <h3 id="group-access" class="group-title">Access</h3>
    <p class="group-desc">Who is allowed to do what with this bucket.</p>

    <div class="panel">
      <div class="control no-divider policy-control">
        <div class="control-text">
          <div class="label">Bucket policy</div>
          <p class="control-desc">
            A bucket policy is a JSON document that grants or denies access to this bucket
            and its objects. If you are not comfortable writing the JSON by hand, build the
            grant visually on a user instead: the
            <a
              href="#/users"
              onclick={(e) => {
                e.preventDefault();
                navigate("/users");
              }}>Users</a
            >
            page has a visual permission builder that writes the policy for you.
          </p>
        </div>

        <div class="policy-editor-wrap">
          <div class="policy-toolbar">
            <button type="button" class="sm" onclick={insertExample}>
              Insert example
            </button>
            <span class="spacer"></span>
            {#if policyValid === true}
              <span class="policy-status valid">Valid JSON</span>
            {:else if policyValid === false}
              <span class="policy-status invalid">Check the JSON below</span>
            {/if}
          </div>
          <textarea
            class="policy-editor mono"
            bind:value={policyText}
            oninput={() => (policyError = "")}
            spellcheck="false"
            placeholder={`No policy set. Paste a policy document, or use "Insert example" to start from a template.`}
            aria-label="Bucket policy JSON"
            aria-invalid={policyValid === false || policyError ? "true" : undefined}
            aria-describedby={policyError ? "policy-error" : undefined}
            class:invalid={policyValid === false || !!policyError}
          ></textarea>
          {#if policyError}
            <span id="policy-error" class="field-error" role="alert">{policyError}</span>
          {/if}
          <div class="row policy-actions">
            <button class="primary" onclick={savePolicy} disabled={savingPolicy}>
              {savingPolicy ? "Saving…" : "Save policy"}
            </button>
            <button
              class="danger"
              onclick={() => (confirmDeletePolicy = true)}
              disabled={deletingPolicy || !config.policy}
            >
              Delete policy
            </button>
          </div>
        </div>
      </div>
    </div>

    <details class="panel aspects">
      <summary>Other S3 aspects on this bucket</summary>
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
    </details>
  </section>
{/if}

<ConfirmDialog
  open={confirmDeletePolicy}
  danger
  title="Delete bucket policy"
  body={`This removes the access policy on "${name}". Access falls back to the default rules until you set a new policy.`}
  confirmLabel={deletingPolicy ? "Deleting…" : "Delete policy"}
  cancelLabel="Keep policy"
  onconfirm={doDeletePolicy}
  oncancel={() => (confirmDeletePolicy = false)}
/>

<style>
  .group {
    margin-bottom: 26px;
  }
  .group-title {
    margin: 0 0 2px;
    font-size: 1.1rem;
  }
  .group-desc {
    margin: 0 0 12px;
    color: var(--text-muted);
    font-size: 0.92rem;
    line-height: 1.5;
  }

  /* A control row: descriptive text on the left, the input on the right, divided by a hairline. */
  .control {
    display: grid;
    grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
    gap: 18px;
    align-items: start;
    padding-bottom: 18px;
    margin-bottom: 18px;
    border-bottom: 1px solid var(--border);
  }
  .control.no-divider {
    padding-bottom: 0;
    margin-bottom: 0;
    border-bottom: none;
  }
  .control-text .label {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 4px;
    color: var(--text);
  }
  .control-desc {
    margin: 0;
    color: var(--text-muted);
    font-size: 0.88rem;
    line-height: 1.5;
  }
  .control-input {
    min-width: 0;
  }
  .control-input .row {
    flex-wrap: wrap;
  }
  .repl-row {
    align-items: center;
  }
  .repl-dest {
    flex: 1 1 160px;
    min-width: 0;
  }
  .repl-prefix {
    flex: 1 1 130px;
    min-width: 0;
  }

  /* The policy control spans both columns: the editor needs full width. */
  .policy-control {
    grid-template-columns: 1fr;
  }
  .policy-editor-wrap {
    display: flex;
    flex-direction: column;
  }
  .policy-toolbar {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 6px;
  }
  .policy-toolbar .spacer {
    flex: 1 1 auto;
  }
  .policy-status {
    font-size: 0.82rem;
    font-weight: 550;
  }
  .policy-status.valid {
    color: var(--success-ink);
  }
  .policy-status.invalid {
    color: var(--danger-ink);
  }
  .policy-editor {
    width: 100%;
    min-height: 200px;
    resize: vertical;
    background: var(--surface-2);
    color: var(--text);
    border: 1px solid var(--border-strong);
    border-radius: var(--r-sm);
    padding: 0.6rem 0.8rem;
    font-size: 0.84rem;
    line-height: 1.5;
  }
  .policy-actions {
    margin-top: 0.7rem;
  }

  .aspects > summary {
    cursor: pointer;
    font-weight: 550;
    font-size: 0.92rem;
  }
  .aspects[open] > summary {
    margin-bottom: 14px;
  }
  .config-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 1rem;
  }

  @media (max-width: 560px) {
    .control {
      grid-template-columns: 1fr;
      gap: 10px;
    }
  }
</style>
