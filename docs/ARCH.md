# Cairn — Engineering Architecture

**A single Go binary that publishes a fully static, zero-server blog. Markdown in git is the only source of truth. It ships its own browser-based block editor with a Notion-grade typing experience, and also accepts content from any external editor or Notion. Setup is one thing: a GitHub repository and a scoped token. The pipeline holds no state. Open source by design.**

> **Cairn**: a stack of stones a person assembles by hand to mark a path and to last. It is built deliberately, it holds its shape without anything tending it, and it outlives the person who set it. That is the tool: content you assemble, a site that stands with zero compute behind it, and a git repository that endures as the one durable thing.

---

## Table of Contents

1. [Design Rationale](#1-design-rationale)
2. [Problem Statement](#2-problem-statement)
3. [Design Principles and Invariants](#3-design-principles-and-invariants)
4. [The Two-Plane Architecture](#4-the-two-plane-architecture)
5. [System Overview](#5-system-overview)
6. [Core Architectural Decisions](#6-core-architectural-decisions)
7. [The Repository Provider Abstraction](#7-the-repository-provider-abstraction)
8. [The Auth and Setup Model](#8-the-auth-and-setup-model)
9. [The Built-in Editor](#9-the-built-in-editor)
10. [Markdown as the Canonical Format](#10-markdown-as-the-canonical-format)
11. [Editor Compatibility Architecture](#11-editor-compatibility-architecture)
12. [The Statelessness Model](#12-the-statelessness-model)
13. [The Binary: Modes of Operation](#13-the-binary-modes-of-operation)
14. [Content Ingestion Architecture](#14-content-ingestion-architecture)
15. [Asset Pipeline Architecture](#15-asset-pipeline-architecture)
16. [Rendering and Build Plane](#16-rendering-and-build-plane)
17. [Deploy Architecture](#17-deploy-architecture)
18. [Plugin System Architecture](#18-plugin-system-architecture)
19. [Ghost Migration as a Separate Concern](#19-ghost-migration-as-a-separate-concern)
20. [Concurrency, Idempotency, Failure, and Draft Safety](#20-concurrency-idempotency-failure-and-draft-safety)
21. [Open Source Considerations](#21-open-source-considerations)
22. [Phasing and Roadmap](#22-phasing-and-roadmap)

---

## 1. Design Rationale

Three decisions together shape the architecture. Each, followed honestly, improves the system rather than adding to it.

**A built-in editor is a first-class authoring surface.** A browser-based block editor with a Notion-style typing experience is part of the tool, served by the Svelte admin. This sounds like it threatens the editor-agnostic invariant. It does not, because of one rule made explicit here: markdown remains the canonical persisted format and the built-in editor is a bidirectional view over markdown, never a new content format. A post written in the built-in editor is the identical markdown file you could open in Obsidian or VSCode. The editor is a client of the contract, exactly like Obsidian or the Notion adapter.

**The admin is a real Svelte application, not a convenience daemon.** With a built-in editor the admin becomes the primary authoring surface for browser-based writing. It is compiled to static assets and embedded into the Go binary at build time, so the one-binary invariant still holds: the binary contains the admin UI.

**Auth collapses to a GitHub repository plus a scoped token.** This is the most consequential decision. The binary does not need a local working copy of the repository at all. It reads and writes content through the GitHub API using a fine-grained token scoped to one repository. That single decision lets the whole system be unified behind one abstraction (the Repository Provider) with two implementations, and it makes the connected and browser-based experience genuinely stateless and runnable anywhere, including ephemeral environments.

The hidden statefulness that earlier designs removed (the asset cache, the Notion mapping table) stays removed, and these decisions are introduced in a way that does not reintroduce state.

---

## 2. Problem Statement

A personal technical blog should not require operating infrastructure, and today it does. The current blog runs Ghost CMS, which means a Ghost container, a MySQL container, a reverse proxy, a backup job, and a permanent upgrade obligation, none of which produces writing. For a blog updated a couple of times a month this is a structurally bad trade: a high fixed operational cost servicing a low-frequency activity.

The platform direction makes it worse. Ghost began focused and has accreted newsletters, paid memberships, billing, federation, and a separate analytics stack, each its own running service, and its container tooling now bundles a webserver that fights the existing reverse proxy. The blog needs none of this and gets heavier every year while the actual requirement, publishing articles, has not changed.

There is also no portable artifact. The blog is bound to a running server, its content and schema and theme and configuration live in different places with different backup stories, and there is no single thing that is "the blog." The goal is to invert all of it: the blog is a directory of markdown files in a git repository, publishing is one action, the thing that serves it has no compute and no state, the thing that builds it runs and leaves nothing behind, the author writes in whatever surface they prefer including a good built-in one, and because the problem is common the tool is open source.

---

## 3. Design Principles and Invariants

These are invariants. Every decision is justified against them and any change that would violate one is rejected regardless of convenience.

**Invariant 1: Git is the single source of truth for source.** Everything authoritative — markdown content, frontmatter, templates, configuration, and the *original* bytes of every asset under `content/assets/` — is a file in a git repository. Object storage is the deterministic mirror of binary assets for delivery; it is fully reconstructable from the repository at any past commit by re-running the asset pipeline. Having the repository means having every input needed to rebuild the blog and its asset bucket. The bucket is a CDN cache, not a system of record.

**Invariant 2: Markdown is the canonical content format.** Every post is persisted as markdown with a frontmatter header. No editor, including the built-in one, introduces a competing format. Editors are views over markdown that must load from markdown and save to markdown without loss for the supported feature set.

**Invariant 3: The pipeline is stateless.** The tool holds no durable state. It does not run a database, keep a cache it cannot discard, or remember past runs in a way that changes future ones. Every run derives what it needs from the repository and from deterministic computation. Equal repository state produces byte-identical output.

**Invariant 4: The served blog has zero compute.** The public hits static files on a CDN and assets on object storage. No application server, no database, no runtime.

**Invariant 5: Editor-agnostic by contract.** The system defines a contract (markdown files with frontmatter in a content directory) that every editor satisfies. The built-in editor and the Notion adapter and Obsidian and VSCode are all clients of that one contract. No editor is privileged at the core.

**Invariant 6: Determinism.** Same inputs, same outputs, every time. No gratuitous timestamps, no unstable ordering, no machine-specific paths in output. This is what makes statelessness and idempotency real.

**Invariant 7: One binary.** A single statically linked Go binary, with the Svelte admin embedded in it, is the entire deliverable. No separate services to deploy.

**Invariant 8: Open source from the first commit.** Nothing about one specific blog is in the code; everything specific is configuration with sane defaults. The author's blog is the reference deployment, not a privileged path.

---

## 4. The Two-Plane Architecture

The single most clarifying idea in Cairn is to recognize the system has two planes that meet only at git. Keeping them separate is what makes the whole thing both flexible and stateless.

**The authoring plane is interactive and event-driven.** Its job is to get conforming markdown, with its assets already content-addressed and uploaded, committed into git. The authoring plane is the built-in Svelte editor, or any external editor writing files, or the Notion adapter pulling a page. It is triggered by humans and webhooks. It ends the moment a commit lands in git.

**The build plane is a pure deterministic function.** Its job is to turn the git repository state into a deployed static site. It is triggered by a commit landing in git, runs the same binary's build path, renders deterministically, and deploys. It is not interactive, holds no state, and can be re-run over any commit to reproduce that commit's site byte for byte.

Git is the seam. The authoring plane only ever writes source (markdown, frontmatter, templates, configuration) into git. The build plane only ever reads that source and produces output. Neither plane shares memory or state with the other; their entire interface is a git commit. This is why the system can be stateless end to end: each plane is independently stateless and the thing between them is the source of truth, which is durable by being git.

This separation also resolves the obvious open question. Build output is never committed to the source branch. The source repository holds only source. The build plane produces output and hands it to the deploy strategy. The repository stays clean.

---

## 5. System Overview

```
AUTHORING PLANE (interactive, stateless)
┌─────────────────────────────────────────────────────────────────┐
│                                                                   │
│  Built-in editor          External editors        Notion          │
│  (Svelte admin,           (Obsidian = a folder    (API source)    │
│   Milkdown,               of .md; VSCode; vim;                     │
│   embedded in binary)      Zed; anything)                          │
│        │                        │                     │           │
│        │                        │              ┌──────▼───────┐   │
│        │                        │              │Notion adapter│   │
│        │                        │              │→ conforming  │   │
│        │                        │              │  markdown    │   │
│        │                        │              └──────┬───────┘   │
│        └────────────┬───────────┴────────────────────┘            │
│                     ▼                                              │
│            asset pipeline (content-addressed → object storage)     │
│                     │                                              │
│                     ▼                                              │
│            Repository Provider .commit(markdown+frontmatter)       │
│                     │                                              │
└─────────────────────┼──────────────────────────────────────────────┘
                       ▼
        ┌──────────────────────────────────┐
        │  GIT (single source of truth)     │   ← the only seam
        │  reached via Repository Provider: │
        │   • LocalGit (go-git)            │
        │   • GitHubApi (scoped token)      │
        └────────────────┬─────────────────┘
                          │  commit lands → triggers build plane
┌─────────────────────────┼──────────────────────────────────────────┐
│ BUILD PLANE (pure, deterministic, stateless)                        │
│                          ▼                                          │
│   same binary, build path: render site deterministically            │
│                          │                                          │
│                          ▼                                          │
│                  deploy strategy (default: static host serves it)   │
└─────────────────────────┬───────────────────────────────────────────┘
              ┌────────────┴─────────────┐
              ▼                           ▼
   ┌────────────────────┐   ┌──────────────────────────┐
   │ Static host         │   │ S3-compatible storage     │
   │ (CF Pages / GH      │   │ (R2 / S3 / MinIO)         │
   │  Pages) ZERO compute │   │ content-addressed assets  │
   └────────────────────┘   └──────────────────────────┘
```

---

## 6. Core Architectural Decisions

Each decision lists the options, the tradeoffs against the invariants, and the chosen path, so future changes can argue with the original reasoning rather than guess at it.

### Decision 1: Editor engine is Milkdown, not TipTap

**Options.** (a) TipTap, the editor the requirement named. (b) Milkdown. (c) Lexical or Editor.js.

**Tradeoffs.** All three give a Notion-style typing experience. The deciding factor is Invariant 2: markdown is canonical and the editor must round-trip markdown without loss. TipTap is ProseMirror with its document model being ProseMirror JSON; markdown is a serialization layer bolted on, and complex blocks (callouts, nested structures, code with language hints) are exactly where its markdown round-trip degrades. Editor.js emits its own JSON and is the worst fit. Lexical is fast but markdown serialization is again an add-on and its Svelte story is weak. Milkdown is also ProseMirror underneath, so the typing feel is the same Notion-grade experience, but it is built around remark with markdown as the actual document model, which means round-trip fidelity is its native behavior rather than an afterthought.

**Decision.** Milkdown. The architecture's hardest constraint is markdown round-trip fidelity, and Milkdown is the only candidate whose design centers that constraint. TipTap was the named choice; it is overruled with this reasoning on the table, and the decision is reversible if the supported block set is constrained enough that TipTap's serialization gap stops mattering. The editor library is also deliberately isolated behind the editor module so swapping it is a contained change, not an architectural one.

### Decision 2: Repository access is an abstraction with two implementations

**Options.** (a) Local git only (go-git, requires a working copy). (b) GitHub API only (scoped token, no working copy). (c) A Repository Provider trait with both as implementations.

**Tradeoffs.** Local-only forces every user to maintain a clone and excludes browser-based and ephemeral operation, which is incompatible with the built-in-editor experience. API-only excludes the terminal author who works against a local clone with their own editor and would lose the offline path. A provider abstraction costs one indirection and a well-defined interface, and in return the entire pipeline becomes independent of how git is reached. CLI mode against a local clone and connected mode against the GitHub API become the same pipeline over two providers.

**Decision.** A Repository Provider abstraction. This is the central unifying decision. Section 7 specifies it. It is what makes "one binary, many modes" true at the architecture level rather than by duplicated code.

### Decision 3: Connected-mode commits dispatch by file-change cardinality — Contents API for one-file drafts, Git Data API for everything atomic

**Options.** (a) Contents API everywhere: simple, one file per call, requires the prior blob SHA, awkward for multi-file atomic changes, soft size limits. (b) Git Data API everywhere: create blobs, assemble a tree, create a commit object, move the ref, atomic across many files and large content; four calls minimum even for a one-line edit. (c) A single trait whose implementation dispatches by file-change cardinality and intent.

**Tradeoffs.** Git Data API is the only correct shape for a publish — post markdown, generated redirects, and any frontmatter-driven aux files must commit atomically or partial publishes happen. But Section 20 also calls for autosaves as frequent draft commits, and those are single-file by definition. Forcing the Git Data API path on a 10-second-interval autosave is four GitHub API calls per save when one would suffice, against a 5000/hour personal rate limit shared with image uploads and Notion sync. The dispatch is fixed by the call site through a `CommitHint`: `CommitHint::Publish` (atomic, multi-file, Git Data API) or `CommitHint::Draft` (single-file, Contents API), with the Repository Provider trait hiding the choice behind one method.

**Decision.** A single `commit(changes, message, hint, expected_head)` operation on the Repository Provider, dispatching internally. Contents API for `CommitHint::Draft` on a one-file change; Git Data API for everything else and unconditionally when the change set has more than one file. Atomicity is preserved exactly where Section 20 needs it (publish), and quota is preserved exactly where Section 20 stresses it (autosave).

### Decision 4: Markdown is canonical; the built-in editor is a bidirectional view

**Options.** (a) The built-in editor persists its own structured format and markdown is an export. (b) Markdown is the only persisted format and the editor parses it on open and serializes it on save.

**Tradeoffs.** Option (a) gives the editor maximum expressive freedom but creates a second source of truth, breaks Invariant 2, and means a post written in the editor cannot be edited in Obsidian without a lossy conversion. Option (b) constrains the editor to what markdown can represent but keeps exactly one format, keeps every editor interchangeable, and keeps the contract intact.

**Decision.** Markdown is canonical. The editor opens markdown into its view and saves the view back to markdown. The set of blocks the editor offers is deliberately bounded to what serializes cleanly, which is a feature, not a limitation: it guarantees a post is portable across the built-in editor, Obsidian, VSCode, and the Notion adapter without lossy conversions ever happening.

### Decision 5: Content-addressed assets with originals in git, bucket as derived CDN mirror

**Options.** (a) Path-based keys plus a cache of what was uploaded. (b) Keys derived from the hash of the asset bytes, originals only in object storage. (c) Keys derived from the hash, originals also committed under `content/assets/` in the repository with object storage as a derived mirror.

**Tradeoffs.** Path-based keys plus a cache is state outside git, which breaks Invariant 3. Hash-only-in-bucket keeps the URL deterministic but leaves Invariant 1 partly broken: the bucket becomes a system of record for binary content, and restoring the blog requires bucket plus repo plus credentials, not just the repo. Hash-with-originals-in-git pays a one-time write to git (already happening for the post, so essentially free in the same atomic commit) and makes the bucket reconstructable from the repository at any past commit by re-running the asset pipeline. The repository becomes the complete backup it claims to be.

**Decision.** Content-addressed assets with `<sha256-of-original>` as the storage key root, originals committed to git under `content/assets/<sha>.<ext>` in the same atomic publish commit as the markdown that references them, and the object-storage bucket populated by the build plane from those originals. Image variants for responsive delivery are keyed `<sha>/<variant>.<ext>` (variants enumerated in configuration, derived deterministically from the original bytes at build time). When a user drops an image into the built-in editor, it is hashed in the browser, uploaded via a presigned PUT minted by the server (Section 15), the markdown is written with the stable URL, and the original bytes are queued into the next commit. The editor still holds no asset state; the bucket is still idempotent on identical bytes; and now the system can rebuild itself from the git repository alone.

### Decision 6: Per-post state lives in that post's frontmatter

**Options.** (a) A sidecar store mapping posts to Notion pages, publish status, sync history. (b) Every such fact in the post's frontmatter.

**Tradeoffs.** A sidecar is state outside the document; frontmatter is already in git, already per-post, and self-describing.

**Decision.** All per-post state is frontmatter. The Notion page identity, the publish-or-draft flag, the slug: all frontmatter. No sidecar, no database, anywhere in the system.

### Decision 7: Admin-access credential and content credential are separate

**Options.** (a) One token does everything. (b) Distinguish the credential that protects access to the admin UI from the credential that grants write access to the content repository.

**Tradeoffs.** Conflating them means anyone who can open the admin can also act on the repository, and it couples the blast radius of an exposed admin to full repository write. Separating them means the admin can be protected independently and the GitHub token can be rotated independently.

**Decision.** Two distinct credentials, specified in Section 8. The GitHub scoped token is the content credential. A separate admin secret protects the admin surface. They have different lifetimes and different blast radii.

---

## 7. The Repository Provider Abstraction

```
                  +-------------------------------------+
                  |     RepositoryProvider (trait)      |
                  |-------------------------------------|
                  |  read(path, at) -> FileRead         |
                  |  list(prefix, at) -> [TreeEntry]    |
                  |  commit(changes, msg, hint, head)   |
                  |  force_commit_to_branch(branch, …)  |
                  |  force_set_ref(branch, tree, msg)   |
                  |  delete_branch(branch)              |
                  |  resolve_ref(name) -> Option<sha>   |
                  +------------------+------------------+
                                     |
                  +------------------+------------------+
                  |                                     |
          +-------v---------+                 +---------v--------+
          | LocalGitProvider|                 | GitHubApiProvider|
          |   (go-git +       |                 |    (reqwest +    |
          |    spawn_block) |                 |    Git Data /    |
          |                 |                 |     Contents)    |
          +-------+---------+                 +---------+--------+
                  |                                     |
                  v                                     v
         working copy on disk                  https://api.github.com
         (CLI, CI, terminal author)            (server, browser editor,
                                                connected mode)
```



This is the spine of the architecture. Everything that reads or writes the source of truth goes through one interface, and the interface has exactly the operations the pipeline needs and no more: read a file at a path, list the content tree, and commit a set of file changes atomically with a message. Nothing in the pipeline knows or cares how those operations are fulfilled.

There are two implementations.

**The LocalGit provider** operates against a local working copy using go-git. Read and list are filesystem operations; commit stages and commits and pushes to the configured remote. This serves the terminal author who keeps a clone, works offline, and uses their own editor. It is also the natural provider when the binary runs inside CI on a checked-out repository.

**The GitHubApi provider** operates against a remote repository over the GitHub API using a fine-grained scoped token, with no working copy at all. Read and list call the API; commit uses the Git Data API to create blobs, assemble a tree, create a commit object, and move the branch ref atomically. This serves the built-in editor and any browser-based or ephemeral operation, because it needs nothing on local disk and nothing persistent.

The payoff is that CLI mode and connected mode are not two code paths. They are the same pipeline over a different provider chosen by configuration. The built-in editor saving a post and a terminal user running publish exercise identical ingestion, identical content-addressed asset handling, identical rendering rules, differing only in which provider commits. This is what "mature and efficient" means here: the variability is isolated to one well-defined seam and the rest of the system is provider-blind.

The abstraction also keeps statelessness honest. The provider holds no state between calls. The GitHubApi provider in particular means a server can handle a publish without ever touching local disk and without a clone to keep in sync, which removes an entire class of "is the local copy stale" problems that a clone-based design would have to manage.

---

## 8. The Auth and Setup Model

Setup is deliberately minimal because minimal setup was an explicit requirement. To stand up the system a user supplies three things: the target GitHub repository, a GitHub fine-grained personal access token scoped to only that repository with read and write on repository contents, and the credentials for an S3-compatible bucket for assets. That is the entire configuration surface that is not already defaulted. Everything else has sane defaults so the zero-config path produces a working blog.

The GitHub token is the content credential. It is fine-grained and scoped to a single repository and a single permission family on purpose: the blast radius of its compromise is one blog's content repository and nothing else, and it is rotatable independently without touching anything else in the system. It is a secret. It is injected at runtime from the environment or an external secret source, held in memory only for the duration of operations that need it, never written to disk by the tool, never committed, never cached. Removing it from the machine removes no blog state because all blog state is in git.

The token's runtime location is fixed and important: it lives in the Go server process's memory, and the browser never sees it. Every editor write — commits, presigned-URL minting, draft-branch updates, publish — goes through the server, which holds the token, signs the request, and returns only the result. This is what scopes the threat model: an XSS on the admin SPA cannot exfiltrate write access to the repository, because the credential that grants write access is not in the page. "Ephemeral environment" in this document means the process is stateless on disk and freely restartable, not that it is a function-as-a-service execution. The realistic deployment shape for connected mode is a small always-on Go process — a VM, a fly.io machine, a Cloudflare Container — not Lambda or Workers, because the editor wants a stable origin and the server is the only place the GitHub token and the S3 credentials can safely live.

The admin secret is a separate credential and a separate concern. The admin surface, because it exposes the built-in editor and publish actions, must not be open. By default the server binds to localhost so the default posture is already closed. When the operator deliberately exposes it, a distinct admin secret gates access. It is deliberately not the GitHub token: the credential that lets someone open the editor is not the credential that grants write access to the repository, so an exposed or shared admin does not imply repository compromise, and either credential can be rotated without disturbing the other.

S3-compatible credentials are the third secret and follow the same runtime-injection, never-persisted rule. The storage layer is specified as an S3-compatible interface rather than one vendor, so the same setup works with R2, S3, or MinIO; the reference deployment uses R2 for zero egress cost but nothing is R2-specific.

The reason this model is correct: it makes the binary stateless and runnable anywhere, because all it needs is three secrets it is given at runtime and a repository it reaches over an API. There is nothing to provision, nothing persistent to back up that is not already git, and the credential design contains blast radius and supports rotation, which is the mature posture for something that holds write access to your blog.

---

## 9. The Built-in Editor

The built-in editor exists so the author has a Notion-grade writing experience that belongs to the tool and works from a browser without depending on any external application. It is the primary authoring surface for browser-based writing and an equal peer of external editors, never a replacement for the contract.

It is delivered as a Svelte single-page application compiled to static assets and embedded into the Go binary at build time. This preserves the one-binary invariant: there is no separate frontend to deploy, the binary contains the admin and the editor. The server mode of the binary serves these embedded assets and exposes a small local API the SPA talks to; that API is a remote interface over the same stateless pipeline, adding endpoints, not state.

The editor engine is Milkdown, for the reasons in Decision 1: it gives the ProseMirror-based Notion-style typing feel while keeping markdown as its actual document model, which is the only way the round-trip fidelity invariant survives contact with a real editor. The set of blocks the editor offers is intentionally bounded to what markdown represents cleanly, including the constructs a technical blog actually needs: headings, lists, fenced code with language, quotes, tables, images, and the handful of admonition-style blocks expressed as conventional markdown extensions. Bounding the block set is a deliberate design choice that guarantees portability rather than a shortfall.

The editor's lifecycle is strictly markdown-in, markdown-out. Opening a post fetches its markdown through the Repository Provider and parses it into the editor view. Editing manipulates the view. Saving serializes the view back to markdown and commits it through the Repository Provider. Asset handling happens inline: dropping or pasting an image uploads it through the asset pipeline, content-addressed, and the returned stable URL is written directly into the markdown at that point, so the editor never holds asset state and the saved markdown is already self-contained.

Because the editor only ever produces canonical markdown committed to git, a post created in it is indistinguishable downstream from one written in Obsidian or pulled from Notion. That indistinguishability is the proof that adding the editor did not compromise editor-agnosticism; it added one more conforming client of the same contract.

---

## 10. Markdown as the Canonical Format

This invariant deserves its own section because it is what holds the whole design together once a built-in editor exists.

There is exactly one persisted representation of a post: markdown with a frontmatter header, in git. There is no second format, no editor-specific document store, no database row that is the "real" version with markdown as an export. Every authoring surface is a transform to and from this one representation. The built-in editor parses markdown to its view and serializes back. The Notion adapter transforms Notion's content into this representation once, on ingestion. External editors write this representation directly. Rendering consumes this representation.

The consequence is that authoring surfaces are interchangeable at the level of an individual post. A post can be started in the built-in editor, refined in Obsidian, corrected with a quick VSCode edit, and none of those transitions involves a lossy conversion, because there is nothing to convert between; they are all editing the same markdown file. This is the property that makes the contract real rather than nominal.

The discipline this imposes on the built-in editor specifically: it may only offer blocks that survive a markdown round trip without loss, and the supported block set is part of the architecture, not an editor implementation detail. Anything that cannot round-trip cleanly is not offered, because offering it would silently create content that the contract cannot honor. This is exactly why Decision 1 chose an editor whose document model is markdown rather than one where markdown is an export.

**Frontmatter contract.** The format every editor must satisfy is concrete and tight, so that "conforming markdown file" is not a hand-wave. Frontmatter is YAML, delimited by `---`, parsed strictly (unknown keys are warnings, malformed YAML is a hard error). Required fields: `title` (string) and `date` (RFC 3339 timestamp with timezone). Optional fields with defaults: `slug` (string; defaults to a deterministic slugification of `title`), `draft` (bool; default `false`), `tags` (list of strings; default `[]`), `summary` (string; auto-derived from the first paragraph if absent), `redirects_from` (list of path strings, for SEO continuity after migration or slug change), `notion_page_id` (string; carries Notion identity for stateless re-sync), `updated` (RFC 3339; sitemap `lastmod` uses this if present, else `date`). The full schema, including the exact slugification rule and the validation policy, lives in `docs/frontmatter.md` in the repository and is the authoritative version; this section names the contract, the docs file pins it. Validation fails closed: a malformed frontmatter aborts ingestion before any commit or upload.

---

## 11. Editor Compatibility Architecture

The requirement is Notion and Obsidian as first-class experiences, the new built-in editor as first-class, and VSCode or anything else with no friction. The architecture meets all of this with one contract and a thin adapter layer for the single genuine exception.

The universal contract is unchanged: a post is a markdown file with frontmatter in the content directory. Every editor that can save a text file satisfies it.

The built-in editor is first-class by being a conforming client of that contract, per Sections 9 and 10. It reads and writes the same markdown everything else does.

Obsidian is first-class with no adapter, because an Obsidian vault on disk is already a directory of markdown files. It is supported by pointing the vault at the content directory. Its vault-specific syntax is normalized on ingestion so it renders consistently.

VSCode, vim, Zed, and every other text or markdown editor are first-class for the same structural reason: they save markdown files to a folder, which is the contract, so they are supported on day one with zero editor-specific code. This is essential for an open-source tool that must not turn every user's editor into a feature request.

Notion is the one source that cannot satisfy a filesystem contract because it is not a filesystem, and that is precisely what the adapter layer is for. The Notion adapter pulls a page's content and properties and materializes a conforming markdown file with frontmatter, including the Notion page identity for stateless re-sync, after which the rest of the pipeline cannot tell the content came from Notion. The adapter is the only editor-specific module in the system, it is optional, and it sits at the edge. First-class Notion, first-class Obsidian, first-class built-in editor, and universal external-editor support are not four features; they are one contract plus one adapter.

---

## 12. The Statelessness Model

Statelessness is credible only if every candidate piece of state is enumerated and shown to live in git, be deterministically derived, or be injected at runtime and never persisted.

Content and metadata: markdown and frontmatter, in git, authoritative. Templates and styling: in git, authoritative. Configuration: a file in git, authoritative, minus secrets.

Secrets: the GitHub scoped token, the admin secret, the storage credentials. Injected at runtime, held in memory only while needed, never written to disk, never committed, never cached. Removing them removes no blog state.

Asset upload memory: eliminated by content-addressing. The question "already uploaded" is answered by deterministic key derivation, not stored history. The built-in editor's inline upload follows the same rule, so even mid-edit there is no asset state.

Notion sync mapping: eliminated as separate state. It is the page identity in the post's frontmatter, in git.

Publish-versus-draft: a frontmatter flag, in git. The build excludes drafts. No published table.

Last-published version: this is git history. The tool does not track it because git already does it perfectly.

Built-in editor session: while the author types, the unsaved document is browser memory only; nothing server-side holds it. On save it becomes a git commit. The server serving the editor reads posts on demand through the Repository Provider and retains nothing between requests. Section 20 covers the draft-loss consideration this raises and the chosen mitigation, which itself stays git-authoritative.

Repository access: the provider holds no state between calls. The GitHubApi provider in particular needs no local clone, so there is no working copy to go stale and nothing on disk to be authoritative.

Build output: a pure function of repository state and deterministic rendering. Regenerable identically at any time. Never committed to the source branch; handed to the deploy strategy by the build plane.

Server runtime: in any mode the server is a request handler over the stateless pipeline. Kill it at any instant, restart it, lose nothing, because it owned nothing. Multiple instances are interchangeable; their only contention point is the atomic git commit, which git serializes.

The test stands: wipe the binary, every cache, the server, the build output, and all secrets. On a fresh machine, supply the three secrets, point the binary at the repository, run it once. The blog reproduces byte-identically. The design makes that true.

---

## 13. The Binary: Modes of Operation

One binary, with the Svelte admin embedded, operating in modes that are all thin entry points over the same stateless pipeline and the same Repository Provider abstraction.

**CLI one-shot.** Invoked to do one thing and exit, holding nothing across invocations. Publish runs the full authoring-plane pipeline for a source (a file, or a Notion page id which runs the adapter first) and commits through the configured provider. Build runs the build-plane render without deploying, used locally and as the inner step of CI. Preview builds and serves locally for inspection and stops when stopped. New scaffolds a correctly-headed draft so any editor can start immediately. Sync runs only the Notion adapter for a page id. Migrate is the one-time Ghost importer, kept separate per Section 19.

**Server with the built-in editor and admin.** The long-running process serves the embedded Svelte SPA and its small API. This is the browser-based authoring surface. It defaults to localhost so the default posture is closed, and a distinct admin secret gates it when exposed. It holds no state; every action it exposes is a pipeline operation that commits through the Repository Provider, which in this mode is normally the GitHubApi provider so the server needs no local clone.

**Server as webhook receiver.** Listens for inbound webhooks (a Notion automation when a page is marked publishable, a git host event) and runs the same pipeline in response, validating the webhook with a runtime-injected signing secret and retaining nothing afterward. This is the automation path.

**Server as headless API.** Exposes the same pipeline operations as an HTTP API for external tools, scripts, or editor extensions, so anyone can wire their own front end without the core growing editor-specific code.

The server behaviors are config-gated and combinable, so the minimal default (CLI only, or editor only) is small and power is opt-in, which serves both attack-surface reduction and the open-source goal of a trivial default.

**Build-plane invocation in CI.** The same binary's build path, run by a CI workflow when a commit lands, using the LocalGit provider over the checked-out repository. This is the build plane from Section 4: pure, deterministic, stateless, producing output for the deploy strategy. CI configuration is not something the user has to invent: `cairn init` writes a `.github/workflows/cairn.yml` into the source skeleton in the same atomic commit that creates the initial repository, and the binary is also published as a reusable GitHub Action (`uses: anthropics/cairn-action@vN`) so the workflow is a few lines that invoke the Action with the bucket and deploy credentials as repository secrets. The author never installs a Go toolchain in CI; the Action carries a pinned binary. This is what makes the build plane's "triggered by a commit landing in git" claim concrete instead of relying on every user to wire up their own CI.

---

## 14. Content Ingestion Architecture

Ingestion turns a source into a normalized post the rest of the pipeline understands, and it has the same shape regardless of which authoring surface produced the source, because of the canonical-markdown invariant.

There are two source types behind one normalization contract. The filesystem and built-in-editor source is markdown that already conforms: frontmatter is parsed and validated, relative asset references are resolved against the post's location so assets kept beside a draft are found. The Notion adapter source calls Notion's API. A relevant external fact shapes this: in early 2026 Notion shipped a Markdown Content API (`GET /v1/pages/{id}/markdown`) that returns a page's full content as a single Notion-flavored markdown string rather than requiring recursive block-tree traversal, so the primary adapter path is closer to fetch-and-clean than reconstruct-a-document. There is a caveat that affects design: that endpoint is restricted to public integrations, so a workspace running an internal bot only has access to the older blocks-based API. The adapter therefore has two implementations behind one trait — a Markdown Content API path (preferred, used after the operator completes a one-time OAuth dance via `cairn auth notion`) and a blocks fallback that traverses the block tree and serializes to canonical markdown (used when only an internal-integration token is available). Either way, the adapter writes a conforming markdown file with constructed frontmatter including `notion_page_id` for stateless re-sync, after which it is indistinguishable from any other source.

Normalization is the single contract every ingested post is forced into before proceeding: a known validated frontmatter schema and a markdown body whose non-standard constructs (Notion's tagged blocks, Obsidian vault syntax, editor quirks) are converted to forms the renderer handles uniformly. Everything downstream operates on this one representation and never needs to know which surface produced it. This is what makes editor-agnosticism real at the pipeline level and not just at the input level.

---

## 15. Asset Pipeline Architecture

```
   browser drops image                  build plane sees a new asset
           |                                          |
           v                                          v
   SHA-256(bytes) ----- the same key for both paths --+
           |
           v
   POST /api/assets/presign  -->  Go server signs PUT
           |                                          |
           |  presigned URL                           |
           v                                          v
   browser PUTs directly to object storage           |
                                                     |
   --- atomic commit lands on `main` ---             |
                                                     v
   originals committed to content/assets/<sha>.<ext> in git
                                                     |
                                                     v
   build plane re-derives variants (<sha>/<width>w.<ext>),
   mirrors original to bucket, emits <picture srcset>
                                                     |
                                                     v
                 Cache-Control: public, max-age=31536000, immutable
                              (CDN holds them forever)
```



The asset pipeline makes a post's images, video, audio, and PDFs servable from object storage without the author managing uploads, statelessly, by content-addressing.

Conceptually: scan the normalized post for asset references, hash each referenced asset's bytes, derive the storage key from the hash, write to object storage at that key, and rewrite every reference in the body to the resulting stable public URL before rendering. Because the key is the content's fingerprint, writes are idempotent, identical assets across the whole blog deduplicate with no bookkeeping, and the produced HTML points at object storage so the served page never depends on the pipeline being present.

The built-in editor uses the same path at write time rather than publish time: a dropped or pasted asset is hashed in the browser, the SHA-256 is sent to the server's presign endpoint, the server returns a single-use presigned PUT URL keyed by that hash, the browser uploads directly to object storage, and the stable public URL is written straight into the markdown immediately. The server never holds the bytes in memory and never proxies the upload (browsers handle large blobs better than a Go server should), the S3 credentials never leave the server, and the editor still holds no asset state. The original bytes are also queued into the next git commit alongside the markdown — Decision 5 makes the repository the complete backup — so a published post arrives in git carrying its asset originals under `content/assets/<sha>.<ext>` in the same atomic commit. Notion-sourced images, which Notion serves from expiring signed URLs, are fetched once by the adapter into git originals and from that point are ordinary content-addressed assets, which is why Notion images become permanently self-hosted instead of rotting when the signed URL expires.

Responsive images are served from variants the build plane derives deterministically from each original. The variant scheme is `<sha>/<variant>.<ext>` where the original is `<sha>/original.<ext>` and additional widths (e.g. `<sha>/1600w.webp`, `<sha>/800w.webp`, `<sha>/400w.webp`) are produced by the build plane and uploaded only if not already present. Because the variant set is a pure function of the original bytes plus a config-defined enumeration, every machine produces the same key set from the same originals, the bucket never grows beyond what the repository implies, and the renderer can expand a single markdown reference into a `<picture>` element with srcset at build time. AVIF is deferred; WebP plus the original format is the v1 cut. Objects are written with `Cache-Control: public, max-age=31536000, immutable` because content-addressed keys are immutable by construction; CDNs cache them effectively forever and re-uploads of identical bytes are no-ops.

Garbage collection is an explicit, separate operation rather than part of the publish path. Over years, posts edited to replace images leave the prior originals committed in git history and the prior bucket keys unreferenced from any current post. At blog scale this is a small amount of cold storage and never a correctness problem, but `cairn gc` exists to clean it: scan every post at HEAD, build the live reference set, list bucket keys not in that set whose age exceeds a configurable retention window, and delete them with confirmation. It is never invoked automatically and never on the publish path, because automatic deletion against a deterministic mirror is the wrong shape — the operator chooses when to spend the time.

The properties tie back to invariants: stateless because there is no upload ledger, deterministic because the same bytes always yield the same URL and the same variant set, cheap because duplicates collapse and immutable caching is free, portable because pointing at a different bucket and re-running reproduces the layout from the git originals. Storage is an S3-compatible interface, not a vendor, so the open-source tool works with R2, S3, or MinIO; the reference deployment uses R2 for zero egress.

---

## 16. Rendering and Build Plane

Rendering is the build plane: a pure deterministic function from repository state to deployed site, triggered by a commit and run by the same binary in CI over the LocalGit provider on the checked-out repository.

It parses each post's markdown to a document model, highlights code at build time so the served page needs no client-side highlighting and no JavaScript for it, and injects the document into author-owned HTML templates with inheritance so one base layout owns head, navigation, and footer. It generates the aggregate pages a blog needs: listing, per-tag listings, static pages. The author controls templates and styling completely; there is no theme inheritance from the tool and no framework assumption, which is the direct fix for the Ghost-to-Hugo theme migration failure that motivated avoiding SSG templating.

The SEO surface is produced here, which is the entire point of a static architecture: per post the head metadata, canonical URL, social metadata, and structured-data block; per site the sitemap, the syndication feed, and crawler directives. All generated from frontmatter and configuration, correct by construction, no per-post manual SEO work. Because output is pre-rendered static HTML on a CDN edge, delivery is effectively instant globally, which is a ranking and experience advantage the server-rendered Ghost setup could not match.

Determinism is enforced here specifically: stable ordering, no gratuitous timestamps in output, no machine-specific paths. This is what makes two runs byte-identical, which is what makes idempotent publishing and stateless operation real rather than aspirational.

**Determinism discipline.** Byte-identical output is not free; the renderer has several non-obvious clocks and the architecture forbids each of them explicitly so that any future contributor can check their change against a fixed list rather than rediscovering each leak the hard way. (1) Syntax highlighting via `chroma` is deterministic only when the theme and syntax-definitions versions are pinned; the syntax pack is vendored in the repository and the theme is resolved through configuration with a default that ships with the binary, never the system. (2) Templates have no access to wall-clock time, environment variables, or filesystem mtimes; the template loader's globals expose only repository-derived values, and any user template attempting to call a time function is rejected at render time rather than producing nondeterministic output. (3) The RSS/Atom `lastBuildDate` is not the wall clock — it is `max(post.date)` over all included posts, so two builds of the same commit produce the same feed. (4) The sitemap `lastmod` per URL is `frontmatter.updated` if present, else `frontmatter.date`, never the file's mtime, which differs between machines. (5) All list output (post indexes, tag pages, sitemap, feed) is sorted with `slug` as a stable tertiary tiebreaker after `date` and `title`, so equal-date posts always order identically. (6) Image variant generation is deterministic for the same input bytes and the same configured variant list; the image library and its options are pinned. Together these rules turn Invariant 6 from an aspiration into an audit list.

---

## 17. Deploy Architecture

Deploy is the last step of the build plane and a configurable strategy, not a hardwired step. The default and reference strategy serves the rendered output from a static host with zero compute. Because the source repository holds only source (the build plane never commits output back to it, per Section 4), the build plane hands the rendered `_site/` to the strategy's upload path; the source branch stays clean and no second "deploy" branch is introduced because a deploy branch would itself be state the system has to babysit.

The reference host is Cloudflare Pages via the **Direct Upload API**, not the git-integrated deploy that watches a branch. This is the only resolution consistent with Section 4: the build plane builds, then uploads the output to a Pages project using a separately scoped Cloudflare API token, producing a deployment directly. The git repository is never the deployment artifact. Assets sit in R2 (zero egress), HTML sits in Pages (zero compute), and the steady-state operational cost is effectively zero. Nothing about this is Cloudflare-specific: GitHub Pages is a documented alternative strategy using a `gh-pages` orphan branch as its upload target (it has to use a branch because that is how GH Pages reads its input — accepted as a deploy-strategy quirk, not a source-repository state), and S3-website or any static host is reachable through the plugin seam.

Preview deployments per draft branch are a future concern. The initial cut ships a single-environment deploy (one Pages project, one bucket); the design does not preclude later strategies that map `cairn/drafts/<slug>/<session>` branches to Pages preview environments, but committing to that surface now would underspecify it.

Whatever the strategy, the served result is the same: static files on a dumb host, assets on dumb object storage, no application server, nothing to operate.

---

## 18. Plugin System Architecture

```
  source markdown
        |
        v
  +-------------+   pre-ingest    raw source seen
  | ingest      |  ------------>  modify body / abort
  +------+------+
         |
         v   post-ingest    canonical post + frontmatter
   --------->  link-check, custom front-matter validators
         |
         v
  +-------------+   pre-asset     bytes about to be hashed
  | asset       |  ------------>  optimisers, format converters
  | pipeline    |
  |             |   post-asset    public URL ready
  +------+------+  ------------>  CDN warmers, registries
         |
         v
  +-------------+   pre-render    document model before HTML
  | render      |  ------------>  shortcode expanders
  +------+------+
         |
         v   post-render    final HTML per page
   --------->  link checker, og:image generators
         |
         v
  +-------------+   post-deploy   live URL + deployed commit
  | deploy      |  ------------>  notifications, search indexers
  +-------------+

  Each hook is a directory:  plugins/<hook>/00-name.sh
  Plugins run in filename order, JSON stdin/stdout, 30s timeout.
  Plugin set lives in git — active plugins travel with the blog.
```



Plugins are a later phase but the seam is fixed now so the core is never redesigned for them and so optional concerns like Ghost migration and asset optimization become plugins rather than core.

The model is external process hooks: the git-hooks pattern applied to this pipeline. The pipeline defines seven fixed named lifecycle points: `pre-ingest` (before normalization, sees raw source), `post-ingest` (after normalization, sees canonical post), `pre-asset` (before content-addressing), `post-asset` (after upload, sees mapping from local refs to public URLs), `pre-render` (sees the document model about to render), `post-render` (sees the rendered HTML and aggregate pages), and `post-deploy` (sees the deployment result for notification or syndication). Each point is a directory in the repository under `plugins/<hook-name>/`. Any executable file in that directory is a plugin for that point, run in filename order, receiving a JSON payload on standard input and returning a JSON result on standard output. A plugin can inspect, request modifications expressed in its result, or perform a side effect. Plugins are language-agnostic by construction because the contract is process plus stdin plus stdout; a plugin can be a shell script, a Python program, or a compiled binary. The exact JSON Schema for input and output at each hook lives in `docs/PLUGIN_CONTRACT.md` in the repository and is the authoritative version.

This is the right seam against the invariants. It is stateless: a hook is a process that runs and exits owning nothing, and because plugin directories are in the repository the active plugin set is itself part of the single source of truth and travels with the blog. It is deterministic in ordering regardless of the plugins, and deterministic overall if the plugins are. It is open-source-friendly because anyone extends the tool in any language without touching the Go core, which is the precondition for an ecosystem rather than a fork pile. And it is sufficient: migration, image optimization, link checking, Mermaid diagram rendering, KaTeX math rendering, comments-system integration, alternative deploy strategies, and post-publish notifications are all expressible at the points defined, which is the concrete justification for fixing exactly these points now. The core ships with no plugins; everything in the core is something the core must do, everything optional — including Mermaid block rendering, math rendering, and the Giscus comments template partial — is a hook.

---

## 19. Ghost Migration as a Separate Concern

Migration is not part of the platform. It is a one-time tool living in the same binary as a subcommand for convenience and a strong candidate to become a plugin once the plugin system exists, precisely because the steady-state system should not carry it.

The problem shape: a Ghost content export is one structured file with every post's body as HTML rather than markdown, plus metadata, and it does not contain images, which are still hosted by the running Ghost instance. So migration has three sub-problems: convert each HTML body to the normalized markdown the pipeline expects, reconstruct correct frontmatter from the export metadata, and pull every referenced image from the live Ghost site so it flows through the standard content-addressed asset pipeline and becomes self-hosted.

Two correctness details matter. Ghost's editor emits non-standard card constructs for images, embeds, bookmarks, and galleries, and these must be mapped to standard equivalents during conversion or migrated posts render wrong; this mapping is the bulk of the real work and the reason migration is its own concern rather than a trivial format swap. And URL continuity must be preserved: if old Ghost URLs differ in shape from new output paths, the migrator emits redirect rules so four years of inbound links and search rankings are not broken, which is a real SEO requirement and not optional.

After migration, its output is just content files in the tree, identical in kind to anything written in the built-in editor or pulled from Notion. From that point the migrated blog is indistinguishable from one always native to this system, which is the sign migration was correctly kept at the edge and the core kept clean of it.

---

## 20. Concurrency, Idempotency, Failure, and Draft Safety

Statelessness is only credible if concurrent runs, failures, and the new browser-editing surface all behave well.

**Idempotency.** A publish over unchanged repository state produces byte-identical output by determinism, content-addressed assets are unconditionally safe to rewrite, and an effectively-empty git commit is a no-op. Re-running publish is always safe and never drifts, which makes retry a valid recovery strategy everywhere.

**Concurrency.** The only collision point is the atomic git commit, because everything before it is local computation and commutative content-addressed writes. Git is the serialization point: a commit that would not fast-forward is rejected and the losing run resolves by re-reading current state, rebuilding deterministically, and re-committing. No external lock service is needed because git is the coordination primitive. The Git Data API provider commits atomically specifically so this guarantee holds in connected mode exactly as it does for local git. Multiple server instances are therefore safe; they contend only at git, and git arbitrates.

**Partial failure.** A run that dies after uploading some assets but before committing does no harm: uploaded assets are content-addressed and either correct or simply unreferenced until a later run references them, and no git state changed so the blog is exactly as it was. A run that dies after commit but before an external deploy side effect converges on the next run because the next run is idempotent. There is no half-committed state because the only commit point is atomic and everything before it is pure or idempotent.

**Draft safety in the built-in editor.** The browser-editing surface introduces one genuine new risk:

```
  author opens post in tab A     author opens same post in tab B
            |                                |
            v                                v
   session-uuid = A-xxx              session-uuid = B-yyy
            |                                |
            v                                v
   types for 1.5s of idle           types different paragraph
            |                                |
            v                                v
   PUT /autosave?session=A-xxx     PUT /autosave?session=B-yyy
            |                                |
            v                                v
   force-pushes branch              force-pushes branch
   cairn/drafts/<slug>/A-xxx        cairn/drafts/<slug>/B-yyy
            |                                |
            +--- two distinct branches, no silent overwrite ---+
                                |
                       author chooses which to publish
                                |
                                v
            squashed into ONE atomic commit on `main`
            via Git Data API; the other draft branch
            stays around until the author resolves or deletes it
```

 work typed but not yet saved lives only in browser memory, so a closed tab before save loses it. The chosen mitigation keeps git authoritative but keeps `main` clean: each editor session writes autosaves to a session-scoped branch `cairn/drafts/<slug>/<session-uuid>` via force-push of a single tree, not as commits to `main`. The session UUID is minted per editor tab on open, so two tabs editing the same post get two distinct draft branches and neither silently overwrites the other; the author sees both and chooses which to publish or merge. Autosaves are single-file by definition, so per Decision 3 they use the Contents API (one call per save) rather than the Git Data API, conserving the GitHub rate budget. Draft branches are excluded from the build by ref-pattern, not by frontmatter scan, which keeps the build's branch selection trivial. On publish, the final draft tree is squashed into a single atomic Git Data API commit on `main` and the draft branch is deleted, so the `main` history records one commit per publish and the autosave noise does not become permanent. A browser-local copy may exist purely as a last-resort safety net and is explicitly never authoritative; the authoritative recovery path is always the draft branch in git. This preserves Invariants 1 and 3 even for interactive editing, gives a clean `main` history that is usable as an audit trail, and resolves the two-tab race by partitioning rather than coordinating.

**Slug collisions.** Two posts that resolve to the same slug fail the build with both source paths surfaced; this is detection, not auto-disambiguation, because silent renaming would break inbound links worse than a build failure does.

**Secret absence.** Missing secrets fail the run closed at the point they are needed, having persisted nothing, leaving the blog untouched. No degraded half-published state.

The general principle holds: every stage is pure or idempotent, the only ordering-significant mutation is funneled through an atomic git commit, and even interactive drafting is reduced to frequent git commits, so the system is stateless and safe under concurrency, failure, and a live editor.

---

## 21. Open Source Considerations

The tool solves the author's problem first but is built from the first commit to be usable by anyone with the same problem, and the invariants are exactly what also make it good for others.

Nothing about one blog is in the code; everything specific is configuration with sane defaults so the zero-config path produces a working blog. Storage is an S3-compatible interface, not a vendor. Deploy is a strategy, not a hardwired host. The editor model is a universal contract, not a set of integrations, so no user's editor is second-class and no user's editor is a maintenance burden. The built-in editor is a conforming client, so users who prefer their own editor lose nothing and users who want a browser experience gain one without the project owing every editor an integration. The Repository Provider abstraction means the same software serves the local-clone user and the GitHub-token user without forks. The plugin seam is language-agnostic external processes, so the community extends the tool without learning Go or touching the core. Migration and other optional or one-time concerns sit at the edge or as plugins so the core stays small and auditable, which is what makes an open-source tool trustworthy and maintainable.

The reference deployment is the author's own blog, run as a normal configuration of the general tool rather than a privileged path, which guarantees the open-source version is the same software the author runs and not a stripped sibling. Dogfooding the reference deployment is the primary correctness signal.

---

## 22. Phasing and Roadmap

Phasing follows the invariants and the two-plane split: build the stateless pipeline and the Repository Provider first because everything hangs off them.

Phase 1 is the stateless authoring-plane pipeline in CLI one-shot mode over the LocalGit provider and the filesystem contract, plus the build plane in CI: ingest a conforming markdown file, content-address and upload its assets, commit source through the provider, and on commit render deterministically and deploy. This alone replaces Ghost for the author with any local editor and Obsidian working immediately by virtue of the contract.

Phase 2 introduces the Repository Provider's GitHubApi implementation and the auth-and-setup model, so the binary can operate against a remote repository with only a scoped token and no clone. This is the precondition for the browser experience.

Phase 3 is the Svelte admin with the embedded Milkdown editor, the primary browser-based authoring surface, including inline content-addressed asset upload and the autosave-as-draft-commit safety model. After this the author can write in the built-in editor, any local editor, or Obsidian interchangeably, all producing the same canonical markdown.

Phase 4 is the Notion adapter, making Notion a first-class source by materializing conforming files with identity-bearing frontmatter for stateless re-sync.

Phase 5 is the Ghost migration tool, run once to bring four years of content across with correct construct mapping and URL-continuity redirects, after which the legacy Ghost stack is decommissioned.

Phase 6 is the remaining server behaviors (webhook receiver, headless API) on top of the proven pipeline without changing it, and the plugin system implemented against the seam fixed here, at which point migration and other optional concerns refactor out of the core into plugins and the community extension story becomes real. Three first-party plugins land in this phase as reference implementations of the contract and as the answers to common technical-blog needs the core deliberately keeps out: a Mermaid `post-ingest` plugin that renders fenced ```mermaid``` blocks to inline SVG at build time (deterministic via a pinned `mermaid` CLI in `package.json` and `pnpm-lock.yaml`), a KaTeX `pre-render` plugin that server-renders inline and display math (deterministic via pinned `katex`), and a Giscus template partial published alongside `templates/` that the author opts into in configuration to add GitHub-Discussions-backed comments to the post template (no analytics; the architecture takes no position there and ships none). `cairn gc` for orphan asset cleanup (Section 15) also lands in this phase, completing the operability story.

Each phase is independently useful and ships value before the next begins, and no phase requires revisiting an invariant or redesigning an earlier phase, which is the practical payoff of fixing the principles, the Repository Provider seam, and the plugin seam up front.

---

*Architecture specification. No code by intent. This document is the authoritative description of Cairn's design — invariants, the two-plane split, the Repository Provider abstraction, the content-addressed asset model, editor-agnosticism, determinism, and the plugin seam. Cairn binaries are versioned by build date (see `docs/release-policy.md`); the architecture itself is not. If a change is large enough to need a version marker, it is large enough to need a new document.*
