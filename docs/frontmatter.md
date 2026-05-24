# Cairn Frontmatter — Schema and Contract

This is the authoritative contract every Cairn-compatible editor must satisfy. The built-in Milkdown editor, Obsidian, VSCode, vim, the Notion adapter, and any future surface all produce frontmatter that conforms to this document. Any conforming source is interchangeable; non-conforming frontmatter is rejected at ingestion.

> Where this conflicts with the architecture spec (`ARCH.md`), the architecture spec wins — file an issue, fix the conflict, then update either side. This file is a contract; both spec and contract evolve together.

---

## Format

A frontmatter block is a YAML document delimited by a line containing exactly `---`, optionally with a trailing newline. It must be the *first* block of the file. Bytes before the opening `---` are an error.

```markdown
---
title: Hello, world
date: 2026-05-18T14:00:00+02:00
---

# Body starts here.
```

Encoding: UTF-8, no BOM. Line endings: LF only (CRLF normalized on ingest, but committed files use LF).

---

## Required fields

### `title` — string

The human-readable title of the post. Single line. Trimmed of leading/trailing whitespace. May contain any Unicode character including punctuation. Empty string is rejected.

### `date` — RFC 3339 timestamp

The publication-intent timestamp. Must include a timezone offset (`Z` or `±HH:MM`). Timestamps without timezone are rejected to keep ordering deterministic across editor locales.

Examples accepted:
- `2026-05-18T14:00:00+02:00`
- `2026-05-18T12:00:00Z`
- `2026-05-18T14:00:00.123+02:00` (sub-second precision allowed, ignored for sort)

Examples rejected:
- `2026-05-18` (no time, no zone)
- `2026-05-18T14:00:00` (no zone)
- `May 18, 2026` (not RFC 3339)

---

## Optional fields with defaults

### `slug` — string, default: derived from `title`

The URL path component for this post. When omitted, derived from `title` by the algorithm in [Slug derivation](#slug-derivation). When explicit, validated against `^[a-z0-9]+(-[a-z0-9]+)*$` — lowercase ASCII, hyphen-separated, no leading/trailing hyphen, no consecutive hyphens. Maximum 80 characters.

### `draft` — bool, default: `false`

When `true`, this post is excluded from the build and from listings, sitemap, and feed. Drafts are still serialized to git in `cairn/drafts/*` branches by the editor's autosave; the `draft` flag in frontmatter is the *human* declaration of intent, separate from the *autosave* state managed by branch names.

### `tags` — list of strings, default: `[]`

Each tag is lowercased and matched against `^[a-z0-9]+(-[a-z0-9]+)*$` after lowercasing. Duplicates within a single post's tag list are warned and de-duplicated. Tags are used to generate aggregate listing pages.

### `summary` — string, default: derived from the first body paragraph

A short description for social-meta and listing-page rendering. When omitted, the renderer derives it from the first paragraph (max 240 chars, sentence-boundary truncation when possible). Explicit values bypass derivation.

### `redirects_from` — list of strings, default: `[]`

Each entry is a *previous* URL path that should redirect to this post's current canonical URL. Used for slug changes and Ghost migration. Each entry must begin with `/`. The build plane emits the redirect map; the deploy strategy is responsible for actually serving the redirects (Cloudflare Pages: `_redirects` file; alternative hosts: their own).

### `notion_page_id` — string, default: absent

When present, identifies the Notion page this post is sourced from. Used for stateless re-sync by the Notion adapter. Format: 32-character lowercase hex (Notion's canonical UUID without hyphens). Absent on posts not sourced from Notion.

### `updated` — RFC 3339 timestamp, default: equal to `date`

When present, communicates "this post was meaningfully updated at this time" — used for the sitemap's `lastmod`. When absent, sitemap `lastmod` equals `date`. Never derived from file mtime, because file mtimes are not portable across machines and would break determinism.

---

## Forbidden conventions

- **No `published` flag.** Inverted of `draft`, which already exists. One flag, not two.
- **No `created` field.** `date` is the authoritative publication intent; "when the file was first written" is in git history and not relevant to the build.
- **No `author`.** Single-author blogs are the default shape; multi-author support comes via configuration plus a future `author` field added carefully. Pre-emptively adding it now is YAGNI.
- **No `path` or `permalink` fields.** The slug plus the configured URL template fully determines the path. A `permalink` override would defeat slug collision detection.
- **No template-engine-specific keys** (no `layout`, no `template`). Template selection is by post type configuration, not frontmatter.

---

## Slug derivation

When `slug` is absent, derive it from `title` deterministically:

1. Lowercase using Unicode case folding.
2. Apply NFKD normalization, then strip combining marks (so `é` becomes `e`, not `é`).
3. Replace any character not in `[a-z0-9]` with a single hyphen.
4. Collapse consecutive hyphens into one.
5. Trim leading and trailing hyphens.
6. Truncate to 80 characters at a hyphen boundary if possible.
7. If the result is empty (e.g., title was all emoji), the build fails with a clear error: "post requires explicit `slug` because the derived slug is empty."

The derivation must be a pure function of `title` alone — no locale, no time, no env.

Slug collisions across the published post set fail the build closed, with both source paths surfaced. The author resolves by setting an explicit `slug` on one of them and (likely) adding a `redirects_from` entry.

---

## Validation policy

Parsing is strict:

- **YAML parse error** → ingestion aborts, no commit, no upload.
- **Missing required field** → ingestion aborts with "missing required field `<name>` in `<path>`".
- **Wrong type** (e.g., `date: true`) → ingestion aborts with a type-checked error.
- **Unknown key** → a warning (logged once per build), but ingestion proceeds. This makes forward-compat additions non-breaking for existing editors. New fields added to this contract appear as warnings on older Cairn versions until the user upgrades.
- **Constraint violation** (e.g., slug with uppercase) → ingestion aborts with a precise error and the offending value.

Validation runs identically on every ingestion path: filesystem, built-in editor, Notion adapter. No path bypasses these rules.

---

## Examples

### Minimal valid post

```markdown
---
title: Hello, world
date: 2026-05-18T12:00:00Z
---

This is the body.
```

### Fully-specified post

```markdown
---
title: "How we moved off Ghost"
date: 2026-05-18T14:00:00+02:00
slug: how-we-moved-off-ghost
draft: false
tags:
  - infrastructure
  - migration
summary: "A short story about replacing a CMS with a directory of markdown files."
redirects_from:
  - /blog/2024-ghost-to-cairn/
  - /posts/ghost-replacement
notion_page_id: 1a2b3c4d5e6f7890123456789abcdef0
updated: 2026-05-19T09:30:00+02:00
---

The body of the post.
```

---

## Versioning

This document describes frontmatter schema **v1**. Future versions:

- **Additive changes** (new optional fields, new validation warnings) are non-breaking. They land in this document without a version bump for the schema itself; the architecture spec's version bump (e.g., v5) covers it.
- **Breaking changes** (new required fields, type changes, removed fields) require a `cairn_schema_version: 2` field on the post and a migration tool that adds the field to existing posts. v1 posts continue to parse on a v1+v2 build.
- **Field renames** are breaking. Avoided in practice.

Until a `cairn_schema_version` is introduced, absence of that key means v1.
