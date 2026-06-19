# Control plane: management API, web console, CLI

> Part of the Cairn reference docs. The section numbers below are stable identifiers used throughout the code and docs; see the index in [`CLAUDE.md`](./CLAUDE.md) and [`../CLAUDE.md`](../CLAUDE.md).

## 22. Management API

### 22.1 Role and shape

The management API is the control surface for operating Cairn, distinct from the S3 data surface. It is JSON over HTTP, versioned in its path, gated to administrators, and it is the single API that both the embedded web UI and the command-line interface consume, so that whatever can be done in a browser can be done from a terminal and the two never drift. It is not on the object hot path, so it favours clear, stable, well-documented request and response shapes over raw throughput, and it returns errors in a JSON envelope rather than the S3 XML error document. It is served by the same process and listener family as the S3 API, separated by path.

### 22.2 Endpoints

The API exposes an overview of the store, returning the bucket and object counts, the logical and physical storage totals and thus the achieved compression ratio, and summary replication health. It exposes listing and creating buckets, and a per-bucket detail that returns the bucket's size in logical and physical bytes, its object and version counts, its versioning and ownership state, and each of its configuration aspects, namely its policy, ACL, CORS, lifecycle, replication, tag set, and public-access-block settings, with operations to read and update each aspect so the UI can present and edit them; the same configuration is settable through the S3 subresource operations, and the management API is a convenience over the same stored state. It exposes a force-delete of a bucket that empties it by paging through its contents rather than loading it, for the common operator need to remove a populated bucket, and the same bounded-paging mechanism backs a recursive prefix (folder) delete that permanently removes every object and version beneath a key prefix in one operation, reporting the count removed and signalling with a continuation flag when a very large folder needs the call repeated. It exposes a paged object listing for the data browser, and the minting of Cairn signed public-read (share) URLs for sharing or testing. It exposes user management, namely creating, listing, updating, and deactivating users and rotating their credentials. It exposes the activity and audit log with a bounded result limit, and a stored time-series of API request metrics for the usage-analytics view, queryable over a selectable range and downsampled server-side so a chart stays light regardless of traffic (Section 26.5). It exposes replication operations, namely listing failed replication entries and retrying them and viewing per-bucket replication status. And it exposes the non-secret portions of the running configuration and the health and readiness state. Every mutating endpoint records an audit entry.

### 22.3 Authentication and authorization of the control plane

The management API authenticates with the same credential mechanisms as the rest of the system, with the Bearer scheme being the natural fit for the UI and the CLI, and it requires the administrator role for all operations, refusing members and anonymous requests. The UI authenticates once to obtain a first-party Bearer token, which it holds in browser storage and sends as an `Authorization: Bearer` header on every subsequent call; because it uses no ambient cookie credentials, cross-site request forgery does not apply. Because the control plane can read and change everything, it is held to the same wire-security expectations as the rest of the system: it is served over TLS, whether terminated by Cairn or by a proxy, and credentials never traverse an untrusted hop in clear.

---


## 23. Embedded web UI

### 23.1 What it is and how it ships

The management UI is a single-page application built with the React framework and its standard build toolchain, and it is compiled into the Cairn binary at build time so that a Cairn deployment is one binary that already contains its own management interface, with no separate UI service to deploy, host, or version-match. The built static assets, the markup, the script bundles, and the styles produced by the UI build are embedded into the binary through a compile-time asset-embedding mechanism that bakes the asset directory into the executable, and the server serves them from memory. This is the operator's stated requirement that the UI be installed and compiled into the binary itself and that management be possible through either the UI or the CLI, and it is satisfied by making the UI a build-time artifact of the same binary.

### 23.2 The build pipeline

Building Cairn with its UI is a two-stage process that the workspace orchestrates so that a normal release build produces a UI-containing binary. The first stage runs the Node toolchain to produce the optimised static asset bundle from the UI source. The second stage compiles the Rust binary with the asset-embedding step pulling that bundle into the executable. The orchestration caches the asset build so that Rust-only changes do not rebuild the UI and UI-only changes do not needlessly recompile unrelated Rust, which keeps the developer loop fast, and a build feature allows producing a binary without the embedded UI for cases that want a smaller artifact or a faster build, in which case the UI routes are simply absent. The result is reproducible: the same sources produce the same binary with the same embedded UI.

### 23.3 Serving and behaviour

The server serves the embedded single-page application under a management UI path, returning the application shell for client-side routes so that the framework's routing works on reload, and serving the script and style assets with appropriate caching headers since they are content-hashed by the build. The application is a client of the management API: it authenticates the operator, then renders the store overview with the storage and compression and replication figures, a bucket view for listing and creating buckets and for editing each bucket's versioning, quota, default encryption, compression, bucket policy, and replication settings while showing its CORS, lifecycle, tagging, and public-access-block configuration read-only, a data browser for paging through objects and for uploading, downloading, deleting, and generating share URLs, a user-management view, an activity and audit view, and a replication-status view that surfaces lag and failures and offers retry. The navigation sidebar lets the operator expand the buckets entry into an inline accordion that lists the buckets and deep-links straight into each one's browser, so a named bucket is one click away without first loading the list. The data browser previews an object by opening it in a new browser tab through a short-lived presigned URL, which delegates rendering of images, PDFs, and anything else to the browser's own native viewers rather than re-implementing them, and it deletes a whole folder by invoking the recursive prefix delete behind a confirmation that states the action is permanent. A dedicated metrics view charts API request volume over time with a one-day, one-week, two-week, or one-month range, alongside a breakdown by operation and the most active buckets, drawn from the request-metrics subsystem (Section 26.5). A Tags view lists every object tag in use across the buckets with its object count and drills into the objects carrying a chosen tag, and the object browser can filter its listing to a single tag (Section 17.2). The UI changes nothing that the API does not expose, so it carries no privileged logic of its own and remains a thin presentation over the control plane.

### 23.4 Security posture of the UI

Because the UI is served by the same process and talks to the same admin-gated API, its security reduces to the API's: it requires an administrator session, it is served over TLS, and it is subject to the same audit logging for the actions it triggers. Because the UI carries a first-party Bearer token in the `Authorization` header rather than an ambient cookie, it presents no cross-site-request-forgery surface; serving it from the same origin as the API simply lets the single-page application reach the management path without cross-origin configuration. The UI exposes no secret material beyond what an administrator is entitled to see, with credentials shown once at creation and only hashes and ciphertext retained thereafter.

---


## 24. Command-line interface

### 24.1 Role

The command-line interface is the terminal-first way to operate Cairn and is a first-class peer of the web UI, not an afterthought. It serves two distinct purposes. For remote administration it is a client of the management API, so that an operator can do from a terminal or a script everything the UI offers, which suits automation and remote management. For node-local operations that must run on the host and that operate directly on the data directory and the database, it provides commands that do not go through the API, because some operations are inherently local or must run when the server is not serving. Shipping both as subcommands of the same binary keeps the deployment a single artifact.

### 24.2 Remote administration commands

As an API client the CLI offers commands mirroring the management API: creating, listing, and removing buckets and force-emptying them; reading any bucket configuration aspect through `config get`, and writing the bucket policy through `config set` taking the document from a file so it can live in version control; managing users and rotating credentials; browsing and listing objects; uploading, downloading, and removing objects; and viewing and retrying replication. It is configured by flags or the corresponding `CAIRN_*` environment variables giving the endpoint, access key, and secret key, and it can emit either human-readable output for interactive use or structured output for scripting, so it composes into automation.

### 24.3 Node-local commands

The local commands run on the host against the data directory and database directly. They include the first-start bootstrap that creates the initial administrator into an empty store, which is inherently local and one-time; an integrity command that runs reconciliation on demand and, in its repair mode, resolves divergences such as rows whose blobs are missing, which is the recovery tool referenced by the durability and backup sections; a backup command that performs the consistent snapshot procedure of Section 31; configuration validation that checks a configuration without starting the server; and the database migration that the server also runs at startup, exposed for operators who prefer to migrate explicitly. These commands are how an operator bootstraps, verifies, backs up, and repairs a deployment from the host shell, complementing the remote administration that the API-client commands provide.

---



