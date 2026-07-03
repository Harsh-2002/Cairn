//! Build script for the `cairn` server binary.
//!
//! The server now links BOTH `cairn-meta` (which statically bundles SQLite via
//! `rusqlite`/`libsqlite3-sys`) AND `cairn-meta-async` (which statically bundles libSQL's own
//! SQLite via `libsql`/`libsql-ffi`), so the `CAIRN_META_BACKEND` selector can choose the engine
//! at runtime. Two complete SQLite C libraries in one link unit collide on the public `sqlite3_*`
//! symbols (`libsql-ffi` does not declare a `links = "sqlite3"` key, so cargo does not detect the
//! collision; it surfaces only at link time). The two are the same public SQLite C ABI, so allowing
//! the duplicate definitions lets the binary link; each store still drives its own engine through
//! its own Rust bindings over that shared public API. (The third backend, Turso, is pure-Rust and
//! bundles no C SQLite, so it does not contribute to the collision.)
//!
//! This is emitted with `rustc-link-arg-bin=cairn=...`, which applies ONLY to the `cairn` binary
//! target — every library and the workspace's other crates are entirely unaffected, so the default
//! build links exactly as before. It mirrors the equivalent `rustc-link-arg-tests` hack in
//! `cairn-meta-async`'s build script (which scopes the same flag to that crate's test binaries).

fn main() {
    // The dual-bundled-SQLite collision only exists when the libSQL backend is compiled in (the
    // `meta-async` feature). Only then emit the `-z muldefs` link arg (first definition wins). The
    // DEFAULT binary links only rusqlite, needs no workaround, and builds on every linker including
    // the aarch64 cross path (cargo-zigbuild/lld rejects every multiple-definition flag).
    if std::env::var_os("CARGO_FEATURE_META_ASYNC").is_some() {
        println!("cargo:rustc-link-arg-bin=cairn=-Wl,-z,muldefs");
    }

    emit_version();
}

/// Bake the user-facing version into the binary via an `OUT_DIR` file, `include_str!`'d as the
/// `CAIRN_VERSION` const (see `main.rs`), consumed by clap (`cairn --version`) and by `SystemInfo`
/// (`GET /system`, the console footer).
///
/// A **release** build injects the calendar version (`vYYYY.MM.DD`) via `CAIRN_RELEASE_VERSION`; the
/// release workflow computes that once and threads it into both the binaries and the git tag, so the
/// binary and the release it ships in always agree. A **local/dev** build has no such env, so it
/// reports the crate version with a `-dev` marker plus the short git commit for traceability — a dev
/// build is never mistaken for a release. The crate's own `CARGO_PKG_VERSION` (`0.1.0`) is never the
/// user-facing string on its own.
///
/// Deliberately a file, **not** `cargo:rustc-env`: a `rustc-env` named `CAIRN_VERSION` also lands in
/// the runtime environment of `cargo run`/`cargo test`, where the server's strict `CAIRN_*` config
/// parser (`deny_unknown_fields`) would reject it as an unknown key. The file keeps the version out
/// of the environment entirely.
fn emit_version() {
    println!("cargo:rerun-if-env-changed=CAIRN_RELEASE_VERSION");
    let version = match std::env::var("CAIRN_RELEASE_VERSION") {
        Ok(v) if !v.trim().is_empty() => v.trim().to_owned(),
        _ => {
            let base = std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_owned());
            match git_short_sha() {
                Some(sha) => format!("{base}-dev+g{sha}"),
                None => format!("{base}-dev"),
            }
        }
    };
    let out = std::path::Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR set by cargo"))
        .join("version.txt");
    std::fs::write(&out, &version).expect("write version.txt");
}

/// The short HEAD commit for a dev build, best-effort. A build from a source tarball (no git) or
/// without `git` on PATH simply omits the suffix. Rerun the script when HEAD moves so an incremental
/// dev rebuild picks up the new commit.
fn git_short_sha() -> Option<String> {
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git").args(args).output().ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
            .filter(|s| !s.is_empty())
    };
    // `--git-path HEAD` resolves correctly even from a worktree or a non-root package dir.
    if let Some(head) = git(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head}");
    }
    git(&["rev-parse", "--short=8", "HEAD"])
}
