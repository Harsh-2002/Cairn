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
}
