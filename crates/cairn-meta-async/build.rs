//! Build script scoped to this crate's *test* binaries only.
//!
//! The parity gate (`tests/contract.rs`) dev-deps `cairn-meta`, which statically bundles SQLite
//! via `rusqlite`/`libsqlite3-sys`, while this crate links libSQL's own statically-bundled SQLite
//! via `libsql`/`libsql-ffi`. Two complete SQLite C libraries in one link unit collide on the
//! public `sqlite3_*` symbols (`libsql-ffi` does not declare a `links = "sqlite3"` key, so cargo
//! does not detect the collision; it surfaces only at link time). The two are the same public
//! SQLite C ABI, so allowing the duplicate definitions lets the test binary link; each store
//! still drives its own engine through its own Rust bindings over that shared public API.
//!
//! This is emitted with `rustc-link-arg-tests`, which applies ONLY to the integration-test
//! binaries of THIS crate — the library, every other workspace crate, and the production server
//! are entirely unaffected, so the rest of the workspace links exactly as before.

fn main() {
    // `-z muldefs` (first definition wins) resolves the duplicate `sqlite3_*` symbols and is
    // accepted by both GNU `ld` and LLVM `lld` — unlike the GNU-only `--allow-multiple-definition`,
    // which lld (e.g. via cargo-zigbuild) rejects.
    println!("cargo:rustc-link-arg-tests=-Wl,-z,muldefs");
}
