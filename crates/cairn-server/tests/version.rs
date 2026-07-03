//! The user-facing version string is build-injected (`build.rs::emit_version` → `CAIRN_VERSION`), so
//! `cairn --version` reports the calendar release (`vYYYY.MM.DD`) rather than the crate's
//! `CARGO_PKG_VERSION` (`0.1.0`). This drives the real binary to prove the wiring end-to-end — the
//! regression guard for the "release reported 0.1.0" bug.

use std::process::Command;

/// `cairn --version` must print an injected version, never the bare crate version.
#[test]
fn version_flag_reports_the_injected_version() {
    let out = Command::new(env!("CARGO_BIN_EXE_cairn"))
        .arg("--version")
        .output()
        .expect("run `cairn --version`");
    assert!(out.status.success(), "`cairn --version` exited non-zero");

    let stdout = String::from_utf8(out.stdout).expect("utf-8 version output");
    // clap prints "cairn <version>".
    let version = stdout
        .trim()
        .strip_prefix("cairn ")
        .unwrap_or_else(|| panic!("unexpected --version output: {stdout:?}"));

    assert!(!version.is_empty(), "empty version string");
    // A dev build carries a `-dev` marker; a release build is the `vYYYY.MM.DD` calendar version.
    // Either way it must be qualified — never the bare crate `CARGO_PKG_VERSION`.
    assert!(
        version.contains("-dev") || version.starts_with('v'),
        "version {version:?} is neither a -dev build nor a vYYYY.MM.DD release"
    );
    assert_ne!(
        version,
        env!("CARGO_PKG_VERSION"),
        "version is the bare crate CARGO_PKG_VERSION; the release injection is not wired"
    );
}
