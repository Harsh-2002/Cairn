//! Startup configuration errors must never echo environment values to captured stderr.

use std::process::Command;

#[test]
fn invalid_environment_values_are_not_printed_by_validate_config() {
    for (name, value) in [
        (
            "CAIRN_TRUSTED_PROXIES",
            "pasted-secret-DO-NOT-LOG\nsecond-line",
        ),
        ("CAIRN_API_ADDR", "pasted-secret-DO-NOT-LOG"),
        ("CAIRN_LISTEN_ADDR", "pasted-secret-DO-NOT-LOG"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_cairn"))
            .arg("validate-config")
            .env_clear()
            .env(name, value)
            .output()
            .expect("run validate-config");
        assert_eq!(output.status.code(), Some(2), "{name} must be rejected");
        assert!(output.stdout.is_empty());
        assert_eq!(
            output.stderr, b"configuration error: invalid CAIRN_* environment configuration\n",
            "{name} must not echo its rejected value"
        );
    }
}

#[test]
fn unsupported_backup_topology_does_not_echo_configured_values() {
    let output = Command::new(env!("CARGO_BIN_EXE_cairn"))
        .args(["backup", "/tmp/cairn-unused-backup-destination"])
        .env_clear()
        .env("CAIRN_META_SHARDS", "2")
        .output()
        .expect("run backup topology preflight");
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"backup/restore supports only CAIRN_META_BACKEND=sqlite with CAIRN_META_SHARDS=1\n"
    );
}
