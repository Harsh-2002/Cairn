//! Startup configuration errors must never echo environment values to captured stderr.

use std::process::Command;

#[test]
fn invalid_environment_values_are_not_printed_by_validate_config() {
    for (name, value) in [
        ("CAIRN_TRUSTED_PROXIES", "pasted-secret-DO-NOT-LOG\nsecond-line"),
        ("CAIRN_API_ADDR", "pasted-secret-DO-NOT-LOG"),
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
            output.stderr,
            b"configuration error: invalid CAIRN_* environment configuration\n",
            "{name} must not echo its rejected value"
        );
    }
}
