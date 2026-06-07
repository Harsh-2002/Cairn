//! The configuration surface (ARCH §28). Configuration is **environment-only**: the whole config
//! is `Config::default()` overlaid with `CAIRN_*` environment variables, so the binary runs on a
//! bare host or inside a container configured purely by env with no file to mount. The config is
//! validated on load so an invalid configuration fails fast with a clear message rather than at
//! first use.

use figment::Figment;
use figment::providers::{Env, Serialized};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Whether logs are emitted as human-readable text or machine-readable JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable text.
    Text,
    /// Newline-delimited JSON.
    Json,
}

/// The full server configuration. A subset of the ARCH §28.2 surface is wired in the
/// skeleton; later waves extend it (compression, quotas, replication, lifecycle, TLS).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Where the server binds.
    pub listen_addr: SocketAddr,
    /// Root of the staging and per-bucket blob directories.
    pub data_dir: PathBuf,
    /// Location of the SQLite metadata file.
    pub db_path: PathBuf,
    /// External base URL used when generating URLs behind ingress.
    pub public_base_url: Option<String>,
    /// TLS certificate path (enables built-in TLS when set together with the key).
    pub tls_cert_path: Option<PathBuf>,
    /// TLS private-key path.
    pub tls_key_path: Option<PathBuf>,
    /// Maximum number of in-flight requests.
    pub concurrency_limit: usize,
    /// Per-request timeout, in seconds.
    pub request_timeout_secs: u64,
    /// Hard per-object size ceiling, in bytes.
    pub max_object_size: u64,
    /// The region label returned by the location operation and used in SigV4 scope checks.
    pub region: String,
    /// The 32-byte master key (64 hex chars) for envelope-encrypting secrets at rest. Required
    /// in production; absent, a fixed development key is used (insecure, for local testing).
    pub master_key: Option<String>,
    /// Log verbosity filter (e.g. `info`, `cairn=debug`).
    pub log_level: String,
    /// Log output format.
    pub log_format: LogFormat,
    /// Enable the development authentication bypass (loopback only; debug builds).
    pub dev_auth: bool,
    /// How often the lifecycle scanner applies each bucket's rules, in seconds.
    pub lifecycle_interval_secs: u64,
    /// How often the multipart sweeper reclaims stale upload sessions, in seconds.
    pub multipart_sweep_interval_secs: u64,
    /// How long an idle multipart upload session lives before the sweeper aborts it, in seconds.
    pub multipart_upload_lifetime_secs: u64,
    /// How often the WAL checkpointer runs a truncating checkpoint, in seconds.
    pub wal_checkpoint_interval_secs: u64,
    /// Replication destination endpoint (e.g. `http://backup-host:9000`). When set, the
    /// replication worker ships outbox entries to this S3-compatible target (ARCH §20).
    pub replication_endpoint: Option<String>,
    /// Destination bucket at the replication endpoint (path-style).
    pub replication_dest_bucket: Option<String>,
    /// Destination access-key id.
    pub replication_access_key: Option<String>,
    /// Destination secret access key.
    pub replication_secret: Option<String>,
    /// Destination signing region (defaults to `region` when unset).
    pub replication_region: Option<String>,
    /// How often the replication worker drains the outbox, in seconds.
    pub replication_interval_secs: u64,
    /// A JSON array of named replication targets (`CAIRN_REPLICATION_TARGETS`). When present each
    /// source bucket's destination is resolved to the matching named target (by the target's
    /// `dest_bucket` or `name`) and shipped with that target's own endpoint, credentials, and TLS
    /// trust (ARCH §20). The single-target `CAIRN_REPLICATION_*` keys above remain as the default
    /// target used for any source bucket that does not match a named target. Each element is a
    /// [`ReplicationTarget`]; parsed with `serde_json` on load.
    pub replication_targets: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:9000".parse().expect("valid default addr"),
            data_dir: PathBuf::from("./data"),
            db_path: PathBuf::from("./data/cairn.db"),
            public_base_url: None,
            tls_cert_path: None,
            tls_key_path: None,
            concurrency_limit: 1024,
            request_timeout_secs: 300,
            max_object_size: 5 * 1024 * 1024 * 1024 * 1024, // 5 TiB
            region: "us-east-1".to_owned(),
            master_key: None,
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            dev_auth: false,
            lifecycle_interval_secs: 3600,
            multipart_sweep_interval_secs: 3600,
            multipart_upload_lifetime_secs: 86_400,
            wal_checkpoint_interval_secs: 300,
            replication_endpoint: None,
            replication_dest_bucket: None,
            replication_access_key: None,
            replication_secret: None,
            replication_region: None,
            replication_interval_secs: 30,
            replication_targets: None,
        }
    }
}

/// One entry of the `CAIRN_REPLICATION_TARGETS` JSON array: a named replication destination with
/// its own endpoint, credentials, and TLS trust knobs (ARCH §20). A source bucket is routed to the
/// target whose `dest_bucket` (or, failing that, `name`) matches the bucket's replication rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationTarget {
    /// A stable name for the target, used to match a source bucket's replication rule when the
    /// rule names the target rather than a destination bucket.
    pub name: String,
    /// The endpoint base URL, e.g. `https://s3.us-west-2.example.com`.
    pub endpoint: String,
    /// The SigV4 signing region for this target.
    pub region: String,
    /// The destination bucket (path-style) at this target.
    pub dest_bucket: String,
    /// The destination access-key id.
    pub access_key: String,
    /// The destination secret access key.
    pub secret: String,
    /// An optional path to a PEM file of CA certificates to trust for this target's TLS endpoint,
    /// instead of the built-in webpki roots. Honoured only for `https://` endpoints.
    #[serde(default)]
    pub ca_path: Option<PathBuf>,
    /// When true, the target's TLS server certificate is **not** verified. Dangerous; intended
    /// only for testing against a self-signed endpoint, and logged loudly when used.
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

impl Config {
    /// Parse the `replication_targets` JSON document into the typed target list. Returns an empty
    /// vector when no targets are configured.
    ///
    /// # Errors
    /// Returns a [`ConfigError::Parse`] if the JSON is malformed or does not match the
    /// [`ReplicationTarget`] shape.
    pub fn parse_replication_targets(&self) -> Result<Vec<ReplicationTarget>, ConfigError> {
        match &self.replication_targets {
            None => Ok(Vec::new()),
            Some(json) => serde_json::from_str(json).map_err(|e| {
                ConfigError::Parse(format!("invalid CAIRN_REPLICATION_TARGETS JSON: {e}"))
            }),
        }
    }
}

impl Config {
    /// Load configuration from the environment only: the built-in [`Config::default`] overlaid
    /// with `CAIRN_*` environment variables, then validated. There is no configuration file — a
    /// Cairn host or container is configured purely by env (ARCH §28).
    ///
    /// # Errors
    /// Returns a [`ConfigError`] if the environment fails to parse or validation fails.
    pub fn load() -> Result<Self, ConfigError> {
        let cfg: Config = Figment::from(Serialized::defaults(Config::default()))
            .merge(Env::prefixed("CAIRN_"))
            .extract()
            .map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Whether built-in TLS is configured.
    #[must_use]
    #[allow(dead_code)] // wired into the listener in the TLS hardening wave
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }

    /// Validate the configuration, rejecting the cases ARCH §28.2 enumerates.
    ///
    /// # Errors
    /// Returns a [`ConfigError`] describing the first invalid setting.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::Invalid("data_dir must not be empty".into()));
        }
        if self.db_path.as_os_str().is_empty() {
            return Err(ConfigError::Invalid("db_path must not be empty".into()));
        }
        if let Some(url) = &self.public_base_url {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(ConfigError::Invalid(
                    "public_base_url must be an http(s) URL".into(),
                ));
            }
        }
        match (&self.tls_cert_path, &self.tls_key_path) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(ConfigError::Invalid(
                    "TLS requires both tls_cert_path and tls_key_path".into(),
                ));
            }
            _ => {}
        }
        if self.request_timeout_secs == 0 {
            return Err(ConfigError::Invalid(
                "request_timeout_secs must be positive".into(),
            ));
        }
        if self.concurrency_limit == 0 {
            return Err(ConfigError::Invalid(
                "concurrency_limit must be positive".into(),
            ));
        }
        if self.max_object_size == 0 {
            return Err(ConfigError::Invalid(
                "max_object_size must be positive".into(),
            ));
        }
        if self.dev_auth && !self.listen_addr.ip().is_loopback() {
            return Err(ConfigError::Invalid(
                "dev_auth is only permitted on a loopback listen_addr".into(),
            ));
        }
        if self.lifecycle_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "lifecycle_interval_secs must be positive".into(),
            ));
        }
        if self.multipart_sweep_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "multipart_sweep_interval_secs must be positive".into(),
            ));
        }
        if self.multipart_upload_lifetime_secs == 0 {
            return Err(ConfigError::Invalid(
                "multipart_upload_lifetime_secs must be positive".into(),
            ));
        }
        if self.wal_checkpoint_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "wal_checkpoint_interval_secs must be positive".into(),
            ));
        }
        // A malformed replication-targets document is an operator error that must surface at load,
        // not when the first drain tries to route an object. Reject targets that set both a CA
        // path and skip-verify, since the two trust knobs are mutually exclusive.
        for target in self.parse_replication_targets()? {
            if target.ca_path.is_some() && target.insecure_skip_verify {
                return Err(ConfigError::Invalid(format!(
                    "replication target {:?} sets both ca_path and insecure_skip_verify",
                    target.name
                )));
            }
        }
        Ok(())
    }
}

/// A configuration load/validation error.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A configuration layer failed to parse.
    #[error("failed to parse configuration: {0}")]
    Parse(String),
    /// A value was invalid.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[cfg(test)]
// `figment::Jail::expect_with` takes a closure returning `figment::Result<()>`, whose `Err`
// variant (`figment::Error`) is large; the type is dictated by figment's API, not ours, so the
// `result_large_err` lint is not actionable for these env-isolation tests.
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;

    fn base() -> Config {
        Config::default()
    }

    #[test]
    fn default_is_valid() {
        assert!(base().validate().is_ok());
    }

    #[test]
    fn rejects_incomplete_tls() {
        let mut c = base();
        c.tls_cert_path = Some(PathBuf::from("/x/cert.pem"));
        assert!(c.validate().is_err());
        c.tls_key_path = Some(PathBuf::from("/x/key.pem"));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_zero_timeout_and_concurrency() {
        let mut c = base();
        c.request_timeout_secs = 0;
        assert!(c.validate().is_err());
        let mut c = base();
        c.concurrency_limit = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_bad_public_url() {
        let mut c = base();
        c.public_base_url = Some("ftp://nope".into());
        assert!(c.validate().is_err());
        c.public_base_url = Some("https://ok.example".into());
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_zero_background_intervals() {
        for mutate in [
            (|c: &mut Config| c.lifecycle_interval_secs = 0) as fn(&mut Config),
            |c: &mut Config| c.multipart_sweep_interval_secs = 0,
            |c: &mut Config| c.multipart_upload_lifetime_secs = 0,
            |c: &mut Config| c.wal_checkpoint_interval_secs = 0,
        ] {
            let mut c = base();
            mutate(&mut c);
            assert!(c.validate().is_err());
        }
    }

    #[test]
    fn accepts_custom_background_intervals() {
        let mut c = base();
        c.lifecycle_interval_secs = 600;
        c.multipart_sweep_interval_secs = 600;
        c.multipart_upload_lifetime_secs = 7200;
        c.wal_checkpoint_interval_secs = 60;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_dev_auth_on_non_loopback() {
        let mut c = base();
        c.dev_auth = true;
        c.listen_addr = "0.0.0.0:9000".parse().unwrap();
        assert!(c.validate().is_err());
        c.listen_addr = "127.0.0.1:9000".parse().unwrap();
        assert!(c.validate().is_ok());
    }

    /// Environment-only loading: with no `CAIRN_*` set, `load` returns the validated defaults.
    /// `Jail` clears the ambient environment, so this also proves the loader needs no config file.
    #[test]
    fn load_env_only_returns_defaults_when_unset() {
        figment::Jail::expect_with(|_jail| {
            let cfg = Config::load().expect("defaults load and validate");
            assert_eq!(cfg.listen_addr, Config::default().listen_addr);
            assert_eq!(cfg.region, "us-east-1");
            assert!(cfg.replication_targets.is_none());
            Ok(())
        });
    }

    /// Environment variables override the defaults — the only configuration source there is.
    /// There is no longer a TOML layer: `load` takes no path and reads `CAIRN_*` exclusively.
    #[test]
    fn load_env_only_applies_overrides() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_REGION", "eu-west-1");
            jail.set_env("CAIRN_LISTEN_ADDR", "0.0.0.0:8080");
            jail.set_env("CAIRN_LOG_FORMAT", "json");
            jail.set_env("CAIRN_REPLICATION_INTERVAL_SECS", "7");
            let cfg = Config::load().expect("env overrides load and validate");
            assert_eq!(cfg.region, "eu-west-1");
            assert_eq!(cfg.listen_addr, "0.0.0.0:8080".parse().unwrap());
            assert_eq!(cfg.log_format, LogFormat::Json);
            assert_eq!(cfg.replication_interval_secs, 7);
            Ok(())
        });
    }

    /// A TOML file present on disk is ignored: configuration comes only from env (and defaults),
    /// proving the file-merge support is gone. The file would have changed `region` if honoured.
    #[test]
    fn load_ignores_any_toml_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("Cairn.toml", "region = \"from-toml\"\n")?;
            let cfg = Config::load().expect("loads without consulting the file");
            assert_eq!(cfg.region, "us-east-1", "the TOML file must not be read");
            Ok(())
        });
    }

    /// The single-target `CAIRN_REPLICATION_*` keys still load (the fallback/default target).
    #[test]
    fn load_keeps_single_target_replication_keys() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_REPLICATION_ENDPOINT", "http://backup:9000");
            jail.set_env("CAIRN_REPLICATION_DEST_BUCKET", "mirror");
            jail.set_env("CAIRN_REPLICATION_ACCESS_KEY", "AKID");
            jail.set_env("CAIRN_REPLICATION_SECRET", "shh");
            let cfg = Config::load().expect("single-target keys load");
            assert_eq!(
                cfg.replication_endpoint.as_deref(),
                Some("http://backup:9000")
            );
            assert_eq!(cfg.replication_dest_bucket.as_deref(), Some("mirror"));
            assert_eq!(cfg.replication_access_key.as_deref(), Some("AKID"));
            assert_eq!(cfg.replication_secret.as_deref(), Some("shh"));
            Ok(())
        });
    }

    /// `CAIRN_REPLICATION_TARGETS` carries a JSON array of named targets parsed with `serde_json`.
    #[test]
    fn load_parses_replication_targets_json() {
        figment::Jail::expect_with(|jail| {
            jail.set_env(
                "CAIRN_REPLICATION_TARGETS",
                r#"[
                    {"name":"west","endpoint":"https://s3.west.example","region":"us-west-2",
                     "dest_bucket":"mirror-west","access_key":"AKW","secret":"sw","ca_path":"/etc/ca.pem"},
                    {"name":"east","endpoint":"http://s3.east.example:9000","region":"us-east-1",
                     "dest_bucket":"mirror-east","access_key":"AKE","secret":"se",
                     "insecure_skip_verify":true}
                ]"#,
            );
            let cfg = Config::load().expect("targets JSON loads and validates");
            let targets = cfg.parse_replication_targets().expect("targets parse");
            assert_eq!(targets.len(), 2);
            assert_eq!(targets[0].name, "west");
            assert_eq!(targets[0].dest_bucket, "mirror-west");
            assert_eq!(targets[0].ca_path, Some(PathBuf::from("/etc/ca.pem")));
            assert!(!targets[0].insecure_skip_verify);
            assert_eq!(targets[1].name, "east");
            assert!(targets[1].insecure_skip_verify);
            assert!(targets[1].ca_path.is_none());
            Ok(())
        });
    }

    /// A malformed `CAIRN_REPLICATION_TARGETS` document fails fast at load.
    #[test]
    fn load_rejects_malformed_replication_targets() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_REPLICATION_TARGETS", "{ not an array");
            assert!(
                Config::load().is_err(),
                "malformed targets JSON must be rejected"
            );
            Ok(())
        });
    }

    /// A target may not request both a custom CA and skip-verify; the two trust knobs conflict.
    #[test]
    fn rejects_target_with_conflicting_trust_knobs() {
        let mut c = base();
        c.replication_targets = Some(
            r#"[{"name":"x","endpoint":"https://e","region":"r","dest_bucket":"d",
                 "access_key":"a","secret":"s","ca_path":"/ca.pem","insecure_skip_verify":true}]"#
                .to_owned(),
        );
        assert!(c.validate().is_err());
    }

    /// `parse_replication_targets` yields an empty list when unset.
    #[test]
    fn parse_targets_empty_when_unset() {
        assert!(base().parse_replication_targets().unwrap().is_empty());
    }
}
