//! The configuration surface (ARCH §28). Values layer flags > env > file > default, and the
//! whole config is validated on load so an invalid configuration fails fast with a clear
//! message rather than at first use.

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
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
    /// Log verbosity filter (e.g. `info`, `cairn=debug`).
    pub log_level: String,
    /// Log output format.
    pub log_format: LogFormat,
    /// Enable the development authentication bypass (loopback only; debug builds).
    pub dev_auth: bool,
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            dev_auth: false,
        }
    }
}

impl Config {
    /// Load configuration, layering an optional TOML file under environment variables
    /// (prefixed `CAIRN_`) over the built-in defaults, then validate.
    ///
    /// # Errors
    /// Returns a [`ConfigError`] if a layer fails to parse or validation fails.
    pub fn load(file: Option<&PathBuf>) -> Result<Self, ConfigError> {
        let mut fig = Figment::from(Serialized::defaults(Config::default()));
        if let Some(path) = file {
            fig = fig.merge(Toml::file(path));
        }
        fig = fig.merge(Env::prefixed("CAIRN_"));
        let cfg: Config = fig
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
    fn rejects_dev_auth_on_non_loopback() {
        let mut c = base();
        c.dev_auth = true;
        c.listen_addr = "0.0.0.0:9000".parse().unwrap();
        assert!(c.validate().is_err());
        c.listen_addr = "127.0.0.1:9000".parse().unwrap();
        assert!(c.validate().is_ok());
    }
}
