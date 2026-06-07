//! Native TLS termination using rustls with the aws-lc-rs provider (ARCH §7.7, §27.2). The
//! server can terminate TLS itself or run behind a terminating proxy on a trusted interface.
//!
//! ## Hot reload (ARCH §27.2)
//! The served configuration lives behind a [`tokio::sync::watch`] channel so the certificate and
//! key can be rotated without dropping the listener. The accept loop reads the *current*
//! [`ServerConfig`] from its watch receiver per connection; a `SIGHUP` handler reloads the
//! cert/key from the same paths and publishes the new config with [`reload_into`]. A bad new
//! cert is logged and the previous config is retained (the channel is not updated), so a rotation
//! mistake never takes the listener down.

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;

/// Load a rustls server configuration from PEM certificate and key files.
///
/// # Errors
/// Returns a message if the files cannot be read or contain no usable key/cert.
pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>, String> {
    // Install the aws-lc-rs provider as the process default (idempotent; ignore if already set).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;
    #[cfg_attr(not(feature = "fast-io"), allow(unused_mut))]
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("invalid certificate/key: {e}"))?;
    // kTLS offload (feature `fast-io`): the kernel can only perform the record crypto if rustls is
    // allowed to hand out the negotiated traffic secrets after the handshake, so opt in to secret
    // extraction here. This is the *only* TLS-config difference the feature introduces; with the
    // feature off the config is byte-for-byte the original (the field defaults to false), so the
    // userspace TLS path and its tests are unchanged. Extraction only exposes the secrets to our
    // own process for the `setsockopt(TLS_TX/TLS_RX)` calls; it is not transmitted anywhere.
    #[cfg(feature = "fast-io")]
    {
        config.enable_secret_extraction = true;
    }
    Ok(Arc::new(config))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("open cert {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parse certs: {e}"))
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|e| format!("open key {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("parse key: {e}"))?
        .ok_or_else(|| format!("no private key found in {}", path.display()))
}

/// Reload the certificate and key from `cert_path`/`key_path` and atomically publish the new
/// [`ServerConfig`] into the watch channel. On success the served config is swapped so subsequent
/// accepts use the rotated certificate; on failure the channel is left untouched so the listener
/// keeps serving the previous config.
///
/// Returns the loaded config on success (the same `Arc` now published) so callers/tests can
/// confirm what was installed.
///
/// # Errors
/// Returns the load/parse error message if the new cert/key cannot be read or assembled. The
/// channel is not updated in that case.
pub fn reload_into(
    tx: &watch::Sender<Arc<ServerConfig>>,
    cert_path: &Path,
    key_path: &Path,
) -> Result<Arc<ServerConfig>, String> {
    let cfg = load_server_config(cert_path, key_path)?;
    // `send` only fails when every receiver has dropped; the accept loop holds one for the
    // server's lifetime, so treat a send failure as benign (shutting down).
    let _ = tx.send(cfg.clone());
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two distinct self-signed cert/key pairs (generated offline with openssl) so the reload
    // test can rotate from one identity to another and observe the served config change. Using
    // embedded PEM keeps the tests dependency-free (no cert-generation crate).
    const CERT_A: &str = include_str!("../testdata/tls_a.crt");
    const KEY_A: &str = include_str!("../testdata/tls_a.key");
    const CERT_B: &str = include_str!("../testdata/tls_b.crt");
    const KEY_B: &str = include_str!("../testdata/tls_b.key");

    /// Write a cert/key pair into `dir` under `stem`, returning their paths.
    fn write_pair(
        dir: &Path,
        stem: &str,
        cert: &str,
        key: &str,
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        let cert_path = dir.join(format!("{stem}.crt"));
        let key_path = dir.join(format!("{stem}.key"));
        std::fs::write(&cert_path, cert).unwrap();
        std::fs::write(&key_path, key).unwrap();
        (cert_path, key_path)
    }

    #[test]
    fn load_server_config_reads_pem_pair() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_pair(dir.path(), "a", CERT_A, KEY_A);
        assert!(load_server_config(&cert, &key).is_ok());
    }

    /// The `fast-io` feature must, and must *only*, flip on rustls secret extraction — kTLS cannot
    /// install the negotiated keys on the socket otherwise. With the feature off the field stays at
    /// its `false` default, so the userspace TLS path is byte-for-byte unchanged.
    #[test]
    fn fast_io_enables_secret_extraction_exactly_when_featured() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_pair(dir.path(), "a", CERT_A, KEY_A);
        let cfg = load_server_config(&cert, &key).unwrap();
        if cfg!(feature = "fast-io") {
            assert!(
                cfg.enable_secret_extraction,
                "fast-io must enable secret extraction for kTLS"
            );
        } else {
            assert!(
                !cfg.enable_secret_extraction,
                "without fast-io the TLS config must be unchanged"
            );
        }
    }

    #[test]
    fn reload_loads_swaps_and_serves_new_config() {
        let dir = tempfile::tempdir().unwrap();

        // The server reads cert/key from fixed paths; start by serving identity A.
        let (cert, key) = write_pair(dir.path(), "live", CERT_A, KEY_A);
        let initial = load_server_config(&cert, &key).unwrap();
        let (tx, rx) = watch::channel(initial.clone());
        // The receiver starts holding the initial config.
        assert!(Arc::ptr_eq(&rx.borrow(), &initial));

        // Rotate the on-disk cert/key to identity B (same paths) and reload.
        std::fs::write(&cert, CERT_B).unwrap();
        std::fs::write(&key, KEY_B).unwrap();
        let reloaded = reload_into(&tx, &cert, &key).expect("reload succeeds");

        // The channel now serves the new config (a distinct allocation), so the next accept
        // picks it up without the listener being touched.
        assert!(!Arc::ptr_eq(&reloaded, &initial));
        assert!(Arc::ptr_eq(&rx.borrow(), &reloaded));
    }

    #[test]
    fn reload_keeps_old_config_on_bad_cert() {
        let dir = tempfile::tempdir().unwrap();

        let (cert, key) = write_pair(dir.path(), "live", CERT_A, KEY_A);
        let initial = load_server_config(&cert, &key).unwrap();
        let (tx, rx) = watch::channel(initial.clone());

        // Replace the cert on disk with garbage: the reload must fail and leave the channel
        // untouched so the listener keeps serving the previous, valid config.
        std::fs::write(&cert, b"not a certificate").unwrap();
        let err = reload_into(&tx, &cert, &key).unwrap_err();
        assert!(!err.is_empty());

        // The previously-served config is retained.
        assert!(Arc::ptr_eq(&rx.borrow(), &initial));
    }
}
