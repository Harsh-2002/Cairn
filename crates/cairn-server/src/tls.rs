//! Native TLS termination using rustls with the aws-lc-rs provider (ARCH §7.7, §27.2). The
//! server can terminate TLS itself or run behind a terminating proxy on a trusted interface.

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

/// Load a rustls server configuration from PEM certificate and key files.
///
/// # Errors
/// Returns a message if the files cannot be read or contain no usable key/cert.
pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>, String> {
    // Install the aws-lc-rs provider as the process default (idempotent; ignore if already set).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("invalid certificate/key: {e}"))?;
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
