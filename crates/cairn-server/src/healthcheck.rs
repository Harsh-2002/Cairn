//! Container readiness probe (ARCH 31.1). Uses the node's configured data listener, without
//! opening metadata, acquiring node locks, or requiring tools in the distroless image.

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Limited};
use hyper_util::rt::TokioIo;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::Config;

const DEADLINE: Duration = Duration::from_secs(4);

pub fn run(cfg: &Config) -> ExitCode {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => {
            eprintln!("health check: cannot start probe runtime");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(probe(cfg, DEADLINE)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("health check: {error}");
            ExitCode::FAILURE
        }
    }
}

fn probe_address(mut addr: SocketAddr) -> Result<SocketAddr, &'static str> {
    if addr.port() == 0 {
        return Err("CAIRN_LISTEN_ADDR must use a fixed port for health checks");
    }
    if addr.ip().is_unspecified() {
        addr.set_ip(match addr {
            SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        });
    }
    Ok(addr)
}

async fn probe(cfg: &Config, deadline: Duration) -> Result<(), &'static str> {
    let addr = probe_address(cfg.listen_addr)?;
    let tls = cfg.tls_cert_path.as_deref().map(tls_client).transpose()?;
    tokio::time::timeout(deadline, async {
        // A node-local CLI probe of the literal bind address, not an API-supplied outbound URL.
        // No DNS, environment HTTP proxy, redirects, public URL, or credentials are involved.
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|_| "cannot connect to configured S3 listener")?;
        if let Some(tls) = tls {
            let stream = tokio_rustls::TlsConnector::from(tls)
                .connect(ServerName::IpAddress(addr.ip().into()), tcp)
                .await
                .map_err(|_| "TLS handshake or configured certificate verification failed")?;
            request_ready(stream, addr).await
        } else {
            request_ready(tcp, addr).await
        }
    })
    .await
    .map_err(|_| "readiness probe timed out")?
}

async fn request_ready(
    stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    addr: SocketAddr,
) -> Result<(), &'static str> {
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(8192)
        .handshake(TokioIo::new(stream))
        .await
        .map_err(|_| "HTTP handshake failed")?;
    let response = async {
        let request = http::Request::builder()
            .uri("/readyz")
            .header(http::header::HOST, addr.to_string())
            .header(http::header::CONNECTION, "close")
            .body(Empty::<Bytes>::new())
            .map_err(|_| "cannot construct readiness request")?;
        let response = sender
            .send_request(request)
            .await
            .map_err(|_| "readiness request failed")?;
        if response.status() != http::StatusCode::OK {
            return Err("S3 listener is not ready (expected HTTP 200)");
        }
        let body = Limited::new(response.into_body(), 16)
            .collect()
            .await
            .map_err(|_| "invalid readiness response body")?
            .to_bytes();
        if body != "ready" {
            return Err("unexpected readiness response");
        }
        Ok(())
    };
    tokio::pin!(response);
    // Drive the connection in this task; dropping the probe also drops all of its socket work.
    tokio::select! {
        result = &mut response => result,
        result = connection => {
            result.map_err(|_| "readiness connection failed")?;
            response.await
        }
    }
}

fn tls_client(path: &std::path::Path) -> Result<Arc<rustls::ClientConfig>, &'static str> {
    let file = std::fs::File::open(path).map_err(|_| "cannot read configured TLS certificate")?;
    let certificate = rustls_pemfile::certs(&mut std::io::BufReader::new(file))
        .next()
        .transpose()
        .map_err(|_| "cannot parse configured TLS certificate")?
        .ok_or("configured TLS certificate is empty")?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = Arc::new(ConfiguredCertificate {
        certificate,
        algorithms: provider.signature_verification_algorithms,
    });
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| "cannot configure probe TLS")?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// Pin the exact locally configured leaf certificate and verify handshake signatures. The
/// local listener IP need not appear in a public certificate's SANs, and private/self-signed
/// certificates need no separate CA setting. This is readiness, not public PKI/expiry monitoring.
#[derive(Debug)]
struct ConfiguredCertificate {
    certificate: CertificateDer<'static>,
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for ConfiguredCertificate {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity != &self.certificate {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn preserves_configured_port_and_maps_wildcard_binds_to_loopback() {
        for (configured, expected) in [
            ("0.0.0.0:7373", "127.0.0.1:7373"),
            ("0.0.0.0:9123", "127.0.0.1:9123"),
            ("[::]:9124", "[::1]:9124"),
            ("127.0.0.2:9125", "127.0.0.2:9125"),
            ("192.0.2.4:9126", "192.0.2.4:9126"),
            ("[2001:db8::4]:9127", "[2001:db8::4]:9127"),
        ] {
            assert_eq!(
                probe_address(configured.parse().unwrap()).unwrap(),
                expected.parse::<SocketAddr>().unwrap()
            );
        }
        assert!(probe_address("127.0.0.1:0".parse().unwrap()).is_err());
    }

    async fn respond(
        mut stream: impl AsyncRead + AsyncWrite + Unpin,
        reply: &[u8],
    ) -> std::io::Result<()> {
        let mut request = Vec::new();
        loop {
            let byte = stream.read_u8().await?;
            request.push(byte);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
            assert!(request.len() < 1024);
        }
        let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
        assert!(request.starts_with("get /readyz http/1.1\r\n"));
        assert!(request.contains("connection: close\r\n"));
        assert!(!request.contains("authorization:") && !request.contains("cookie:"));
        stream.write_all(reply).await?;
        stream.shutdown().await
    }

    #[tokio::test]
    async fn accepts_only_complete_bounded_ready_responses_without_redirecting() {
        for (reply, healthy) in [
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nready"[..], true),
            (&b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nready\r\n0\r\n\r\n"[..], true),
            (&b"HTTP/1.1 503 Unavailable\r\nContent-Length: 9\r\n\r\nnot ready"[..], false),
            (&b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/readyz\r\nContent-Length: 0\r\n\r\n"[..], false),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"[..], false),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nrea"[..], false),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 17\r\n\r\nreadyxxxxxxxxxxxx"[..], false),
            (&b"not HTTP\r\n\r\n"[..], false),
        ] {
            let (client, server) = tokio::io::duplex(4096);
            let (result, _) = tokio::join!(
                request_ready(client, "127.0.0.1:9999".parse().unwrap()),
                respond(server, reply)
            );
            assert_eq!(result.is_ok(), healthy, "{reply:?}: {result:?}");
        }
    }

    #[tokio::test]
    // Figment's Jail fixes its callback error type to the library's large figment::Error.
    #[allow(clippy::result_large_err)]
    async fn follows_environment_port_with_headless_console_and_no_local_state() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut configured = None;
        figment::Jail::expect_with(|jail| {
            jail.set_env("CAIRN_LISTEN_ADDR", format!("0.0.0.0:{}", addr.port()));
            jail.set_env("CAIRN_WEB_ADDR", "off");
            configured = Some(Config::load().unwrap());
            Ok(())
        });
        let cfg = Config {
            data_dir: dir.path().join("must-not-create"),
            db_path: dir.path().join("must-not-create/cairn.db"),
            ..configured.unwrap()
        };
        let (result, ()) = tokio::join!(probe(&cfg, DEADLINE), async {
            let (stream, _) = listener.accept().await.unwrap();
            respond(stream, b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nready")
                .await
                .unwrap();
        });
        assert_eq!(result, Ok(()));
        assert!(!cfg.data_dir.exists());
    }

    #[tokio::test]
    async fn closed_or_stalled_listener_fails_within_the_probe_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = Config {
            listen_addr: listener.local_addr().unwrap(),
            ..Config::default()
        };
        // Accepting TCP alone is not healthy; this peer never supplies an HTTP response.
        let result = probe(&cfg, Duration::from_millis(30)).await;
        assert_eq!(result, Err("readiness probe timed out"));
        drop(listener);
        assert_eq!(
            probe(&cfg, DEADLINE).await,
            Err("cannot connect to configured S3 listener")
        );
    }

    #[tokio::test]
    async fn native_tls_requires_the_configured_certificate_and_real_handshake_signatures() {
        for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
            for matching in [true, false] {
                let dir = tempfile::tempdir().unwrap();
                let cert = dir.path().join("cert.pem");
                let key = dir.path().join("key.pem");
                let other = dir.path().join("other.pem");
                std::fs::write(&cert, include_bytes!("../testdata/tls_a.crt")).unwrap();
                std::fs::write(&key, include_bytes!("../testdata/tls_a.key")).unwrap();
                std::fs::write(&other, include_bytes!("../testdata/tls_b.crt")).unwrap();
                let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
                let certs =
                    rustls_pemfile::certs(&mut &include_bytes!("../testdata/tls_a.crt")[..])
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                let private_key =
                    rustls_pemfile::private_key(&mut &include_bytes!("../testdata/tls_a.key")[..])
                        .unwrap()
                        .unwrap();
                let server = rustls::ServerConfig::builder_with_provider(provider)
                    .with_protocol_versions(&[version])
                    .unwrap()
                    .with_no_client_auth()
                    .with_single_cert(certs, private_key)
                    .unwrap();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let cfg = Config {
                    listen_addr: listener.local_addr().unwrap(),
                    tls_cert_path: Some(if matching { cert } else { other }),
                    tls_key_path: Some(key),
                    ..Config::default()
                };
                let (result, ()) = tokio::join!(probe(&cfg, DEADLINE), async {
                    let (stream, _) = listener.accept().await.unwrap();
                    let stream = tokio_rustls::TlsAcceptor::from(Arc::new(server))
                        .accept(stream)
                        .await;
                    if matching {
                        respond(
                            stream.unwrap(),
                            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nready",
                        )
                        .await
                        .unwrap();
                    } else {
                        assert!(stream.is_err());
                    }
                });
                assert_eq!(result.is_ok(), matching, "{version:?}: {result:?}");
            }
        }
    }

    #[test]
    fn missing_empty_or_malformed_certificate_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cert.pem");
        assert!(tls_client(&path).is_err());
        for bytes in [
            "",
            "-----BEGIN CERTIFICATE-----\ninvalid!\n-----END CERTIFICATE-----",
        ] {
            std::fs::write(&path, bytes).unwrap();
            assert!(tls_client(&path).is_err());
        }
    }

    #[tokio::test]
    async fn matching_certificate_with_wrong_signing_key_is_rejected() {
        #[derive(Debug)]
        struct MismatchedKey(Arc<rustls::sign::CertifiedKey>);
        impl rustls::server::ResolvesServerCert for MismatchedKey {
            fn resolve(
                &self,
                _hello: rustls::server::ClientHello<'_>,
            ) -> Option<Arc<rustls::sign::CertifiedKey>> {
                Some(self.0.clone())
            }
        }
        for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let certs = rustls_pemfile::certs(&mut &include_bytes!("../testdata/tls_a.crt")[..])
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let wrong_key =
                rustls_pemfile::private_key(&mut &include_bytes!("../testdata/tls_b.key")[..])
                    .unwrap()
                    .unwrap();
            // The resolver deliberately bypasses with_single_cert's key-match validation to
            // exercise the client's signature check after the exact certificate pin succeeds.
            let certified = rustls::sign::CertifiedKey::new(
                certs,
                provider.key_provider.load_private_key(wrong_key).unwrap(),
            );
            let server = rustls::ServerConfig::builder_with_provider(provider)
                .with_protocol_versions(&[version])
                .unwrap()
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(MismatchedKey(Arc::new(certified))));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let cfg = Config {
                listen_addr: listener.local_addr().unwrap(),
                tls_cert_path: Some(
                    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tls_a.crt"),
                ),
                ..Config::default()
            };
            let (result, ()) = tokio::join!(probe(&cfg, DEADLINE), async {
                let (stream, _) = listener.accept().await.unwrap();
                assert!(
                    tokio_rustls::TlsAcceptor::from(Arc::new(server))
                        .accept(stream)
                        .await
                        .is_err()
                );
            });
            assert!(result.is_err(), "{version:?} accepted an invalid signature");
        }
    }
}
