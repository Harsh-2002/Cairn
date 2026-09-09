//! Public endpoint resolution shared by diagnostics, presigning and persistent shares (ARCH 28).

use crate::adapter::ListenerRole;
use serde::Serialize;
use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Origin {
    pub scheme: String,
    host: String,
    port: u16,
}

impl Origin {
    pub fn parse(value: &str) -> Option<Self> {
        if value.contains(['#', '?']) || value.trim() != value {
            return None;
        }
        let uri = value.parse::<http::Uri>().ok()?;
        let scheme = uri.scheme_str()?.to_ascii_lowercase();
        if !matches!(scheme.as_str(), "http" | "https") || !matches!(uri.path(), "" | "/") {
            return None;
        }
        let authority = uri.authority()?;
        if authority.as_str().contains('@') {
            return None;
        }
        let raw_host = authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']');
        if raw_host.is_empty() {
            return None;
        }
        let host = raw_host
            .parse::<IpAddr>()
            .map_or_else(|_| raw_host.to_ascii_lowercase(), |ip| ip.to_string());
        let suffix = authority.as_str().strip_prefix(authority.host())?;
        let port = if suffix.is_empty() {
            if scheme == "https" { 443 } else { 80 }
        } else {
            suffix.strip_prefix(':')?.parse::<u16>().ok()?
        };
        if port == 0 {
            return None;
        }
        Some(Self { scheme, host, port })
    }

    pub fn authority(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        if (self.scheme == "https" && self.port == 443)
            || (self.scheme == "http" && self.port == 80)
        {
            host
        } else {
            format!("{host}:{}", self.port)
        }
    }

    fn loopback(&self) -> bool {
        self.host == "localhost" || self.host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
    }
}

impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}", self.scheme, self.authority())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Endpoints {
    pub api_addr: SocketAddr,
    pub console_addr: Option<SocketAddr>,
    pub api_public_url: Option<Origin>,
    pub console_public_url: Option<Origin>,
}

#[derive(Clone, Copy)]
pub(crate) struct EndpointRequest<'a> {
    pub role: ListenerRole,
    pub host: &'a str,
    pub secure: bool,
    pub direct_loopback: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EndpointIssue {
    pub code: &'static str,
    pub setting: &'static str,
    pub message: &'static str,
}

impl EndpointIssue {
    fn missing(role: ListenerRole) -> Self {
        match role {
            ListenerRole::Api => Self {
                code: "ApiPublicUrlRequired",
                setting: "CAIRN_API_PUBLIC_URL",
                message: "Set CAIRN_API_PUBLIC_URL to the externally reachable S3/API URL before using uploads, previews or direct links.",
            },
            ListenerRole::Console => Self {
                code: "ConsolePublicUrlRequired",
                setting: "CAIRN_CONSOLE_PUBLIC_URL",
                message: "Set CAIRN_CONSOLE_PUBLIC_URL to the externally reachable console URL before creating console download links.",
            },
        }
    }

    pub fn same_origin() -> Self {
        Self {
            code: "EndpointOriginConflict",
            setting: "CAIRN_API_PUBLIC_URL",
            message: "The API URL matches the console origin. Set CAIRN_API_PUBLIC_URL to a separate S3/API hostname or port.",
        }
    }
}

#[derive(Serialize)]
pub(crate) struct EndpointStatus {
    pub api_url: Option<String>,
    pub console_url: Option<String>,
    pub console_enabled: bool,
    pub issues: Vec<EndpointIssue>,
}

impl Endpoints {
    pub fn resolve(
        &self,
        role: ListenerRole,
        req: EndpointRequest<'_>,
    ) -> Result<Origin, EndpointIssue> {
        if role == ListenerRole::Console && self.console_addr.is_none() {
            return Err(EndpointIssue {
                code: "ConsoleDisabled",
                setting: "CAIRN_CONSOLE_ADDR",
                message: "Console downloads are disabled. Enable CAIRN_CONSOLE_ADDR or select API delivery.",
            });
        }
        let configured = match role {
            ListenerRole::Api => &self.api_public_url,
            ListenerRole::Console => &self.console_public_url,
        };
        if let Some(origin) = configured {
            return Ok(origin.clone());
        }
        let mut origin = Origin::parse(&format!(
            "{}://{}",
            if req.secure { "https" } else { "http" },
            req.host
        ))
        .ok_or_else(|| EndpointIssue::missing(role))?;
        if role == req.role {
            return Ok(origin);
        }
        if req.direct_loopback && origin.loopback() {
            origin.port = match role {
                ListenerRole::Api => self.api_addr.port(),
                ListenerRole::Console => self.console_addr.expect("enabled above").port(),
            };
            if origin.port != 0 {
                return Ok(origin);
            }
        }
        Err(EndpointIssue::missing(role))
    }

    pub fn api_origin(
        &self,
        req: EndpointRequest<'_>,
        browser_origin: Option<&str>,
    ) -> Result<Origin, EndpointIssue> {
        let api = self.resolve(ListenerRole::Api, req)?;
        let actual_console = (req.role == ListenerRole::Console)
            .then(|| {
                Origin::parse(&format!(
                    "{}://{}",
                    if req.secure { "https" } else { "http" },
                    req.host
                ))
            })
            .flatten();
        let console = self.resolve(ListenerRole::Console, req).ok();
        let browser = browser_origin.and_then(Origin::parse);
        if [console.as_ref(), actual_console.as_ref(), browser.as_ref()]
            .into_iter()
            .flatten()
            .any(|origin| *origin == api)
        {
            return Err(EndpointIssue::same_origin());
        }
        if actual_console
            .as_ref()
            .or(browser.as_ref())
            .is_some_and(|origin| origin.scheme == "https")
            && api.scheme != "https"
        {
            return Err(EndpointIssue {
                code: "ApiMixedContent",
                setting: "CAIRN_API_PUBLIC_URL",
                message: "The HTTPS console cannot transfer objects over HTTP. Set CAIRN_API_PUBLIC_URL to an HTTPS API URL.",
            });
        }
        Ok(api)
    }

    pub fn status(&self, req: EndpointRequest<'_>) -> EndpointStatus {
        let mut issues = Vec::new();
        let api_url = self
            .api_origin(req, None)
            .map(|o| o.to_string())
            .map_err(|e| issues.push(e))
            .ok();
        let console_url = self
            .resolve(ListenerRole::Console, req)
            .map(|o| o.to_string())
            .map_err(|e| {
                if self.console_addr.is_some() {
                    issues.push(e);
                }
            })
            .ok();
        EndpointStatus {
            api_url,
            console_url,
            console_enabled: self.console_addr.is_some(),
            issues,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoints() -> Endpoints {
        Endpoints {
            api_addr: "127.0.0.1:7373".parse().unwrap(),
            console_addr: Some("127.0.0.1:7374".parse().unwrap()),
            api_public_url: None,
            console_public_url: None,
        }
    }

    #[test]
    fn origins_are_canonical_and_reject_non_origins() {
        assert_eq!(
            Origin::parse("https://EXAMPLE.test:443/"),
            Origin::parse("https://example.test")
        );
        assert_eq!(
            Origin::parse("http://[0:0:0:0:0:0:0:1]:80"),
            Origin::parse("http://[::1]")
        );
        for bad in [
            "https://",
            "ftp://example.test",
            "https://a/b",
            "https://a?",
            "https://a#x",
            "https://u:p@a",
            "https://a:99999",
            "https://a:0",
            " https://a",
        ] {
            assert!(Origin::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn cross_listener_inference_is_loopback_only() {
        let e = endpoints();
        let local = EndpointRequest {
            role: ListenerRole::Console,
            host: "localhost:7374",
            secure: false,
            direct_loopback: true,
        };
        assert_eq!(
            e.api_origin(local, None).unwrap().to_string(),
            "http://localhost:7373"
        );
        let remote = EndpointRequest {
            host: "console.example.test",
            ..local
        };
        assert_eq!(
            e.api_origin(remote, None).unwrap_err().code,
            "ApiPublicUrlRequired"
        );
        assert!(
            e.api_origin(
                EndpointRequest {
                    direct_loopback: false,
                    ..local
                },
                None
            )
            .is_err()
        );
    }

    #[test]
    fn configured_origins_and_headless_resolution() {
        let mut e = endpoints();
        let req = EndpointRequest {
            role: ListenerRole::Console,
            host: "console.example.test",
            secure: true,
            direct_loopback: false,
        };
        e.api_public_url = Origin::parse("https://console.example.test:443");
        assert_eq!(
            e.api_origin(req, None).unwrap_err().code,
            "EndpointOriginConflict"
        );
        e.api_public_url = Origin::parse("http://api.example.test");
        assert_eq!(e.api_origin(req, None).unwrap_err().code, "ApiMixedContent");
        e.api_public_url = Origin::parse("https://api.example.test");
        assert!(e.api_origin(req, None).is_ok());
        e.console_addr = None;
        assert_eq!(
            e.resolve(ListenerRole::Console, req).unwrap_err().code,
            "ConsoleDisabled"
        );
        assert!(
            e.api_origin(
                EndpointRequest {
                    role: ListenerRole::Api,
                    ..req
                },
                None
            )
            .is_ok()
        );
    }
}
