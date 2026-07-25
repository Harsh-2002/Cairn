//! Human-readable HTML error pages for browsers, with byte-identical XML/JSON kept for every
//! machine client (ARCH 25).
//!
//! An S3 store is addressed by both programs and people: an SDK follows a presigned link, but a
//! person also pastes a share URL into a browser or typos a console path. AWS S3 answers a browser
//! with raw `<Error>` XML; Cloudflare R2 answers with a readable page. Cairn does the latter —
//! **only** when the client is unambiguously a browser doing a top-level navigation, so no SDK, CLI,
//! `fetch()`, or conformance test ever sees anything but the exact XML/JSON it already got.
//!
//! The page is deliberately self-contained: inline CSS, no images, no scripts, no webfonts, no
//! external requests of any kind. It has to render correctly on the S3 listener, where `/assets/*`
//! does not exist, and while the node is degraded enough to be returning a 5xx.
//!
//! Safety: the resource path is attacker-controlled, so every interpolation is HTML-escaped, and the
//! response additionally carries a `default-src 'none'` CSP plus `nosniff` — a reflected-XSS on this
//! page is structurally impossible, not merely escaped-away.

use http::{Method, StatusCode};

/// The request headers this negotiation reads. Both branches echo it as `Vary` so a shared cache
/// can never hand one client class the other's body shape.
pub const VARY: &str = "accept, sec-fetch-dest, upgrade-insecure-requests";

/// Should this request be answered with an HTML page instead of XML/JSON?
///
/// True only for a **browser top-level navigation**. Every condition below was chosen against
/// measured client behaviour, not assumption:
///
/// * `GET` — never `HEAD` (no body allowed) and never a mutating verb.
/// * `Accept` lists `text/html` as an exact media type. Measured: botocore, the `aws` CLI, raw
///   `http.client`/`urllib` (what the conformance harness uses), Go clients (minio-go, rclone,
///   warp), and Cairn's own hyper clients send either no `Accept` at all or `*/*` — never
///   `text/html`. So this alone already keeps every machine client byte-identical.
/// * A browser signal beyond `Accept`, because `Accept: text/html` is *not* browser-exclusive:
///   `java.net.HttpURLConnection` hardcodes `Accept: text/html, image/gif, …`. Browsers mark a
///   real top-level navigation with either `Sec-Fetch-Dest: document` or (on plain-HTTP origins,
///   where Fetch Metadata is suppressed entirely) `Upgrade-Insecure-Requests: 1`. Requiring one of
///   the two admits browsers on both http:// and https:// deployments while excluding a bare Java
///   URL connection, which sends neither.
///
/// Subresource loads are excluded twice over: `<img>`/`<video>`/`<audio>`/`fetch()` send `*/*` (or
/// an image list) rather than `text/html`, and carry `Sec-Fetch-Dest: image|video|audio|empty`.
/// `<a download>` sends no `Accept` at all. A failed `<iframe>`/`<object>` embed may still render
/// the page, which is a strictly better outcome than raw XML in the frame.
#[must_use]
pub fn wants_html_pairs(method: &Method, headers: &[(String, String)]) -> bool {
    if method != Method::GET {
        return false;
    }
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    };
    // Compare the media type exactly, ignoring any parameters: `text/html;q=0.8` matches, the
    // unrelated type `text/htmlx` must not.
    let accepts_html = get("accept").is_some_and(|a| {
        a.split(',')
            .any(|t| t.split(';').next().map(str::trim) == Some("text/html"))
    });
    if !accepts_html {
        return false;
    }
    match get("sec-fetch-dest") {
        // Fetch Metadata present: only a top-level document qualifies. `<iframe>`/`<object>` are
        // also `Sec-Fetch-Mode: navigate`, so mode is not a usable signal — dest is.
        Some(dest) => dest.eq_ignore_ascii_case("document"),
        // Fetch Metadata absent. Browsers suppress `Sec-Fetch-*` entirely on non-trustworthy
        // (plain-http) origins, which is a supported Cairn deployment, so fall back to the
        // navigation-only hint browsers still send there.
        None => get("upgrade-insecure-requests").is_some(),
    }
}

/// The copy shown for a failure: a short title and one plain-language explanation.
struct Copy {
    title: &'static str,
    detail: &'static str,
    /// An optional closing line pointing at the thing that usually fixes it.
    hint: Option<&'static str>,
}

/// Map an S3 error code (preferred — it is more specific) and HTTP status to human copy.
///
/// Voice: plain, declarative, second person, no apology theatre and no blame. Says what happened,
/// then what the reader can actually do about it.
fn copy_for(status: StatusCode, code: &str) -> Copy {
    match code {
        "NoSuchKey" | "NoSuchVersion" => Copy {
            title: "Object not found",
            detail: "This object doesn't exist in this bucket, or it isn't publicly readable at this URL.",
            hint: Some(
                "Check the key in the address. If you own the bucket, open the console to confirm the object is there and that its permissions allow this request.",
            ),
        },
        "NoSuchBucket" => Copy {
            title: "Bucket not found",
            detail: "There is no bucket by this name on this node.",
            hint: Some(
                "Check the bucket name in the address. If you own it, the console lists every bucket on this node.",
            ),
        },
        "NoSuchUpload" => Copy {
            title: "Upload not found",
            detail: "That multipart upload has already completed, was aborted, or expired.",
            hint: None,
        },
        "AccessDenied" => Copy {
            title: "Access denied",
            detail: "You don't have permission to read this, and it isn't public.",
            hint: Some(
                "If you own the bucket, check its policy, ACL, and Block Public Access settings in the console — or sign in and browse it there instead.",
            ),
        },
        "SignatureDoesNotMatch" => Copy {
            title: "This link isn't valid",
            detail: "The signature on this request doesn't match. A presigned link stops working if any part of the URL is changed, or if it was signed with a different key.",
            hint: Some("Ask whoever shared it for a fresh link."),
        },
        "InvalidAccessKeyId" => Copy {
            title: "Unknown access key",
            detail: "The access key in this request isn't registered on this node.",
            hint: Some(
                "Check that you're pointing at the right endpoint, and that the key hasn't been deleted.",
            ),
        },
        "RequestTimeTooSkewed" => Copy {
            title: "Clock out of sync",
            detail: "This request is timestamped too far from the server's clock, so its signature can't be trusted.",
            hint: Some("Check the clock on the machine that made the request."),
        },
        "ExpiredToken" | "TokenRefreshRequired" => Copy {
            title: "This link has expired",
            detail: "The credentials on this request are no longer valid.",
            hint: Some("Ask whoever shared it for a fresh link."),
        },
        "InvalidRange" => Copy {
            title: "Range not satisfiable",
            detail: "The requested byte range falls outside this object.",
            hint: None,
        },
        "PreconditionFailed" => Copy {
            title: "Precondition failed",
            detail: "A condition on this request (such as If-Match) wasn't met, so nothing was changed.",
            hint: None,
        },
        "BucketNotEmpty" => Copy {
            title: "Bucket isn't empty",
            detail: "This bucket still holds objects, so it can't be deleted.",
            hint: Some("Empty it first — the console can clear a bucket in one step."),
        },
        "NotImplemented" => Copy {
            title: "Not supported",
            detail: "Cairn doesn't implement this operation.",
            hint: Some(
                "The S3 API support matrix in the docs lists everything this node does support.",
            ),
        },
        "InsufficientStorage" => Copy {
            title: "Out of space",
            detail: "This node has no room left to store the request.",
            hint: Some("Free space on the data filesystem, or raise the bucket's quota."),
        },
        "SlowDown" => Copy {
            title: "Too many requests",
            detail: "This node is shedding load to stay responsive.",
            hint: Some("Wait a moment and try again."),
        },
        "InternalError" => Copy {
            title: "Something went wrong",
            detail: "The node hit an unexpected error while handling this request. Nothing was lost — the request simply didn't complete.",
            hint: Some("The request id below appears in the server logs next to the cause."),
        },
        // Fall back on the status class when the code is unfamiliar (or absent).
        _ => match status {
            StatusCode::NOT_FOUND => Copy {
                title: "Not found",
                detail: "There's nothing at this address.",
                hint: Some("Check the URL for a typo."),
            },
            StatusCode::UNAUTHORIZED => Copy {
                title: "Sign in required",
                detail: "This page needs credentials that this request didn't carry.",
                hint: Some("Open the console and sign in."),
            },
            StatusCode::FORBIDDEN => Copy {
                title: "Access denied",
                detail: "You don't have permission to view this.",
                hint: None,
            },
            StatusCode::GONE => Copy {
                title: "This link has expired",
                detail: "This share link has expired or was revoked, so it no longer resolves to an object.",
                hint: Some("Ask whoever shared it for a fresh link."),
            },
            StatusCode::METHOD_NOT_ALLOWED => Copy {
                title: "Method not allowed",
                detail: "This address doesn't accept that kind of request.",
                hint: None,
            },
            StatusCode::TOO_MANY_REQUESTS => Copy {
                title: "Too many requests",
                detail: "This node is shedding load to stay responsive.",
                hint: Some("Wait a moment and try again."),
            },
            StatusCode::SERVICE_UNAVAILABLE => Copy {
                title: "Node unavailable",
                detail: "This node is starting up, shutting down, or temporarily overloaded.",
                hint: Some("Try again shortly. /readyz reports when it is ready to serve."),
            },
            StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => Copy {
                title: "Request timed out",
                detail: "The request took too long to complete and was stopped.",
                hint: None,
            },
            s if s.is_server_error() => Copy {
                title: "Something went wrong",
                detail: "The node hit an unexpected error while handling this request.",
                hint: Some("The request id below appears in the server logs next to the cause."),
            },
            _ => Copy {
                title: "Bad request",
                detail: "This request wasn't valid, so it was rejected.",
                hint: None,
            },
        },
    }
}

/// Escape text for inclusion in HTML element content or a double-quoted attribute.
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// Truncate an attacker-supplied string to a sane display length (a multi-KB key must not become a
/// multi-KB page). Cuts on a char boundary and marks the elision.
fn clamp(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_owned();
    }
    let head: String = s.chars().take(max).collect();
    format!("{head}…")
}

/// The stylesheet. Monochrome, typography-only, light + dark, no external resources.
const STYLE: &str = "\
:root{color-scheme:light dark;--bg:#fff;--fg:#0a0a0a;--muted:#5e5e5e;--faint:#8a8a8a;--line:#e5e5e5;--sub:#fafafa}\
@media(prefers-color-scheme:dark){:root{--bg:#0a0a0a;--fg:#ededed;--muted:#b0b0b0;--faint:#8a8a8a;--line:#2e2e2e;--sub:#151515}}\
*{box-sizing:border-box}\
html{-webkit-text-size-adjust:100%}\
body{margin:0;min-height:100vh;min-height:100svh;display:flex;align-items:center;justify-content:center;\
padding:max(1.5rem,env(safe-area-inset-top)) max(1.5rem,env(safe-area-inset-right)) max(1.5rem,env(safe-area-inset-bottom)) max(1.5rem,env(safe-area-inset-left));\
background:var(--bg);color:var(--fg);\
font-family:ui-sans-serif,system-ui,-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;\
font-size:16px;line-height:1.6;-webkit-font-smoothing:antialiased}\
main{width:100%;max-width:34rem}\
.mark{display:flex;align-items:center;gap:.55ch;font-size:.875rem;font-weight:600;letter-spacing:-.01em;margin-bottom:2.5rem}\
.mark i{display:block;width:.8em;height:.8em;border-radius:.22em;background:var(--fg);flex:none}\
.code{margin:0 0 .35rem;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.8125rem;letter-spacing:.08em;text-transform:uppercase;color:var(--faint)}\
h1{margin:0 0 .85rem;font-size:1.875rem;line-height:1.15;letter-spacing:-.02em;font-weight:600;text-wrap:balance}\
p{margin:0 0 .85rem;color:var(--muted);max-width:60ch;text-wrap:pretty}\
p.hint{color:var(--faint);font-size:.9375rem}\
hr{border:0;border-top:1px solid var(--line);margin:2rem 0 1.25rem}\
dl{margin:0;display:grid;grid-template-columns:auto 1fr;gap:.3rem 1.25rem;font-size:.8125rem}\
dt{color:var(--faint)}\
dd{margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:var(--muted);overflow-wrap:anywhere}\
@media(max-width:28rem){dl{grid-template-columns:1fr;gap:.05rem}dt{margin-top:.55rem}h1{font-size:1.625rem}}\
";

/// Render the full error page.
///
/// `code` is the S3 error code (`NoSuchKey`, …) or `""` when there isn't one; `resource` is the
/// request path; `request_id` is echoed so an operator can grep the logs for this exact failure.
#[must_use]
pub fn render(status: StatusCode, code: &str, resource: &str, request_id: &str) -> String {
    let c = copy_for(status, code);
    let status_num = status.as_u16();

    let mut details = String::new();
    if !code.is_empty() {
        details.push_str(&format!("<dt>Code</dt><dd>{}</dd>", esc(&clamp(code, 64))));
    }
    if !resource.is_empty() {
        details.push_str(&format!(
            "<dt>Resource</dt><dd>{}</dd>",
            esc(&clamp(resource, 200))
        ));
    }
    if !request_id.is_empty() {
        details.push_str(&format!(
            "<dt>Request id</dt><dd>{}</dd>",
            esc(&clamp(request_id, 64))
        ));
    }

    let hint = c
        .hint
        .map(|h| format!("<p class=\"hint\">{}</p>", esc(h)))
        .unwrap_or_default();
    let details_block = if details.is_empty() {
        String::new()
    } else {
        format!("<hr><dl>{details}</dl>")
    };

    format!(
        "<!doctype html>\n<html lang=\"en\"><head>\
<meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1,viewport-fit=cover\">\
<meta name=\"color-scheme\" content=\"light dark\">\
<meta name=\"robots\" content=\"noindex\">\
<title>{title} — Cairn</title>\
<style>{STYLE}</style></head>\
<body><main>\
<div class=\"mark\"><i></i>Cairn</div>\
<p class=\"code\">Error {status_num}</p>\
<h1>{title}</h1>\
<p>{detail}</p>\
{hint}\
{details_block}\
</main></body></html>\n",
        title = esc(c.title),
        detail = esc(c.detail),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn hdrs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn browser_navigation_gets_html() {
        let h = hdrs(&[
            (
                "accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9",
            ),
            ("sec-fetch-dest", "document"),
            ("upgrade-insecure-requests", "1"),
        ]);
        assert!(wants_html_pairs(&Method::GET, &h));
        // http:// navigation: browsers suppress Sec-Fetch-* entirely on non-trustworthy origins,
        // so `Upgrade-Insecure-Requests` is the only navigation signal left. Plain-HTTP Cairn is a
        // supported deployment, so this must still get a page.
        let plain_http = hdrs(&[
            ("accept", "text/html,application/xhtml+xml"),
            ("upgrade-insecure-requests", "1"),
        ]);
        assert!(wants_html_pairs(&Method::GET, &plain_http));
    }

    #[test]
    fn non_browser_sending_text_html_is_excluded() {
        // `java.net.HttpURLConnection` hardcodes an Accept containing text/html but sends neither
        // Fetch Metadata nor Upgrade-Insecure-Requests. It is a machine client: keep the XML.
        let java = hdrs(&[(
            "accept",
            "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2",
        )]);
        assert!(!wants_html_pairs(&Method::GET, &java));
    }

    #[test]
    fn sdk_clients_never_get_html() {
        // botocore / aws-cli / minio-go / rclone shapes: no Accept, or */*.
        assert!(!wants_html_pairs(&Method::GET, &[]));
        assert!(!wants_html_pairs(&Method::GET, &hdrs(&[("accept", "*/*")])));
        assert!(!wants_html_pairs(
            &Method::GET,
            &hdrs(&[("accept", "application/xml")])
        ));
    }

    #[test]
    fn subresource_loads_and_non_get_keep_machine_body() {
        let img = hdrs(&[("accept", "text/html"), ("sec-fetch-dest", "image")]);
        assert!(
            !wants_html_pairs(&Method::GET, &img),
            "an <img> load is not a page"
        );
        let xhr = hdrs(&[("accept", "text/html"), ("sec-fetch-dest", "empty")]);
        assert!(
            !wants_html_pairs(&Method::GET, &xhr),
            "fetch()/XHR is not a page"
        );
        // HEAD must never grow a body; mutations are always machine traffic.
        let doc = hdrs(&[("accept", "text/html"), ("sec-fetch-dest", "document")]);
        assert!(!wants_html_pairs(&Method::HEAD, &doc));
        assert!(!wants_html_pairs(&Method::PUT, &doc));
        assert!(!wants_html_pairs(&Method::DELETE, &doc));
    }

    #[test]
    fn accept_matching_is_token_exact() {
        // "text/htmlx" must not count, but a q-value or parameter on the real token must.
        assert!(!wants_html_pairs(
            &Method::GET,
            &hdrs(&[("accept", "text/htmlx")])
        ));
        assert!(wants_html_pairs(
            &Method::GET,
            &hdrs(&[
                ("accept", "application/json, text/html;q=0.8"),
                ("sec-fetch-dest", "document"),
            ])
        ));
    }

    #[test]
    fn page_is_self_contained_and_escaped() {
        let html = render(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "/b/<script>alert(1)</script>",
            "req-1",
        );
        assert!(html.contains("Object not found"));
        assert!(html.contains("Error 404"));
        assert!(html.contains("req-1"), "request id is shown for support");
        // The attacker-controlled path is escaped, never raw.
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;"));
        // Zero external requests: no img/script/link/@import anywhere.
        assert!(!html.contains("<img"));
        assert!(!html.contains("<script"));
        assert!(!html.contains("<link"));
        assert!(!html.contains("@import"));
        assert!(!html.contains("http://"));
    }

    #[test]
    fn every_browser_reachable_status_has_specific_copy() {
        // A generic fallback is fine, but the codes a person actually hits must read specifically.
        for (code, status, want) in [
            ("NoSuchKey", StatusCode::NOT_FOUND, "Object not found"),
            ("NoSuchBucket", StatusCode::NOT_FOUND, "Bucket not found"),
            ("AccessDenied", StatusCode::FORBIDDEN, "Access denied"),
            (
                "SignatureDoesNotMatch",
                StatusCode::FORBIDDEN,
                "This link isn't valid",
            ),
            ("", StatusCode::GONE, "This link has expired"),
            ("", StatusCode::TOO_MANY_REQUESTS, "Too many requests"),
            (
                "InternalError",
                StatusCode::INTERNAL_SERVER_ERROR,
                "Something went wrong",
            ),
            ("", StatusCode::SERVICE_UNAVAILABLE, "Node unavailable"),
        ] {
            let html = render(status, code, "/x", "r");
            // Compare against the escaped form — copy containing an apostrophe is emitted as
            // `&#39;`, which is what a browser renders back as `'`.
            assert!(
                html.contains(&esc(want)),
                "status {status} code {code:?} should read '{want}'"
            );
        }
    }

    #[test]
    fn internal_error_page_leaks_no_detail() {
        // Audit #28 holds on the HTML surface too: the page renders fixed copy, never the cause.
        let html = render(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "/b/k",
            "req-5",
        );
        assert!(html.contains("Something went wrong"));
        assert!(html.contains("req-5"));
        assert!(!html.to_lowercase().contains("panic"));
    }

    #[test]
    fn long_hostile_inputs_are_clamped() {
        let long_key = "a".repeat(10_000);
        let html = render(StatusCode::NOT_FOUND, "NoSuchKey", &long_key, "r");
        assert!(html.len() < 12_000, "page stays small: {}", html.len());
        assert!(html.contains('…'));
    }

    #[test]
    fn unknown_code_falls_back_by_status_class() {
        let html = render(StatusCode::BAD_REQUEST, "SomeFutureCode", "/x", "r");
        assert!(html.contains("Bad request"));
        assert!(
            html.contains("SomeFutureCode"),
            "the raw code is still shown"
        );
    }
}
