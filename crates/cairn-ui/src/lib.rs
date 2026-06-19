//! `cairn-ui` — the embedded React management single-page application (ARCH 23).
//!
//! The management UI is a React SPA built by Vite into `ui/dist`. That bundle is
//! baked into the Cairn server binary at compile time via [`rust_embed`], so a Cairn
//! deployment is a single binary that already contains its own management interface
//! with no separate UI service to deploy or version-match (ARCH 23.1).
//!
//! This crate exposes a tiny serving surface that the server's management-UI route
//! family sits on top of:
//!
//! - [`asset`] resolves an embedded file by request path, returning its content-type
//!   and bytes. The empty path and `"index.html"` both resolve to the application
//!   shell.
//! - [`spa_shell`] returns the application shell directly, for client-side routes that
//!   do not correspond to a real asset (so framework routing survives a reload —
//!   ARCH 23.3).
//!
//! Content types are inferred from the file extension via the `mime-guess` feature of
//! `rust-embed`, falling back to `application/octet-stream`.

#![forbid(unsafe_code)]

use std::borrow::Cow;

/// The built static asset bundle (`ui/dist`), embedded at compile time.
///
/// The folder is resolved relative to this crate's directory, so the path holds
/// regardless of the working directory the build runs from. The front-end must be
/// built (`npm run build` in `ui/`) before this crate compiles.
#[derive(rust_embed::RustEmbed)]
#[folder = "../../ui/dist"]
struct Assets;

/// The canonical SPA entry document.
const INDEX: &str = "index.html";

/// Resolve an embedded asset by request path.
///
/// The empty path and `"index.html"` both map to the application shell. A leading
/// slash is tolerated. Returns the guessed content-type (from the file extension)
/// and the file bytes, or [`None`] if no such asset is embedded.
///
/// ```
/// let (content_type, body) = cairn_ui::asset("index.html").expect("index embedded");
/// assert!(content_type.starts_with("text/html"));
/// assert!(!body.is_empty());
/// ```
pub fn asset(path: &str) -> Option<(String, Cow<'static, [u8]>)> {
    let trimmed = path.trim_start_matches('/');
    let lookup = if trimmed.is_empty() { INDEX } else { trimmed };

    let file = Assets::get(lookup)?;
    let content_type = file.metadata.mimetype().to_string();
    Some((content_type, file.data))
}

/// Return the SPA shell (`index.html`) for a client-side route.
///
/// The server returns this for management-UI paths that are not concrete assets so
/// that the framework's client-side router can take over on a fresh load or reload
/// (ARCH 23.3).
///
/// # Panics
///
/// Panics only if the build embedded no `index.html`, which cannot happen for a
/// successfully built front-end (the crate would otherwise have nothing to serve).
pub fn spa_shell() -> (String, Cow<'static, [u8]>) {
    asset(INDEX).expect("ui/dist/index.html must be embedded; build the front-end first")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_is_embedded_and_is_html() {
        let (content_type, body) = asset("index.html").expect("index.html is embedded");
        assert!(
            content_type.starts_with("text/html"),
            "unexpected content-type: {content_type}"
        );
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("<html"), "index.html should contain <html");
    }

    #[test]
    fn empty_and_root_path_resolve_to_index() {
        let (_, root) = asset("").expect("empty path resolves to index");
        let (_, slash) = asset("/").expect("slash resolves to index");
        let (_, index) = asset("index.html").expect("index resolves");
        assert_eq!(root.as_ref(), index.as_ref());
        assert_eq!(slash.as_ref(), index.as_ref());
    }

    #[test]
    fn spa_shell_is_non_empty_html() {
        let (content_type, body) = spa_shell();
        assert!(!body.is_empty(), "spa shell must be non-empty");
        assert!(content_type.starts_with("text/html"));
        assert!(String::from_utf8_lossy(&body).contains("<html"));
    }

    #[test]
    fn missing_asset_is_none() {
        assert!(asset("does/not/exist.bin").is_none());
    }

    #[test]
    fn leading_slash_is_tolerated() {
        assert!(asset("/index.html").is_some());
    }

    /// The shell must reference its hashed JS/CSS bundles, and every asset it
    /// references must itself be embedded and resolve with a sensible
    /// content-type. This guards the embed pipeline against a stale or empty
    /// `ui/dist` (e.g. a forgotten `npm run build`): if Vite emitted an index
    /// that points at bundles, those bundles have to be present too.
    #[test]
    fn index_referenced_bundles_are_embedded() {
        let (_, body) = asset("index.html").expect("index.html is embedded");
        let html = String::from_utf8_lossy(&body);

        let mut referenced = 0usize;
        for needle in ["src=\"", "href=\""] {
            let mut rest = html.as_ref();
            while let Some(start) = rest.find(needle) {
                let after = &rest[start + needle.len()..];
                let end = after.find('"').expect("attribute value is quoted");
                let raw = &after[..end];
                rest = &after[end..];

                // Only follow local asset references the bundler emits.
                if !raw.contains("assets/") {
                    continue;
                }
                referenced += 1;
                let (content_type, bytes) = asset(raw).unwrap_or_else(|| {
                    panic!("index references {raw}, which is not embedded; rebuild ui/dist")
                });
                assert!(!bytes.is_empty(), "{raw} is embedded but empty");
                if raw.ends_with(".js") {
                    assert!(
                        content_type.contains("javascript"),
                        "unexpected content-type for {raw}: {content_type}"
                    );
                } else if raw.ends_with(".css") {
                    assert!(
                        content_type.starts_with("text/css"),
                        "unexpected content-type for {raw}: {content_type}"
                    );
                }
            }
        }

        assert!(
            referenced >= 2,
            "expected the shell to reference at least the JS and CSS bundles, found {referenced}"
        );
    }
}
