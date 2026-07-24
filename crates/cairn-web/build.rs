//! Guarantees the `rust_embed` folder (`web/dist`) exists so the crate always *compiles*, even on a
//! fresh checkout where the web console has not been built yet (`#[derive(RustEmbed)]` is a hard error
//! if its `#[folder]` is missing — that is what broke every CI compile job).
//!
//! Behaviour:
//! * If `web/dist/index.html` already exists (a real `npm run build` ran), do nothing — the real
//!   bundle is embedded untouched.
//! * Otherwise scaffold a minimal placeholder shell so the crate compiles. The placeholder
//!   deliberately references NO `assets/` bundles, so the `index_referenced_bundles_are_embedded`
//!   test (which guards against a forgotten `npm run build`) still fails on a placeholder — only a
//!   real web console build satisfies it. Production binaries and CI both build the real web console first.

use std::path::Path;

const PLACEHOLDER: &str = "<!doctype html>\n<html lang=\"en\">\n<head><meta charset=\"utf-8\">\
<title>Cairn</title></head>\n<body>\n<p>The Cairn management web console bundle was not built. \
Run <code>npm install &amp;&amp; npm run build</code> in <code>web/</code>, then rebuild.</p>\n\
</body>\n</html>\n";

fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo");
    let dist = Path::new(&manifest).join("../../web/dist");
    let index = dist.join("index.html");

    if !index.exists() {
        if let Err(e) =
            std::fs::create_dir_all(&dist).and_then(|()| std::fs::write(&index, PLACEHOLDER))
        {
            // Don't fail the build on a read-only tree; rust_embed will surface the real error.
            println!("cargo:warning=cairn-web: could not scaffold placeholder web/dist: {e}");
        }
    }

    // Re-embed whenever the bundle changes (e.g. a later `npm run build` replaces the placeholder).
    println!("cargo:rerun-if-changed=../../web/dist");
}
