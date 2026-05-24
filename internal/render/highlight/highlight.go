// Package highlight wraps chroma with a pinned configuration so two runs
// over the same input produce byte-identical HTML. No external file loading
// or environment-dependent config.
package highlight

import (
	"bytes"
	"strings"

	"github.com/alecthomas/chroma/v2"
	"github.com/alecthomas/chroma/v2/formatters/html"
	"github.com/alecthomas/chroma/v2/lexers"
	"github.com/alecthomas/chroma/v2/styles"
)

// pinnedStyle is the single source of theme truth for code highlighting.
// "github" is chroma's closest analogue to syntect's "InspiredGitHub".
var pinnedStyle *chroma.Style

// pinnedFormatter wraps every highlight in <pre><code> with inline styles.
var pinnedFormatter *html.Formatter

func init() {
	pinnedStyle = styles.Get("github")
	if pinnedStyle == nil {
		pinnedStyle = styles.Fallback
	}
	pinnedFormatter = html.New(html.WithClasses(false), html.WithLineNumbers(false))
}

// Highlighter renders syntax-highlighted HTML. The zero value is usable.
type Highlighter struct{}

// New returns a Highlighter. Stateless; instances are interchangeable.
func New() *Highlighter { return &Highlighter{} }

// HighlightHTML returns code wrapped in <pre><code> with inline-styled tokens.
// Unknown languages fall back to plain text. Always succeeds; on internal
// error returns an escaped <pre><code> fallback.
func (h *Highlighter) HighlightHTML(code, lang string) string {
	lexer := lexers.Get(lang)
	if lexer == nil {
		lexer = lexers.Fallback
	}
	lexer = chroma.Coalesce(lexer)
	iter, err := lexer.Tokenise(nil, code)
	if err != nil {
		return fallbackHTML(code)
	}
	var buf bytes.Buffer
	if err := pinnedFormatter.Format(&buf, pinnedStyle, iter); err != nil {
		return fallbackHTML(code)
	}
	return buf.String()
}

func fallbackHTML(code string) string {
	var b strings.Builder
	b.WriteString("<pre><code>")
	for _, r := range code {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		default:
			b.WriteRune(r)
		}
	}
	b.WriteString("</code></pre>")
	return b.String()
}
