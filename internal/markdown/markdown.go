// Package markdown parses and serializes the markdown body of Cairn posts.
//
// The pipeline guarantee is canonical-form stability: for any input s,
// Canonical(Canonical(s)) == Canonical(s). This is the load-bearing test of
// Invariant 2 (markdown is the canonical content format).
//
// Frontmatter handling is not the responsibility of this package — the caller
// must strip the `---` delimited block before passing the body here.
package markdown

import (
	"bytes"
	"regexp"

	mathjax "github.com/litao91/goldmark-mathjax"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
)

// goldmark-mathjax does not recognize single-line `$$content$$` math blocks;
// it expects the multi-line form. Normalize before parse so both forms become
// the canonical multi-line one.
var singleLineMathBlock = regexp.MustCompile(`(?m)^([ \t]*)\$\$([^\n]+?)\$\$[ \t]*$`)

// normalizeMathBlocks rewrites every single-line `$$content$$` to multi-line
// form. Idempotent on already-normalized input.
func normalizeMathBlocks(source []byte) []byte {
	return singleLineMathBlock.ReplaceAll(source, []byte("$1$$\n$2\n$$"))
}

// NormalizeForRender exposes the math-block normalization to other packages
// that need to parse the same source we'd parse internally.
func NormalizeForRender(source []byte) []byte { return normalizeMathBlocks(source) }

// goldmarkInstance is the Cairn-configured goldmark used for both parse and
// HTML rendering. The extension set is deliberately bounded: every extension
// here must round-trip cleanly through the canonical serializer.
//
// Excluded: typographer (rewrites quotes/apostrophes), heading attributes
// (CommonMark-incompatible), wikilinks (non-standard).
var goldmarkInstance = goldmark.New(
	goldmark.WithExtensions(
		extension.Table,
		extension.Strikethrough,
		extension.TaskList,
		extension.Footnote,
		mathjax.MathJax,
	),
	goldmark.WithParserOptions(parser.WithAutoHeadingID()),
)

// Parse parses source into a goldmark AST after normalizing math blocks.
func Parse(source []byte) ast.Node {
	reader := text.NewReader(normalizeMathBlocks(source))
	return goldmarkInstance.Parser().Parse(reader)
}

// ToHTML converts markdown to HTML using the Cairn-configured renderer.
// Output is deterministic for the same input.
func ToHTML(source []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := goldmarkInstance.Convert(normalizeMathBlocks(source), &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
