package render

import (
	"bytes"
	"fmt"
	"strings"

	mathjax "github.com/litao91/goldmark-mathjax"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/renderer"
	"github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/goldmark/util"

	"github.com/Harsh-2002/Cairn/internal/markdown"
	"github.com/Harsh-2002/Cairn/internal/render/highlight"
)

// renderedBody holds the HTML plus the flags templates use to decide whether
// to inject mermaid/math scripts.
type renderedBody struct {
	HTML       string
	HasMermaid bool
	HasMath    bool
}

// renderBody parses markdown and returns rendered HTML. The mermaid/math
// flags reflect whether those constructs appeared in this body.
func renderBody(source []byte) (renderedBody, error) {
	source = markdown.NormalizeForRender(source)
	state := &renderState{highlighter: highlight.New()}
	md := goldmark.New(
		goldmark.WithExtensions(
			extension.Table,
			extension.Strikethrough,
			extension.TaskList,
			extension.Footnote,
			mathjax.MathJax,
		),
		goldmark.WithParserOptions(parser.WithAutoHeadingID()),
		goldmark.WithRendererOptions(
			html.WithUnsafe(),
			renderer.WithNodeRenderers(
				util.Prioritized(&cairnNodeRenderer{state: state}, 100),
			),
		),
	)
	var buf bytes.Buffer
	if err := md.Convert(source, &buf); err != nil {
		return renderedBody{}, err
	}
	return renderedBody{HTML: buf.String(), HasMermaid: state.hasMermaid, HasMath: state.hasMath}, nil
}

type renderState struct {
	highlighter *highlight.Highlighter
	hasMermaid  bool
	hasMath     bool
}

// cairnNodeRenderer overrides goldmark's default HTML rendering for three
// node kinds:
//   - FencedCodeBlock: chroma highlight; mermaid -> <pre class="mermaid">
//   - InlineMath: <math display="inline">
//   - MathBlock: <math display="block">
type cairnNodeRenderer struct {
	state *renderState
}

func (r *cairnNodeRenderer) RegisterFuncs(reg renderer.NodeRendererFuncRegisterer) {
	reg.Register(ast.KindFencedCodeBlock, r.renderFencedCode)
	reg.Register(mathjax.KindInlineMath, r.renderInlineMath)
	reg.Register(mathjax.KindMathBlock, r.renderMathBlock)
}

func (r *cairnNodeRenderer) renderFencedCode(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}
	n := node.(*ast.FencedCodeBlock)
	lang := string(n.Language(source))
	var code strings.Builder
	for i := 0; i < n.Lines().Len(); i++ {
		seg := n.Lines().At(i)
		code.Write(seg.Value(source))
	}
	if lang == "mermaid" {
		r.state.hasMermaid = true
		_, _ = fmt.Fprintf(w, "<pre class=\"mermaid\">%s</pre>", htmlEscape(strings.TrimRight(code.String(), "\n")))
		return ast.WalkSkipChildren, nil
	}
	_, _ = w.WriteString(r.state.highlighter.HighlightHTML(code.String(), lang))
	return ast.WalkSkipChildren, nil
}

func (r *cairnNodeRenderer) renderInlineMath(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}
	r.state.hasMath = true
	var tex strings.Builder
	for c := node.FirstChild(); c != nil; c = c.NextSibling() {
		if t, ok := c.(*ast.Text); ok {
			tex.Write(t.Segment.Value(source))
		}
	}
	_, _ = fmt.Fprintf(w, "<math display=\"inline\"><mtext>%s</mtext></math>", htmlEscape(tex.String()))
	return ast.WalkSkipChildren, nil
}

func (r *cairnNodeRenderer) renderMathBlock(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}
	r.state.hasMath = true
	mb := node.(*mathjax.MathBlock)
	var tex strings.Builder
	for i := 0; i < mb.Lines().Len(); i++ {
		seg := mb.Lines().At(i)
		tex.Write(seg.Value(source))
	}
	_, _ = fmt.Fprintf(w, "<math display=\"block\"><mtext>%s</mtext></math>", htmlEscape(strings.TrimRight(tex.String(), "\n")))
	return ast.WalkSkipChildren, nil
}

func htmlEscape(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '"':
			b.WriteString("&quot;")
		case '\'':
			b.WriteString("&#39;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
