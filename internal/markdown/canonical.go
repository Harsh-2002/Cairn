package markdown

import (
	"fmt"
	"strings"

	mathjax "github.com/litao91/goldmark-mathjax"
	"github.com/yuin/goldmark/ast"
	extast "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/text"
)

// Canonical returns markdown in Cairn's canonical form. The output is the
// fixed point of the parse/serialize cycle: Canonical(Canonical(s)) == Canonical(s).
func Canonical(source []byte) string {
	norm := normalizeMathBlocks(source)
	c := &canonicalCtx{src: norm}
	doc := goldmarkInstance.Parser().Parse(text.NewReader(norm))
	out := c.renderBlocks(doc)
	if out == "" {
		return ""
	}
	if !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	return out
}

type canonicalCtx struct {
	src []byte
}

// renderBlocks renders all children of node as block-level nodes separated by
// a single blank line.
func (c *canonicalCtx) renderBlocks(node ast.Node) string {
	var parts []string
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		s := c.renderBlock(child)
		if s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, "\n")
}

// renderBlock returns the canonical form of a single block-level node.
// The result always ends with a single newline (the trailing newline is the
// block's own line terminator, not the blank-line separator between blocks).
func (c *canonicalCtx) renderBlock(node ast.Node) string {
	switch n := node.(type) {
	case *ast.Heading:
		return strings.Repeat("#", n.Level) + " " + c.renderInlines(n) + "\n"
	case *ast.Paragraph:
		return c.renderInlines(n) + "\n"
	case *ast.TextBlock:
		return c.renderInlines(n) + "\n"
	case *ast.Blockquote:
		body := c.renderBlocks(n)
		return prefixLines(body, "> ", "> ")
	case *ast.List:
		return c.renderList(n)
	case *ast.ListItem:
		return c.renderBlocks(n)
	case *ast.FencedCodeBlock:
		lang := string(n.Language(c.src))
		content := readLines(n, c.src)
		return "```" + lang + "\n" + content + "```\n"
	case *ast.CodeBlock:
		content := readLines(n, c.src)
		return "```\n" + content + "```\n"
	case *ast.ThematicBreak:
		return "---\n"
	case *ast.HTMLBlock:
		return readLines(n, c.src)
	case *extast.Table:
		return c.renderTable(n)
	case *extast.FootnoteList:
		return c.renderFootnoteList(n)
	case *mathjax.MathBlock:
		return c.renderMathBlock(n)
	}
	// Unknown block — fall back to its children to avoid losing content.
	return c.renderBlocks(node)
}

// renderInlines renders all inline children of node.
func (c *canonicalCtx) renderInlines(node ast.Node) string {
	var b strings.Builder
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		b.WriteString(c.renderInline(child))
	}
	return b.String()
}

// renderInline returns the canonical form of a single inline node.
func (c *canonicalCtx) renderInline(node ast.Node) string {
	switch n := node.(type) {
	case *ast.Text:
		s := string(n.Value(c.src))
		if n.HardLineBreak() {
			return s + "\\\n"
		}
		if n.SoftLineBreak() {
			return s + "\n"
		}
		return s
	case *ast.String:
		return string(n.Value)
	case *ast.Emphasis:
		marker := "*"
		if n.Level == 2 {
			marker = "**"
		}
		return marker + c.renderInlines(n) + marker
	case *ast.CodeSpan:
		return "`" + c.renderInlines(n) + "`"
	case *ast.Link:
		text := c.renderInlines(n)
		url := string(n.Destination)
		title := string(n.Title)
		if title != "" {
			return fmt.Sprintf(`[%s](%s "%s")`, text, url, title)
		}
		return fmt.Sprintf("[%s](%s)", text, url)
	case *ast.AutoLink:
		return "<" + string(n.URL(c.src)) + ">"
	case *ast.Image:
		text := c.renderInlines(n)
		url := string(n.Destination)
		title := string(n.Title)
		if title != "" {
			return fmt.Sprintf(`![%s](%s "%s")`, text, url, title)
		}
		return fmt.Sprintf("![%s](%s)", text, url)
	case *ast.RawHTML:
		segs := n.Segments
		var b strings.Builder
		for i := 0; i < segs.Len(); i++ {
			seg := segs.At(i)
			b.Write(seg.Value(c.src))
		}
		return b.String()
	case *extast.Strikethrough:
		return "~~" + c.renderInlines(n) + "~~"
	case *extast.TaskCheckBox:
		if n.IsChecked {
			return "[x] "
		}
		return "[ ] "
	case *extast.FootnoteLink:
		return fmt.Sprintf("[^%d]", n.Index)
	case *mathjax.InlineMath:
		var b strings.Builder
		for ch := n.FirstChild(); ch != nil; ch = ch.NextSibling() {
			if t, ok := ch.(*ast.Text); ok {
				b.Write(t.Segment.Value(c.src))
			}
		}
		return "$" + b.String() + "$"
	}
	// Fallback: dive into children.
	return c.renderInlines(node)
}

// renderList renders an ordered or unordered list.
func (c *canonicalCtx) renderList(list *ast.List) string {
	var b strings.Builder
	ordered := list.IsOrdered()
	start := list.Start
	if !ordered {
		start = 0
	}
	i := 0
	for item := list.FirstChild(); item != nil; item = item.NextSibling() {
		var marker, contIndent string
		if ordered {
			num := start + i
			marker = fmt.Sprintf("%d. ", num)
			contIndent = strings.Repeat(" ", len(marker))
		} else {
			marker = "- "
			contIndent = "  "
		}
		body := c.renderBlocks(item)
		b.WriteString(prefixLines(body, marker, contIndent))
		i++
	}
	return b.String()
}

// renderTable produces a GFM pipe table with computed column widths.
func (c *canonicalCtx) renderTable(table *extast.Table) string {
	var headerCells []string
	var alignments []extast.Alignment
	var bodyRows [][]string

	for child := table.FirstChild(); child != nil; child = child.NextSibling() {
		switch n := child.(type) {
		case *extast.TableHeader:
			for cell := n.FirstChild(); cell != nil; cell = cell.NextSibling() {
				headerCells = append(headerCells, c.renderInlines(cell))
				if tc, ok := cell.(*extast.TableCell); ok {
					alignments = append(alignments, tc.Alignment)
				} else {
					alignments = append(alignments, extast.AlignNone)
				}
			}
		case *extast.TableRow:
			var row []string
			for cell := n.FirstChild(); cell != nil; cell = cell.NextSibling() {
				row = append(row, c.renderInlines(cell))
			}
			bodyRows = append(bodyRows, row)
		}
	}

	// Determine total column count from the widest row seen.
	cols := len(headerCells)
	for _, r := range bodyRows {
		if len(r) > cols {
			cols = len(r)
		}
	}
	if cols == 0 {
		return ""
	}
	// Pad header and alignments to width.
	for len(headerCells) < cols {
		headerCells = append(headerCells, "")
	}
	for len(alignments) < cols {
		alignments = append(alignments, extast.AlignNone)
	}
	// Compute per-column widths.
	widths := make([]int, cols)
	for i, h := range headerCells {
		if len(h) > widths[i] {
			widths[i] = len(h)
		}
	}
	for _, r := range bodyRows {
		for i, cell := range r {
			if i < cols && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	// Minimum cell width is 3 so alignment markers (`:--`, `--:`, `:-:`) fit.
	for i := range widths {
		if widths[i] < 3 {
			widths[i] = 3
		}
	}

	var b strings.Builder
	writeRow := func(row []string) {
		b.WriteByte('|')
		for i := 0; i < cols; i++ {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			b.WriteByte(' ')
			b.WriteString(cell)
			b.WriteString(strings.Repeat(" ", widths[i]-len(cell)))
			b.WriteString(" |")
		}
		b.WriteByte('\n')
	}
	writeRow(headerCells)
	// Separator with alignment markers.
	b.WriteByte('|')
	for i := 0; i < cols; i++ {
		w := widths[i]
		switch alignments[i] {
		case extast.AlignLeft:
			b.WriteString(":" + strings.Repeat("-", w) + "-|")
		case extast.AlignRight:
			b.WriteString("-" + strings.Repeat("-", w) + ":|")
		case extast.AlignCenter:
			b.WriteString(":" + strings.Repeat("-", w) + ":|")
		default:
			b.WriteString(strings.Repeat("-", w+2) + "|")
		}
	}
	b.WriteByte('\n')
	for _, r := range bodyRows {
		writeRow(r)
	}
	return b.String()
}

// renderFootnoteList renders the [^N]: definition block list.
func (c *canonicalCtx) renderFootnoteList(list *extast.FootnoteList) string {
	var parts []string
	for note := list.FirstChild(); note != nil; note = note.NextSibling() {
		if fn, ok := note.(*extast.Footnote); ok {
			body := c.renderBlocks(fn)
			body = strings.TrimRight(body, "\n")
			parts = append(parts, fmt.Sprintf("[^%d]: %s\n", fn.Index, strings.TrimSpace(body)))
		}
	}
	return strings.Join(parts, "\n")
}

// renderMathBlock renders a $$...$$ math block in canonical multi-line form.
func (c *canonicalCtx) renderMathBlock(m *mathjax.MathBlock) string {
	content := strings.TrimRight(readLines(m, c.src), "\n")
	if content == "" {
		return "$$\n$$\n"
	}
	return "$$\n" + content + "\n$$\n"
}

// readLines reads a node's stored Lines() segments back as a single string.
func readLines(node ast.Node, src []byte) string {
	var b strings.Builder
	segs := node.Lines()
	for i := 0; i < segs.Len(); i++ {
		seg := segs.At(i)
		b.Write(seg.Value(src))
	}
	return b.String()
}

// prefixLines prepends firstLine to the first line of text and continuation
// to every subsequent line. Blank lines get the continuation prefix with
// trailing whitespace stripped (so "> " becomes ">" on a blank line).
func prefixLines(text, firstLine, continuation string) string {
	if text == "" {
		return ""
	}
	stripped := strings.TrimRight(text, "\n")
	lines := strings.Split(stripped, "\n")
	for i, line := range lines {
		p := continuation
		if i == 0 {
			p = firstLine
		}
		if line == "" {
			lines[i] = strings.TrimRight(p, " ")
		} else {
			lines[i] = p + line
		}
	}
	return strings.Join(lines, "\n") + "\n"
}
