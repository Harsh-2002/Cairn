package render

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/frontmatter"
)

func mkPost(t *testing.T, title, date, body string) core.Post {
	t.Helper()
	yaml := "title: " + title + "\ndate: " + date
	p, err := frontmatter.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	path, _ := core.NewRepoPath("posts/x.md")
	return core.Post{Frontmatter: p.Frontmatter, Body: body, SourcePath: path}
}

func mkDraft(t *testing.T, title, date string) core.Post {
	t.Helper()
	yaml := "title: " + title + "\ndate: " + date + "\ndraft: true"
	p, err := frontmatter.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	path, _ := core.NewRepoPath("posts/d.md")
	return core.Post{Frontmatter: p.Frontmatter, Body: "draft", SourcePath: path}
}

func TestRenderPostContainsTitleAndHTMLBody(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	html, err := r.RenderPost(mkPost(t, "Hello", "2026-05-18T12:00:00Z", "**bold** body"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, "Hello") {
		t.Errorf("title missing")
	}
	if !strings.Contains(html, "<strong>bold</strong>") {
		t.Errorf("bold not rendered; got %s", html)
	}
	if !strings.Contains(html, "/hello/") {
		t.Errorf("slug url missing")
	}
}

func TestRenderPostIncludesCanonicalLink(t *testing.T) {
	cfg := DefaultSiteConfig()
	cfg.BaseURL = "https://blog.example.com"
	r, err := WithDefaults(cfg)
	if err != nil {
		t.Fatal(err)
	}
	html, err := r.RenderPost(mkPost(t, "Test", "2026-05-18T12:00:00Z", "body"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, "https://blog.example.com/test/") {
		t.Errorf("canonical url missing")
	}
}

func TestBuildToDirWritesExpectedFiles(t *testing.T) {
	tmp := t.TempDir()
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	posts := []core.Post{
		mkPost(t, "First", "2026-05-18T12:00:00Z", "p1"),
		mkPost(t, "Second", "2026-05-19T12:00:00Z", "p2"),
	}
	if err := r.BuildToDir(posts, tmp); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"index.html", "first/index.html", "second/index.html", "sitemap.xml", "feed.xml"} {
		if _, err := os.Stat(filepath.Join(tmp, p)); err != nil {
			t.Errorf("missing: %s", p)
		}
	}
}

func TestDraftsExcludedFromBuild(t *testing.T) {
	tmp := t.TempDir()
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	posts := []core.Post{
		mkPost(t, "Real", "2026-05-18T12:00:00Z", "body"),
		mkDraft(t, "Draft", "2026-05-19T12:00:00Z"),
	}
	if err := r.BuildToDir(posts, tmp); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(tmp, "real/index.html")); err != nil {
		t.Errorf("real should exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, "draft/index.html")); err == nil {
		t.Errorf("draft should not exist")
	}
}

func TestBuildIsByteIdentical(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	posts := []core.Post{
		mkPost(t, "A", "2026-05-18T12:00:00Z", "**aa**"),
		mkPost(t, "B", "2026-05-19T12:00:00Z", "*bb*"),
		mkPost(t, "C", "2026-05-17T12:00:00Z", "cc"),
	}
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	if err := r.BuildToDir(posts, dir1); err != nil {
		t.Fatal(err)
	}
	if err := r.BuildToDir(posts, dir2); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"index.html", "sitemap.xml", "feed.xml", "a/index.html", "b/index.html", "c/index.html"} {
		a, err := os.ReadFile(filepath.Join(dir1, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		b, err := os.ReadFile(filepath.Join(dir2, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if !bytes.Equal(a, b) {
			t.Errorf("non-deterministic: %s", rel)
		}
	}
}

func TestDateDisplayIsLocaleIndependent(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	v, err := r.buildPostView(mkPost(t, "X", "2026-05-18T12:00:00Z", ""))
	if err != nil {
		t.Fatal(err)
	}
	if v.DateDisplay != "May 18, 2026" {
		t.Errorf("date_display = %q; want May 18, 2026", v.DateDisplay)
	}
}

func TestFencedCodeIsHighlighted(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	html, err := r.RenderPost(mkPost(t, "Code", "2026-05-18T12:00:00Z", "```rust\nfn main() {}\n```\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, "<pre") {
		t.Errorf("expected highlighted <pre>")
	}
	if !strings.Contains(html, "fn") {
		t.Errorf("expected source content preserved")
	}
}

func TestMermaidBlockEmitsPreClassAndInjectsScript(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	html, err := r.RenderPost(mkPost(t, "Diagram", "2026-05-18T12:00:00Z", "```mermaid\ngraph LR\nA-->B\n```\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, "<pre class=\"mermaid\">") {
		t.Errorf("expected mermaid pre")
	}
	if !strings.Contains(html, "mermaid@11") {
		t.Errorf("expected mermaid script tag")
	}
}

func TestPagesWithoutMermaidOmitScript(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	html, err := r.RenderPost(mkPost(t, "Plain", "2026-05-18T12:00:00Z", "Just text.\n"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(html, "mermaid") {
		t.Errorf("mermaid script should not appear: %s", html)
	}
}

func TestMathFlagsEnableMathStyling(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	html, err := r.RenderPost(mkPost(t, "Math", "2026-05-18T12:00:00Z", "Einstein wrote $E = mc^2$.\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, "<math") {
		t.Errorf("expected <math> tag")
	}
}

func TestSitemapIncludesEachPost(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	view, err := r.buildPostView(mkPost(t, "Alpha", "2026-05-18T12:00:00Z", ""))
	if err != nil {
		t.Fatal(err)
	}
	sm, err := r.RenderSitemap([]PostView{view})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sm, "<urlset") {
		t.Errorf("expected <urlset>")
	}
	if !strings.Contains(sm, "/alpha/") {
		t.Errorf("expected /alpha/ in sitemap")
	}
}

func TestFeedUsesMaxLastMod(t *testing.T) {
	r, err := WithDefaults(DefaultSiteConfig())
	if err != nil {
		t.Fatal(err)
	}
	v1, _ := r.buildPostView(mkPost(t, "Older", "2026-05-10T12:00:00Z", ""))
	v2, _ := r.buildPostView(mkPost(t, "Newer", "2026-05-20T12:00:00Z", ""))
	feed, err := r.RenderFeed([]PostView{v1, v2})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(feed, "2026-05-20") {
		t.Errorf("feed should reference the newer post's date")
	}
}
