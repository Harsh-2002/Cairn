// Package render is the build plane: turn ingested posts into a static site.
// Pure deterministic function of (posts, config, templates). The renderer
// never reads the clock, env, or filesystem mtimes (Invariant 6).
package render

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/flosch/pongo2/v6"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/frontmatter"
)

// Renderer holds compiled pongo2 templates plus site config.
type Renderer struct {
	config    SiteConfig
	loader    *Loader
	templates map[string]*pongo2.Template
}

// templateNames lists the templates each theme must provide.
var templateNames = []string{"post.html", "index.html", "sitemap.xml", "feed.xml"}

// NewRenderer constructs a Renderer for the given theme. sourceRoot is the
// user's source directory; any <sourceRoot>/templates/<name> file overrides
// the bundled theme.
func NewRenderer(config SiteConfig, theme, sourceRoot string) (*Renderer, error) {
	if config.Language == "" {
		config.Language = "en"
	}
	loader := NewLoader(sourceRoot, theme)
	templates := make(map[string]*pongo2.Template, len(templateNames))
	for _, name := range templateNames {
		src, err := loader.Resolve(name)
		if err != nil {
			return nil, fmt.Errorf("resolve template %s: %w", name, err)
		}
		tmpl, err := pongo2.FromString(src)
		if err != nil {
			return nil, fmt.Errorf("parse template %s: %w", name, err)
		}
		templates[name] = tmpl
	}
	return &Renderer{config: config, loader: loader, templates: templates}, nil
}

// WithDefaults is a convenience for callers that don't need theming.
func WithDefaults(config SiteConfig) (*Renderer, error) {
	return NewRenderer(config, DefaultTheme, "")
}

// RenderPost returns the HTML for a single post.
func (r *Renderer) RenderPost(post core.Post) (string, error) {
	view, err := r.buildPostView(post)
	if err != nil {
		return "", err
	}
	return r.exec("post.html", pongo2.Context{"site": r.siteCtx(), "post": postCtx(view)})
}

// RenderIndex returns the HTML for the index listing.
func (r *Renderer) RenderIndex(views []PostView) (string, error) {
	return r.exec("index.html", pongo2.Context{"site": r.siteCtx(), "posts": postsCtx(views)})
}

// RenderSitemap returns the sitemap.xml.
func (r *Renderer) RenderSitemap(views []PostView) (string, error) {
	return r.exec("sitemap.xml", pongo2.Context{
		"site":         r.siteCtx(),
		"posts":        postsCtx(views),
		"feed_updated": feedUpdated(views),
	})
}

// RenderFeed returns the Atom feed.
func (r *Renderer) RenderFeed(views []PostView) (string, error) {
	return r.exec("feed.xml", pongo2.Context{
		"site":         r.siteCtx(),
		"posts":        postsCtx(views),
		"feed_updated": feedUpdated(views),
	})
}

// siteCtx builds the snake_case map templates expect for the `site` variable,
// including precomputed URLs so templates avoid string concat (pongo2 doesn't
// support Jinja's `(a ~ b)|filter` form).
func (r *Renderer) siteCtx() map[string]any {
	base := strings.TrimRight(r.config.BaseURL, "/")
	return map[string]any{
		"title":          r.config.Title,
		"description":    r.config.Description,
		"base_url":       base,
		"author":         r.config.Author,
		"language":       r.config.Language,
		"home_url":       base + "/",
		"feed_url":       base + "/feed.xml",
		"static_css_url": base + "/static/theme.css",
	}
}

func postCtx(v PostView) map[string]any {
	return map[string]any{
		"title":        v.Title,
		"slug":         v.Slug,
		"url":          v.URL,
		"summary":      v.Summary,
		"tags":         v.Tags,
		"date":         v.Date,
		"date_display": v.DateDisplay,
		"lastmod":      v.LastMod,
		"html":         v.HTML,
		"has_mermaid":  v.HasMermaid,
		"has_math":     v.HasMath,
	}
}

func postsCtx(views []PostView) []map[string]any {
	out := make([]map[string]any, len(views))
	for i, v := range views {
		out[i] = postCtx(v)
	}
	return out
}

// BuildToDir renders the full site into outputDir. Output:
//   - <outputDir>/index.html
//   - <outputDir>/<slug>/index.html per non-draft post
//   - <outputDir>/sitemap.xml
//   - <outputDir>/feed.xml
//   - <outputDir>/static/ copied from active theme
//
// Posts are sorted descending by date, ascending by slug as tiebreaker.
// Drafts are excluded.
func (r *Renderer) BuildToDir(posts []core.Post, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}

	var published []core.Post
	for _, p := range posts {
		if !p.Frontmatter.Draft {
			published = append(published, p)
		}
	}
	// Sort: date desc, slug asc as stable tiebreaker.
	slices.SortStableFunc(published, func(a, b core.Post) int {
		ad := a.Frontmatter.Date.Time()
		bd := b.Frontmatter.Date.Time()
		switch {
		case bd.Before(ad):
			return -1
		case ad.Before(bd):
			return 1
		}
		as, _ := a.Frontmatter.EffectiveSlug()
		bs, _ := b.Frontmatter.EffectiveSlug()
		return strings.Compare(as, bs)
	})

	views := make([]PostView, 0, len(published))
	for _, post := range published {
		html, err := r.RenderPost(post)
		if err != nil {
			return err
		}
		slug, err := post.Frontmatter.EffectiveSlug()
		if err != nil {
			return err
		}
		postDir := filepath.Join(outputDir, slug)
		if err := os.MkdirAll(postDir, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(postDir, "index.html"), []byte(html), 0o644); err != nil {
			return err
		}
		v, err := r.buildPostView(post)
		if err != nil {
			return err
		}
		views = append(views, v)
	}

	indexHTML, err := r.RenderIndex(views)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outputDir, "index.html"), []byte(indexHTML), 0o644); err != nil {
		return err
	}
	sitemapXML, err := r.RenderSitemap(views)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outputDir, "sitemap.xml"), []byte(sitemapXML), 0o644); err != nil {
		return err
	}
	feedXML, err := r.RenderFeed(views)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outputDir, "feed.xml"), []byte(feedXML), 0o644); err != nil {
		return err
	}

	return r.loader.CopyStatic(outputDir)
}

func (r *Renderer) exec(name string, ctx pongo2.Context) (string, error) {
	tmpl, ok := r.templates[name]
	if !ok {
		return "", fmt.Errorf("template %q not loaded", name)
	}
	out, err := tmpl.Execute(ctx)
	if err != nil {
		return "", fmt.Errorf("render %s: %w", name, err)
	}
	return out, nil
}

func (r *Renderer) buildPostView(post core.Post) (PostView, error) {
	slug, err := post.Frontmatter.EffectiveSlug()
	if err != nil {
		return PostView{}, err
	}
	body, err := renderBody([]byte(post.Body))
	if err != nil {
		return PostView{}, err
	}
	pv := PostView{
		Title:       post.Frontmatter.Title,
		Slug:        slug,
		URL:         strings.TrimRight(r.config.BaseURL, "/") + "/" + slug + "/",
		Tags:        post.Frontmatter.Tags,
		Date:        post.Frontmatter.Date.String(),
		DateDisplay: formatDateDisplay(post.Frontmatter.Date),
		LastMod:     post.Frontmatter.LastMod().String(),
		HTML:        body.HTML,
		HasMermaid:  body.HasMermaid,
		HasMath:     body.HasMath,
	}
	if post.Frontmatter.Summary != nil {
		pv.Summary = *post.Frontmatter.Summary
		pv.HasSummary = true
	}
	return pv, nil
}

// formatDateDisplay produces "Month D, YYYY" in en-US order. Locale-
// independent by construction.
func formatDateDisplay(d frontmatter.Date) string {
	t := d.Time()
	months := [...]string{"January", "February", "March", "April", "May", "June",
		"July", "August", "September", "October", "November", "December"}
	return fmt.Sprintf("%s %d, %d", months[int(t.Month())-1], t.Day(), t.Year())
}

// feedUpdated returns the most recent LastMod across the posts. Falls back
// to the Unix epoch when there are no posts so the value is deterministic.
func feedUpdated(views []PostView) string {
	if len(views) == 0 {
		return "1970-01-01T00:00:00Z"
	}
	dates := make([]string, len(views))
	for i, v := range views {
		dates[i] = v.LastMod
	}
	sort.Strings(dates)
	return dates[len(dates)-1]
}
