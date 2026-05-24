package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	htmltomd "github.com/JohannesKaufmann/html-to-markdown/v2"
	"github.com/spf13/cobra"

	"github.com/Harsh-2002/Cairn/internal/frontmatter"
)

func newMigrateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate content from another platform into content/posts/.",
	}
	cmd.AddCommand(newMigrateGhostCmd())
	return cmd
}

func newMigrateGhostCmd() *cobra.Command {
	var output, sourcePrefix string
	cmd := &cobra.Command{
		Use:   "ghost <export.json>",
		Short: "Import a Ghost JSON export into content/posts/.",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runMigrateGhost(args[0], output, sourcePrefix)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", ".", "Output root.")
	cmd.Flags().StringVar(&sourcePrefix, "source-prefix", "", "URL path prefix Ghost served posts from.")
	return cmd
}

// ghostExport is the minimal subset of Ghost's export JSON we read.
type ghostExport struct {
	DB []struct {
		Data struct {
			Posts []ghostPost `json:"posts"`
		} `json:"data"`
	} `json:"db"`
}

type ghostPost struct {
	Title         string `json:"title"`
	Slug          string `json:"slug"`
	HTML          string `json:"html"`
	CustomExcerpt string `json:"custom_excerpt"`
	PublishedAt   string `json:"published_at"`
	UpdatedAt     string `json:"updated_at"`
	Status        string `json:"status"`
	URL           string `json:"url"`
}

func runMigrateGhost(exportPath, output, sourcePrefix string) error {
	raw, err := os.ReadFile(exportPath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", exportPath, err)
	}
	var exp ghostExport
	if err := json.Unmarshal(raw, &exp); err != nil {
		return fmt.Errorf("parsing Ghost export: %w", err)
	}
	if len(exp.DB) == 0 {
		return fmt.Errorf("Ghost export has no `db` entry")
	}
	posts := exp.DB[0].Data.Posts
	if len(posts) == 0 {
		return fmt.Errorf("Ghost export has no posts")
	}
	postsDir := filepath.Join(output, "content/posts")
	if err := os.MkdirAll(postsDir, 0o755); err != nil {
		return err
	}
	migrated := 0
	for _, p := range posts {
		if p.Status != "" && p.Status != "published" && p.Status != "draft" {
			continue
		}
		slug := p.Slug
		if slug == "" {
			derived, ok := frontmatter.DeriveSlug(p.Title)
			if !ok {
				fmt.Fprintf(os.Stderr, "skip: empty slug for %q\n", p.Title)
				continue
			}
			slug = derived
		}
		date := pickDate(p.PublishedAt, p.UpdatedAt)
		mdBody, err := htmltomd.ConvertString(p.HTML)
		if err != nil {
			fmt.Fprintf(os.Stderr, "convert html for %s: %v\n", slug, err)
			continue
		}
		draft := p.Status == "draft"
		fm := fmt.Sprintf("---\ntitle: %q\ndate: %s\nslug: %s\ndraft: %t\n", p.Title, date, slug, draft)
		if p.CustomExcerpt != "" {
			fm += fmt.Sprintf("summary: %q\n", p.CustomExcerpt)
		}
		redirects := buildRedirects(p, sourcePrefix)
		for _, r := range redirects {
			fm += fmt.Sprintf("redirects_from: [%q]\n", r)
		}
		fm += "---\n\n"
		target := filepath.Join(postsDir, slug+".md")
		if err := os.WriteFile(target, []byte(fm+strings.TrimSpace(mdBody)+"\n"), 0o644); err != nil {
			return err
		}
		migrated++
	}
	fmt.Printf("Migrated %d posts into %s\n", migrated, postsDir)
	return nil
}

func pickDate(published, updated string) string {
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, published); err == nil {
			return t.UTC().Format(time.RFC3339)
		}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, updated); err == nil {
			return t.UTC().Format(time.RFC3339)
		}
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func buildRedirects(p ghostPost, sourcePrefix string) []string {
	if p.URL == "" {
		return nil
	}
	rel := p.URL
	if i := strings.Index(rel, "://"); i >= 0 {
		if slash := strings.Index(rel[i+3:], "/"); slash >= 0 {
			rel = rel[i+3+slash:]
		}
	}
	if sourcePrefix != "" {
		rel = strings.TrimPrefix(rel, "/"+strings.Trim(sourcePrefix, "/"))
	}
	if rel == "" || rel == "/" {
		return nil
	}
	if !strings.HasPrefix(rel, "/") {
		rel = "/" + rel
	}
	return []string{rel}
}
