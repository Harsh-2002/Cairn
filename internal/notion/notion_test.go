package notion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Harsh-2002/Cairn/internal/frontmatter"
)

func TestRenderToMarkdownProducesConformingPost(t *testing.T) {
	page := Page{
		ID:       "1a2b3c4d5e6f7890123456789abcdef0",
		Title:    "Test Title",
		Markdown: "# Heading\n\nBody.",
	}
	if d, err := frontmatter.NewDate("2026-05-18T12:00:00Z"); err == nil {
		page.Date = d.Time()
	}
	out := RenderToMarkdown(page, "test-title")
	if !strings.HasPrefix(out, "---\n") {
		t.Errorf("missing frontmatter open")
	}
	if !strings.Contains(out, `title: "Test Title"`) {
		t.Errorf("title missing in output")
	}
	if !strings.Contains(out, "notion_page_id: 1a2b3c4d5e6f7890123456789abcdef0") {
		t.Errorf("notion id missing in output: %s", out)
	}
	if !strings.Contains(out, "# Heading") {
		t.Errorf("body content missing: %s", out)
	}
	// Sanity: parses as frontmatter.
	closeIdx := strings.Index(out[4:], "\n---\n")
	if closeIdx <= 0 {
		t.Fatal("malformed frontmatter")
	}
	fm := out[4 : 4+closeIdx]
	parsed, err := frontmatter.Parse(fm)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Frontmatter.Title != "Test Title" {
		t.Errorf("parsed title = %q", parsed.Frontmatter.Title)
	}
	if parsed.Frontmatter.NotionPageID == nil || *parsed.Frontmatter.NotionPageID != "1a2b3c4d5e6f7890123456789abcdef0" {
		t.Errorf("notion id roundtrip broken")
	}
}

func TestFetchPageViaMarkdownAPISucceeds(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pages/abc/markdown", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Notion-Version") != NotionVersion {
			t.Errorf("missing Notion-Version header")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"markdown": "# Hello\n\nBody."})
	})
	mux.HandleFunc("/v1/pages/abc", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"created_time":     "2026-05-18T12:00:00.000Z",
			"last_edited_time": "2026-05-18T12:00:00.000Z",
			"properties": map[string]any{
				"Name": map[string]any{
					"type": "title",
					"title": []any{
						map[string]any{"plain_text": "Hello, "},
						map[string]any{"plain_text": "world"},
					},
				},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	a := NewMarkdownAPIAdapter("test-token").WithBaseURL(srv.URL)
	p, err := a.FetchPage(context.Background(), "abc")
	if err != nil {
		t.Fatal(err)
	}
	if p.Title != "Hello, world" {
		t.Errorf("title = %q; want Hello, world", p.Title)
	}
	if p.ID != "abc" {
		t.Errorf("id = %q", p.ID)
	}
	if !strings.Contains(p.Markdown, "# Hello") {
		t.Errorf("body lost: %q", p.Markdown)
	}
}

func TestMissingPageReturnsNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pages/missing/markdown", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(404) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	a := NewMarkdownAPIAdapter("t").WithBaseURL(srv.URL)
	_, err := a.FetchPage(context.Background(), "missing")
	var nfe *NotFoundError
	if !errors.As(err, &nfe) {
		t.Errorf("err = %v; want *NotFoundError", err)
	}
}

func TestUnauthorizedMapsCorrectly(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pages/abc/markdown", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	a := NewMarkdownAPIAdapter("bad").WithBaseURL(srv.URL)
	_, err := a.FetchPage(context.Background(), "abc")
	if !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("err = %v; want ErrUnauthenticated", err)
	}
}

func TestBlocksAPIRendersHeading(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pages/abc", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"created_time": "2026-05-18T12:00:00.000Z",
			"properties":   map[string]any{},
		})
	})
	mux.HandleFunc("/v1/blocks/abc/children", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{
				map[string]any{
					"type": "heading_1",
					"heading_1": map[string]any{
						"rich_text": []any{map[string]any{"plain_text": "Hi"}},
					},
				},
				map[string]any{
					"type": "paragraph",
					"paragraph": map[string]any{
						"rich_text": []any{map[string]any{"plain_text": "Body."}},
					},
				},
			},
			"has_more": false,
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	a := NewBlocksAPIAdapter("t").WithBaseURL(srv.URL)
	p, err := a.FetchPage(context.Background(), "abc")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.Markdown, "# Hi") {
		t.Errorf("heading missing: %q", p.Markdown)
	}
	if !strings.Contains(p.Markdown, "Body.") {
		t.Errorf("paragraph missing: %q", p.Markdown)
	}
}
