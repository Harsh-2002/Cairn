// Package notion implements the Cairn Notion adapter. Materializes Notion
// pages into conforming Cairn markdown files via one of two paths behind
// the same Adapter interface:
//
//   - MarkdownAPIAdapter (preferred): public integrations, one call per page.
//   - BlocksAPIAdapter (fallback): walks the block tree for internal tokens.
//
// After ingestion the result is indistinguishable from a post authored in
// any other editor.
package notion

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Harsh-2002/Cairn/internal/markdown"
)

// Notion API constants.
const (
	NotionAPI     = "https://api.notion.com"
	NotionVersion = "2026-03-11"
)

// Page is a page ingested from Notion. The markdown body should be passed
// through markdown.Canonical before writing to disk.
type Page struct {
	ID       string
	Title    string
	Date     time.Time
	Markdown string
}

// Adapter fetches a Notion page by ID (32 hex chars, no hyphens).
type Adapter interface {
	FetchPage(ctx context.Context, pageID string) (Page, error)
}

// Sentinel errors. Use errors.Is to test.
var (
	ErrUnauthenticated = errors.New("Notion: authentication failed")
)

// NotFoundError signals a missing page.
type NotFoundError struct{ PageID string }

func (e *NotFoundError) Error() string { return fmt.Sprintf("Notion: page not found: %s", e.PageID) }

// APIError wraps a non-success status.
type APIError struct{ Msg string }

func (e *APIError) Error() string { return "Notion API error: " + e.Msg }

// ParseError wraps a malformed response.
type ParseError struct{ Err error }

func (e *ParseError) Error() string { return "Notion parse error: " + e.Err.Error() }
func (e *ParseError) Unwrap() error { return e.Err }

// NetworkError wraps a transport failure.
type NetworkError struct{ Err error }

func (e *NetworkError) Error() string { return "Notion network error: " + e.Err.Error() }
func (e *NetworkError) Unwrap() error { return e.Err }

// RenderToMarkdown materializes a Page into a full markdown file with
// frontmatter. The body is canonicalised; the result is ready to write.
func RenderToMarkdown(page Page, slug string) string {
	body := markdown.Canonical([]byte(page.Markdown))
	body = strings.TrimLeft(body, "\n")
	return fmt.Sprintf(
		"---\ntitle: %q\ndate: %s\nslug: %s\nnotion_page_id: %s\n---\n\n%s",
		page.Title,
		page.Date.UTC().Format(time.RFC3339),
		slug,
		page.ID,
		body,
	)
}

// extractTitle finds the page property whose type is "title" and concatenates
// its rich-text segments.
func extractTitle(properties map[string]any) string {
	for _, v := range properties {
		obj, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if obj["type"] != "title" {
			continue
		}
		arr, ok := obj["title"].([]any)
		if !ok {
			continue
		}
		var b strings.Builder
		for _, seg := range arr {
			segObj, ok := seg.(map[string]any)
			if !ok {
				continue
			}
			if pt, ok := segObj["plain_text"].(string); ok {
				b.WriteString(pt)
			}
		}
		s := strings.TrimSpace(b.String())
		if s != "" {
			return b.String()
		}
	}
	return ""
}
