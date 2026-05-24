package notion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// MarkdownAPIAdapter pulls a page using Notion's Markdown Content API.
// Public integrations only — internal tokens should use BlocksAPIAdapter.
type MarkdownAPIAdapter struct {
	baseURL string
	token   string
	client  *http.Client
}

// NewMarkdownAPIAdapter constructs an adapter using the production Notion API.
func NewMarkdownAPIAdapter(token string) *MarkdownAPIAdapter {
	return &MarkdownAPIAdapter{
		baseURL: NotionAPI,
		token:   token,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// WithBaseURL overrides the base URL (for tests/staging).
func (a *MarkdownAPIAdapter) WithBaseURL(u string) *MarkdownAPIAdapter {
	a.baseURL = u
	return a
}

// FetchPage implements Adapter.
func (a *MarkdownAPIAdapter) FetchPage(ctx context.Context, pageID string) (Page, error) {
	mdResp, err := a.doGET(ctx, "v1/pages/"+pageID+"/markdown")
	if err != nil {
		return Page{}, err
	}
	defer mdResp.Body.Close()
	switch mdResp.StatusCode {
	case http.StatusOK:
		// ok
	case http.StatusUnauthorized, http.StatusForbidden:
		return Page{}, ErrUnauthenticated
	case http.StatusNotFound:
		return Page{}, &NotFoundError{PageID: pageID}
	default:
		return Page{}, &APIError{Msg: "markdown endpoint: " + mdResp.Status}
	}
	var mdBody struct {
		Markdown string `json:"markdown"`
	}
	if err := json.NewDecoder(mdResp.Body).Decode(&mdBody); err != nil {
		return Page{}, &ParseError{Err: err}
	}

	pageResp, err := a.doGET(ctx, "v1/pages/"+pageID)
	if err != nil {
		return Page{}, err
	}
	defer pageResp.Body.Close()
	if pageResp.StatusCode < 200 || pageResp.StatusCode >= 300 {
		return Page{}, &APIError{Msg: "page endpoint: " + pageResp.Status}
	}
	var page struct {
		CreatedTime    string         `json:"created_time"`
		LastEditedTime string         `json:"last_edited_time"`
		Properties     map[string]any `json:"properties"`
	}
	if err := json.NewDecoder(pageResp.Body).Decode(&page); err != nil {
		return Page{}, &ParseError{Err: err}
	}

	title := extractTitle(page.Properties)
	if title == "" {
		title = "Untitled"
	}
	date, err := time.Parse(time.RFC3339, page.CreatedTime)
	if err != nil {
		date, err = time.Parse(time.RFC3339, page.LastEditedTime)
		if err != nil {
			return Page{}, &ParseError{Err: fmt.Errorf("date: %w", err)}
		}
	}
	return Page{
		ID:       pageID,
		Title:    title,
		Date:     date,
		Markdown: mdBody.Markdown,
	}, nil
}

func (a *MarkdownAPIAdapter) doGET(ctx context.Context, path string) (*http.Response, error) {
	url := strings.TrimRight(a.baseURL, "/") + "/" + strings.TrimLeft(path, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &NetworkError{Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Notion-Version", NotionVersion)
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, &NetworkError{Err: err}
	}
	return resp, nil
}

// drainAndClose is a helper for callers that need to discard a response body.
func drainAndClose(r io.Reader, closer io.Closer) {
	_, _ = io.Copy(io.Discard, r)
	_ = closer.Close()
}
