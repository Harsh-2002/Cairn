package notion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// BlocksAPIAdapter walks the Notion block tree to assemble markdown. Used as
// the fallback when the Markdown Content API is not available (internal
// integration tokens). Handles common block types: heading_1-3, paragraph,
// bulleted_list_item, numbered_list_item, code, quote, divider.
type BlocksAPIAdapter struct {
	baseURL string
	token   string
	client  *http.Client
}

// NewBlocksAPIAdapter constructs an adapter against the production Notion API.
func NewBlocksAPIAdapter(token string) *BlocksAPIAdapter {
	return &BlocksAPIAdapter{
		baseURL: NotionAPI,
		token:   token,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// WithBaseURL overrides the API base URL.
func (a *BlocksAPIAdapter) WithBaseURL(u string) *BlocksAPIAdapter {
	a.baseURL = u
	return a
}

// FetchPage implements Adapter.
func (a *BlocksAPIAdapter) FetchPage(ctx context.Context, pageID string) (Page, error) {
	pageResp, err := a.doGET(ctx, "v1/pages/"+pageID)
	if err != nil {
		return Page{}, err
	}
	defer pageResp.Body.Close()
	switch pageResp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return Page{}, ErrUnauthenticated
	case http.StatusNotFound:
		return Page{}, &NotFoundError{PageID: pageID}
	default:
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

	body, err := a.renderChildren(ctx, pageID)
	if err != nil {
		return Page{}, err
	}
	return Page{ID: pageID, Title: title, Date: date, Markdown: body}, nil
}

// renderChildren fetches block children of parent and renders each to markdown.
// Pagination is handled via has_more / next_cursor.
func (a *BlocksAPIAdapter) renderChildren(ctx context.Context, parent string) (string, error) {
	var b strings.Builder
	cursor := ""
	for {
		path := "v1/blocks/" + parent + "/children?page_size=100"
		if cursor != "" {
			path += "&start_cursor=" + cursor
		}
		resp, err := a.doGET(ctx, path)
		if err != nil {
			return "", err
		}
		var body struct {
			Results    []map[string]any `json:"results"`
			HasMore    bool             `json:"has_more"`
			NextCursor string           `json:"next_cursor"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			return "", &ParseError{Err: err}
		}
		resp.Body.Close()
		for _, block := range body.Results {
			rendered, err := a.renderBlock(ctx, block)
			if err != nil {
				return "", err
			}
			if rendered != "" {
				b.WriteString(rendered)
				if !strings.HasSuffix(rendered, "\n\n") {
					b.WriteString("\n\n")
				}
			}
		}
		if !body.HasMore {
			break
		}
		cursor = body.NextCursor
	}
	return strings.TrimRight(b.String(), "\n") + "\n", nil
}

// renderBlock dispatches a single block to its renderer by type.
func (a *BlocksAPIAdapter) renderBlock(ctx context.Context, block map[string]any) (string, error) {
	kind, _ := block["type"].(string)
	switch kind {
	case "heading_1":
		return "# " + richText(block, kind), nil
	case "heading_2":
		return "## " + richText(block, kind), nil
	case "heading_3":
		return "### " + richText(block, kind), nil
	case "paragraph":
		return richText(block, kind), nil
	case "bulleted_list_item":
		return "- " + richText(block, kind), nil
	case "numbered_list_item":
		return "1. " + richText(block, kind), nil
	case "quote":
		return "> " + richText(block, kind), nil
	case "code":
		lang := ""
		if obj, ok := block["code"].(map[string]any); ok {
			lang, _ = obj["language"].(string)
		}
		return "```" + lang + "\n" + richText(block, "code") + "\n```", nil
	case "divider":
		return "---", nil
	}
	return "", nil
}

// richText extracts the rich_text array under block[kind].rich_text and
// concatenates plain_text segments.
func richText(block map[string]any, kind string) string {
	obj, ok := block[kind].(map[string]any)
	if !ok {
		return ""
	}
	arr, ok := obj["rich_text"].([]any)
	if !ok {
		return ""
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
	return b.String()
}

func (a *BlocksAPIAdapter) doGET(ctx context.Context, path string) (*http.Response, error) {
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
