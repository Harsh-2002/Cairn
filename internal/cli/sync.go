package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Harsh-2002/Cairn/internal/frontmatter"
	"github.com/Harsh-2002/Cairn/internal/notion"
)

func newSyncCmd() *cobra.Command {
	var blocksAPI bool
	var notionToken string
	cmd := &cobra.Command{
		Use:   "sync <notion-page-id>",
		Short: "Pull a Notion page into content/posts/<slug>.md.",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSync(args[0], blocksAPI, notionToken)
		},
	}
	cmd.Flags().BoolVar(&blocksAPI, "blocks-api", false, "Use the Blocks API fallback (internal-integration tokens).")
	cmd.Flags().StringVar(&notionToken, "notion-token", "", "Notion integration token; falls back to NOTION_TOKEN env var.")
	return cmd
}

func runSync(pageID string, blocksAPI bool, tokenFlag string) error {
	token := tokenFlag
	if token == "" {
		token = os.Getenv("NOTION_TOKEN")
	}
	if token == "" {
		return fmt.Errorf("no Notion token — pass --notion-token or set NOTION_TOKEN")
	}
	cleanID := strings.ReplaceAll(pageID, "-", "")
	var adapter notion.Adapter
	if blocksAPI {
		adapter = notion.NewBlocksAPIAdapter(token)
	} else {
		adapter = notion.NewMarkdownAPIAdapter(token)
	}
	page, err := adapter.FetchPage(context.Background(), cleanID)
	if err != nil {
		return fmt.Errorf("fetching Notion page: %w", err)
	}
	slug, ok := frontmatter.DeriveSlug(page.Title)
	if !ok {
		return fmt.Errorf("could not derive slug from title %q", page.Title)
	}
	postsDir := "content/posts"
	if err := os.MkdirAll(postsDir, 0o755); err != nil {
		return err
	}
	target := filepath.Join(postsDir, slug+".md")
	body := notion.RenderToMarkdown(page, slug)
	if err := os.WriteFile(target, []byte(body), 0o644); err != nil {
		return err
	}
	fmt.Printf("Synced Notion page %s → %s\n", cleanID, target)
	return nil
}
