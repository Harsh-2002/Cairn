package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/Harsh-2002/Cairn/internal/frontmatter"
)

func newNewCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "new <title>",
		Short: "Scaffold a new draft post.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runNew(args[0])
		},
	}
}

func runNew(title string) error {
	slug, ok := frontmatter.DeriveSlug(title)
	if !ok {
		return fmt.Errorf("title %q derives an empty slug — pick a different title", title)
	}
	postsDir := "content/posts"
	if err := os.MkdirAll(postsDir, 0o755); err != nil {
		return err
	}
	target := filepath.Join(postsDir, slug+".md")
	if _, err := os.Stat(target); err == nil {
		return fmt.Errorf("%s already exists — pick a different title or move the existing file", target)
	}
	// time.Now is allowed here: cairn new is a one-shot scaffold action.
	// The resulting file is committed by the user; subsequent builds use the
	// committed timestamp, not the wall clock.
	now := time.Now().UTC().Format(time.RFC3339)
	content := fmt.Sprintf("---\ntitle: %q\ndate: %s\ndraft: true\n---\n\nWrite here.\n",
		strings.ReplaceAll(title, `"`, `\"`), now)
	if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
		return err
	}
	fmt.Printf("Created %s\n", target)
	return nil
}
