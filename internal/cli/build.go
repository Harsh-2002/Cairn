package cli

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/Harsh-2002/Cairn/internal/render"
)

func newBuildCmd() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "build [source]",
		Short: "Render the site to a static output directory.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			source := "."
			if len(args) == 1 {
				source = args[0]
			}
			return runBuild(source, output)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "_site", "Output directory.")
	return cmd
}

func runBuild(source, output string) error {
	cfg, err := loadConfig(source)
	if err != nil {
		return err
	}
	if cfg.Site.Title == "" {
		cfg.Site = render.DefaultSiteConfig()
	}
	postsRoot := filepath.Join(source, cfg.Content.PostsDir)
	posts, err := loadPosts(source, postsRoot)
	if err != nil {
		return err
	}
	if len(posts) == 0 {
		return fmt.Errorf("no posts found under %s — write at least one before building", postsRoot)
	}
	r, err := render.NewRenderer(cfg.Site, cfg.Theme.Name, source)
	if err != nil {
		return err
	}
	if err := r.BuildToDir(posts, output); err != nil {
		return err
	}
	fmt.Printf("Built %d posts to %s\n", len(posts), output)
	return nil
}
