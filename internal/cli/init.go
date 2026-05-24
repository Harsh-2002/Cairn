package cli

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/Harsh-2002/Cairn/internal/render"
)

//go:embed deploy/templates
var deployTemplates embed.FS

func newInitCmd() *cobra.Command {
	var title, baseURL, author, theme, deploy string
	cmd := &cobra.Command{
		Use:   "init [dir]",
		Short: "Scaffold a new Cairn blog.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}
			return runInit(dir, title, baseURL, author, theme, deploy)
		},
	}
	cmd.Flags().StringVar(&title, "title", "A new blog", "Site title.")
	cmd.Flags().StringVar(&baseURL, "base-url", "https://example.com", "Site base URL.")
	cmd.Flags().StringVar(&author, "author", "Anonymous", "Author name.")
	cmd.Flags().StringVar(&theme, "theme", render.DefaultTheme, "Theme: stones (default), drift, press.")
	cmd.Flags().StringVar(&deploy, "deploy", "github-pages", "Deploy strategy: github-pages | cloudflare-pages | none.")
	return cmd
}

func runInit(dir, title, baseURL, author, theme, deploy string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() == ".git" || e.Name() == ".gitignore" {
			continue
		}
		return fmt.Errorf("%s is not empty (contains %s)", dir, e.Name())
	}

	if !isKnownTheme(theme) {
		return fmt.Errorf("unknown theme %q (known: stones, drift, press)", theme)
	}

	tomlBody := fmt.Sprintf(`[site]
title = %q
description = "A new blog built with Cairn."
base_url = %q
author = %q
language = "en"

[content]
posts_dir = "content/posts"

[theme]
name = %q

[deploy]
strategy = %q
`, title, baseURL, author, theme, deploy)

	if err := os.WriteFile(filepath.Join(dir, "cairn.toml"), []byte(tomlBody), 0o644); err != nil {
		return err
	}
	postsDir := filepath.Join(dir, "content", "posts")
	if err := os.MkdirAll(postsDir, 0o755); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	sample := fmt.Sprintf("---\ntitle: \"Hello, world\"\ndate: %s\n---\n\nWelcome to your new Cairn blog.\n", now)
	if err := os.WriteFile(filepath.Join(postsDir, "hello-world.md"), []byte(sample), 0o644); err != nil {
		return err
	}

	if deploy != "none" {
		yml, err := deployYAML(deploy)
		if err != nil {
			return err
		}
		wfDir := filepath.Join(dir, ".github", "workflows")
		if err := os.MkdirAll(wfDir, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(wfDir, "cairn.yml"), yml, 0o644); err != nil {
			return err
		}
	}

	gitignore := "/_site/\n/target/\n"
	_ = os.WriteFile(filepath.Join(dir, ".gitignore"), []byte(gitignore), 0o644)
	fmt.Printf("Initialized Cairn blog in %s\n", dir)
	return nil
}

func deployYAML(strategy string) ([]byte, error) {
	switch strategy {
	case "github-pages":
		return deployTemplates.ReadFile("deploy/templates/github_pages.yml")
	case "cloudflare-pages":
		return deployTemplates.ReadFile("deploy/templates/cloudflare_pages.yml")
	case "none":
		return nil, nil
	}
	return nil, fmt.Errorf("unknown deploy strategy %q", strategy)
}

func isKnownTheme(t string) bool {
	for _, k := range render.KnownThemes {
		if k == t {
			return true
		}
	}
	return false
}
