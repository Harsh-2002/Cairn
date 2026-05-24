// Package cli wires the cobra subcommands for the cairn binary.
package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/frontmatter"
	"github.com/Harsh-2002/Cairn/internal/markdown"
	"github.com/Harsh-2002/Cairn/internal/render"
)

// NewRootCommand builds the cobra command tree.
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "cairn",
		Short: "A stateless static blog generator.",
	}
	root.AddCommand(
		newBuildCmd(),
		newNewCmd(),
		newInitCmd(),
		newGCCmd(),
		newSyncCmd(),
		newMigrateCmd(),
		newUpgradeCmd(),
		newServeCmd(),
	)
	return root
}

// Execute runs the root command. Called from cmd/cairn/main.go.
func Execute() error {
	return NewRootCommand().Execute()
}

// config mirrors the layout of cairn.toml.
type config struct {
	Site    render.SiteConfig `toml:"site"`
	Content contentConfig     `toml:"content"`
	Theme   themeConfig       `toml:"theme"`
	Deploy  deployConfig      `toml:"deploy"`
}

type contentConfig struct {
	PostsDir string `toml:"posts_dir"`
}

type themeConfig struct {
	Name string `toml:"name"`
}

type deployConfig struct {
	Strategy string `toml:"strategy"`
}

// loadConfig reads <source>/cairn.toml.
func loadConfig(source string) (*config, error) {
	data, err := os.ReadFile(filepath.Join(source, "cairn.toml"))
	if err != nil {
		return nil, fmt.Errorf("reading cairn.toml: %w", err)
	}
	var cfg config
	if _, err := toml.Decode(string(data), &cfg); err != nil {
		return nil, fmt.Errorf("parsing cairn.toml: %w", err)
	}
	if cfg.Content.PostsDir == "" {
		cfg.Content.PostsDir = "content/posts"
	}
	if cfg.Theme.Name == "" {
		cfg.Theme.Name = render.DefaultTheme
	}
	return &cfg, nil
}

// loadPosts walks postsRoot and returns parsed Post values, sorted by source path.
func loadPosts(sourceRoot, postsRoot string) ([]core.Post, error) {
	info, err := os.Stat(postsRoot)
	if err != nil || !info.IsDir() {
		return nil, nil
	}
	var posts []core.Post
	err = filepath.WalkDir(postsRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		rp, err := core.NewRepoPath(rel)
		if err != nil {
			return fmt.Errorf("path %s: %w", rel, err)
		}
		post, err := parsePost(string(raw), rp)
		if err != nil {
			return fmt.Errorf("parsing %s: %w", path, err)
		}
		posts = append(posts, post)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(posts, func(i, j int) bool { return posts[i].SourcePath.AsStr() < posts[j].SourcePath.AsStr() })
	return posts, nil
}

// parsePost splits the frontmatter + body and canonicalises the body.
func parsePost(source string, rp core.RepoPath) (core.Post, error) {
	source = strings.TrimPrefix(source, "\ufeff")
	if !strings.HasPrefix(source, "---\n") {
		return core.Post{}, fmt.Errorf("post must start with `---` frontmatter delimiter")
	}
	rest := source[4:]
	closeIdx := strings.Index(rest, "\n---\n")
	bodyStart := -1
	switch {
	case closeIdx >= 0:
		bodyStart = closeIdx + len("\n---\n")
	case strings.HasSuffix(rest, "\n---"):
		closeIdx = len(rest) - 4
		bodyStart = len(rest)
	default:
		return core.Post{}, fmt.Errorf("missing closing `---` delimiter")
	}
	fmYAML := rest[:closeIdx]
	body := ""
	if bodyStart >= 0 && bodyStart < len(rest) {
		body = rest[bodyStart:]
	}
	body = strings.TrimLeft(body, "\n")

	parsed, err := frontmatter.Parse(fmYAML)
	if err != nil {
		return core.Post{}, fmt.Errorf("frontmatter: %w", err)
	}
	for _, k := range parsed.UnknownKeys {
		fmt.Fprintf(os.Stderr, "warning: unknown frontmatter key `%s` in %s\n", k, rp.AsStr())
	}
	return core.Post{
		Frontmatter: parsed.Frontmatter,
		Body:        markdown.Canonical([]byte(body)),
		SourcePath:  rp,
	}, nil
}
