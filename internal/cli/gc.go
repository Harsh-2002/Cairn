package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func newGCCmd() *cobra.Command {
	var delete bool
	var retentionDays int
	cmd := &cobra.Command{
		Use:   "gc [source]",
		Short: "List (and optionally delete) orphan assets under content/assets/.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			source := "."
			if len(args) == 1 {
				source = args[0]
			}
			return runGC(source, delete, retentionDays)
		},
	}
	cmd.Flags().BoolVar(&delete, "delete", false, "Actually delete the orphans (otherwise reports only).")
	cmd.Flags().IntVar(&retentionDays, "retention-days", 30, "Files newer than this are kept.")
	return cmd
}

func runGC(source string, delete bool, retentionDays int) error {
	assetsDir := filepath.Join(source, "content/assets")
	postsDir := filepath.Join(source, "content/posts")

	if _, err := os.Stat(assetsDir); err != nil {
		fmt.Printf("No content/assets directory under %s — nothing to scan.\n", source)
		return nil
	}

	references, err := collectAssetReferences(postsDir)
	if err != nil {
		return fmt.Errorf("scanning %s: %w", postsDir, err)
	}
	files, err := listAssetFiles(assetsDir)
	if err != nil {
		return err
	}

	retention := time.Duration(retentionDays) * 24 * time.Hour
	now := time.Now()

	var orphans []string
	for _, p := range files {
		base := filepath.Base(p)
		stem := base
		if dot := strings.LastIndex(base, "."); dot > 0 {
			stem = base[:dot]
		}
		if len(stem) != 64 || !isHex(stem) {
			continue
		}
		if _, used := references[stem]; used {
			continue
		}
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		if retention > 0 && now.Sub(info.ModTime()) < retention {
			continue
		}
		orphans = append(orphans, p)
	}

	if len(orphans) == 0 {
		fmt.Println("No orphan assets.")
		return nil
	}
	fmt.Printf("Orphan assets (%d total):\n", len(orphans))
	for _, p := range orphans {
		rel, _ := filepath.Rel(source, p)
		fmt.Println("  " + rel)
	}
	if !delete {
		fmt.Printf("\nRun with --delete to remove. Files newer than %dd are kept.\n", retentionDays)
		return nil
	}
	removed := 0
	for _, p := range orphans {
		if err := os.Remove(p); err != nil {
			fmt.Fprintf(os.Stderr, "  failed to remove %s: %v\n", p, err)
			continue
		}
		removed++
	}
	fmt.Printf("\nRemoved %d of %d orphan files.\n", removed, len(orphans))
	return nil
}

func collectAssetReferences(postsDir string) (map[string]struct{}, error) {
	set := map[string]struct{}{}
	if _, err := os.Stat(postsDir); err != nil {
		return set, nil
	}
	err := filepath.WalkDir(postsDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(p) != ".md" {
			return nil
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for _, sha := range extractAssetSHAs(string(raw)) {
			set[sha] = struct{}{}
		}
		return nil
	})
	return set, err
}

// extractAssetSHAs scans source for 64-hex tokens bordered by non-hex.
func extractAssetSHAs(source string) []string {
	var out []string
	b := []byte(source)
	for i := 0; i+64 <= len(b); {
		window := b[i : i+64]
		if isLowerHexBytes(window) {
			leftOK := i == 0 || !isHexByte(b[i-1])
			rightOK := i+64 == len(b) || !isHexByte(b[i+64])
			if leftOK && rightOK {
				out = append(out, string(window))
				i += 64
				continue
			}
		}
		i++
	}
	return out
}

func listAssetFiles(dir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			out = append(out, p)
		}
		return nil
	})
	return out, err
}

func isHex(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

func isLowerHexBytes(b []byte) bool {
	for _, c := range b {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

func isHexByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}
