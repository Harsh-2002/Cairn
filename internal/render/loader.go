package render

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed themes
var themesFS embed.FS

// KnownThemes is the set of theme names bundled in the binary.
var KnownThemes = []string{"stones", "drift", "press"}

// DefaultTheme is the theme used when the caller does not specify one.
const DefaultTheme = "stones"

// Loader resolves templates and static assets from one of:
//  1. <source_root>/templates/<name> on disk (user override)
//  2. embedded themes/<theme>/templates/<name> (bundled)
type Loader struct {
	sourceRoot string
	theme      string
}

// NewLoader constructs a Loader for the given user source root and theme.
func NewLoader(sourceRoot, theme string) *Loader {
	return &Loader{sourceRoot: sourceRoot, theme: theme}
}

// LoaderError categorises Loader failures.
type LoaderError struct {
	Kind LoaderErrorKind
	Name string // template name (for NotFound) or theme name (for UnknownTheme)
}

// LoaderErrorKind discriminates LoaderError.
type LoaderErrorKind int

const (
	LoaderErrNotFound LoaderErrorKind = iota
	LoaderErrUnknownTheme
)

func (e *LoaderError) Error() string {
	switch e.Kind {
	case LoaderErrNotFound:
		return fmt.Sprintf("template %q not found", e.Name)
	case LoaderErrUnknownTheme:
		return fmt.Sprintf("unknown theme %q — known: %s", e.Name, strings.Join(KnownThemes, ", "))
	}
	return "loader error"
}

// Resolve returns the source of the named template. User overrides at
// <sourceRoot>/templates/<name> win over the bundled theme.
func (l *Loader) Resolve(name string) (string, error) {
	if l.sourceRoot != "" {
		override := filepath.Join(l.sourceRoot, "templates", name)
		if data, err := os.ReadFile(override); err == nil {
			return string(data), nil
		}
	}
	if !isKnownTheme(l.theme) {
		return "", &LoaderError{Kind: LoaderErrUnknownTheme, Name: l.theme}
	}
	key := "themes/" + l.theme + "/templates/" + name
	data, err := themesFS.ReadFile(key)
	if err != nil {
		return "", &LoaderError{Kind: LoaderErrNotFound, Name: name}
	}
	return string(data), nil
}

// CopyStatic copies the active theme's static/ directory into <output>/static/.
// Idempotent: re-running on identical inputs produces byte-identical files.
func (l *Loader) CopyStatic(outputDir string) error {
	if !isKnownTheme(l.theme) {
		return &LoaderError{Kind: LoaderErrUnknownTheme, Name: l.theme}
	}
	prefix := "themes/" + l.theme + "/static/"
	destRoot := filepath.Join(outputDir, "static")

	var keys []string
	err := fs.WalkDir(themesFS, "themes/"+l.theme+"/static", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		keys = append(keys, p)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(keys)
	for _, key := range keys {
		rel := strings.TrimPrefix(key, prefix)
		dst := filepath.Join(destRoot, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		data, err := themesFS.ReadFile(key)
		if err != nil {
			return err
		}
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func isKnownTheme(t string) bool {
	for _, k := range KnownThemes {
		if k == t {
			return true
		}
	}
	return false
}
