package markdown

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAllFixturesAreStable asserts that for every .md fixture under
// testdata/fixtures, Canonical(Canonical(s)) == Canonical(s). This is the
// load-bearing test of Invariant 2 (markdown is the canonical format).
func TestAllFixturesAreStable(t *testing.T) {
	entries, err := os.ReadDir("testdata/fixtures")
	if err != nil {
		t.Fatalf("could not read fixtures dir: %v", err)
	}
	var mdFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			mdFiles = append(mdFiles, e.Name())
		}
	}
	if len(mdFiles) < 30 {
		t.Fatalf("expected at least 30 fixtures, found %d", len(mdFiles))
	}

	var failures []string
	for _, name := range mdFiles {
		src, err := os.ReadFile(filepath.Join("testdata/fixtures", name))
		if err != nil {
			t.Fatalf("read fixture %s: %v", name, err)
		}
		once := Canonical(src)
		twice := Canonical([]byte(once))
		if once != twice {
			failures = append(failures, name)
			t.Logf("%s NOT STABLE\n--- canonical(source) ---\n%s\n--- canonical(canonical(source)) ---\n%s\n", name, once, twice)
		}
	}
	if len(failures) > 0 {
		t.Fatalf("%d of %d fixtures are not roundtrip-stable: %v", len(failures), len(mdFiles), failures)
	}
}

// TestCanonicalFormIsParseable asserts that the canonical form of every
// fixture can itself be parsed and produces non-empty events.
func TestCanonicalFormIsParseable(t *testing.T) {
	entries, err := os.ReadDir("testdata/fixtures")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		src, err := os.ReadFile(filepath.Join("testdata/fixtures", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		once := Canonical(src)
		doc := Parse([]byte(once))
		if doc.ChildCount() == 0 {
			t.Errorf("%s: canonical form parses to zero block children", e.Name())
		}
	}
}
