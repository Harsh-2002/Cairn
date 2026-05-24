package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Harsh-2002/Cairn/internal/cli"
)

// repoRoot returns the path to the Cairn repository root, two levels above
// this test file (internal/cli/).
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

// TestBuildIsDeterministic runs `cairn build` twice over examples/blog and
// asserts that every output file is byte-identical between runs. This is the
// load-bearing check for Invariant 6.
func TestBuildIsDeterministic(t *testing.T) {
	root := repoRoot(t)
	source := filepath.Join(root, "examples", "blog")
	if _, err := os.Stat(source); err != nil {
		t.Skipf("examples/blog missing: %v", err)
	}
	out1 := t.TempDir()
	out2 := t.TempDir()

	runBuild := func(out string) {
		// Invoke through the cobra tree so the test exercises the same code
		// path as the binary.
		cmd := cli.NewRootCommand()
		cmd.SetArgs([]string{"build", source, "-o", out})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("build %s: %v", out, err)
		}
	}
	runBuild(out1)
	runBuild(out2)

	// Walk out1 and compare each file to its counterpart in out2.
	err := filepath.WalkDir(out1, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(out1, p)
		if err != nil {
			return err
		}
		a, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(filepath.Join(out2, rel))
		if err != nil {
			return err
		}
		if !bytes.Equal(a, b) {
			t.Errorf("non-deterministic: %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestBuildProducesPerPostDirectory asserts the output tree shape matches
// what the Rust binary produces for the same input.
func TestBuildProducesPerPostDirectory(t *testing.T) {
	root := repoRoot(t)
	source := filepath.Join(root, "examples", "blog")
	if _, err := os.Stat(source); err != nil {
		t.Skipf("examples/blog missing: %v", err)
	}
	out := t.TempDir()
	cmd := cli.NewRootCommand()
	cmd.SetArgs([]string{"build", source, "-o", out})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"index.html", "sitemap.xml", "feed.xml", "static"} {
		if _, err := os.Stat(filepath.Join(out, p)); err != nil {
			t.Errorf("expected output: %s missing (%v)", p, err)
		}
	}
	for _, slug := range []string{"hello-world", "leaving-ghost", "the-two-plane-architecture"} {
		if _, err := os.Stat(filepath.Join(out, slug, "index.html")); err != nil {
			t.Errorf("missing post page: %s/index.html (%v)", slug, err)
		}
	}
}
