package core

import (
	"errors"
	"testing"
)

func TestRepoPathAcceptsNormal(t *testing.T) {
	p, err := NewRepoPath("content/posts/hello.md")
	if err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	if p.AsStr() != "content/posts/hello.md" {
		t.Errorf("got %q", p.AsStr())
	}
}

func TestRepoPathRejectsLeadingSlash(t *testing.T) {
	_, err := NewRepoPath("/abs")
	assertKind(t, err, RepoPathLeadingSlash)
}

func TestRepoPathRejectsParentRef(t *testing.T) {
	_, err := NewRepoPath("a/../b")
	assertKind(t, err, RepoPathParentRef)
}

func TestRepoPathRejectsEmptySegment(t *testing.T) {
	_, err := NewRepoPath("a//b")
	assertKind(t, err, RepoPathEmptySegment)
}

func TestRepoPathRejectsBackslash(t *testing.T) {
	_, err := NewRepoPath("a\\b")
	assertKind(t, err, RepoPathBackslash)
}

func TestRepoPathRejectsControl(t *testing.T) {
	_, err := NewRepoPath("a\x00b")
	assertKind(t, err, RepoPathControl)
}

func TestRepoPathRejectsEmpty(t *testing.T) {
	_, err := NewRepoPath("")
	assertKind(t, err, RepoPathEmpty)
}

func TestRepoPathParent(t *testing.T) {
	p, err := NewRepoPath("a/b/c.md")
	if err != nil {
		t.Fatal(err)
	}
	parent, ok := p.Parent()
	if !ok || parent.AsStr() != "a/b" {
		t.Errorf("parent = (%q, %v); want (a/b, true)", parent.AsStr(), ok)
	}
	q, err := NewRepoPath("top.md")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Parent(); ok {
		t.Errorf("expected no parent for top-level path")
	}
}

func TestRepoPathFileNameAndExtension(t *testing.T) {
	p, err := NewRepoPath("a/b/c.md")
	if err != nil {
		t.Fatal(err)
	}
	if p.FileName() != "c.md" {
		t.Errorf("file_name = %q; want c.md", p.FileName())
	}
	if ext, ok := p.Extension(); !ok || ext != "md" {
		t.Errorf("extension = (%q, %v); want (md, true)", ext, ok)
	}
	q, err := NewRepoPath("a/b/Makefile")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Extension(); ok {
		t.Errorf("expected no extension on Makefile")
	}
}

func assertKind(t *testing.T, err error, want RepoPathErrorKind) {
	t.Helper()
	var pe *RepoPathError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *RepoPathError, got %T: %v", err, err)
	}
	if pe.Kind != want {
		t.Errorf("kind = %d; want %d", pe.Kind, want)
	}
}
