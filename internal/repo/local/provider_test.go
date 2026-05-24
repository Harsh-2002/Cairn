package local

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/repo"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func initRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	_, err := git.PlainInitWithOptions(dir, &git.PlainInitOptions{
		InitOptions: git.InitOptions{DefaultBranch: plumbing.NewBranchReferenceName("main")},
		Bare:        false,
	})
	if err != nil {
		t.Fatalf("git init: %v", err)
	}
	return dir
}

func newProvider(t *testing.T, dir string) *Provider {
	t.Helper()
	p, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return p.WithAuthor("Test", "test@local")
}

func rp(t *testing.T, s string) core.RepoPath {
	t.Helper()
	r, err := core.NewRepoPath(s)
	if err != nil {
		t.Fatalf("NewRepoPath(%q): %v", s, err)
	}
	return r
}

func TestInitialCommitCreatesBranch(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()

	head, err := p.ResolveRef(ctx, "main")
	if err != nil || head != nil {
		t.Fatalf("expected no main ref, got %v %v", head, err)
	}

	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "README.md"), []byte("# hi")))
	sha, err := p.Commit(ctx, cs, "initial", core.CommitHintPublish, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sha) != 40 {
		t.Errorf("commit ref length = %d; want 40", len(sha))
	}
	head, err = p.ResolveRef(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || *head != sha {
		t.Errorf("head = %v; want %v", head, sha)
	}
}

func TestReadReturnsCommittedBytes(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()

	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "a/b.md"), []byte("hello")))
	if _, err := p.Commit(ctx, cs, "add a/b.md", core.CommitHintPublish, nil); err != nil {
		t.Fatal(err)
	}
	read, err := p.Read(ctx, rp(t, "a/b.md"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.Bytes, []byte("hello")) {
		t.Errorf("bytes = %q; want hello", read.Bytes)
	}
	if read.Path.AsStr() != "a/b.md" {
		t.Errorf("path = %q", read.Path.AsStr())
	}
	if len(read.Blob) != 40 {
		t.Errorf("blob = %q; want 40-hex", read.Blob)
	}
}

func TestReadMissingReturnsNotFound(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()
	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "only.md"), []byte("x")))
	if _, err := p.Commit(ctx, cs, "seed", core.CommitHintPublish, nil); err != nil {
		t.Fatal(err)
	}
	_, err := p.Read(ctx, rp(t, "missing.md"), nil)
	var nfe *repo.NotFoundError
	if !errors.As(err, &nfe) {
		t.Errorf("err = %v; want *NotFoundError", err)
	}
}

func TestListReturnsSortedFilesUnderPrefix(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()
	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "posts/b.md"), []byte("b")))
	cs.Push(core.Write(rp(t, "posts/a.md"), []byte("a")))
	cs.Push(core.Write(rp(t, "assets/img.png"), []byte("png")))
	if _, err := p.Commit(ctx, cs, "seed", core.CommitHintPublish, nil); err != nil {
		t.Fatal(err)
	}
	posts, err := p.List(ctx, rp(t, "posts"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(posts) != 2 {
		t.Fatalf("len = %d; want 2", len(posts))
	}
	if posts[0].Path.AsStr() != "posts/a.md" || posts[1].Path.AsStr() != "posts/b.md" {
		t.Errorf("got %v %v", posts[0].Path, posts[1].Path)
	}
}

func TestMultiFileCommitIsAtomicAndObservable(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()
	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "one.md"), []byte("1")))
	cs.Push(core.Write(rp(t, "two.md"), []byte("2")))
	cs.Push(core.Write(rp(t, "nested/three.md"), []byte("3")))
	sha, err := p.Commit(ctx, cs, "three at once", core.CommitHintPublish, nil)
	if err != nil {
		t.Fatal(err)
	}
	head, _ := p.ResolveRef(ctx, "main")
	if head == nil || *head != sha {
		t.Errorf("head = %v; want %v", head, sha)
	}
	for _, c := range []struct {
		path, want string
	}{{"one.md", "1"}, {"two.md", "2"}, {"nested/three.md", "3"}} {
		r, err := p.Read(ctx, rp(t, c.path), nil)
		if err != nil {
			t.Fatalf("read %s: %v", c.path, err)
		}
		if string(r.Bytes) != c.want {
			t.Errorf("read %s = %q; want %q", c.path, r.Bytes, c.want)
		}
	}
}

func TestDeleteRemovesFileFromTree(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()
	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "keep.md"), []byte("k")))
	cs.Push(core.Write(rp(t, "drop.md"), []byte("d")))
	if _, err := p.Commit(ctx, cs, "seed", core.CommitHintPublish, nil); err != nil {
		t.Fatal(err)
	}
	var cs2 core.FileChangeSet
	cs2.Push(core.Delete(rp(t, "drop.md")))
	if _, err := p.Commit(ctx, cs2, "delete drop", core.CommitHintPublish, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Read(ctx, rp(t, "keep.md"), nil); err != nil {
		t.Errorf("keep.md should still exist, got %v", err)
	}
	_, err := p.Read(ctx, rp(t, "drop.md"), nil)
	var nfe *repo.NotFoundError
	if !errors.As(err, &nfe) {
		t.Errorf("drop.md err = %v; want NotFoundError", err)
	}
}

func TestOptimisticConcurrencyRejectsStaleExpectedHead(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()

	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "a.md"), []byte("1")))
	first, err := p.Commit(ctx, cs, "first", core.CommitHintPublish, nil)
	if err != nil {
		t.Fatal(err)
	}
	var cs2 core.FileChangeSet
	cs2.Push(core.Write(rp(t, "b.md"), []byte("2")))
	if _, err := p.Commit(ctx, cs2, "second", core.CommitHintPublish, nil); err != nil {
		t.Fatal(err)
	}
	var cs3 core.FileChangeSet
	cs3.Push(core.Write(rp(t, "c.md"), []byte("3")))
	_, err = p.Commit(ctx, cs3, "third", core.CommitHintPublish, &first)
	if !errors.Is(err, repo.ErrConflict) {
		t.Errorf("err = %v; want ErrConflict", err)
	}
}

func TestOptimisticConcurrencyAcceptsCurrentExpectedHead(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()

	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "a.md"), []byte("1")))
	first, err := p.Commit(ctx, cs, "first", core.CommitHintPublish, nil)
	if err != nil {
		t.Fatal(err)
	}
	var cs2 core.FileChangeSet
	cs2.Push(core.Write(rp(t, "b.md"), []byte("2")))
	second, err := p.Commit(ctx, cs2, "second", core.CommitHintPublish, &first)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) == 0 {
		t.Errorf("commit ref empty")
	}
}

func TestResolveRefMissingReturnsNil(t *testing.T) {
	dir := initRepo(t)
	p := newProvider(t, dir)
	ctx := context.Background()
	r, err := p.ResolveRef(ctx, "does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		t.Errorf("expected nil, got %v", *r)
	}
}
