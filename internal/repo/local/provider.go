// Package local implements repo.Provider over a local working copy via go-git.
package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/repo"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
)

// Provider is a repo.Provider over a local on-disk git repository.
type Provider struct {
	repoPath    string
	branch      string
	authorName  string
	authorEmail string
	clock       func() time.Time

	mu   sync.Mutex
	repo *git.Repository
}

// Open returns a Provider pointing at an existing git repository.
func Open(repoPath string) (*Provider, error) {
	r, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, &repo.BackendError{Err: err}
	}
	return &Provider{
		repoPath:    repoPath,
		branch:      "main",
		authorName:  "Cairn",
		authorEmail: "cairn@local",
		clock:       time.Now,
		repo:        r,
	}, nil
}

// WithBranch sets the branch this provider commits to. Default "main".
func (p *Provider) WithBranch(branch string) *Provider {
	p.branch = branch
	return p
}

// WithAuthor sets the author/committer identity. Default "Cairn <cairn@local>".
func (p *Provider) WithAuthor(name, email string) *Provider {
	p.authorName = name
	p.authorEmail = email
	return p
}

// WithClock overrides time.Now for tests that need deterministic commit times.
func (p *Provider) WithClock(clock func() time.Time) *Provider {
	p.clock = clock
	return p
}

func (p *Provider) refName() plumbing.ReferenceName {
	return plumbing.NewBranchReferenceName(p.branch)
}

func (p *Provider) signature() object.Signature {
	return object.Signature{Name: p.authorName, Email: p.authorEmail, When: p.clock()}
}

// treeAt returns the tree at the given commit ref, or HEAD of the configured
// branch when at is nil. Returns (nil, nil) when the branch has no commits yet.
func (p *Provider) treeAt(at *core.CommitRef) (*object.Tree, error) {
	var commitHash plumbing.Hash
	if at != nil {
		commitHash = plumbing.NewHash(string(*at))
		if commitHash.IsZero() {
			return nil, &repo.InvalidInputError{Msg: fmt.Sprintf("invalid commit ref %q", *at)}
		}
	} else {
		ref, err := p.repo.Reference(p.refName(), true)
		if err != nil {
			if errors.Is(err, plumbing.ErrReferenceNotFound) {
				return nil, nil
			}
			return nil, &repo.BackendError{Err: err}
		}
		commitHash = ref.Hash()
	}
	commit, err := p.repo.CommitObject(commitHash)
	if err != nil {
		return nil, &repo.BackendError{Err: err}
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, &repo.BackendError{Err: err}
	}
	return tree, nil
}

// Read implements repo.Provider.
func (p *Provider) Read(ctx context.Context, rp core.RepoPath, at *core.CommitRef) (repo.FileRead, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return repo.FileRead{}, err
	}
	tree, err := p.treeAt(at)
	if err != nil {
		return repo.FileRead{}, err
	}
	if tree == nil {
		return repo.FileRead{}, &repo.RefNotFoundError{Ref: p.refName().String()}
	}
	entry, err := tree.File(rp.AsStr())
	if err != nil {
		if errors.Is(err, object.ErrFileNotFound) {
			return repo.FileRead{}, &repo.NotFoundError{Path: rp}
		}
		return repo.FileRead{}, &repo.BackendError{Err: err}
	}
	bytes, err := readBlobBytes(entry)
	if err != nil {
		return repo.FileRead{}, &repo.BackendError{Err: err}
	}
	return repo.FileRead{
		Path:  rp,
		Bytes: bytes,
		Blob:  core.BlobRef(entry.Hash.String()),
	}, nil
}

func readBlobBytes(f *object.File) ([]byte, error) {
	r, err := f.Blob.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// List implements repo.Provider.
func (p *Provider) List(ctx context.Context, prefix core.RepoPath, at *core.CommitRef) ([]repo.TreeEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tree, err := p.treeAt(at)
	if err != nil {
		return nil, err
	}
	if tree == nil {
		return nil, nil
	}
	var out []repo.TreeEntry
	prefixStr := prefix.AsStr()
	err = tree.Files().ForEach(func(f *object.File) error {
		if !strings.HasPrefix(f.Name, prefixStr) {
			return nil
		}
		rp, perr := core.NewRepoPath(f.Name)
		if perr != nil {
			return nil
		}
		out = append(out, repo.TreeEntry{
			Path: rp,
			Blob: core.BlobRef(f.Hash.String()),
			Size: f.Size,
		})
		return nil
	})
	if err != nil {
		return nil, &repo.BackendError{Err: err}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path.AsStr() < out[j].Path.AsStr() })
	return out, nil
}

// Commit implements repo.Provider.
func (p *Provider) Commit(ctx context.Context, changes core.FileChangeSet, message string, hint core.CommitHint, expectedHead *core.CommitRef) (core.CommitRef, error) {
	_ = hint
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	ref, err := p.repo.Reference(p.refName(), true)
	var parentHash plumbing.Hash
	var parent *object.Commit
	switch {
	case err == nil:
		parentHash = ref.Hash()
		parent, err = p.repo.CommitObject(parentHash)
		if err != nil {
			return "", &repo.BackendError{Err: err}
		}
	case errors.Is(err, plumbing.ErrReferenceNotFound):
		// No parent yet; first commit on the branch.
	default:
		return "", &repo.BackendError{Err: err}
	}
	if expectedHead != nil {
		actual := ""
		if parent != nil {
			actual = parent.Hash.String()
		}
		if actual != string(*expectedHead) {
			return "", repo.ErrConflict
		}
	}
	var baseTree *object.Tree
	if parent != nil {
		baseTree, err = parent.Tree()
		if err != nil {
			return "", &repo.BackendError{Err: err}
		}
	}
	treeHash, err := applyChanges(p.repo, changes, baseTree)
	if err != nil {
		return "", err
	}
	sig := p.signature()
	commit := &object.Commit{
		Author:    sig,
		Committer: sig,
		Message:   message,
		TreeHash:  treeHash,
	}
	if !parentHash.IsZero() {
		commit.ParentHashes = []plumbing.Hash{parentHash}
	}
	commitHash, err := encodeAndStore(p.repo.Storer, commit)
	if err != nil {
		return "", &repo.BackendError{Err: err}
	}
	if err := p.setRef(p.refName(), commitHash); err != nil {
		return "", err
	}
	return core.CommitRef(commitHash.String()), nil
}

// ForceSetRef implements repo.Provider.
func (p *Provider) ForceSetRef(ctx context.Context, branch string, tree core.TreeRef, message string) (core.CommitRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	treeHash := plumbing.NewHash(string(tree))
	if treeHash.IsZero() {
		return "", &repo.InvalidInputError{Msg: fmt.Sprintf("invalid tree ref %q", tree)}
	}
	if _, err := p.repo.TreeObject(treeHash); err != nil {
		return "", &repo.BackendError{Err: err}
	}
	sig := p.signature()
	commit := &object.Commit{
		Author:    sig,
		Committer: sig,
		Message:   message,
		TreeHash:  treeHash,
	}
	commitHash, err := encodeAndStore(p.repo.Storer, commit)
	if err != nil {
		return "", &repo.BackendError{Err: err}
	}
	if err := p.setRef(plumbing.NewBranchReferenceName(branch), commitHash); err != nil {
		return "", err
	}
	return core.CommitRef(commitHash.String()), nil
}

// ForceCommitToBranch implements repo.Provider.
func (p *Provider) ForceCommitToBranch(ctx context.Context, branch string, changes core.FileChangeSet, message string) (core.CommitRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	refName := plumbing.NewBranchReferenceName(branch)
	var parentHash plumbing.Hash
	var parent *object.Commit
	ref, err := p.repo.Reference(refName, true)
	switch {
	case err == nil:
		parentHash = ref.Hash()
		parent, err = p.repo.CommitObject(parentHash)
		if err != nil {
			return "", &repo.BackendError{Err: err}
		}
	case errors.Is(err, plumbing.ErrReferenceNotFound):
	default:
		return "", &repo.BackendError{Err: err}
	}
	var baseTree *object.Tree
	if parent != nil {
		baseTree, err = parent.Tree()
		if err != nil {
			return "", &repo.BackendError{Err: err}
		}
	}
	treeHash, err := applyChanges(p.repo, changes, baseTree)
	if err != nil {
		return "", err
	}
	sig := p.signature()
	commit := &object.Commit{
		Author:    sig,
		Committer: sig,
		Message:   message,
		TreeHash:  treeHash,
	}
	if !parentHash.IsZero() {
		commit.ParentHashes = []plumbing.Hash{parentHash}
	}
	commitHash, err := encodeAndStore(p.repo.Storer, commit)
	if err != nil {
		return "", &repo.BackendError{Err: err}
	}
	if err := p.setRef(refName, commitHash); err != nil {
		return "", err
	}
	return core.CommitRef(commitHash.String()), nil
}

// DeleteBranch implements repo.Provider.
func (p *Provider) DeleteBranch(ctx context.Context, branch string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	refName := plumbing.NewBranchReferenceName(branch)
	if err := p.repo.Storer.RemoveReference(refName); err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return nil
		}
		return &repo.BackendError{Err: err}
	}
	return nil
}

// ResolveRef implements repo.Provider.
func (p *Provider) ResolveRef(ctx context.Context, name string) (*core.CommitRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var refName plumbing.ReferenceName
	if strings.HasPrefix(name, "refs/") {
		refName = plumbing.ReferenceName(name)
	} else {
		refName = plumbing.NewBranchReferenceName(name)
	}
	ref, err := p.repo.Reference(refName, true)
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return nil, nil
		}
		return nil, &repo.BackendError{Err: err}
	}
	c, err := p.repo.CommitObject(ref.Hash())
	if err != nil {
		return nil, &repo.BackendError{Err: err}
	}
	cr := core.CommitRef(c.Hash.String())
	return &cr, nil
}

// setRef force-updates a reference to point at hash.
func (p *Provider) setRef(name plumbing.ReferenceName, hash plumbing.Hash) error {
	if err := p.repo.Storer.SetReference(plumbing.NewHashReference(name, hash)); err != nil {
		return &repo.BackendError{Err: err}
	}
	return nil
}

// encodeAndStore writes any object.Object-like value to the storer and
// returns its hash.
func encodeAndStore(s storer.EncodedObjectStorer, c *object.Commit) (plumbing.Hash, error) {
	obj := s.NewEncodedObject()
	if err := c.Encode(obj); err != nil {
		return plumbing.ZeroHash, err
	}
	return s.SetEncodedObject(obj)
}
