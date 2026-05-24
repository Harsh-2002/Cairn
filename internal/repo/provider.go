// Package repo defines the Cairn RepositoryProvider abstraction and shared
// types. Every read and write of the source of truth flows through one of
// these interfaces; no code outside this package and its subpackages reaches
// for go-git or the GitHub REST client directly.
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Harsh-2002/Cairn/internal/core"
)

// Provider is the single seam through which Cairn touches a source-of-truth
// git repository. Implementations: local (go-git over a working copy) and
// github (REST API, no local clone).
type Provider interface {
	// Read returns the bytes of a single file at the given commit (or at the
	// branch HEAD if at is nil).
	Read(ctx context.Context, path core.RepoPath, at *core.CommitRef) (FileRead, error)

	// List returns every blob path under prefix at the given commit (or branch
	// HEAD), sorted lexically.
	List(ctx context.Context, prefix core.RepoPath, at *core.CommitRef) ([]TreeEntry, error)

	// Commit applies changes atomically as a new commit on the provider's
	// configured branch. When expectedHead is non-nil and the branch HEAD does
	// not match, returns ErrConflict (optimistic concurrency check).
	Commit(ctx context.Context, changes core.FileChangeSet, message string, hint core.CommitHint, expectedHead *core.CommitRef) (core.CommitRef, error)

	// ForceSetRef creates a parentless commit pointing at tree and force-moves
	// branch to it. Used by editors that hold a precomputed tree.
	ForceSetRef(ctx context.Context, branch string, tree core.TreeRef, message string) (core.CommitRef, error)

	// ForceCommitToBranch applies changes and force-pushes the result to
	// branch. Used for autosaves to cairn/drafts/<slug>/<session> branches.
	ForceCommitToBranch(ctx context.Context, branch string, changes core.FileChangeSet, message string) (core.CommitRef, error)

	// DeleteBranch removes a branch ref (no-op if missing).
	DeleteBranch(ctx context.Context, branch string) error

	// ResolveRef returns the commit at the given ref, or nil if it doesn't exist.
	ResolveRef(ctx context.Context, name string) (*core.CommitRef, error)
}

// FileRead is the result of a single-file read.
type FileRead struct {
	Path  core.RepoPath
	Bytes []byte
	Blob  core.BlobRef
}

// TreeEntry is one item in a directory listing.
type TreeEntry struct {
	Path core.RepoPath
	Blob core.BlobRef
	Size int64
}

// Sentinel errors. Use errors.Is to test for these.
var (
	ErrConflict        = errors.New("commit rejected: branch HEAD moved past expected sha (optimistic concurrency conflict)")
	ErrUnauthenticated = errors.New("authentication failed")
)

// NotFoundError is returned when a path does not exist at the given commit.
type NotFoundError struct{ Path core.RepoPath }

func (e *NotFoundError) Error() string { return fmt.Sprintf("file not found: %s", e.Path) }

// RefNotFoundError is returned when a branch or other ref does not exist.
type RefNotFoundError struct{ Ref string }

func (e *RefNotFoundError) Error() string { return fmt.Sprintf("ref not found: %s", e.Ref) }

// RateLimitedError signals that the backend is rate-limiting us; retry after
// the given delay.
type RateLimitedError struct{ RetryAfter time.Duration }

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("rate limited; retry after %s", e.RetryAfter)
}

// InvalidInputError signals a client-side validation failure.
type InvalidInputError struct{ Msg string }

func (e *InvalidInputError) Error() string { return fmt.Sprintf("invalid input: %s", e.Msg) }

// NetworkError wraps a transient transport failure.
type NetworkError struct{ Err error }

func (e *NetworkError) Error() string { return fmt.Sprintf("network error: %s", e.Err) }
func (e *NetworkError) Unwrap() error { return e.Err }

// BackendError wraps an unexpected backend failure (git plumbing, REST 500, etc.).
type BackendError struct{ Err error }

func (e *BackendError) Error() string { return fmt.Sprintf("backend error: %s", e.Err) }
func (e *BackendError) Unwrap() error { return e.Err }
