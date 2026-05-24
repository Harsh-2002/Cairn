package core

import (
	"fmt"
	"strings"
	"unicode"
)

// RepoPath is a validated path within the source repository. Always
// forward-slash separated, no leading slash, no "..", no empty segments,
// no control characters.
type RepoPath struct {
	s string
}

// RepoPathErrorKind discriminates RepoPathError; mirrors the Rust enum.
type RepoPathErrorKind int

const (
	RepoPathEmpty RepoPathErrorKind = iota
	RepoPathLeadingSlash
	RepoPathParentRef
	RepoPathEmptySegment
	RepoPathBackslash
	RepoPathControl
)

// RepoPathError is returned by NewRepoPath when validation fails.
type RepoPathError struct {
	Kind RepoPathErrorKind
	Path string
}

func (e *RepoPathError) Error() string {
	switch e.Kind {
	case RepoPathEmpty:
		return "repository path is empty"
	case RepoPathLeadingSlash:
		return fmt.Sprintf("repository path must not start with `/`: %s", e.Path)
	case RepoPathParentRef:
		return fmt.Sprintf("repository path must not contain `..`: %s", e.Path)
	case RepoPathEmptySegment:
		return fmt.Sprintf("repository path must not contain empty segments: %s", e.Path)
	case RepoPathBackslash:
		return fmt.Sprintf("repository path must use forward slashes only: %s", e.Path)
	case RepoPathControl:
		return fmt.Sprintf("repository path must not contain control characters: %s", e.Path)
	}
	return "repository path is invalid"
}

// NewRepoPath validates and constructs a RepoPath.
func NewRepoPath(s string) (RepoPath, error) {
	if s == "" {
		return RepoPath{}, &RepoPathError{Kind: RepoPathEmpty}
	}
	if strings.HasPrefix(s, "/") {
		return RepoPath{}, &RepoPathError{Kind: RepoPathLeadingSlash, Path: s}
	}
	if strings.Contains(s, "\\") {
		return RepoPath{}, &RepoPathError{Kind: RepoPathBackslash, Path: s}
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return RepoPath{}, &RepoPathError{Kind: RepoPathControl, Path: s}
		}
	}
	for _, seg := range strings.Split(s, "/") {
		if seg == "" {
			return RepoPath{}, &RepoPathError{Kind: RepoPathEmptySegment, Path: s}
		}
		if seg == ".." {
			return RepoPath{}, &RepoPathError{Kind: RepoPathParentRef, Path: s}
		}
	}
	return RepoPath{s: s}, nil
}

// String implements fmt.Stringer.
func (p RepoPath) String() string { return p.s }

// AsStr returns the underlying string (kept for parity with as_str()).
func (p RepoPath) AsStr() string { return p.s }

// Parent returns the parent path and true, or (zero, false) if there is no parent.
func (p RepoPath) Parent() (RepoPath, bool) {
	i := strings.LastIndex(p.s, "/")
	if i < 0 {
		return RepoPath{}, false
	}
	return RepoPath{s: p.s[:i]}, true
}

// FileName returns the last segment of the path.
func (p RepoPath) FileName() string {
	i := strings.LastIndex(p.s, "/")
	if i < 0 {
		return p.s
	}
	return p.s[i+1:]
}

// Extension returns the extension after the last '.' in the file name, or "" if none.
func (p RepoPath) Extension() (string, bool) {
	name := p.FileName()
	i := strings.LastIndex(name, ".")
	if i < 0 {
		return "", false
	}
	return name[i+1:], true
}
