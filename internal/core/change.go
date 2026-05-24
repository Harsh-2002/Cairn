package core

// FileOpKind discriminates the operation in a FileChange. Bytes are carried
// on FileChange itself (and only meaningful for FileOpWrite).
type FileOpKind int

const (
	FileOpWrite FileOpKind = iota
	FileOpDelete
)

// FileChange is one entry in an atomic commit: either a write of bytes to a
// path, or a deletion of a path. Use Write or Delete to construct.
type FileChange struct {
	Path  RepoPath
	Op    FileOpKind
	Bytes []byte
}

// Write constructs a FileChange that writes bytes to path.
func Write(path RepoPath, bytes []byte) FileChange {
	return FileChange{Path: path, Op: FileOpWrite, Bytes: bytes}
}

// Delete constructs a FileChange that deletes the path.
func Delete(path RepoPath) FileChange {
	return FileChange{Path: path, Op: FileOpDelete}
}

// FileChangeSet is an ordered set of changes applied atomically by a Provider.
type FileChangeSet struct {
	Changes []FileChange
}

// NewFileChangeSet returns an empty change set.
func NewFileChangeSet() FileChangeSet { return FileChangeSet{} }

// Push appends a change.
func (s *FileChangeSet) Push(c FileChange) { s.Changes = append(s.Changes, c) }

// Len returns the number of changes.
func (s FileChangeSet) Len() int { return len(s.Changes) }

// IsEmpty reports whether the set has zero changes.
func (s FileChangeSet) IsEmpty() bool { return len(s.Changes) == 0 }
