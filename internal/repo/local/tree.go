package local

import (
	"sort"
	"strings"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/repo"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// node is one position in the in-memory tree we build while applying a
// FileChangeSet. A node is either a directory (children non-empty), a leaf
// blob (blob set), or marked deleted.
type node struct {
	children map[string]*node
	blob     *plumbing.Hash
	deleted  bool
}

func newNode() *node { return &node{children: map[string]*node{}} }

// seed populates root from an existing tree so applyChanges starts from the
// branch HEAD's state.
func seed(repo *git.Repository, tree *object.Tree, root *node) error {
	for _, e := range tree.Entries {
		switch {
		case e.Mode == filemode.Regular || e.Mode == filemode.Executable || e.Mode == filemode.Symlink:
			c := newNode()
			h := e.Hash
			c.blob = &h
			root.children[e.Name] = c
		case e.Mode == filemode.Dir:
			sub, err := repo.TreeObject(e.Hash)
			if err != nil {
				return err
			}
			c := newNode()
			if err := seed(repo, sub, c); err != nil {
				return err
			}
			root.children[e.Name] = c
		}
	}
	return nil
}

func insertBlob(n *node, segs []string, blob plumbing.Hash) {
	if len(segs) == 0 {
		return
	}
	c, ok := n.children[segs[0]]
	if !ok {
		c = newNode()
		n.children[segs[0]] = c
	}
	if len(segs) == 1 {
		c.blob = &blob
		c.deleted = false
		c.children = map[string]*node{}
		return
	}
	c.blob = nil
	c.deleted = false
	insertBlob(c, segs[1:], blob)
}

func markDeleted(n *node, segs []string) {
	if len(segs) == 0 {
		return
	}
	c, ok := n.children[segs[0]]
	if !ok {
		return
	}
	if len(segs) == 1 {
		c.deleted = true
		return
	}
	markDeleted(c, segs[1:])
}

// writeTree recursively writes a tree object and returns its hash.
func writeTree(repo *git.Repository, n *node) (plumbing.Hash, error) {
	var entries []object.TreeEntry
	names := make([]string, 0, len(n.children))
	for name := range n.children {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		c := n.children[name]
		if c.deleted && len(c.children) == 0 {
			continue
		}
		if c.blob != nil && len(c.children) == 0 {
			entries = append(entries, object.TreeEntry{
				Name: name,
				Mode: filemode.Regular,
				Hash: *c.blob,
			})
			continue
		}
		if len(c.children) > 0 {
			sub, err := writeTree(repo, c)
			if err != nil {
				return plumbing.ZeroHash, err
			}
			// Skip empty subtrees.
			subTree, err := repo.TreeObject(sub)
			if err != nil {
				return plumbing.ZeroHash, err
			}
			if len(subTree.Entries) == 0 {
				continue
			}
			entries = append(entries, object.TreeEntry{
				Name: name,
				Mode: filemode.Dir,
				Hash: sub,
			})
		}
	}
	tree := &object.Tree{Entries: entries}
	obj := repo.Storer.NewEncodedObject()
	if err := tree.Encode(obj); err != nil {
		return plumbing.ZeroHash, err
	}
	return repo.Storer.SetEncodedObject(obj)
}

// applyChanges materializes a FileChangeSet on top of an optional base tree
// and returns the resulting tree's hash.
func applyChanges(r *git.Repository, changes core.FileChangeSet, baseTree *object.Tree) (plumbing.Hash, error) {
	root := newNode()
	if baseTree != nil {
		if err := seed(r, baseTree, root); err != nil {
			return plumbing.ZeroHash, &repo.BackendError{Err: err}
		}
	}
	for _, ch := range changes.Changes {
		segs := strings.Split(ch.Path.AsStr(), "/")
		switch ch.Op {
		case core.FileOpWrite:
			blobHash, err := writeBlob(r, ch.Bytes)
			if err != nil {
				return plumbing.ZeroHash, &repo.BackendError{Err: err}
			}
			insertBlob(root, segs, blobHash)
		case core.FileOpDelete:
			markDeleted(root, segs)
		}
	}
	h, err := writeTree(r, root)
	if err != nil {
		return plumbing.ZeroHash, &repo.BackendError{Err: err}
	}
	return h, nil
}

// writeBlob stores raw bytes as a blob object and returns its hash.
func writeBlob(r *git.Repository, bytes []byte) (plumbing.Hash, error) {
	obj := r.Storer.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len(bytes)))
	w, err := obj.Writer()
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if _, err := w.Write(bytes); err != nil {
		w.Close()
		return plumbing.ZeroHash, err
	}
	if err := w.Close(); err != nil {
		return plumbing.ZeroHash, err
	}
	return r.Storer.SetEncodedObject(obj)
}
