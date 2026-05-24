package core

import (
	"bytes"
	"testing"
)

func TestFileChangeConstructors(t *testing.T) {
	p, err := NewRepoPath("a.md")
	if err != nil {
		t.Fatal(err)
	}
	w := Write(p, []byte("hi"))
	if w.Path != p || w.Op != FileOpWrite || !bytes.Equal(w.Bytes, []byte("hi")) {
		t.Errorf("write got %+v", w)
	}
	d := Delete(p)
	if d.Op != FileOpDelete || d.Bytes != nil {
		t.Errorf("delete got %+v", d)
	}
}

func TestChangeSetBasics(t *testing.T) {
	var cs FileChangeSet
	if !cs.IsEmpty() {
		t.Errorf("new set should be empty")
	}
	p, err := NewRepoPath("a")
	if err != nil {
		t.Fatal(err)
	}
	cs.Push(Write(p, []byte("x")))
	if cs.Len() != 1 {
		t.Errorf("len = %d; want 1", cs.Len())
	}
}
