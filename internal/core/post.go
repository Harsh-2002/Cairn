package core

import "github.com/Harsh-2002/Cairn/internal/frontmatter"

// Post is a post after ingestion: parsed and validated frontmatter, canonical
// markdown body, and the path it originated from in the source repository.
type Post struct {
	Frontmatter frontmatter.Frontmatter
	Body        string
	SourcePath  RepoPath
}
