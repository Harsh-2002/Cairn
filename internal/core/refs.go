package core

// CommitRef is an opaque commit identifier (a SHA-1 string for both LocalGit
// and GitHubApi providers).
type CommitRef string

// TreeRef is an opaque tree identifier used by the Git Data API path.
type TreeRef string

// BlobRef is a blob SHA returned by reads; enables optimistic concurrency on update.
type BlobRef string
