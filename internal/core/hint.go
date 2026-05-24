package core

// CommitHint signals to a Provider which commit mechanism to prefer. The
// trait method takes this as a hint; implementations may ignore it.
type CommitHint int

const (
	// CommitHintPublish is an atomic, multi-file commit. The GitHubApiProvider
	// uses the Git Data API for this.
	CommitHintPublish CommitHint = iota
	// CommitHintDraft is a single-file autosave to a draft branch. The
	// GitHubApiProvider may use the Contents API for this.
	CommitHintDraft
	// CommitHintAssetMirror writes asset originals into the source repo;
	// semantically asset writes rather than user-visible commits.
	CommitHintAssetMirror
)
