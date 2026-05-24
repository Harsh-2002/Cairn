package core

import "fmt"

// AssetRef is a content-addressed reference to an asset. The hash uniquely
// identifies the original bytes; the extension is preserved for content-type
// and downstream tooling.
type AssetRef struct {
	SHA256 string
	Ext    string
}

// AssetRefErrorKind discriminates AssetRefError.
type AssetRefErrorKind int

const (
	AssetRefInvalidHash AssetRefErrorKind = iota
	AssetRefInvalidExtension
)

// AssetRefError is returned by NewAssetRef when validation fails.
type AssetRefError struct {
	Kind  AssetRefErrorKind
	Value string
}

func (e *AssetRefError) Error() string {
	switch e.Kind {
	case AssetRefInvalidHash:
		return fmt.Sprintf("sha256 `%s` must be 64 lowercase hex characters", e.Value)
	case AssetRefInvalidExtension:
		return fmt.Sprintf("extension `%s` must be 1–8 lowercase alphanumeric characters with no leading dot", e.Value)
	}
	return "asset ref is invalid"
}

// NewAssetRef validates and constructs an AssetRef. The hash must be exactly
// 64 lowercase hex characters; the extension must be 1–8 lowercase alphanumeric
// characters with no leading dot.
func NewAssetRef(sha256, ext string) (AssetRef, error) {
	if len(sha256) != 64 || !isLowerHex(sha256) {
		return AssetRef{}, &AssetRefError{Kind: AssetRefInvalidHash, Value: sha256}
	}
	if len(ext) == 0 || len(ext) > 8 || !isLowerAlnum(ext) {
		return AssetRef{}, &AssetRefError{Kind: AssetRefInvalidExtension, Value: ext}
	}
	return AssetRef{SHA256: sha256, Ext: ext}, nil
}

// StorageKey is the object-storage key for the original bytes.
func (a AssetRef) StorageKey() string {
	return a.SHA256 + "/original." + a.Ext
}

// VariantKey is the object-storage key for a named variant (e.g., "1200w").
func (a AssetRef) VariantKey(variant string) string {
	return a.SHA256 + "/" + variant + "." + a.Ext
}

// GitPath is the repository path where the original bytes are committed.
// A valid AssetRef always produces a valid RepoPath; the panic on the unwrap
// would indicate corruption in this package.
func (a AssetRef) GitPath() RepoPath {
	p, err := NewRepoPath("content/assets/" + a.SHA256 + "." + a.Ext)
	if err != nil {
		panic("AssetRef components produced an invalid RepoPath: " + err.Error())
	}
	return p
}

func isLowerHex(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

func isLowerAlnum(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'z':
		default:
			return false
		}
	}
	return true
}
