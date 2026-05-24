// Package asset implements Cairn's content-addressed asset pipeline.
// Originals live in git under content/assets/<sha>.<ext>; the object store
// is a derived CDN mirror keyed by SHA-256. Keys: <sha>/original.<ext>
// and <sha>/<variant>.<ext>.
package asset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"gocloud.dev/blob"

	"github.com/Harsh-2002/Cairn/internal/core"
)

// Pipeline wraps an object-storage bucket exposing content-addressed put,
// exists, and get keyed by AssetRef.
type Pipeline struct {
	bucket *blob.Bucket
}

// New constructs a Pipeline over the given bucket. The caller owns the
// bucket and is responsible for closing it.
func New(bucket *blob.Bucket) *Pipeline { return &Pipeline{bucket: bucket} }

// RefFor computes an AssetRef from raw bytes and an extension. Pure: same
// input always produces the same key. The extension is lowercased.
func RefFor(bytes []byte, ext string) (core.AssetRef, error) {
	sum := sha256.Sum256(bytes)
	return core.NewAssetRef(hex.EncodeToString(sum[:]), strings.ToLower(ext))
}

// UploadOriginal writes original bytes at <sha>/original.<ext>. Idempotent:
// re-uploading identical bytes is a no-op semantically.
func (p *Pipeline) UploadOriginal(ctx context.Context, ref core.AssetRef, bytes []byte) error {
	return p.put(ctx, ref.StorageKey(), bytes)
}

// UploadVariant writes a named variant (e.g. "1200w") at <sha>/<variant>.<ext>.
func (p *Pipeline) UploadVariant(ctx context.Context, ref core.AssetRef, variant string, bytes []byte) error {
	return p.put(ctx, ref.VariantKey(variant), bytes)
}

// HasOriginal reports whether the original exists in the bucket.
func (p *Pipeline) HasOriginal(ctx context.Context, ref core.AssetRef) (bool, error) {
	exists, err := p.bucket.Exists(ctx, ref.StorageKey())
	if err != nil {
		return false, fmt.Errorf("asset exists: %w", err)
	}
	return exists, nil
}

// FetchOriginal reads the original bytes.
func (p *Pipeline) FetchOriginal(ctx context.Context, ref core.AssetRef) ([]byte, error) {
	r, err := p.bucket.NewReader(ctx, ref.StorageKey(), nil)
	if err != nil {
		return nil, fmt.Errorf("asset open: %w", err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("asset read: %w", err)
	}
	return data, nil
}

func (p *Pipeline) put(ctx context.Context, key string, bytes []byte) error {
	w, err := p.bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("asset put: %w", err)
	}
	if _, err := w.Write(bytes); err != nil {
		_ = w.Close()
		return fmt.Errorf("asset write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("asset close: %w", err)
	}
	return nil
}

// ErrInvalidExtension is returned when an extension has invalid characters.
// Re-exported for convenience.
var ErrInvalidExtension = errors.New("invalid extension")
