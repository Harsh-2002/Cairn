// Package signer abstracts the S3 presigner so handlers don't depend on a
// specific bucket SDK. The production implementation uses gocloud.dev/blob's
// SignedURL; tests use MockSigner which returns a deterministic fake URL.
package signer

import (
	"context"
	"net/url"
	"strings"
	"time"

	"gocloud.dev/blob"
)

// URLSigner mints time-limited PUT URLs that browsers use to upload directly
// to object storage without our server holding the bytes.
type URLSigner interface {
	SignPut(ctx context.Context, key string, expiry time.Duration) (string, error)
}

// MockSigner returns a deterministic URL for development and tests. The URL
// is not actually signed; it just identifies which key was requested.
type MockSigner struct {
	baseURL string
}

// NewMockSigner returns a signer that prepends baseURL to every key.
func NewMockSigner(baseURL string) *MockSigner {
	return &MockSigner{baseURL: strings.TrimRight(baseURL, "/")}
}

// SignPut implements URLSigner.
func (m *MockSigner) SignPut(_ context.Context, key string, expiry time.Duration) (string, error) {
	u := m.baseURL + "/" + strings.TrimLeft(key, "/")
	if expiry > 0 {
		// Keep the URL stable and inspectable.
		parsed, _ := url.Parse(u)
		q := parsed.Query()
		q.Set("expiresIn", expiry.String())
		parsed.RawQuery = q.Encode()
		u = parsed.String()
	}
	return u, nil
}

// BucketSigner uses a gocloud.dev/blob.Bucket to sign PUT URLs. Suitable for
// real S3, GCS, Azure, R2, and MinIO.
type BucketSigner struct {
	bucket *blob.Bucket
}

// NewBucketSigner constructs a BucketSigner over the given bucket.
func NewBucketSigner(bucket *blob.Bucket) *BucketSigner { return &BucketSigner{bucket: bucket} }

// SignPut implements URLSigner.
func (s *BucketSigner) SignPut(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return s.bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Method: "PUT",
		Expiry: expiry,
	})
}
