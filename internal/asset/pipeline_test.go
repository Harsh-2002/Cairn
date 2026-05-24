package asset

import (
	"bytes"
	"context"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

func newPipeline(t *testing.T) *Pipeline {
	t.Helper()
	b, err := blob.OpenBucket(context.Background(), "mem://")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return New(b)
}

func TestRefForIsPureFunction(t *testing.T) {
	a, err := RefFor([]byte("hello"), "png")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := RefFor([]byte("hello"), "png")
	if a != b {
		t.Errorf("RefFor not pure: %v vs %v", a, b)
	}
}

func TestRefForDifferentBytesDifferentKeys(t *testing.T) {
	a, _ := RefFor([]byte("hello"), "png")
	b, _ := RefFor([]byte("world"), "png")
	if a.SHA256 == b.SHA256 {
		t.Errorf("different bytes produced same sha")
	}
}

func TestRefForSameBytesDifferentExt(t *testing.T) {
	a, _ := RefFor([]byte("hello"), "png")
	b, _ := RefFor([]byte("hello"), "jpg")
	if a.SHA256 != b.SHA256 {
		t.Errorf("sha should match across ext: %v %v", a.SHA256, b.SHA256)
	}
	if a.StorageKey() == b.StorageKey() {
		t.Errorf("storage keys should differ across ext")
	}
}

func TestRefForUppercaseExtNormalized(t *testing.T) {
	a, _ := RefFor([]byte("x"), "PNG")
	if a.Ext != "png" {
		t.Errorf("ext = %q; want png", a.Ext)
	}
}

func TestUploadAndFetchRoundtrip(t *testing.T) {
	p := newPipeline(t)
	ctx := context.Background()
	data := []byte("binary content")
	ref, _ := RefFor(data, "bin")
	if err := p.UploadOriginal(ctx, ref, data); err != nil {
		t.Fatal(err)
	}
	ok, err := p.HasOriginal(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("HasOriginal = (%v, %v)", ok, err)
	}
	got, err := p.FetchOriginal(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q; want %q", got, data)
	}
}

func TestHasOriginalReturnsFalseForMissing(t *testing.T) {
	p := newPipeline(t)
	ref, _ := RefFor([]byte("never uploaded"), "bin")
	ok, err := p.HasOriginal(context.Background(), ref)
	if err != nil || ok {
		t.Errorf("HasOriginal = (%v, %v); want (false, nil)", ok, err)
	}
}

func TestUploadIsIdempotent(t *testing.T) {
	p := newPipeline(t)
	ctx := context.Background()
	data := []byte("same")
	ref, _ := RefFor(data, "bin")
	if err := p.UploadOriginal(ctx, ref, data); err != nil {
		t.Fatal(err)
	}
	if err := p.UploadOriginal(ctx, ref, data); err != nil {
		t.Fatal(err)
	}
	got, _ := p.FetchOriginal(ctx, ref)
	if !bytes.Equal(got, data) {
		t.Errorf("post-reupload mismatch")
	}
}

func TestVariantKeysDistinctFromOriginal(t *testing.T) {
	p := newPipeline(t)
	ctx := context.Background()
	src := []byte("src image bytes")
	ref, _ := RefFor(src, "png")
	if err := p.UploadOriginal(ctx, ref, src); err != nil {
		t.Fatal(err)
	}
	if err := p.UploadVariant(ctx, ref, "1200w", []byte("resized")); err != nil {
		t.Fatal(err)
	}
	got, _ := p.FetchOriginal(ctx, ref)
	if !bytes.Equal(got, src) {
		t.Errorf("variant upload mutated original")
	}
}
