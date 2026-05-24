package core

import (
	"errors"
	"testing"
)

const validHash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

func TestAssetRefAcceptsValid(t *testing.T) {
	a, err := NewAssetRef(validHash, "png")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := a.StorageKey(), validHash+"/original.png"; got != want {
		t.Errorf("StorageKey = %q; want %q", got, want)
	}
	if got, want := a.VariantKey("1200w"), validHash+"/1200w.png"; got != want {
		t.Errorf("VariantKey = %q; want %q", got, want)
	}
	if got, want := a.GitPath().AsStr(), "content/assets/"+validHash+".png"; got != want {
		t.Errorf("GitPath = %q; want %q", got, want)
	}
}

func TestAssetRefRejectsShortHash(t *testing.T) {
	_, err := NewAssetRef("abc", "png")
	assertAssetKind(t, err, AssetRefInvalidHash)
}

func TestAssetRefRejectsUppercaseHash(t *testing.T) {
	_, err := NewAssetRef("ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789", "png")
	assertAssetKind(t, err, AssetRefInvalidHash)
}

func TestAssetRefRejectsEmptyExt(t *testing.T) {
	_, err := NewAssetRef(validHash, "")
	assertAssetKind(t, err, AssetRefInvalidExtension)
}

func TestAssetRefRejectsUppercaseExt(t *testing.T) {
	_, err := NewAssetRef(validHash, "PNG")
	assertAssetKind(t, err, AssetRefInvalidExtension)
}

func TestAssetRefRejectsExtWithDot(t *testing.T) {
	_, err := NewAssetRef(validHash, ".png")
	assertAssetKind(t, err, AssetRefInvalidExtension)
}

func assertAssetKind(t *testing.T, err error, want AssetRefErrorKind) {
	t.Helper()
	var ae *AssetRefError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *AssetRefError, got %T: %v", err, err)
	}
	if ae.Kind != want {
		t.Errorf("kind = %d; want %d", ae.Kind, want)
	}
}
