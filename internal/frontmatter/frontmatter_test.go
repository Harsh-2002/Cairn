package frontmatter

import (
	"errors"
	"reflect"
	"testing"
)

func parseOK(t *testing.T, yaml string) Frontmatter {
	t.Helper()
	p, err := Parse(yaml)
	if err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	return p.Frontmatter
}

func TestMinimalValidFrontmatter(t *testing.T) {
	fm := parseOK(t, "title: Hello\ndate: 2026-05-18T12:00:00Z")
	if fm.Title != "Hello" {
		t.Errorf("title = %q", fm.Title)
	}
	if fm.Slug != nil {
		t.Errorf("slug = %v; want nil", fm.Slug)
	}
	if fm.Draft {
		t.Errorf("draft = true; want false")
	}
	if len(fm.Tags) != 0 {
		t.Errorf("tags = %v; want empty", fm.Tags)
	}
	if len(fm.RedirectsFrom) != 0 {
		t.Errorf("redirects_from = %v; want empty", fm.RedirectsFrom)
	}
	if fm.Summary != nil || fm.NotionPageID != nil || fm.Updated != nil {
		t.Errorf("expected all optional pointers nil; got summary=%v notion=%v updated=%v",
			fm.Summary, fm.NotionPageID, fm.Updated)
	}
}

func TestFullySpecifiedFrontmatter(t *testing.T) {
	yaml := `
title: "Hello, world"
date: 2026-05-18T14:00:00+02:00
slug: hello-world
draft: false
tags: [infra, migration]
summary: A test post.
redirects_from: ["/old/path"]
notion_page_id: 1a2b3c4d5e6f7890123456789abcdef0
updated: 2026-05-19T09:00:00+02:00
`
	parsed, err := Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	fm := parsed.Frontmatter
	if fm.Title != "Hello, world" {
		t.Errorf("title = %q", fm.Title)
	}
	if fm.Slug == nil || *fm.Slug != "hello-world" {
		t.Errorf("slug = %v; want hello-world", fm.Slug)
	}
	if !reflect.DeepEqual(fm.Tags, []string{"infra", "migration"}) {
		t.Errorf("tags = %v", fm.Tags)
	}
	if !reflect.DeepEqual(fm.RedirectsFrom, []string{"/old/path"}) {
		t.Errorf("redirects_from = %v", fm.RedirectsFrom)
	}
	if fm.NotionPageID == nil || *fm.NotionPageID != "1a2b3c4d5e6f7890123456789abcdef0" {
		t.Errorf("notion_page_id = %v", fm.NotionPageID)
	}
	if fm.Updated == nil {
		t.Errorf("updated = nil; want set")
	}
	if len(parsed.UnknownKeys) != 0 {
		t.Errorf("unknown_keys = %v; want empty", parsed.UnknownKeys)
	}
}

func TestMissingTitleFails(t *testing.T) {
	if _, err := Parse("date: 2026-05-18T12:00:00Z"); err == nil {
		t.Errorf("expected error for missing title")
	}
}

func TestMissingDateFails(t *testing.T) {
	if _, err := Parse("title: Hello"); err == nil {
		t.Errorf("expected error for missing date")
	}
}

func TestDateWithoutTimezoneFails(t *testing.T) {
	_, err := Parse(`title: Hello
date: "2026-05-18T12:00:00"`)
	if err == nil {
		t.Errorf("expected error for naive timestamp")
	}
}

func TestDateWithZAccepted(t *testing.T) {
	if _, err := Parse("title: H\ndate: 2026-05-18T12:00:00Z"); err != nil {
		t.Errorf("expected ok, got %v", err)
	}
}

func TestDateWithOffsetAccepted(t *testing.T) {
	if _, err := Parse("title: H\ndate: 2026-05-18T12:00:00+02:00"); err != nil {
		t.Errorf("expected ok, got %v", err)
	}
}

func TestEmptyTitleFails(t *testing.T) {
	_, err := Parse(`title: ""
date: 2026-05-18T12:00:00Z`)
	assertFMKind(t, err, ErrEmptyTitle)
}

func TestWhitespaceTitleFails(t *testing.T) {
	_, err := Parse(`title: "   "
date: 2026-05-18T12:00:00Z`)
	assertFMKind(t, err, ErrEmptyTitle)
}

func TestUnknownKeyWarnsButParses(t *testing.T) {
	parsed, err := Parse("title: H\ndate: 2026-05-18T12:00:00Z\nmystery: 42\nlayout: post")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parsed.UnknownKeys, []string{"layout", "mystery"}) {
		t.Errorf("unknown_keys = %v; want [layout mystery]", parsed.UnknownKeys)
	}
}

func TestMalformedYAMLFails(t *testing.T) {
	_, err := Parse("title: : :")
	assertFMKind(t, err, ErrYAMLParse)
}

func TestNonMappingFails(t *testing.T) {
	_, err := Parse("- a\n- b")
	assertFMKind(t, err, ErrNotAMapping)
}

func TestEffectiveSlugUsesExplicit(t *testing.T) {
	fm := parseOK(t, "title: Whatever\ndate: 2026-05-18T12:00:00Z\nslug: my-custom-slug")
	s, err := fm.EffectiveSlug()
	if err != nil || s != "my-custom-slug" {
		t.Errorf("EffectiveSlug = (%q, %v)", s, err)
	}
}

func TestEffectiveSlugDerivesFromTitle(t *testing.T) {
	fm := parseOK(t, "title: Hello World\ndate: 2026-05-18T12:00:00Z")
	s, err := fm.EffectiveSlug()
	if err != nil || s != "hello-world" {
		t.Errorf("EffectiveSlug = (%q, %v)", s, err)
	}
}

func TestEffectiveSlugEmptyDerivationErrors(t *testing.T) {
	fm := parseOK(t, "title: \"🎉\"\ndate: 2026-05-18T12:00:00Z")
	_, err := fm.EffectiveSlug()
	assertFMKind(t, err, ErrEmptyDerivedSlug)
}

func TestTagsStoredAsGiven(t *testing.T) {
	fm := parseOK(t, "title: T\ndate: 2026-05-18T12:00:00Z\ntags: [Foo, BAR]")
	if !reflect.DeepEqual(fm.Tags, []string{"Foo", "BAR"}) {
		t.Errorf("tags = %v; want [Foo BAR]", fm.Tags)
	}
}

func TestInvalidTagFails(t *testing.T) {
	_, err := Parse("title: T\ndate: 2026-05-18T12:00:00Z\ntags: [\"hello world\"]")
	assertFMKind(t, err, ErrInvalidTag)
}

func TestRedirectMissingSlashFails(t *testing.T) {
	_, err := Parse("title: T\ndate: 2026-05-18T12:00:00Z\nredirects_from: [\"old/path\"]")
	assertFMKind(t, err, ErrInvalidRedirect)
}

func TestRedirectWithSlashOk(t *testing.T) {
	if _, err := Parse("title: T\ndate: 2026-05-18T12:00:00Z\nredirects_from: [\"/old/path\"]"); err != nil {
		t.Errorf("expected ok, got %v", err)
	}
}

func TestInvalidNotionIDLength(t *testing.T) {
	_, err := Parse("title: T\ndate: 2026-05-18T12:00:00Z\nnotion_page_id: abc123")
	assertFMKind(t, err, ErrInvalidNotionPageID)
}

func TestInvalidNotionIDUppercase(t *testing.T) {
	_, err := Parse("title: T\ndate: 2026-05-18T12:00:00Z\nnotion_page_id: 1A2B3C4D5E6F7890123456789ABCDEF0")
	assertFMKind(t, err, ErrInvalidNotionPageID)
}

func TestValidNotionIDAccepted(t *testing.T) {
	fm := parseOK(t, "title: T\ndate: 2026-05-18T12:00:00Z\nnotion_page_id: 1a2b3c4d5e6f7890123456789abcdef0")
	if fm.NotionPageID == nil || *fm.NotionPageID != "1a2b3c4d5e6f7890123456789abcdef0" {
		t.Errorf("notion_page_id = %v", fm.NotionPageID)
	}
}

func TestLastModUsesUpdatedWhenPresent(t *testing.T) {
	fm := parseOK(t, "title: T\ndate: 2026-05-18T12:00:00Z\nupdated: 2026-05-20T10:00:00Z")
	if !fm.LastMod().Equal(*fm.Updated) {
		t.Errorf("LastMod = %v; want %v", fm.LastMod(), fm.Updated)
	}
}

func TestLastModFallsBackToDate(t *testing.T) {
	fm := parseOK(t, "title: T\ndate: 2026-05-18T12:00:00Z")
	if !fm.LastMod().Equal(fm.Date) {
		t.Errorf("LastMod = %v; want %v", fm.LastMod(), fm.Date)
	}
}

func TestDateRoundtripPreservesOffset(t *testing.T) {
	cases := []string{
		"2026-05-18T12:00:00Z",
		"2026-05-18T14:00:00+02:00",
		"2026-05-18T14:00:00-05:30",
	}
	for _, c := range cases {
		fm := parseOK(t, "title: T\ndate: "+c)
		if fm.Date.String() != c {
			t.Errorf("roundtrip %q -> %q", c, fm.Date.String())
		}
	}
}

func assertFMKind(t *testing.T, err error, want ErrorKind) {
	t.Helper()
	var fe *FrontmatterError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FrontmatterError, got %T: %v", err, err)
	}
	if fe.Kind != want {
		t.Errorf("kind = %d; want %d (err: %v)", fe.Kind, want, err)
	}
}
