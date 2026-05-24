package frontmatter

import (
	"strings"
	"testing"
)

func TestSlugBasic(t *testing.T) {
	got, ok := DeriveSlug("Hello, world!")
	if !ok || got != "hello-world" {
		t.Errorf("got (%q, %v); want (hello-world, true)", got, ok)
	}
}

func TestSlugCollapsesPunctuation(t *testing.T) {
	got, ok := DeriveSlug("One!!  Two??  Three")
	if !ok || got != "one-two-three" {
		t.Errorf("got %q", got)
	}
}

func TestSlugTrimsHyphens(t *testing.T) {
	if s, _ := DeriveSlug("!Hello!"); s != "hello" {
		t.Errorf("got %q; want hello", s)
	}
	if s, _ := DeriveSlug("---hello---"); s != "hello" {
		t.Errorf("got %q; want hello", s)
	}
}

func TestSlugStripsAccents(t *testing.T) {
	if s, _ := DeriveSlug("Café résumé"); s != "cafe-resume" {
		t.Errorf("got %q; want cafe-resume", s)
	}
	if s, _ := DeriveSlug("Naïve façade"); s != "naive-facade" {
		t.Errorf("got %q; want naive-facade", s)
	}
}

func TestSlugTruncatesLongTitlesAtHyphen(t *testing.T) {
	long := strings.Repeat("a ", 60)
	s, ok := DeriveSlug(long)
	if !ok {
		t.Fatalf("expected derivation to succeed")
	}
	if len(s) > 80 {
		t.Errorf("slug too long: %d chars", len(s))
	}
	if strings.HasSuffix(s, "-") {
		t.Errorf("slug ends with hyphen: %q", s)
	}
}

func TestSlugEmptyForEmojiOnly(t *testing.T) {
	if _, ok := DeriveSlug("🎉🎊✨"); ok {
		t.Errorf("expected empty derivation for emoji-only title")
	}
}

func TestSlugEmptyForPurePunctuation(t *testing.T) {
	if _, ok := DeriveSlug("!@#$%^&*()"); ok {
		t.Errorf("expected empty derivation for punctuation-only title")
	}
}

func TestSlugHandlesCJKAsEmpty(t *testing.T) {
	if _, ok := DeriveSlug("你好"); ok {
		t.Errorf("expected empty derivation for CJK-only title")
	}
}

func TestInvalidSlugUppercase(t *testing.T) {
	if validateSlug("Hello") == nil {
		t.Errorf("expected error")
	}
}

func TestInvalidSlugLeadingHyphen(t *testing.T) {
	if validateSlug("-hello") == nil {
		t.Errorf("expected error")
	}
}

func TestInvalidSlugTrailingHyphen(t *testing.T) {
	if validateSlug("hello-") == nil {
		t.Errorf("expected error")
	}
}

func TestInvalidSlugDoubleHyphen(t *testing.T) {
	if validateSlug("hello--world") == nil {
		t.Errorf("expected error")
	}
}

func TestInvalidSlugSpecialChars(t *testing.T) {
	for _, s := range []string{"hello_world", "hello world", "hello.world"} {
		if validateSlug(s) == nil {
			t.Errorf("%q: expected error", s)
		}
	}
}

func TestValidSlugExamples(t *testing.T) {
	for _, s := range []string{"hello", "hello-world", "a-b-c-d-e", "abc123", "123abc"} {
		if err := validateSlug(s); err != nil {
			t.Errorf("%q: expected ok, got %v", s, err)
		}
	}
}
