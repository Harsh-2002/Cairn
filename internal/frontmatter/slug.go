package frontmatter

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// DeriveSlug applies the slug algorithm from docs/frontmatter.md:
//  1. Lowercase via Unicode case folding.
//  2. NFKD normalize; drop combining marks.
//  3. Collapse non-[a-z0-9] runs into single hyphens.
//  4. Trim leading/trailing hyphens.
//  5. Truncate to 80 chars at a hyphen boundary if possible.
//
// Returns (slug, true) on success, ("", false) if the title contained no
// characters that survive normalization (emoji-only, CJK-only, etc.).
func DeriveSlug(title string) (string, bool) {
	lower := strings.ToLower(title)
	normalized := norm.NFKD.String(lower)

	var b strings.Builder
	b.Grow(len(normalized))
	lastWasHyphen := true
	for _, r := range normalized {
		if unicode.IsMark(r) {
			continue
		}
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastWasHyphen = false
		} else if !lastWasHyphen {
			b.WriteByte('-')
			lastWasHyphen = true
		}
	}

	s := b.String()
	for strings.HasSuffix(s, "-") {
		s = s[:len(s)-1]
	}

	if len(s) > 80 {
		cut := strings.LastIndex(s[:80], "-")
		if cut > 0 {
			s = s[:cut]
		} else {
			s = s[:80]
		}
	}

	if s == "" {
		return "", false
	}
	return s, true
}

// validateSlug checks that s matches ^[a-z0-9]+(-[a-z0-9]+)*$ and is ≤80 chars.
func validateSlug(s string) error {
	if s == "" || len(s) > 80 {
		return &FrontmatterError{Kind: ErrInvalidSlug, Value: s}
	}
	lastWasHyphen := true
	hasChar := false
	for _, r := range s {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			lastWasHyphen = false
			hasChar = true
		case r == '-':
			if lastWasHyphen {
				return &FrontmatterError{Kind: ErrInvalidSlug, Value: s}
			}
			lastWasHyphen = true
		default:
			return &FrontmatterError{Kind: ErrInvalidSlug, Value: s}
		}
	}
	if lastWasHyphen || !hasChar {
		return &FrontmatterError{Kind: ErrInvalidSlug, Value: s}
	}
	return nil
}

func isValidNotionID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}
